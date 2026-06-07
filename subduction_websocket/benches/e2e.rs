//! End-to-end benchmarks for the Subduction sync protocol over WebSocket.
//!
//! Run with:
//!
//! ```sh
//! cargo bench -p subduction_websocket --features tokio_client,tokio_server --bench e2e
//! ```
//!
//! ## Benchmark Philosophy
//!
//! | Goal                        | Approach |
//! |-----------------------------|----------|
//! | Measure real protocol cost  | Full WebSocket server+client over loopback |
//! | Include handshake overhead  | Each connection performs mutual authentication |
//! | Isolate sync from setup     | Use `iter_batched` to separate infrastructure from measurement |
//! | Test scaling behavior       | Vary commit count, blob size, and client count |
//!
//! ## What's Tested
//!
//! - WebSocket connection + Ed25519 mutual handshake latency
//! - Full sync round-trip (fingerprint diff + data transfer + blob resolution)
//! - Batch sync scaling with increasing commit counts
//! - Large blob transfer throughput
//! - Bidirectional sync (both peers have unique data)
//! - Incremental sync (pre-synced peers, one new commit)
//! - Concurrent client fan-in (N clients syncing with one server)
//!
//! ## CI Slim Mode
//!
//! GitHub-hosted Ubuntu runners cap at ~7 GiB of RAM. The full sweep
//! (256 KiB / 1 MiB blobs, 4 / 8 concurrent clients, 100-commit batches)
//! pushes the runner past that cap by the time the heavier blob cases
//! run, due to per-iteration accumulation of WebSocket / `MemoryStorage`
//! state. Set `SUBDUCTION_BENCH_CI_SLIM=1` in the bench environment to
//! drop the heaviest cases. The workflow at `.github/workflows/benches.yml`
//! sets this for the e2e-websocket matrix leg.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(feature = "pprof-profile")]
use criterion_pprof::criterion::{Output, PProfProfiler};
use future_form::Sendable;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sedimentree_core::{
    blob::Blob, depth::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    connection::message::SyncMessage,
    handler::sync::SyncHandler,
    handshake::audience::Audience,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{
        TimeoutTokio, TrackedTokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer,
    },
};
use tempfile::TempDir;
use tokio_util::task::TaskTracker;

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);
const TIMEOUT: CallTimeout = CallTimeout::TimeoutMillis(10_000);

/// Whether to run the slim CI sweep. Controlled by the
/// `SUBDUCTION_BENCH_CI_SLIM` environment variable: empty or unset
/// runs the full sweep, any non-empty value enables slim mode.
fn ci_slim() -> bool {
    std::env::var_os("SUBDUCTION_BENCH_CI_SLIM").is_some_and(|v| !v.is_empty())
}

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn commit_id_from_rng(rng: &mut StdRng) -> CommitId {
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes);
    CommitId::new(bytes)
}

fn blob_from_seed(rng: &mut StdRng, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    rng.fill(data.as_mut_slice());
    Blob::new(data)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Spin up a fresh server with `MemoryStorage`, wrapped in a [`ServerGuard`]
/// for automatic cleanup on drop.
async fn fresh_server(seed: u8) -> (ServerGuard, PeerId, SocketAddr) {
    let sig = signer(seed);
    let peer_id = PeerId::from(sig.verifying_key());
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");

    let server = TokioWebSocketServer::setup(
        addr,
        TimeoutTokio,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        sig,
        None,
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await
    .expect("server setup");

    let bound = server.address();
    (
        ServerGuard {
            server,
            rt: tokio::runtime::Handle::current(),
        },
        peer_id,
        bound,
    )
}

/// Like [`fresh_server`] but backed by [`FsStorage`] in a temp dir — the
/// production server storage. Lets the concurrent bench compare FS contention
/// against the in-memory backend.
async fn fresh_server_fs(seed: u8) -> (FsServerGuard, PeerId, SocketAddr) {
    let sig = signer(seed);
    let peer_id = PeerId::from(sig.verifying_key());
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");

    let dir = TempDir::new().expect("temp dir");
    let storage = FsStorage::new(dir.path().to_path_buf()).expect("fs storage");

    let server = TokioWebSocketServer::setup(
        addr,
        TimeoutTokio,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        sig,
        None,
        storage,
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await
    .expect("fs server setup");

    let bound = server.address();
    (
        FsServerGuard {
            server,
            _dir: dir,
            rt: tokio::runtime::Handle::current(),
        },
        peer_id,
        bound,
    )
}

/// `fresh_server`'s [`FsStorage`] counterpart guard. Holds the `TempDir` so the
/// on-disk store outlives the server.
struct FsServerGuard {
    server: TokioWebSocketServer<
        FsStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >,
    _dir: TempDir,
    rt: tokio::runtime::Handle,
}

impl Drop for FsServerGuard {
    fn drop(&mut self) {
        self.rt.block_on(self.server.stop_and_drain());
    }
}

type ClientSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner>,
        SyncHandler<
            Sendable,
            MemoryStorage,
            TokioWebSocketClient<MemorySigner>,
            OpenPolicy,
            CountLeadingZeroBytes,
        >,
        OpenPolicy,
        MemorySigner,
        TimeoutTokio,
        TrackedTokioSpawn,
    >,
>;

/// RAII wrapper that runs [`TokioWebSocketServer::stop_and_drain`] on
/// drop. `rt` is captured at construction because `Handle::current()`
/// panics outside an active `block_on`.
struct ServerGuard {
    server: TokioWebSocketServer<
        MemoryStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >,
    rt: tokio::runtime::Handle,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.rt.block_on(self.server.stop_and_drain());
    }
}

impl std::ops::Deref for ServerGuard {
    type Target = TokioWebSocketServer<
        MemoryStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

/// RAII guard that gracefully tears down a [`ClientSubduction`] on drop:
/// shuts it down, aborts the WebSocket transport tasks, awaits everything,
/// and drains the connection-manager tracker. `rt` is captured because
/// `Handle::current()` panics outside an active `block_on`.
struct ClientGuard {
    client: ClientSubduction,
    rt: tokio::runtime::Handle,
    listener_task: Option<tokio::task::JoinHandle<Result<(), futures_util::future::Aborted>>>,
    manager_task: Option<tokio::task::JoinHandle<Result<(), futures_util::future::Aborted>>>,
    // `Option` so `Drop` can `take()` them without needing a runtime in
    // scope (unlike `mem::replace(_, tokio::spawn(_))`).
    ws_listener_task: Option<tokio::task::JoinHandle<()>>,
    ws_sender_task: Option<tokio::task::JoinHandle<()>>,
    ws_keepalive_task: Option<tokio::task::JoinHandle<()>>,
    // Tracks `connection_loop`s spawned by the client's `ConnectionManager`.
    tasks: TaskTracker,
}

/// Max time to wait for tasks to drain before falling back to `abort()`.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        let Some(listener_task) = self.listener_task.take() else {
            return;
        };
        let Some(manager_task) = self.manager_task.take() else {
            return;
        };
        let Some(ws_listener_task) = self.ws_listener_task.take() else {
            return;
        };
        let Some(ws_sender_task) = self.ws_sender_task.take() else {
            return;
        };
        let Some(ws_keepalive_task) = self.ws_keepalive_task.take() else {
            return;
        };

        // Dropping a `tokio::task::JoinHandle` does NOT abort the task
        // (unlike `async-std`); capture the handles up front so we can
        // explicitly abort if the timeout fires.
        let listener_abort = listener_task.abort_handle();
        let manager_abort = manager_task.abort_handle();

        let client = self.client.clone();
        let tracker = self.tasks.clone();

        client.shutdown();

        self.rt.block_on(async move {
            // WS tasks are parked on the tungstenite stream; abort to
            // unpark, then await below so their captured `Arc<WebSocket>`
            // is released before we return.
            ws_listener_task.abort();
            ws_sender_task.abort();
            ws_keepalive_task.abort();

            let (l_res, m_res, _, _, _) = futures::future::join5(
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, listener_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, manager_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, ws_listener_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, ws_sender_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, ws_keepalive_task),
            )
            .await;

            if l_res.is_err() {
                tracing::warn!("ClientGuard: listener didn't drain, aborting");
                listener_abort.abort();
            }
            if m_res.is_err() {
                tracing::warn!("ClientGuard: manager didn't drain, aborting");
                manager_abort.abort();
            }

            tracker.close();
            if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, tracker.wait())
                .await
                .is_err()
            {
                tracing::warn!(
                    "ClientGuard: {} connection_loop task(s) didn't drain",
                    tracker.len()
                );
            }
        });
    }
}

impl std::ops::Deref for ClientGuard {
    type Target = ClientSubduction;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl ClientGuard {
    /// Clone the inner `Arc<Subduction<…>>` for sharing across tasks.
    ///
    /// The guard itself is intentionally not `Clone`: only one guard per
    /// connection should run its abort-on-drop, since aborting the same
    /// `JoinHandle` twice is harmless but cloning the guard fields would
    /// double-count.
    fn inner(&self) -> ClientSubduction {
        self.client.clone()
    }
}

/// Assert that a `full_sync` call completed successfully.
#[allow(clippy::type_complexity)]
fn assert_full_sync(
    result: (
        bool,
        subduction_core::connection::stats::SyncStats,
        Vec<(
            subduction_core::authenticated::Authenticated<
                TokioWebSocketClient<MemorySigner>,
                Sendable,
            >,
            subduction_core::connection::managed::CallError<subduction_websocket::error::SendError>,
        )>,
        Vec<(
            SedimentreeId,
            subduction_core::subduction::error::IoError<
                Sendable,
                MemoryStorage,
                TokioWebSocketClient<MemorySigner>,
                SyncMessage,
            >,
        )>,
    ),
) {
    let (had_success, _stats, call_errs, io_errs) = result;
    assert!(
        call_errs.is_empty(),
        "full_sync encountered call errors: {call_errs:?}"
    );
    assert!(
        io_errs.is_empty(),
        "full_sync encountered IO errors: {io_errs:?}"
    );
    assert!(had_success, "full_sync reported no successful syncs");
}

/// Per-connection call errors returned by `sync_with_peer`.
type SyncCallErrs = Vec<(
    subduction_core::authenticated::Authenticated<TokioWebSocketClient<MemorySigner>, Sendable>,
    subduction_core::connection::managed::CallError<subduction_websocket::error::SendError>,
)>;

/// The `Result` type of [`Subduction::sync_with_peer`] for these benches.
type SyncWithPeerResult<S> = Result<
    (
        bool,
        subduction_core::connection::stats::SyncStats,
        SyncCallErrs,
    ),
    subduction_core::subduction::error::IoError<
        Sendable,
        S,
        TokioWebSocketClient<MemorySigner>,
        SyncMessage,
    >,
>;

/// Assert a single-document, single-peer [`sync_with_peer`] succeeded with no
/// call errors. This is the path the concurrent bench exercises (the per-document
/// verb production uses) rather than `full_sync_with_all_peers`.
fn assert_sync<S>(result: SyncWithPeerResult<S>)
where
    S: subduction_core::storage::traits::Storage<Sendable> + std::fmt::Debug,
{
    let (had_success, _stats, call_errs) = result.expect("sync_with_peer IO error");
    assert!(
        call_errs.is_empty(),
        "sync_with_peer encountered call errors: {call_errs:?}"
    );
    assert!(had_success, "sync_with_peer reported no success");
}

/// Create a `Subduction` client, connect to the server, start background
/// tasks. Returns a [`ClientGuard`] which owns every spawned `JoinHandle`
/// and aborts them all on drop. See [`ClientGuard`] for the rationale —
/// the leak this prevents is the proximate cause of bench-CI OOM-kills.
async fn connected_client(
    seed: u8,
    server_peer_id: PeerId,
    server_addr: SocketAddr,
) -> ClientGuard {
    let client_signer = signer(seed);

    // Shared with the `Subduction` builder via `TrackedTokioSpawn` so
    // teardown can await every spawned `connection_loop`.
    let tasks = TaskTracker::new();
    let spawner = TrackedTokioSpawn::new(tasks.clone());

    let (client, _handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(client_signer.clone())
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(spawner)
        .timer(TimeoutTokio)
        .build::<Sendable, TokioWebSocketClient<MemorySigner>>();

    // `listener_fut` already runs `Subduction::listen()` internally —
    // do NOT spawn an additional `client.listen()` call.
    let manager_task = tokio::spawn(manager_fut);
    let listener_task = tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", server_addr.ip(), server_addr.port())
        .parse()
        .expect("valid uri");

    let (client_ws, ws_listener, ws_sender, ws_keepalive) =
        TokioWebSocketClient::new(uri, client_signer, Audience::known(server_peer_id))
            .await
            .expect("client connect");

    let ws_listener_task = tokio::spawn(async {
        if let Err(e) = ws_listener.await {
            tracing::error!("ws_listener task failed: {e:?}");
        }
    });
    let ws_sender_task = tokio::spawn(async {
        if let Err(e) = ws_sender.await {
            tracing::error!("ws_sender task failed: {e:?}");
        }
    });
    let ws_keepalive_task = tokio::spawn(async move {
        let outcome = ws_keepalive.await;
        tracing::debug!(?outcome, "ws_keepalive task exited");
    });

    client
        .add_connection(client_ws)
        .await
        .expect("add_connection");

    ClientGuard {
        client,
        rt: tokio::runtime::Handle::current(),
        listener_task: Some(listener_task),
        manager_task: Some(manager_task),
        ws_listener_task: Some(ws_listener_task),
        ws_sender_task: Some(ws_sender_task),
        ws_keepalive_task: Some(ws_keepalive_task),
        tasks,
    }
}

// ─── Handshake ───────────────────────────────────────────────────────────────

fn bench_handshake(c: &mut Criterion) {
    let rt = runtime();

    c.bench_function("handshake", |b| {
        b.iter_batched(
            || {
                // Setup: spin up a fresh server per iteration to avoid port exhaustion.
                rt.block_on(fresh_server(0))
            },
            |(_server, server_peer_id, bound)| {
                // Measured: TCP connect + mutual Ed25519 handshake.
                // The server's accept loop handles registration automatically.
                rt.block_on(async {
                    let client_signer = signer(42);
                    let uri: tungstenite::http::Uri =
                        format!("ws://{}:{}", bound.ip(), bound.port())
                            .parse()
                            .expect("valid uri");

                    // `TokioWebSocketClient::new` awaits the handshake
                    // internally before returning. The returned `listener`
                    // / `sender` / `keepalive` futures only matter for
                    // post-handshake message I/O, which this bench doesn't
                    // exercise — so we drop them immediately along with
                    // the connection rather than leaking spawn handles
                    // per iteration.
                    let (_ws, _listener, _sender, _keepalive) = TokioWebSocketClient::new(
                        uri,
                        client_signer,
                        Audience::known(server_peer_id),
                    )
                    .await
                    .expect("connect");
                });
            },
            BatchSize::PerIteration,
        );
    });
}

// ─── Single-Commit Sync ─────────────────────────────────────────────────────

fn bench_single_commit_sync(c: &mut Criterion) {
    let rt = runtime();

    c.bench_function("sync/single_commit", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (server, server_peer_id, bound) = fresh_server(0).await;
                    let client = connected_client(1, server_peer_id, bound).await;

                    let sed_id = SedimentreeId::new([0u8; 32]);
                    let mut rng = StdRng::seed_from_u64(0);
                    let head = commit_id_from_rng(&mut rng);
                    let blob = blob_from_seed(&mut rng, 64);
                    client
                        .add_commit(sed_id, head, BTreeSet::new(), blob)
                        .await
                        .expect("add commit");

                    (server, client, sed_id)
                })
            },
            |(_server, client, sed_id)| {
                rt.block_on(async {
                    client
                        .sync_with_all_peers(sed_id, false, TIMEOUT)
                        .await
                        .expect("sync");
                });
            },
            BatchSize::PerIteration,
        );
    });
}

// ─── Batch Sync (Varying Sizes) ─────────────────────────────────────────────

fn bench_batch_sync(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sync/batch");

    // Skip the 100-commit case in CI slim mode: setup builds 100 commits
    // per iteration, multiplying per-iter memory pressure.
    let counts: &[u64] = if ci_slim() {
        &[1, 10, 50]
    } else {
        &[1, 10, 50, 100]
    };
    for &count in counts {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (server, server_peer_id, bound) = fresh_server(0).await;
                        let client = connected_client(1, server_peer_id, bound).await;

                        let sed_id = SedimentreeId::new([0u8; 32]);
                        let mut rng = StdRng::seed_from_u64(0);

                        for _ in 0..count {
                            let head = commit_id_from_rng(&mut rng);
                            let blob = blob_from_seed(&mut rng, 64);
                            client
                                .add_commit(sed_id, head, BTreeSet::new(), blob)
                                .await
                                .expect("add commit");
                        }

                        (server, client)
                    })
                },
                |(_server, client)| {
                    rt.block_on(async {
                        assert_full_sync(client.full_sync_with_all_peers(TIMEOUT).await);
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ─── Large Blob Sync ─────────────────────────────────────────────────────────

fn bench_large_blob_sync(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sync/blob_size");

    // The 256 KiB and 1 MiB cases reproducibly OOM-kill GitHub-hosted
    // runners (7 GiB RAM) once accumulated state from prior benches is
    // factored in. Drop them in CI slim mode.
    let cases: &[(&str, usize)] = if ci_slim() {
        &[("1KB", 1_024), ("64KB", 64 * 1_024)]
    } else {
        &[
            ("1KB", 1_024),
            ("64KB", 64 * 1_024),
            ("256KB", 256 * 1_024),
            ("1MB", 1_024 * 1_024),
        ]
    };
    for &(label, size) in cases {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("blob", label), &size, |b, &size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (server, server_peer_id, bound) = fresh_server(0).await;
                        let client = connected_client(1, server_peer_id, bound).await;

                        let sed_id = SedimentreeId::new([0u8; 32]);
                        let mut rng = StdRng::seed_from_u64(0);
                        let head = commit_id_from_rng(&mut rng);
                        let blob = blob_from_seed(&mut rng, size);
                        client
                            .add_commit(sed_id, head, BTreeSet::new(), blob)
                            .await
                            .expect("add commit");

                        (server, client)
                    })
                },
                |(_server, client)| {
                    rt.block_on(async {
                        assert_full_sync(client.full_sync_with_all_peers(TIMEOUT).await);
                    });
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ─── Bidirectional Sync ──────────────────────────────────────────────────────

fn bench_bidirectional_sync(c: &mut Criterion) {
    let rt = runtime();

    c.bench_function("sync/bidirectional", |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (server, server_peer_id, bound) = fresh_server(0).await;
                    let client = connected_client(1, server_peer_id, bound).await;

                    let sed_id = SedimentreeId::new([0u8; 32]);
                    let mut rng = StdRng::seed_from_u64(0);

                    for _ in 0..5 {
                        let head = commit_id_from_rng(&mut rng);
                        let blob = blob_from_seed(&mut rng, 64);
                        server
                            .subduction()
                            .add_commit(sed_id, head, BTreeSet::new(), blob)
                            .await
                            .expect("server add commit");
                    }

                    for _ in 0..5 {
                        let head = commit_id_from_rng(&mut rng);
                        let blob = blob_from_seed(&mut rng, 64);
                        client
                            .add_commit(sed_id, head, BTreeSet::new(), blob)
                            .await
                            .expect("client add commit");
                    }

                    (server, client)
                })
            },
            |(_server, client)| {
                rt.block_on(async {
                    assert_full_sync(client.full_sync_with_all_peers(TIMEOUT).await);
                });
            },
            BatchSize::PerIteration,
        );
    });
}

// ─── Incremental Sync ────────────────────────────────────────────────────────

fn bench_incremental_sync(c: &mut Criterion) {
    let rt = runtime();

    c.bench_function("sync/incremental", |b| {
        let (server, client, sed_id) = rt.block_on(async {
            let (server, server_peer_id, bound) = fresh_server(0).await;
            let client = connected_client(1, server_peer_id, bound).await;

            let sed_id = SedimentreeId::new([0u8; 32]);
            let mut rng = StdRng::seed_from_u64(0);

            // Pre-populate with 50 commits
            for _ in 0..50 {
                let head = commit_id_from_rng(&mut rng);
                let blob = blob_from_seed(&mut rng, 64);
                server
                    .subduction()
                    .add_commit(sed_id, head, BTreeSet::new(), blob)
                    .await
                    .expect("server add commit");
            }

            // Sync to get client up to date
            assert_full_sync(client.full_sync_with_all_peers(TIMEOUT).await);
            tokio::time::sleep(Duration::from_millis(100)).await;

            (server, client, sed_id)
        });

        let mut seed_counter: u64 = 1000;

        b.iter(|| {
            rt.block_on(async {
                let mut rng = StdRng::seed_from_u64(seed_counter);
                seed_counter += 1;

                let head = commit_id_from_rng(&mut rng);
                let blob = blob_from_seed(&mut rng, 64);
                client
                    .add_commit(sed_id, head, BTreeSet::new(), blob)
                    .await
                    .expect("add commit");

                client
                    .sync_with_all_peers(sed_id, false, TIMEOUT)
                    .await
                    .expect("sync");
            });
        });

        drop(server);
    });
}

// ─── Concurrent Clients ──────────────────────────────────────────────────────

fn bench_concurrent_clients(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sync/concurrent_clients");

    // 4- and 8-client cases each spawn that many parallel `connected_client`
    // setups per iteration, multiplying every per-iter resource cost.
    // Drop them in CI slim mode.
    let counts: &[u64] = if ci_slim() { &[1, 2] } else { &[1, 2, 4, 8] };
    for &num_clients in counts {
        group.throughput(Throughput::Elements(num_clients));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            &num_clients,
            |b, &num_clients| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let (server, server_peer_id, bound) = fresh_server(0).await;

                            let sed_id = SedimentreeId::new([0u8; 32]);
                            let mut clients = Vec::new();

                            for i in 0..num_clients {
                                let seed = u8::try_from(i + 10).expect("client seed fits u8");
                                let client = connected_client(seed, server_peer_id, bound).await;

                                let mut rng = StdRng::seed_from_u64(i);
                                let head = commit_id_from_rng(&mut rng);
                                let blob = blob_from_seed(&mut rng, 64);
                                client
                                    .add_commit(sed_id, head, BTreeSet::new(), blob)
                                    .await
                                    .expect("add commit");

                                clients.push(client);
                            }

                            (server, server_peer_id, sed_id, clients)
                        })
                    },
                    |(_server, server_peer_id, sed_id, clients)| {
                        rt.block_on(async {
                            // Each client syncs the shared document with the
                            // server via `sync_with_peer` — the per-document
                            // verb production uses.
                            let mut handles = Vec::new();
                            for client in &clients {
                                let c = client.inner();
                                handles.push(tokio::spawn(async move {
                                    assert_sync(
                                        c.sync_with_peer(&server_peer_id, sed_id, true, TIMEOUT)
                                            .await,
                                    );
                                }));
                            }

                            for h in handles {
                                h.await.expect("sync task");
                            }
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

/// [`bench_concurrent_clients`] against the production [`FsStorage`] backend
/// instead of `MemoryStorage`. Confirms whether the same-document
/// read-contention that `MemoryStorage` can exhibit also affects the on-disk
/// store the server uses.
fn bench_concurrent_clients_fs(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("sync/concurrent_clients_fs");

    let counts: &[u64] = if ci_slim() { &[1, 2] } else { &[1, 2, 4, 8] };
    for &num_clients in counts {
        group.throughput(Throughput::Elements(num_clients));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            &num_clients,
            |b, &num_clients| {
                b.iter_batched(
                    || {
                        rt.block_on(async {
                            let (server, server_peer_id, bound) = fresh_server_fs(0).await;

                            let sed_id = SedimentreeId::new([0u8; 32]);
                            let mut clients = Vec::new();

                            for i in 0..num_clients {
                                let seed = u8::try_from(i + 10).expect("client seed fits u8");
                                let client = connected_client(seed, server_peer_id, bound).await;

                                let mut rng = StdRng::seed_from_u64(i);
                                let head = commit_id_from_rng(&mut rng);
                                let blob = blob_from_seed(&mut rng, 64);
                                client
                                    .add_commit(sed_id, head, BTreeSet::new(), blob)
                                    .await
                                    .expect("add commit");

                                clients.push(client);
                            }

                            (server, server_peer_id, sed_id, clients)
                        })
                    },
                    |(_server, server_peer_id, sed_id, clients)| {
                        rt.block_on(async {
                            let mut handles = Vec::new();
                            for client in &clients {
                                let c = client.inner();
                                handles.push(tokio::spawn(async move {
                                    assert_sync(
                                        c.sync_with_peer(&server_peer_id, sed_id, true, TIMEOUT)
                                            .await,
                                    );
                                }));
                            }

                            for h in handles {
                                h.await.expect("sync task");
                            }
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

/// Construct the [`Criterion`] used by every bench in this file.
///
/// The `e2e` benches each spin up a fresh WebSocket server + client per
/// iteration — so wall-clock time is dominated by per-iteration setup, not
/// the measured operation. Default Criterion settings (100 samples, 3 s
/// warm-up, 5 s measurement) end up budget-overrunning on shared CI
/// runners where the per-iteration setup is ~10× slower than developer
/// hardware.
///
/// We cap sample size at 20 and warm-up at 1 s. That preserves enough
/// statistical signal for regression spotting while keeping the total
/// suite well inside the 60-minute job budget configured in
/// `.github/workflows/benches.yml`.
fn ci_friendly_criterion() -> Criterion {
    let crit = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    #[cfg(feature = "pprof-profile")]
    let crit = crit.with_profiler(PProfProfiler::new(997, Output::Flamegraph(None)));
    crit
}

criterion_group! {
    name = benches;
    config = ci_friendly_criterion();
    targets =
        bench_handshake,
        bench_single_commit_sync,
        bench_batch_sync,
        bench_large_blob_sync,
        bench_bidirectional_sync,
        bench_incremental_sync,
        bench_concurrent_clients,
        bench_concurrent_clients_fs,
}

criterion_main!(benches);
