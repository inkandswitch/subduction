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
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    connection::message::SyncMessage,
    handler::sync::SyncHandler,
    handshake::audience::Audience,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{
        TimeoutTokio, TrackedTokioSpawn, client::TokioWebSocketClient,
        server::TokioWebSocketServer,
    },
};
use tokio_util::task::TaskTracker;

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);
const TIMEOUT: Duration = Duration::from_secs(10);

/// Whether to run the slim CI sweep. Controlled by the
/// `SUBDUCTION_BENCH_CI_SLIM` environment variable. See the module-level
/// docs for rationale.
fn ci_slim() -> bool {
    std::env::var_os("SUBDUCTION_BENCH_CI_SLIM").is_some()
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
    >,
>;

/// RAII guard that drains every server-side task on drop.
///
/// `TokioWebSocketServer::stop` only _signals_ shutdown — it cancels the
/// server's [`CancellationToken`] and aborts the accept loop, but does
/// not wait for the per-connection listener/sender or the
/// `Subduction` listener/manager wrappers to actually exit. Those
/// tasks hold `Arc<Subduction>` and the entire `MemoryStorage`, so if
/// the bench harness moves on to the next iteration before they're
/// polled, the storage accumulates monotonically and OOM-kills the
/// runner at `sync/batch/50`.
///
/// `stop_and_drain` cancels the token and then `await`s every tracked
/// task to completion. We capture a runtime handle at construction so
/// `Drop` can `block_on` the async tear-down without needing
/// `Handle::current()` (which would panic outside an active
/// `block_on`).
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
        // `stop_and_drain` is async and consumes the cancellation
        // signal exactly once. Idempotent re-drops are harmless but
        // unnecessary.
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

/// RAII guard around a [`ClientSubduction`] that gracefully shuts it down
/// and aborts every task spawned at construction time.
///
/// Without this, the bench harness leaked four tokio tasks per iteration
/// (the `Subduction` listener and manager loops, plus the WebSocket
/// listener and sender) across hundreds of `iter_batched(PerIteration)`
/// iterations. Each leaked task held an `Arc<Subduction>` (and therefore
/// an `Arc<MemoryStorage>` plus a tungstenite WebSocket buffer), so the
/// runner's RSS grew monotonically until the GitHub-hosted Ubuntu host
/// OOM-killed the runner agent — which manifested as
/// `##[error]The runner has received a shutdown signal` partway through
/// the 256 KiB blob bench.
///
/// On drop we:
/// 1. Call [`Subduction::shutdown`] (graceful) on the captured tokio
///    runtime, which closes the listener/manager input channels and
///    drains in-flight handler work.
/// 2. `await` the listener and manager `JoinHandle`s (with a small
///    timeout) so the loops actually exit before we abort their
///    transport-side dependencies.
/// 3. `abort()` the four background `JoinHandle`s as a backstop in case
///    anything is still polling.
///
/// Step (3) releases the `Arc<WebSocket>` the WS tasks were holding,
/// allowing the underlying tungstenite stream to be reclaimed.
struct ClientGuard {
    client: ClientSubduction,
    // Handle to the runtime that `connected_client` was called on. We
    // capture this at construction time because `Drop` may run outside
    // any active `block_on`, and `Handle::current()` would panic there.
    rt: tokio::runtime::Handle,
    // The listener and manager futures returned by `SubductionBuilder::build`
    // are wrapped in `futures::future::Abortable`, so their `JoinHandle`
    // outputs are `Result<(), Aborted>`.
    listener_task: Option<tokio::task::JoinHandle<Result<(), futures_util::future::Aborted>>>,
    manager_task: Option<tokio::task::JoinHandle<Result<(), futures_util::future::Aborted>>>,
    // `Option` so `Drop` can `take()` them and move them into the
    // async teardown block (where we abort and `await`). `take()`
    // works without a runtime in scope, unlike
    // `std::mem::replace(_, tokio::spawn(_))`.
    ws_listener_task: Option<tokio::task::JoinHandle<()>>,
    ws_sender_task: Option<tokio::task::JoinHandle<()>>,
    // Tracks every `connection_loop` task the client's
    // `ConnectionManager` spawns on `add_connection`. Without this,
    // those tasks survive past `shutdown` because the manager loop
    // simply exits on channel-close without awaiting its per-peer
    // tasks — they stay parked in `conn.recv()` holding
    // `Arc<WebSocket>` until the underlying tungstenite stream errors
    // out, which is asynchronous and uncoordinated with the rest of
    // teardown.
    tasks: TaskTracker,
}

/// How long to wait for the listener / manager to drain before aborting them
/// as a backstop. Generous: we don't expect to hit this in normal runs, but
/// don't want a stuck handler to hang the bench teardown.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        // Pull the listener/manager handles out so we can move them into
        // the async block. `take()` lets us own them even though Drop
        // only has `&mut self`.
        let Some(listener_task) = self.listener_task.take() else {
            return;
        };
        let Some(manager_task) = self.manager_task.take() else {
            return;
        };

        // The two WS tasks need to be moved into the async block too,
        // so we can `await` them after aborting. `JoinHandle::abort()`
        // only sets the cancellation flag — the task isn't dropped (and
        // therefore its captured `Arc<WebSocket>` isn't released) until
        // the runtime polls it. If we abort without awaiting, the
        // captured state lingers until the next runtime tick, which is
        // typically after `ClientGuard::Drop` has already returned and
        // the next iteration's setup is allocating fresh resources.
        // That overlap accumulates `Arc<WebSocket>` references across
        // iterations and is the dominant contribution to the 1.2 GiB
        // RSS at `sync/batch/50`.
        let Some(ws_listener_task) = self.ws_listener_task.take() else {
            return;
        };
        let Some(ws_sender_task) = self.ws_sender_task.take() else {
            return;
        };

        let client = self.client.clone();

        // Capture abort handles BEFORE moving the JoinHandles into the
        // timeout futures. Dropping a `tokio::task::JoinHandle` does
        // **not** abort the underlying task — that's `async-std` /
        // `smol` semantics, but tokio detaches the task instead. If we
        // don't explicitly `.abort()` after the timeout fires, the
        // listener/manager keep running, retaining `Arc<Subduction>`
        // (and its `MemoryStorage`, NonceCache, sharded connection map)
        // across iterations.
        let listener_abort = listener_task.abort_handle();
        let manager_abort = manager_task.abort_handle();

        let tracker = self.tasks.clone();

        self.rt.block_on(async move {
            // 1. Signal graceful shutdown to the listener/manager.
            client.shutdown().await;

            // 2. Abort the WS transport tasks. They don't observe
            //    channel closure — they're parked on the tungstenite
            //    stream. Abort unparks them; the await below makes
            //    sure they actually drop (releasing `Arc<WebSocket>`)
            //    before we return.
            ws_listener_task.abort();
            ws_sender_task.abort();

            // 3. Await all four task handles (with a single timeout
            //    budget). For aborted tasks the JoinHandle resolves
            //    to `Err(JoinError::is_cancelled())`, which is fine.
            //    For listener/manager the channel-close path returns
            //    `Ok(Ok(()))`. Either way the future is dropped and
            //    its captured state released.
            let (l_res, m_res, _, _) = futures::future::join4(
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, listener_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, manager_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, ws_listener_task),
                tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, ws_sender_task),
            )
            .await;

            // 4. If listener/manager didn't drain via channel-close
            //    (e.g., a handler hung), explicitly abort. The WS
            //    tasks were already aborted in step 2.
            if l_res.is_err() {
                tracing::warn!(
                    "ClientGuard: listener task didn't drain in {SHUTDOWN_DRAIN_TIMEOUT:?}, aborting"
                );
                listener_abort.abort();
            }
            if m_res.is_err() {
                tracing::warn!(
                    "ClientGuard: manager task didn't drain in {SHUTDOWN_DRAIN_TIMEOUT:?}, aborting"
                );
                manager_abort.abort();
            }

            // 5. Drain the `connection_loop` tasks spawned by the
            //    connection manager. Closing the tracker prevents new
            //    `add_connection` calls from spawning more (they'd
            //    fail anyway since the manager loop has exited).
            //    `wait` returns once every previously-spawned task
            //    has completed — at which point every `Arc<WebSocket>`
            //    captured by a `connection_loop` is released and the
            //    `Subduction` can finally drop on the last `Arc` going
            //    away. We bound this with a timeout matching the rest
            //    of the teardown — if the connection_loop tasks
            //    haven't exited within `SHUTDOWN_DRAIN_TIMEOUT`,
            //    something is wedged badly enough that aborting
            //    Subduction on its `Drop` (via its stored
            //    `AbortHandle`s) is our last resort.
            tracker.close();
            if tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, tracker.wait())
                .await
                .is_err()
            {
                tracing::warn!(
                    "ClientGuard tracker.wait timed out after {SHUTDOWN_DRAIN_TIMEOUT:?} \
                     ({} tasks remaining); proceeding with hard drop",
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

    // Per-client `TaskTracker`. Shared with the `Subduction` connection
    // manager via `TrackedTokioSpawn` so we can await every spawned
    // `connection_loop` on teardown.
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

    let (client_ws, ws_listener, ws_sender) =
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
                    // / `sender` futures only matter for post-handshake
                    // message I/O, which this bench doesn't exercise — so
                    // we drop them immediately along with the connection
                    // rather than leaking spawn handles per iteration.
                    let (_ws, _listener, _sender) = TokioWebSocketClient::new(
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
                        .sync_with_all_peers(sed_id, false, Some(TIMEOUT))
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
                        assert_full_sync(client.full_sync_with_all_peers(Some(TIMEOUT)).await);
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
                        assert_full_sync(client.full_sync_with_all_peers(Some(TIMEOUT)).await);
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
                    assert_full_sync(client.full_sync_with_all_peers(Some(TIMEOUT)).await);
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
            assert_full_sync(client.full_sync_with_all_peers(Some(TIMEOUT)).await);
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
                    .sync_with_all_peers(sed_id, false, Some(TIMEOUT))
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
    let counts: &[u64] = if ci_slim() {
        &[1, 2]
    } else {
        &[1, 2, 4, 8]
    };
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

                            (server, clients)
                        })
                    },
                    |(_server, clients)| {
                        rt.block_on(async {
                            let mut handles = Vec::new();
                            for client in &clients {
                                let c = client.inner();
                                handles.push(tokio::spawn(async move {
                                    assert_full_sync(
                                        c.full_sync_with_all_peers(Some(TIMEOUT)).await,
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
}

criterion_main!(benches);
