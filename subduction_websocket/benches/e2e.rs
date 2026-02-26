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
use criterion_pprof::criterion::{Output, PProfProfiler};
use future_form::Sendable;
use rand::{Rng, SeedableRng, rngs::StdRng};
use sedimentree_core::{blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    connection::{handshake::Audience, nonce_cache::NonceCache},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer},
};

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);
const TIMEOUT: Duration = Duration::from_secs(10);

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
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
        TIMEOUT,
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
    (ServerGuard(server), peer_id, bound)
}

type ClientSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >,
>;

/// RAII guard that calls [`TokioWebSocketServer::stop`] on drop to ensure the
/// accept loop and associated tasks are cleaned up between benchmark iterations.
struct ServerGuard(
    TokioWebSocketServer<
        MemoryStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >,
);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.0.stop();
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
        &self.0
    }
}

/// Assert that a `full_sync` call completed successfully.
#[allow(clippy::type_complexity)]
fn assert_full_sync(
    result: (
        bool,
        subduction_core::connection::stats::SyncStats,
        Vec<(
            subduction_core::connection::authenticated::Authenticated<
                TokioWebSocketClient<MemorySigner, TimeoutTokio>,
                Sendable,
            >,
            subduction_websocket::error::CallError,
        )>,
        Vec<(
            SedimentreeId,
            subduction_core::subduction::error::IoError<
                Sendable,
                MemoryStorage,
                TokioWebSocketClient<MemorySigner, TimeoutTokio>,
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

/// Create a `Subduction` client, connect to the server, start background tasks.
async fn connected_client(
    seed: u8,
    server_peer_id: PeerId,
    server_addr: SocketAddr,
) -> ClientSubduction {
    let client_signer = signer(seed);
    let (client, listener_fut, manager_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >::new(
        None,
        client_signer.clone(),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    // `listener_fut` already runs `Subduction::listen()` internally —
    // do NOT spawn an additional `client.listen()` call.
    tokio::spawn(manager_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", server_addr.ip(), server_addr.port())
        .parse()
        .expect("valid uri");

    let (client_ws, ws_listener, ws_sender) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        TIMEOUT,
        client_signer,
        Audience::known(server_peer_id),
    )
    .await
    .expect("client connect");

    tokio::spawn(async {
        if let Err(e) = ws_listener.await {
            tracing::error!("ws_listener task failed: {e:?}");
        }
    });
    tokio::spawn(async {
        if let Err(e) = ws_sender.await {
            tracing::error!("ws_sender task failed: {e:?}");
        }
    });

    client.register(client_ws).await.expect("register");
    client
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

                    let (_ws, listener, sender) = TokioWebSocketClient::new(
                        uri,
                        TimeoutTokio,
                        TIMEOUT,
                        client_signer,
                        Audience::known(server_peer_id),
                    )
                    .await
                    .expect("connect");

                    tokio::spawn(async { listener.await.ok() });
                    tokio::spawn(async { sender.await.ok() });
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
                    let blob = blob_from_seed(&mut rng, 64);
                    client
                        .add_commit(sed_id, BTreeSet::new(), blob)
                        .await
                        .expect("add commit");

                    (server, client, sed_id)
                })
            },
            |(_server, client, sed_id)| {
                rt.block_on(async {
                    client
                        .sync_all(sed_id, false, Some(TIMEOUT))
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

    for count in [1, 10, 50, 100] {
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
                            let blob = blob_from_seed(&mut rng, 64);
                            client
                                .add_commit(sed_id, BTreeSet::new(), blob)
                                .await
                                .expect("add commit");
                        }

                        (server, client)
                    })
                },
                |(_server, client)| {
                    rt.block_on(async {
                        assert_full_sync(client.full_sync(Some(TIMEOUT)).await);
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

    for &(label, size) in &[
        ("1KB", 1_024),
        ("64KB", 64 * 1_024),
        ("256KB", 256 * 1_024),
        ("1MB", 1_024 * 1_024),
    ] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("blob", label), &size, |b, &size| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (server, server_peer_id, bound) = fresh_server(0).await;
                        let client = connected_client(1, server_peer_id, bound).await;

                        let sed_id = SedimentreeId::new([0u8; 32]);
                        let mut rng = StdRng::seed_from_u64(0);
                        let blob = blob_from_seed(&mut rng, size);
                        client
                            .add_commit(sed_id, BTreeSet::new(), blob)
                            .await
                            .expect("add commit");

                        (server, client)
                    })
                },
                |(_server, client)| {
                    rt.block_on(async {
                        assert_full_sync(client.full_sync(Some(TIMEOUT)).await);
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
                        let blob = blob_from_seed(&mut rng, 64);
                        server
                            .subduction()
                            .add_commit(sed_id, BTreeSet::new(), blob)
                            .await
                            .expect("server add commit");
                    }

                    for _ in 0..5 {
                        let blob = blob_from_seed(&mut rng, 64);
                        client
                            .add_commit(sed_id, BTreeSet::new(), blob)
                            .await
                            .expect("client add commit");
                    }

                    (server, client)
                })
            },
            |(_server, client)| {
                rt.block_on(async {
                    assert_full_sync(client.full_sync(Some(TIMEOUT)).await);
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
                let blob = blob_from_seed(&mut rng, 64);
                server
                    .subduction()
                    .add_commit(sed_id, BTreeSet::new(), blob)
                    .await
                    .expect("server add commit");
            }

            // Sync to get client up to date
            assert_full_sync(client.full_sync(Some(TIMEOUT)).await);
            tokio::time::sleep(Duration::from_millis(100)).await;

            (server, client, sed_id)
        });

        let mut seed_counter: u64 = 1000;

        b.iter(|| {
            rt.block_on(async {
                let mut rng = StdRng::seed_from_u64(seed_counter);
                seed_counter += 1;

                let blob = blob_from_seed(&mut rng, 64);
                client
                    .add_commit(sed_id, BTreeSet::new(), blob)
                    .await
                    .expect("add commit");

                client
                    .sync_all(sed_id, false, Some(TIMEOUT))
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

    for num_clients in [1, 2, 4, 8] {
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
                                let blob = blob_from_seed(&mut rng, 64);
                                client
                                    .add_commit(sed_id, BTreeSet::new(), blob)
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
                                let c = client.clone();
                                handles.push(tokio::spawn(async move {
                                    assert_full_sync(c.full_sync(Some(TIMEOUT)).await);
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

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(997, Output::Flamegraph(None)));
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
