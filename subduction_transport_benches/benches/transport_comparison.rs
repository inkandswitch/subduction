//! Cross-transport comparison bench.
//!
//! Measures the same four workloads over four transports so the delta between them tells us
//! _where_ the sync protocol's wall-clock cost actually lives.
//!
//! | Workload          | What it measures                                              |
//! |-------------------|---------------------------------------------------------------|
//! | `handshake`       | Connect + mutual Ed25519 handshake (no data payload)          |
//! | `sync_1_commit`   | Single-commit full sync (one-shot round trip)                 |
//! | `sync_100_commits`| Batched sync of 100 pre-populated commits on the server side  |
//! | `blob_1mb`        | One 1 MB blob payload synced end-to-end                       |
//!
//! | Transport        | Baseline meaning                                      |
//! |------------------|-------------------------------------------------------|
//! | `channel`        | In-memory control. Anything _above_ this is I/O cost. |
//! | `websocket`      | The workspace's production target for browser sync.   |
//! | `http_longpoll`  | Per-message HTTP overhead, no multiplex.              |
//! | `iroh`           | QUIC with mandatory TLS. Highest setup, likely lowest payload cost for blobs. |
//!
//! # Running
//!
//! ```sh
//! cargo bench -p subduction_transport_benches --bench transport_comparison
//!
//! # Quick smoke-test (each bench runs once, no stats):
//! cargo bench -p subduction_transport_benches --bench transport_comparison -- --test
//!
//! # Only the channel baseline (no network setup):
//! cargo bench -p subduction_transport_benches --bench transport_comparison -- channel/
//! ```
//!
//! # Notes
//!
//! - All four transports are run against loopback (`127.0.0.1` for WS/longpoll, iroh endpoint
//!   addr in-process for Iroh).
//! - Each iteration rebuilds the server + client pair so port allocations don't accumulate.
//!   This inflates per-iteration wall-clock but makes the numbers honest.
//! - The `channel` baseline uses `ChannelTransport::pair()` from
//!   `subduction_core::connection::test_utils` and `Authenticated::new_for_test`, skipping
//!   the real handshake. Its handshake timing is effectively "in-process connection setup".

#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::unwrap_used,
    clippy::wildcard_imports,
    let_underscore_drop,
    missing_docs,
    unreachable_pub
)]

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
    test_utils::blob_from_seed,
};
use subduction_bench_support::harness::criterion::default_criterion;
use subduction_core::{
    authenticated::Authenticated,
    connection::test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    handler::sync::SyncHandler,
    handshake::audience::Audience,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::{signer::memory::MemorySigner, test_utils::signer_from_seed};
use subduction_http_longpoll::{
    client::HttpLongPollClient, http_client::reqwest_client::ReqwestHttpClient,
    server::LongPollHandler, session::SessionId, transport::HttpLongPollTransport,
};
use subduction_iroh::transport::IrohTransport;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    timeout::FuturesTimerTimeout,
    tokio::{TimeoutTokio, client::TokioWebSocketClient, server::TokioWebSocketServer},
};

// ============================================================================
// Shared constants
// ============================================================================

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);
const POLL_TIMEOUT: Duration = Duration::from_secs(2);

// ============================================================================
// Runtime
// ============================================================================

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ============================================================================
// Workload generators
// ============================================================================

/// A single deterministic commit id from a seed — small but distinct per seed.
fn commit_id(seed: u64) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    CommitId::new(bytes)
}

/// A single small blob.
fn small_blob(seed: u64) -> Blob {
    blob_from_seed(seed, 64)
}

/// Small macro that pre-populates a server Subduction handle with `$count` linear-chained
/// commits on `$sed_id`. Each commit has a tiny 64-byte blob — the goal is to exercise the
/// sync protocol's _count_-dominated path, not payload bandwidth.
///
/// A macro (rather than a generic fn) avoids the need to name every trait bound on
/// `Subduction<'static, Sendable, ...>` — those bounds differ across the four transports in
/// this bench.
macro_rules! populate_server {
    ($server:expr, $sed_id:expr, $count:expr) => {{
        let mut prev: Option<CommitId> = None;
        for i in 0..$count {
            let head = commit_id(i as u64);
            let parents: BTreeSet<CommitId> = prev.map(|p| BTreeSet::from([p])).unwrap_or_default();
            let blob = small_blob(i as u64);
            $server
                .add_commit($sed_id, head, parents, blob)
                .await
                .expect("add_commit");
            prev = Some(head);
        }
    }};
}

// ============================================================================
// ChannelTransport: in-memory control
// ============================================================================

mod channel_bench {
    use super::*;

    type Conn = MessageTransport<ChannelTransport>;

    pub type Node = Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            Conn,
            SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
            OpenPolicy,
            MemorySigner,
            InstantTimeout,
        >,
    >;

    pub fn spawn_node(seed: u64) -> (Node, MemorySigner) {
        let signer = signer_from_seed(seed);
        let (sd, _handler, listener, manager) = SubductionBuilder::new()
            .signer(signer.clone())
            .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(InstantTimeout)
            .build::<Sendable, Conn>();
        tokio::spawn(listener);
        tokio::spawn(manager);
        (sd, signer)
    }

    pub async fn connect_pair() -> (Node, Node, MemorySigner, MemorySigner) {
        let (server, server_sig) = spawn_node(0);
        let (client, client_sig) = spawn_node(1);

        let (t_a, t_b) = ChannelTransport::pair();
        let conn_a = MessageTransport::new(t_a);
        let conn_b = MessageTransport::new(t_b);

        let server_peer = PeerId::from(server_sig.verifying_key());
        let client_peer = PeerId::from(client_sig.verifying_key());

        let auth_server: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(conn_a, client_peer);
        let auth_client: Authenticated<Conn, Sendable> =
            Authenticated::new_for_test(conn_b, server_peer);

        server
            .add_connection(auth_server)
            .await
            .expect("server add_connection");
        client
            .add_connection(auth_client)
            .await
            .expect("client add_connection");

        (server, client, server_sig, client_sig)
    }
}

// ============================================================================
// WebSocket
// ============================================================================

mod ws_bench {
    use super::*;

    pub type ClientConn = TokioWebSocketClient<MemorySigner>;

    pub type Client = Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            ClientConn,
            SyncHandler<Sendable, MemoryStorage, ClientConn, OpenPolicy, CountLeadingZeroBytes>,
            OpenPolicy,
            MemorySigner,
            TimeoutTokio,
        >,
    >;

    pub type ServerHandle = TokioWebSocketServer<
        MemoryStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >;

    /// RAII guard to stop the accept loop on drop.
    pub struct ServerGuard(pub ServerHandle);
    impl Drop for ServerGuard {
        fn drop(&mut self) {
            self.0.stop();
        }
    }

    pub async fn fresh_server(seed: u64) -> (ServerGuard, PeerId, SocketAddr) {
        let sig = signer_from_seed(seed);
        let peer_id = PeerId::from(sig.verifying_key());
        let bind: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");

        let server = TokioWebSocketServer::setup(
            bind,
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
        .expect("ws server setup");
        let addr = server.address();
        (ServerGuard(server), peer_id, addr)
    }

    pub async fn connect_client(
        seed: u64,
        server_peer_id: PeerId,
        server_addr: SocketAddr,
    ) -> Client {
        let sig = signer_from_seed(seed);
        let (client, _handler, listener_fut, manager_fut) = SubductionBuilder::new()
            .signer(sig.clone())
            .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(TimeoutTokio)
            .build::<Sendable, ClientConn>();
        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let uri = format!("ws://{}:{}", server_addr.ip(), server_addr.port())
            .parse()
            .expect("valid uri");
        let (client_ws, ws_listener, ws_sender) =
            TokioWebSocketClient::new(uri, sig, Audience::known(server_peer_id))
                .await
                .expect("ws client connect");
        tokio::spawn(async move {
            let _ = ws_listener.await;
        });
        tokio::spawn(async move {
            let _ = ws_sender.await;
        });

        client
            .add_connection(client_ws)
            .await
            .expect("client add_connection");
        client
    }
}

// ============================================================================
// HTTP long-poll
// ============================================================================

mod lp_bench {
    use super::*;

    pub type Conn = MessageTransport<HttpLongPollTransport>;

    pub type Node = Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            Conn,
            SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
            OpenPolicy,
            MemorySigner,
            FuturesTimerTimeout,
        >,
    >;

    pub struct Server {
        pub subduction: Node,
        pub peer_id: PeerId,
        pub address: SocketAddr,
        _cancel: async_channel::Sender<()>,
    }

    pub async fn start_server(seed: u64) -> Server {
        let sig = signer_from_seed(seed);
        let peer_id = PeerId::from(sig.verifying_key());

        let (subduction, _handler, listener_fut, manager_fut): (Node, _, _, _) =
            SubductionBuilder::new()
                .signer(sig.clone())
                .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
                .spawner(TokioSpawn)
                .timer(FuturesTimerTimeout)
                .build::<Sendable, Conn>();
        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let handler = LongPollHandler::new(
            sig,
            Arc::new(NonceCache::default()),
            peer_id,
            // No discovery — clients connect by known peer id.
            None,
            HANDSHAKE_MAX_DRIFT,
            FuturesTimerTimeout,
        )
        .with_poll_timeout(POLL_TIMEOUT);

        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = tcp.local_addr().expect("local_addr");

        let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
        let accept_subduction = subduction.clone();
        tokio::spawn(async move {
            accept_loop(tcp, accept_subduction, handler, cancel_rx).await;
        });

        Server {
            subduction,
            peer_id,
            address,
            _cancel: cancel_tx,
        }
    }

    async fn accept_loop(
        tcp: tokio::net::TcpListener,
        subduction: Node,
        handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
        cancel: async_channel::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = cancel.recv() => break,
                res = tcp.accept() => {
                    if let Ok((stream, _addr)) = res {
                        let handler = handler.clone();
                        let subduction = subduction.clone();
                        tokio::spawn(async move {
                            serve_one(stream, handler, subduction).await;
                        });
                    }
                }
            }
        }
    }

    async fn serve_one(
        tcp: tokio::net::TcpStream,
        handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
        subduction: Node,
    ) {
        use hyper_util::rt::TokioIo;
        let io = TokioIo::new(tcp);

        let service = hyper::service::service_fn(move |req| {
            let handler = handler.clone();
            let subduction = subduction.clone();
            async move {
                let resp = match handler.handle(req).await {
                    Ok(resp) => resp,
                    Err(e) => hyper::Response::new(http_body_util::Full::new(
                        hyper::body::Bytes::from(e.to_string()),
                    )),
                };
                if resp.status() == hyper::StatusCode::OK
                    && let Some(session_hdr) = resp
                        .headers()
                        .get(subduction_http_longpoll::SESSION_ID_HEADER)
                    && let Ok(sid_str) = session_hdr.to_str()
                    && let Some(sid) = SessionId::from_hex(sid_str)
                    && let Some(auth) = handler.take_authenticated(&sid).await
                {
                    let _ = subduction
                        .add_connection(auth.map(MessageTransport::new))
                        .await;
                }
                Ok::<_, hyper::Error>(resp)
            }
        });

        let builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
        let _ = builder.serve_connection(io, service).await;
    }

    pub async fn connect_client(
        seed: u64,
        server_peer_id: PeerId,
        server_addr: SocketAddr,
    ) -> Node {
        let sig = signer_from_seed(seed);

        let (client, _handler, listener_fut, manager_fut): (Node, _, _, _) =
            SubductionBuilder::new()
                .signer(sig.clone())
                .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
                .spawner(TokioSpawn)
                .timer(FuturesTimerTimeout)
                .build::<Sendable, Conn>();
        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let base_url = format!("http://{server_addr}");
        let lp_client = HttpLongPollClient::new(&base_url, ReqwestHttpClient::new());

        let result = lp_client
            .connect(
                &sig,
                server_peer_id,
                subduction_core::timestamp::TimestampSeconds::now(),
            )
            .await
            .expect("lp client connect");

        tokio::spawn(result.poll_task);
        tokio::spawn(result.send_task);

        client
            .add_connection(result.authenticated.map(MessageTransport::new))
            .await
            .expect("client add_connection");
        client
    }
}

// ============================================================================
// Iroh
// ============================================================================

mod iroh_bench {
    use super::*;

    pub type Conn = MessageTransport<IrohTransport>;

    pub type Node = Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            Conn,
            SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>,
            OpenPolicy,
            MemorySigner,
            FuturesTimerTimeout,
        >,
    >;

    pub struct Server {
        pub subduction: Node,
        pub endpoint: iroh::Endpoint,
        pub peer_id: PeerId,
    }

    pub async fn start_server(seed: u64) -> Server {
        let sig = signer_from_seed(seed);
        let peer_id = PeerId::from(sig.verifying_key());

        let (subduction, _handler, listener_fut, manager_fut): (Node, _, _, _) =
            SubductionBuilder::new()
                .signer(sig.clone())
                .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
                .spawner(TokioSpawn)
                .timer(FuturesTimerTimeout)
                .build::<Sendable, Conn>();
        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![subduction_iroh::ALPN.to_vec()])
            .bind()
            .await
            .expect("bind iroh endpoint");

        let accept_subduction = subduction.clone();
        let accept_signer = sig;
        let accept_ep = endpoint.clone();
        tokio::spawn(async move {
            let nonce_cache = NonceCache::default();
            while let Ok(result) = subduction_iroh::server::accept_one(
                &accept_ep,
                &accept_signer,
                &nonce_cache,
                peer_id,
                None,
                HANDSHAKE_MAX_DRIFT,
            )
            .await
            {
                tokio::spawn(result.listener_task);
                tokio::spawn(result.sender_task);
                let auth = result.authenticated.map(MessageTransport::new);
                let _ = accept_subduction.add_connection(auth).await;
            }
        });

        Server {
            subduction,
            endpoint,
            peer_id,
        }
    }

    pub async fn connect_client(seed: u64, server: &Server) -> Node {
        let sig = signer_from_seed(seed);

        let (client, _handler, listener_fut, manager_fut): (Node, _, _, _) =
            SubductionBuilder::new()
                .signer(sig.clone())
                .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
                .spawner(TokioSpawn)
                .timer(FuturesTimerTimeout)
                .build::<Sendable, Conn>();
        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let client_ep = iroh::Endpoint::builder()
            .bind()
            .await
            .expect("bind client endpoint");

        let server_addr = server.endpoint.addr();
        let result = subduction_iroh::client::connect(
            &client_ep,
            server_addr,
            &sig,
            Audience::known(server.peer_id),
        )
        .await
        .expect("iroh client connect");
        tokio::spawn(result.listener_task);
        tokio::spawn(result.sender_task);

        client
            .add_connection(result.authenticated.map(MessageTransport::new))
            .await
            .expect("client add_connection");
        client
    }
}

// ============================================================================
// Benches
// ============================================================================

/// How many times we sample connection-setup + sync to bound total bench wall-clock.
/// Defaults chosen to fit in <~60 s per group on a laptop.
const HANDSHAKE_SAMPLES: usize = 20;
const SYNC_SAMPLES: usize = 15;

fn bench_handshake(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("transport_comparison/handshake");
    group.sample_size(HANDSHAKE_SAMPLES);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::from_parameter("channel"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (_s, _c, _sk, _ck) = channel_bench::connect_pair().await;
            });
        });
    });

    group.bench_function(BenchmarkId::from_parameter("websocket"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let (server_guard, peer, addr) = ws_bench::fresh_server(0).await;
                let client = ws_bench::connect_client(1, peer, addr).await;
                drop(client);
                drop(server_guard);
            });
        });
    });

    group.bench_function(BenchmarkId::from_parameter("http_longpoll"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let server = lp_bench::start_server(0).await;
                let client = lp_bench::connect_client(1, server.peer_id, server.address).await;
                drop(client);
                drop(server);
            });
        });
    });

    group.bench_function(BenchmarkId::from_parameter("iroh"), |b| {
        b.iter(|| {
            rt.block_on(async {
                let server = iroh_bench::start_server(0).await;
                let client = iroh_bench::connect_client(1, &server).await;
                drop(client);
                drop(server);
            });
        });
    });

    group.finish();
}

fn bench_sync_commits(c: &mut Criterion) {
    let rt = runtime();
    let sed_id = SedimentreeId::new([0xab; 32]);

    for &count in &[1usize, 100] {
        let mut group = c.benchmark_group("transport_comparison/sync_commits");
        group.sample_size(SYNC_SAMPLES);
        group.measurement_time(Duration::from_secs(10));
        group.throughput(Throughput::Elements(count as u64));

        // --- channel ---
        group.bench_with_input(BenchmarkId::new("channel", count), &count, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (server, client, server_sig, _ck) = channel_bench::connect_pair().await;
                        populate_server!(server, sed_id, n);
                        let server_peer = PeerId::from(server_sig.verifying_key());
                        (server, client, server_peer)
                    })
                },
                |(server, client, server_peer)| {
                    rt.block_on(async move {
                        let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                        drop(server);
                        drop(client);
                    });
                },
                BatchSize::PerIteration,
            );
        });

        // --- websocket ---
        group.bench_with_input(BenchmarkId::new("websocket", count), &count, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let (server_guard, server_peer, addr) = ws_bench::fresh_server(0).await;
                        let client = ws_bench::connect_client(1, server_peer, addr).await;
                        populate_server!(server_guard.0.subduction(), sed_id, n);
                        (server_guard, client, server_peer)
                    })
                },
                |(server_guard, client, server_peer)| {
                    rt.block_on(async move {
                        let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                        drop(client);
                        drop(server_guard);
                    });
                },
                BatchSize::PerIteration,
            );
        });

        // --- http_longpoll ---
        group.bench_with_input(BenchmarkId::new("http_longpoll", count), &count, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let server = lp_bench::start_server(0).await;
                        let server_peer = server.peer_id;
                        let addr = server.address;
                        let client = lp_bench::connect_client(1, server_peer, addr).await;
                        populate_server!(server.subduction, sed_id, n);
                        (server, client, server_peer)
                    })
                },
                |(server, client, server_peer)| {
                    rt.block_on(async move {
                        let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                        drop(client);
                        drop(server);
                    });
                },
                BatchSize::PerIteration,
            );
        });

        // --- iroh ---
        group.bench_with_input(BenchmarkId::new("iroh", count), &count, |b, &n| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let server = iroh_bench::start_server(0).await;
                        let server_peer = server.peer_id;
                        let client = iroh_bench::connect_client(1, &server).await;
                        populate_server!(server.subduction, sed_id, n);
                        (server, client, server_peer)
                    })
                },
                |(server, client, server_peer)| {
                    rt.block_on(async move {
                        let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                        drop(client);
                        drop(server);
                    });
                },
                BatchSize::PerIteration,
            );
        });

        group.finish();
    }
}

fn bench_blob_1mb(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("transport_comparison/blob_1mb");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));
    group.throughput(Throughput::Bytes(1024 * 1024));

    let sed_id = SedimentreeId::new([0xcd; 32]);
    let blob_size = 1024 * 1024;

    // Populate with a single commit carrying a `blob_size`-byte blob. Written as a macro for
    // the same reason as `populate_server!`: the trait bounds on `Subduction<'static, ...>`
    // differ across transports, so a generic fn would be awkward to spell.
    macro_rules! populate_with_blob {
        ($server:expr, $sed_id:expr, $blob_size:expr) => {{
            let head = commit_id(0);
            let blob = blob_from_seed(0, $blob_size);
            $server
                .add_commit($sed_id, head, BTreeSet::new(), blob)
                .await
                .expect("add_commit");
        }};
    }

    // --- channel ---
    group.bench_function(BenchmarkId::from_parameter("channel"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (server, client, server_sig, _ck) = channel_bench::connect_pair().await;
                    populate_with_blob!(server, sed_id, blob_size);
                    let server_peer = PeerId::from(server_sig.verifying_key());
                    (server, client, server_peer)
                })
            },
            |(server, client, server_peer)| {
                rt.block_on(async move {
                    let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                    drop(server);
                    drop(client);
                });
            },
            BatchSize::PerIteration,
        );
    });

    // --- websocket ---
    group.bench_function(BenchmarkId::from_parameter("websocket"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let (server_guard, server_peer, addr) = ws_bench::fresh_server(0).await;
                    let client = ws_bench::connect_client(1, server_peer, addr).await;
                    populate_with_blob!(server_guard.0.subduction(), sed_id, blob_size);
                    (server_guard, client, server_peer)
                })
            },
            |(server_guard, client, server_peer)| {
                rt.block_on(async move {
                    let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                    drop(client);
                    drop(server_guard);
                });
            },
            BatchSize::PerIteration,
        );
    });

    // --- http_longpoll ---
    group.bench_function(BenchmarkId::from_parameter("http_longpoll"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let server = lp_bench::start_server(0).await;
                    let server_peer = server.peer_id;
                    let addr = server.address;
                    let client = lp_bench::connect_client(1, server_peer, addr).await;
                    populate_with_blob!(server.subduction, sed_id, blob_size);
                    (server, client, server_peer)
                })
            },
            |(server, client, server_peer)| {
                rt.block_on(async move {
                    let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                    drop(client);
                    drop(server);
                });
            },
            BatchSize::PerIteration,
        );
    });

    // --- iroh ---
    group.bench_function(BenchmarkId::from_parameter("iroh"), |b| {
        b.iter_batched(
            || {
                rt.block_on(async {
                    let server = iroh_bench::start_server(0).await;
                    let server_peer = server.peer_id;
                    let client = iroh_bench::connect_client(1, &server).await;
                    populate_with_blob!(server.subduction, sed_id, blob_size);
                    (server, client, server_peer)
                })
            },
            |(server, client, server_peer)| {
                rt.block_on(async move {
                    let _ = client.full_sync_with_peer(&server_peer, false, None).await;
                    drop(client);
                    drop(server);
                });
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_handshake,
        bench_sync_commits,
        bench_blob_1mb,
}
criterion_main!(benches);
