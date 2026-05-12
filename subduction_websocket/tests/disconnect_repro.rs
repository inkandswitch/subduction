//! Reproduces "lots of WebSocket disconnections in server logs while
//! browser/client is alive and registers timeouts".
//!
//! Each test captures every `tracing` event the server emits, then
//! asserts that no spurious disconnect/close was logged for a peer
//! whose client end is still operating normally. The peer that does
//! the activity is a `TokioWebSocketClient` (no browser) — so any
//! disconnect logged is a server-side false positive, not a real one.

#![allow(
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::let_underscore_future,
    clippy::match_same_arms,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::used_underscore_binding,
    let_underscore_drop,
    missing_docs
)]

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId, loose_commit::id::CommitId,
};
use std::{
    collections::BTreeSet,
    io::Write,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use subduction_core::{
    handler::sync::SyncHandler,
    handshake::audience::Audience,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer},
};
use testresult::TestResult;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

// ─── Tracing capture ────────────────────────────────────────────────────────

/// Thread-safe in-memory writer for tracing events.
#[derive(Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    fn snapshot(&self) -> String {
        let buf = self.0.lock().expect("log capture mutex");
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// Lines that mention disconnect-like events from the server side.
    fn disconnect_lines(&self) -> Vec<String> {
        self.snapshot()
            .lines()
            .filter(|line| {
                line.contains("disconnected")
                    || line.contains("closed, removing")
                    || line.contains("error reading from websocket")
            })
            .map(str::to_owned)
            .collect()
    }
}

impl Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Tee to stdout for test debugging.
        let _ = std::io::stdout().write_all(buf);
        self.0
            .lock()
            .expect("log capture mutex")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> fmt::MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

static TRACING_INIT: OnceLock<LogCapture> = OnceLock::new();

fn init_capture() -> LogCapture {
    TRACING_INIT
        .get_or_init(|| {
            let capture = LogCapture::default();
            let layer = fmt::layer()
                .with_writer(capture.clone())
                .with_ansi(false)
                .with_target(true);
            let filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,subduction_websocket=debug"));
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .init();
            capture
        })
        .clone()
}

// ─── Test fixture types ─────────────────────────────────────────────────────

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);

fn test_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn random_commit_id(seed: u64) -> CommitId {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    CommitId::new(bytes)
}

type ClientSub = Arc<
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

type ServerSub = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        MessageTransport<subduction_websocket::tokio::unified::UnifiedWebSocket>,
        SyncHandler<
            Sendable,
            MemoryStorage,
            MessageTransport<subduction_websocket::tokio::unified::UnifiedWebSocket>,
            OpenPolicy,
            CountLeadingZeroBytes,
        >,
        OpenPolicy,
        MemorySigner,
        TimeoutTokio,
    >,
>;

async fn fresh_server() -> TestResult<(
    TokioWebSocketServer<
        MemoryStorage,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
        TimeoutTokio,
    >,
    ServerSub,
    PeerId,
    SocketAddr,
)> {
    let signer = test_signer(0);
    let server_peer_id = PeerId::from(signer.verifying_key());

    let (subduction, _handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(signer.clone())
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<Sendable, MessageTransport<subduction_websocket::tokio::unified::UnifiedWebSocket>>();

    tokio::spawn(async move {
        let _ = listener_fut.await;
    });
    tokio::spawn(async move {
        let _ = manager_fut.await;
    });

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server = TokioWebSocketServer::new(
        addr,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        subduction.clone(),
    )
    .await?;
    let bound = server.address();

    Ok((server, subduction, server_peer_id, bound))
}

async fn connect_client(
    seed: u8,
    server_peer_id: PeerId,
    bound: SocketAddr,
) -> TestResult<ClientSub> {
    let signer = test_signer(seed);
    let (client, _handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(signer.clone())
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<Sendable, TokioWebSocketClient<MemorySigner>>();

    tokio::spawn(async move {
        let _ = listener_fut.await;
    });
    tokio::spawn(async move {
        let _ = manager_fut.await;
    });

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, ws_listener, ws_sender) =
        TokioWebSocketClient::new(uri, signer, Audience::known(server_peer_id)).await?;

    tokio::spawn(async move {
        let _ = ws_listener.await;
    });
    tokio::spawn(async move {
        let _ = ws_sender.await;
    });

    client.add_connection(client_ws).await?;
    Ok(client)
}

fn random_commit() -> (CommitId, BTreeSet<CommitId>, Blob) {
    use rand::RngCore;
    let mut bytes = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut bytes);
    let blob = Blob::new(bytes.to_vec());
    let mut id_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut id_bytes);
    (CommitId::new(id_bytes), BTreeSet::new(), blob)
}

// ─── Scenarios ──────────────────────────────────────────────────────────────

/// Sustained back-and-forth on a single connection. If the server is
/// spuriously dropping the conn under normal traffic, this surfaces it
/// because the logs will show "WebSocket listener disconnected" while
/// the test is still pushing valid messages through the client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sustained_activity_no_disconnect() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, server_sub, server_peer_id, bound) = fresh_server().await?;
    let client = connect_client(1, server_peer_id, bound).await?;

    let sed_id = SedimentreeId::new([1u8; 32]);

    // 200 iterations of: add a commit, sync, brief pause. Enough to
    // exercise many round-trips. ~30s wall on typical hardware.
    for i in 0..200u64 {
        let (head, parents, blob) = random_commit();
        client.add_commit(sed_id, head, parents, blob).await?;
        client
            .sync_with_all_peers(sed_id, false, Some(Duration::from_secs(5)))
            .await?;
        if i % 25 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Connection should still be present on the server.
    let connected = server_sub.connected_peer_ids().await;
    assert_eq!(
        connected.len(),
        1,
        "server should still see the client (saw {connected:?})"
    );

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "spurious disconnect logs during sustained activity: {new_disconnects:#?}"
    );

    Ok(())
}

/// Burst of concurrent sync requests on a single connection. This pushes
/// many messages through the outbound channel quickly. If the sender
/// task fails on any transient backpressure-like condition, the
/// asymmetric listener-still-alive / sender-dead state should surface.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_send_burst_no_disconnect() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, _server_sub, server_peer_id, bound) = fresh_server().await?;
    let client = connect_client(2, server_peer_id, bound).await?;

    let sed_id = SedimentreeId::new([2u8; 32]);

    // Add 100 commits then fan out N concurrent full syncs.
    for _ in 0..100u64 {
        let (head, parents, blob) = random_commit();
        client.add_commit(sed_id, head, parents, blob).await?;
    }

    let mut joins = Vec::new();
    for _ in 0..16u64 {
        let c = client.clone();
        joins.push(tokio::spawn(async move {
            c.sync_with_all_peers(sed_id, false, Some(Duration::from_secs(10)))
                .await
        }));
    }
    for j in joins {
        let _ = j.await?;
    }

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "spurious disconnect logs under concurrent send burst: {new_disconnects:#?}"
    );

    Ok(())
}

/// Idle period then activity. If long idle periods cause the server's
/// side of the WebSocket to fail (e.g., TCP keepalive timeout or
/// tungstenite getting confused), the resume sync after idle will hang
/// or surface a disconnect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_then_resume_no_disconnect() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, _server_sub, server_peer_id, bound) = fresh_server().await?;
    let client = connect_client(3, server_peer_id, bound).await?;

    let sed_id = SedimentreeId::new([3u8; 32]);

    let (head, parents, blob) = random_commit();
    client.add_commit(sed_id, head, parents, blob).await?;
    client
        .sync_with_all_peers(sed_id, false, Some(Duration::from_secs(5)))
        .await?;

    // Idle for 5 seconds (much shorter than typical NAT timeouts, but
    // long enough to surface anything that fires on a heartbeat
    // missed-tick basis).
    tokio::time::sleep(Duration::from_secs(5)).await;

    let (head2, parents2, blob2) = random_commit();
    client.add_commit(sed_id, head2, parents2, blob2).await?;
    client
        .sync_with_all_peers(sed_id, false, Some(Duration::from_secs(5)))
        .await?;

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "spurious disconnect logs after idle period: {new_disconnects:#?}"
    );

    Ok(())
}

/// Many concurrent peers each doing sustained activity. Browser-style
/// load profile: a handful of long-lived tabs each pushing periodic
/// updates. This is the workload most similar to the user's
/// production observation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_peers_sustained_no_disconnect() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, server_sub, server_peer_id, bound) = fresh_server().await?;

    let num_peers = 8usize;
    let mut clients = Vec::new();
    for i in 0..num_peers {
        let c = connect_client(u8::try_from(i + 10)?, server_peer_id, bound).await?;
        clients.push(c);
    }

    // Wait for all to register on the server.
    for _ in 0..50 {
        if server_sub.connected_peer_ids().await.len() == num_peers {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        server_sub.connected_peer_ids().await.len(),
        num_peers,
        "all peers should be registered"
    );

    let sed_id = SedimentreeId::new([4u8; 32]);

    let mut joins = Vec::new();
    for (i, client) in clients.iter().enumerate() {
        let c = client.clone();
        joins.push(tokio::spawn(async move {
            for j in 0..50u64 {
                let id = random_commit_id(u64::try_from(i)? * 10_000 + j);
                let blob = Blob::new(vec![u8::try_from(i % 256)?; 64]);
                c.add_commit(sed_id, id, BTreeSet::new(), blob).await?;
                c.sync_with_all_peers(sed_id, false, Some(Duration::from_secs(10)))
                    .await?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<(), eyre::Report>(())
        }));
    }
    for j in joins {
        let _ = j.await?;
    }

    assert_eq!(
        server_sub.connected_peer_ids().await.len(),
        num_peers,
        "all peers should still be registered after activity"
    );

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "spurious disconnect logs across {num_peers} peers under sustained activity: {new_disconnects:#?}"
    );

    Ok(())
}

/// Heavy outbound traffic to a slow consumer. The client connects but
/// then deliberately throttles its sync rate. The server has lots of
/// data the client must request; if the server's outbound buffer fills
/// or any tungstenite write transiently fails (e.g., the OS write
/// buffer is full because the client hasn't ACKed yet), the
/// sender task dies and we see the asymmetric "listener alive, sender
/// dead" cascade.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumer_no_sender_death() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, server_sub, server_peer_id, bound) = fresh_server().await?;
    let client = connect_client(8, server_peer_id, bound).await?;

    let sed_id = SedimentreeId::new([8u8; 32]);

    // Server-side: 200 commits with 16 KiB blobs ≈ 3.2 MiB.
    for i in 0..200u64 {
        let id = random_commit_id(i);
        let blob = Blob::new(vec![u8::try_from(i % 256)?; 16 * 1024]);
        server_sub
            .add_commit(sed_id, id, BTreeSet::new(), blob)
            .await?;
    }

    // Client pulls in one shot — exercises the server's outbound path
    // heavily.
    client
        .sync_with_all_peers(sed_id, false, Some(Duration::from_secs(30)))
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "spurious disconnect logs under heavy outbound traffic: {new_disconnects:#?}"
    );

    Ok(())
}

/// Simulates a Caddy/proxy that idle-closes connections after a
/// short timeout. Goal: reproduce the user's report of
/// "peer X disconnected: sender task stopped" cascade when the proxy
/// drops the TCP connection while the application stack thinks it's
/// alive.
///
/// Topology: client ──tcp──> idle_proxy ──tcp──> server.
/// `idle_proxy` shuttles bytes for `idle_timeout`. If no bytes flow
/// in either direction for the timeout, the proxy drops both halves,
/// simulating Caddy's silent connection close.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_proxy_close_repro_sender_stopped() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();
    let pre_dc_count_all = capture
        .snapshot()
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count();

    let (_server, server_sub, server_peer_id, server_bound) = fresh_server().await?;

    // Idle-killing proxy.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_bound = proxy_listener.local_addr()?;
    let idle_timeout = Duration::from_secs(2);

    tokio::spawn(async move {
        loop {
            let Ok((client_conn, _)) = proxy_listener.accept().await else {
                return;
            };
            let backend_addr = server_bound;
            tokio::spawn(async move {
                let Ok(backend_conn) = tokio::net::TcpStream::connect(backend_addr).await else {
                    return;
                };
                let (mut c_r, mut c_w) = client_conn.into_split();
                let (mut b_r, mut b_w) = backend_conn.into_split();

                let c_to_b = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, c_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if b_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = b_w.shutdown().await;
                };
                let b_to_c = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, b_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if c_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = c_w.shutdown().await;
                };
                let _ = futures::future::join(c_to_b, b_to_c).await;
            });
        }
    });

    // Connect a client THROUGH the proxy and subscribe it.
    let client = connect_client(20, server_peer_id, proxy_bound).await?;

    let sed_id = SedimentreeId::new([0xCAu8; 32]);
    // Subscribe to the sedimentree so the server will broadcast updates to us.
    client
        .sync_with_all_peers(sed_id, true, Some(Duration::from_secs(5)))
        .await?;

    // Idle longer than the proxy timeout so it closes both halves.
    // The server's sender_task will die when it next tries to send
    // (or its listener will exit due to the reset).
    tokio::time::sleep(idle_timeout + Duration::from_secs(2)).await;

    // Now provoke a server-side broadcast to the (now-dead) subscriber.
    // The server's handler will try `conn.send(...)` on the broken
    // connection; if the sender_task is dead, the outbound channel
    // returns SendError ("sender task stopped").
    let (head, parents, blob) = random_commit();
    let _ = server_sub.add_commit(sed_id, head, parents, blob).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    let snapshot = capture.snapshot();
    let new_dc_lines = capture.disconnect_lines();
    let new_dc_after = new_dc_lines.len() - pre_dc_count;
    let new_sender_stopped: usize = snapshot
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count()
        - pre_dc_count_all;

    eprintln!(
        "\n=== idle_proxy_close_repro: {new_dc_after} new disconnect line(s), {new_sender_stopped} mention 'sender task stopped' ===\n"
    );
    for line in &new_dc_lines[pre_dc_count..] {
        eprintln!("  {line}");
    }
    eprintln!();

    // The user reports `sender task stopped` (from `SendError`).
    // We've already proven the same root cause locally: a proxy
    // idle-close triggers the server's listener + sender tasks to
    // exit. In this single-conn test the handler doesn't get a chance
    // to broadcast before the connection is cleaned up; with multiple
    // subscribers (production), the handler hits `conn.send` on a
    // peer whose sender task already died and logs
    // "peer X disconnected: sender task stopped".
    assert!(
        new_dc_after > 0,
        "expected at least one disconnect-type log after proxy idle-close"
    );
    let _ = new_sender_stopped; // multi-subscriber variant proves the rest

    Ok(())
}

/// Two subscribers: A through a proxy that idle-closes the underlying
/// TCP connection; B directly to the server. A's transport gets killed
/// by the proxy; B then triggers broadcasts that would have hit A's
/// dead connection.
///
/// **Bug under test**: when `WebSocket::listen()` exited with
/// `Err(ResetWithoutClosingHandshake)`, it only logged the disconnect
/// — it didn't close the inbound channel. `connection_loop` stayed
/// parked on `recv()`, the conn stayed in the map, and every broadcast
/// attempt logged `"peer X disconnected: sender task stopped"`. Bedrock
/// production observed 1162 cascade lines/day from this.
///
/// **Fix**: `WebSocket::listen()` now closes both inbound and outbound
/// channels on exit, which propagates the disconnect promptly through
/// `connection_loop` (which exits, the conn is removed from the map,
/// and broadcasts no longer target the dead peer). One TCP reset
/// should produce **zero** "sender task stopped" log lines.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_proxy_close_does_not_cascade() -> TestResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let capture = init_capture();
    let snapshot_before = capture.snapshot();
    let pre_sender_stopped: usize = snapshot_before
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count();

    let (_server, server_sub, server_peer_id, server_bound) = fresh_server().await?;

    // Idle-killing proxy.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_bound = proxy_listener.local_addr()?;
    let idle_timeout = Duration::from_secs(2);

    tokio::spawn(async move {
        loop {
            let Ok((client_conn, _)) = proxy_listener.accept().await else {
                return;
            };
            let backend_addr = server_bound;
            tokio::spawn(async move {
                let Ok(backend_conn) = tokio::net::TcpStream::connect(backend_addr).await else {
                    return;
                };
                let (mut c_r, mut c_w) = client_conn.into_split();
                let (mut b_r, mut b_w) = backend_conn.into_split();

                let c_to_b = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, c_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if b_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = b_w.shutdown().await;
                };
                let b_to_c = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, b_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if c_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = c_w.shutdown().await;
                };
                let _ = futures::future::join(c_to_b, b_to_c).await;
            });
        }
    });

    let sed_id = SedimentreeId::new([0xCBu8; 32]);

    // Subscriber A: through the proxy (will be killed by proxy idle).
    let client_a = connect_client(21, server_peer_id, proxy_bound).await?;
    client_a
        .sync_with_all_peers(sed_id, true, Some(Duration::from_secs(5)))
        .await?;

    // Subscriber B: directly to server (will stay alive and trigger broadcast).
    let client_b = connect_client(22, server_peer_id, server_bound).await?;
    client_b
        .sync_with_all_peers(sed_id, true, Some(Duration::from_secs(5)))
        .await?;

    // Idle longer than the proxy timeout — A's connection through
    // the proxy gets killed.
    tokio::time::sleep(idle_timeout + Duration::from_millis(500)).await;

    // B adds a commit → server broadcasts to all subscribers
    // (including the dead A). The handler tries `conn.send(...)` on
    // A's now-dead transport. The sender_task for A has died (or is
    // about to die on this write attempt) → SendError → log line.
    for _ in 0..5u64 {
        let (head, parents, blob) = random_commit();
        client_b.add_commit(sed_id, head, parents, blob).await?;
        // Also have the server add commits to broadcast.
        let (head2, parents2, blob2) = random_commit();
        let _ = server_sub.add_commit(sed_id, head2, parents2, blob2).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    let snapshot_after = capture.snapshot();
    let new_sender_stopped = snapshot_after
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count()
        - pre_sender_stopped;

    let dc_lines: Vec<&str> = snapshot_after
        .lines()
        .filter(|l| {
            l.contains("disconnected")
                || l.contains("sender task stopped")
                || l.contains("closed, removing")
                || l.contains("error reading from websocket")
        })
        .collect();
    let user_visible: Vec<&&str> = dc_lines.iter().filter(|l| l.contains("disconnected")).collect();

    eprintln!(
        "\n=== cascade-suppression test: {new_sender_stopped} 'sender task stopped' line(s) ===\n"
    );
    for line in user_visible.iter().rev().take(10) {
        eprintln!("  {line}");
    }
    eprintln!();

    // With the listen-on-exit channel-close fix, the dead connection
    // is removed from `Subduction`'s map promptly via the standard
    // `connection_closed` cascade. Subsequent broadcasts therefore
    // don't even attempt to send to it, so no "sender task stopped"
    // logs should fire. Before the fix this number was typically
    // ~num_broadcasts (one cascade log per attempted send).
    assert_eq!(
        new_sender_stopped, 0,
        "expected no 'sender task stopped' lines after fix; got {new_sender_stopped}"
    );

    Ok(())
}

/// Same scenario as `idle_proxy_close_multi_subscriber_sender_stopped`
/// but the server uses a 500 ms keepalive interval, which is shorter
/// than the proxy's 2 s idle timeout. The keepalive Pings should keep
/// the connection alive and prevent the cascade entirely.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keepalive_prevents_proxy_idle_cascade() -> TestResult {
    use future_form::Sendable;
    use subduction_websocket::tokio::TrackedTokioSpawn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_util::task::TaskTracker;
    use subduction_core::transport::message::MessageTransport;
    use subduction_websocket::tokio::unified::UnifiedWebSocket;

    let capture = init_capture();
    let snapshot_before = capture.snapshot();
    let pre_sender_stopped: usize = snapshot_before
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count();
    let pre_listener_disc: usize = snapshot_before
        .lines()
        .filter(|l| l.contains("WebSocket listener disconnected"))
        .count();

    // Server with a short keepalive interval (under the proxy idle).
    let server_signer = test_signer(0);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let tasks = TaskTracker::new();
    let spawner = TrackedTokioSpawn::new(tasks.clone());

    let (subduction, _handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(server_signer.clone())
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(spawner)
        .timer(TimeoutTokio)
        .build::<Sendable, MessageTransport<UnifiedWebSocket>>();

    tasks.spawn(async move {
        let _ = listener_fut.await;
    });
    tasks.spawn(async move {
        let _ = manager_fut.await;
    });

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let _server = TokioWebSocketServer::new_with_tracker(
        addr,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        Duration::from_millis(500), // keepalive, shorter than proxy idle
        subduction.clone(),
        tasks,
    )
    .await?;
    let server_bound = _server.address();

    // Idle-killing proxy.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_bound = proxy_listener.local_addr()?;
    let idle_timeout = Duration::from_secs(2);

    tokio::spawn(async move {
        loop {
            let Ok((client_conn, _)) = proxy_listener.accept().await else {
                return;
            };
            let backend_addr = server_bound;
            tokio::spawn(async move {
                let Ok(backend_conn) = tokio::net::TcpStream::connect(backend_addr).await else {
                    return;
                };
                let (mut c_r, mut c_w) = client_conn.into_split();
                let (mut b_r, mut b_w) = backend_conn.into_split();

                let c_to_b = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, c_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if b_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = b_w.shutdown().await;
                };
                let b_to_c = async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match tokio::time::timeout(idle_timeout, b_r.read(&mut buf)).await {
                            Ok(Ok(0)) | Err(_) => break,
                            Ok(Ok(n)) => {
                                if c_w.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                            Ok(Err(_)) => break,
                        }
                    }
                    let _ = c_w.shutdown().await;
                };
                let _ = futures::future::join(c_to_b, b_to_c).await;
            });
        }
    });

    let sed_id = SedimentreeId::new([0xCDu8; 32]);

    // Subscribe a client through the proxy.
    let client = connect_client(41, server_peer_id, proxy_bound).await?;
    client
        .sync_with_all_peers(sed_id, true, Some(Duration::from_secs(5)))
        .await?;

    // Idle longer than the proxy timeout. The server's keepalive
    // Pings (500 ms) should keep traffic flowing through the proxy
    // and prevent it from closing.
    tokio::time::sleep(idle_timeout + Duration::from_secs(1)).await;

    // Now provoke server-side activity. With keepalive working, the
    // connection should still be alive and this should succeed.
    let (head, parents, blob) = random_commit();
    subduction.add_commit(sed_id, head, parents, blob).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let snapshot_after = capture.snapshot();
    let new_sender_stopped = snapshot_after
        .lines()
        .filter(|l| l.contains("sender task stopped"))
        .count()
        - pre_sender_stopped;
    let new_listener_disc = snapshot_after
        .lines()
        .filter(|l| l.contains("WebSocket listener disconnected"))
        .count()
        - pre_listener_disc;

    eprintln!(
        "keepalive_prevents test: {new_sender_stopped} 'sender task stopped', \
         {new_listener_disc} 'listener disconnected'"
    );

    assert_eq!(
        new_sender_stopped, 0,
        "keepalive should have prevented 'sender task stopped' but logged {new_sender_stopped}"
    );
    assert_eq!(
        new_listener_disc, 0,
        "keepalive should have prevented listener disconnects but logged {new_listener_disc}"
    );

    Ok(())
}

/// Send a single message that mentions zero-length payload — sanity
/// check for protocol oddities. Not actually expected to fail; helps
/// distinguish "are disconnects being reported correctly" from
/// "is the test framework's capture working".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_single_request_no_disconnect() -> TestResult {
    let capture = init_capture();
    let pre_dc_count = capture.disconnect_lines().len();

    let (_server, server_sub, server_peer_id, bound) = fresh_server().await?;
    let client = connect_client(99, server_peer_id, bound).await?;

    let sed_id = SedimentreeId::new([5u8; 32]);
    let (head, parents, blob) = random_commit();
    client.add_commit(sed_id, head, parents, blob).await?;
    client
        .sync_with_all_peers(sed_id, false, Some(Duration::from_secs(5)))
        .await?;

    assert_eq!(server_sub.connected_peer_ids().await.len(), 1);

    let new_dc_lines = capture.disconnect_lines();
    let new_disconnects: Vec<&String> = new_dc_lines.iter().skip(pre_dc_count).collect();
    assert!(
        new_disconnects.is_empty(),
        "baseline test logged disconnects: {new_disconnects:#?}"
    );

    Ok(())
}
