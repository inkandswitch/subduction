//! Integration tests for the HTTP long-poll transport.
//!
//! Exercises the full flow: server setup, client connection, Ed25519 handshake,
//! and data sync via Subduction's protocol over HTTP request-response pairs.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};

use future_form::Sendable;
use sedimentree_core::{blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    connection::{
        handshake::{Audience, DiscoveryId},
        nonce_cache::NonceCache,
        test_utils::TokioSpawn,
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_http_longpoll::{
    client::HttpLongPollClient,
    connection::HttpLongPollConnection,
    http_client::ReqwestHttpClient,
    server::LongPollHandler,
    session::SessionId,
};
use subduction_websocket::timeout::FuturesTimerTimeout;
use testresult::TestResult;
use tokio::net::TcpListener;

const POLL_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);
const SERVICE_NAME: &str = "test-service";

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        HttpLongPollConnection<FuturesTimerTimeout>,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
    >,
>;

fn init_tracing() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .init();
    });
}

fn signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

// ─── Test Server Harness ─────────────────────────────────────────────────────

struct TestServer {
    subduction: TestSubduction,
    address: SocketAddr,
    /// Dropping the sender signals cancellation to the accept loop.
    _cancel: async_channel::Sender<()>,
}

impl TestServer {
    async fn start(seed: u8) -> Self {
        let sig = signer(seed);
        let peer_id = PeerId::from(sig.verifying_key());
        let discovery_id = Some(DiscoveryId::new(SERVICE_NAME.as_bytes()));
        let discovery_audience: Option<Audience> = discovery_id.map(Audience::discover_id);

        let (subduction, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
            discovery_id,
            sig.clone(),
            MemoryStorage::default(),
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::new(),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

        tokio::spawn(listener_fut);
        tokio::spawn(manager_fut);

        let handler = LongPollHandler::new(
            sig,
            Arc::new(NonceCache::default()),
            peer_id,
            discovery_audience,
            HANDSHAKE_MAX_DRIFT,
            REQUEST_TIMEOUT,
            FuturesTimerTimeout,
        )
        .with_poll_timeout(POLL_TIMEOUT);

        let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = tcp.local_addr().expect("local_addr");

        let (cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);
        let accept_subduction = subduction.clone();

        tokio::spawn(async move {
            accept_loop(tcp, accept_subduction, handler, cancel_rx).await;
        });

        Self {
            subduction,
            address,
            _cancel: cancel_tx,
        }
    }
}

async fn accept_loop(
    tcp: TcpListener,
    subduction: TestSubduction,
    handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
    cancel: async_channel::Receiver<()>,
) {
    use tokio::task::JoinSet;

    let mut conns = JoinSet::new();

    loop {
        tokio::select! {
            _ = cancel.recv() => break,
            res = tcp.accept() => {
                match res {
                    Ok((stream, addr)) => {
                        let handler = handler.clone();
                        let subduction = subduction.clone();

                        conns.spawn(async move {
                            serve_http_connection(stream, addr, handler, subduction).await;
                        });
                    }
                    Err(e) => {
                        tracing::error!("accept error: {e}");
                    }
                }
            }
        }
    }

    while conns.join_next().await.is_some() {}
}

async fn serve_http_connection(
    tcp: tokio::net::TcpStream,
    addr: SocketAddr,
    handler: LongPollHandler<MemorySigner, FuturesTimerTimeout>,
    subduction: TestSubduction,
) {
    use hyper_util::rt::TokioIo;

    let io = TokioIo::new(tcp);

    let service = hyper::service::service_fn(move |req| {
        let handler = handler.clone();
        let subduction = subduction.clone();
        async move {
            let resp = handler.handle(req).await?;

            // After a successful handshake, register with Subduction
            if resp.status() == hyper::StatusCode::OK
                && let Some(session_hdr) = resp
                    .headers()
                    .get(subduction_http_longpoll::SESSION_ID_HEADER)
                && let Ok(sid_str) = session_hdr.to_str()
                && let Some(sid) = SessionId::from_hex(sid_str)
                && let Some(auth) = handler.take_authenticated(&sid).await
                && let Err(e) = subduction.register(auth).await
            {
                tracing::error!("failed to register HTTP long-poll connection: {e}");
            }

            Ok::<_, hyper::Error>(resp)
        }
    });

    let builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    let conn = builder.serve_connection(io, service);

    if let Err(e) = conn.await {
        tracing::debug!("HTTP connection from {addr} ended: {e}");
    }
}

// ─── Client Helper ───────────────────────────────────────────────────────────

async fn connected_client(seed: u8, server_addr: SocketAddr) -> TestSubduction {
    let client_signer = signer(seed);

    let (client, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
        None,
        client_signer.clone(),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::new(),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(listener_fut);
    tokio::spawn(manager_fut);

    let base_url = format!("http://{server_addr}");
    let lp_client = HttpLongPollClient::new(
        &base_url,
        ReqwestHttpClient::new(),
        FuturesTimerTimeout,
        TokioSpawn,
        REQUEST_TIMEOUT,
    );

    // Use a future that never resolves — the test controls lifetime via drop.
    let cancel_fut = futures::future::pending::<()>();

    let (auth, _session_id) = lp_client
        .connect_discover(&client_signer, SERVICE_NAME, cancel_fut)
        .await
        .expect("client connect");

    client.register(auth).await.expect("register");
    client
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn handshake_connects_peers() -> TestResult {
    init_tracing();

    let server = TestServer::start(0).await;
    let client = connected_client(1, server.address).await;

    // Give the connection manager time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server_peers = server.subduction.connected_peer_ids().await;
    let client_peers = client.connected_peer_ids().await;

    let server_peer_id = server.subduction.peer_id();
    let client_peer_id = client.peer_id();

    assert!(
        server_peers.contains(&client_peer_id),
        "server should see client as connected peer. server sees: {server_peers:?}, expected client {client_peer_id}"
    );
    assert!(
        client_peers.contains(&server_peer_id),
        "client should see server as connected peer. client sees: {client_peers:?}, expected server {server_peer_id}"
    );

    Ok(())
}

#[tokio::test]
async fn client_to_server_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(10).await;
    let client = connected_client(11, server.address).await;

    // Give connection time to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    let sed_id = SedimentreeId::new([1u8; 32]);
    let blob = Blob::new(b"hello from client".to_vec());
    client
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await
        .expect("add commit");

    let (had_success, _stats, call_errs, io_errs) = client.full_sync(Some(REQUEST_TIMEOUT)).await;

    assert!(call_errs.is_empty(), "full_sync call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "full_sync IO errors: {io_errs:?}");
    assert!(
        had_success,
        "full_sync should have had at least one success"
    );

    // Give server time to process incoming data
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server.subduction.get_commits(sed_id).await;
    assert!(
        server_commits.is_some(),
        "server should have commits for sed_id"
    );
    let commits = server_commits.unwrap();
    assert!(
        !commits.is_empty(),
        "server should have at least one commit"
    );

    Ok(())
}

#[tokio::test]
async fn server_to_client_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(20).await;
    let client = connected_client(21, server.address).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let sed_id = SedimentreeId::new([2u8; 32]);
    let blob = Blob::new(b"hello from server".to_vec());
    server
        .subduction
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await
        .expect("add commit");

    // Use sync_all (not full_sync) because the client doesn't know about
    // this sedimentree yet — full_sync only iterates locally known IDs.
    let result = client
        .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
        .await
        .expect("sync_all");

    let had_success = result.values().any(|(success, _, _)| *success);
    let call_errs: Vec<_> = result
        .values()
        .flat_map(|(_, _, errs)| errs.iter())
        .collect();

    assert!(call_errs.is_empty(), "sync_all call errors: {call_errs:?}");
    assert!(had_success, "sync_all should have had at least one success");

    // Give time for blob resolution
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client_commits = client.get_commits(sed_id).await;
    assert!(
        client_commits.is_some(),
        "client should have commits for sed_id"
    );
    let commits = client_commits.unwrap();
    assert!(
        !commits.is_empty(),
        "client should have at least one commit"
    );

    Ok(())
}

#[tokio::test]
async fn bidirectional_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(30).await;
    let client = connected_client(31, server.address).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    let sed_id = SedimentreeId::new([3u8; 32]);

    // Server adds commits
    for i in 0..3u8 {
        let mut data = b"server-commit-".to_vec();
        data.push(i);
        let blob = Blob::new(data);
        server
            .subduction
            .add_commit(sed_id, BTreeSet::new(), blob)
            .await
            .expect("server add commit");
    }

    // Client adds commits
    for i in 0..3u8 {
        let mut data = b"client-commit-".to_vec();
        data.push(i);
        let blob = Blob::new(data);
        client
            .add_commit(sed_id, BTreeSet::new(), blob)
            .await
            .expect("client add commit");
    }

    // Client syncs
    let (had_success, _stats, call_errs, io_errs) = client.full_sync(Some(REQUEST_TIMEOUT)).await;

    assert!(call_errs.is_empty(), "full_sync call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "full_sync IO errors: {io_errs:?}");
    assert!(
        had_success,
        "full_sync should have had at least one success"
    );

    // Give time for blob resolution
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server
        .subduction
        .get_commits(sed_id)
        .await
        .expect("server should have commits");
    let client_commits = client
        .get_commits(sed_id)
        .await
        .expect("client should have commits");

    // Both should have at least 6 commits (3 from each side)
    assert!(
        server_commits.len() >= 6,
        "server should have >= 6 commits, got {}",
        server_commits.len()
    );
    assert!(
        client_commits.len() >= 6,
        "client should have >= 6 commits, got {}",
        client_commits.len()
    );

    Ok(())
}
