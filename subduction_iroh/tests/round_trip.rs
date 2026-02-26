//! Integration tests for the Iroh (QUIC) transport.
//!
//! Exercises the full flow: iroh endpoint setup, QUIC connection, Ed25519
//! handshake, and data sync via Subduction's protocol over a QUIC stream.

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
    sync::{Arc, OnceLock},
    time::Duration,
};

use future_form::Sendable;
use rand::RngCore;
use sedimentree_core::{blob::Blob, commit::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    connection::{handshake::Audience, nonce_cache::NonceCache, test_utils::TokioSpawn},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_iroh::connection::IrohConnection;
use subduction_websocket::timeout::FuturesTimerTimeout;
use testresult::TestResult;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        IrohConnection<FuturesTimerTimeout>,
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

fn random_blob(size: usize) -> Blob {
    let mut bytes = vec![0u8; size];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    Blob::new(bytes)
}

// ─── Test Helpers ────────────────────────────────────────────────────────────

struct TestServer {
    subduction: TestSubduction,
    endpoint: iroh::Endpoint,
    peer_id: PeerId,
}

impl TestServer {
    async fn start(seed: u8) -> Self {
        let sig = signer(seed);
        let peer_id = PeerId::from(sig.verifying_key());

        let (subduction, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
            None,
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

        let endpoint = iroh::Endpoint::builder()
            .alpns(vec![subduction_iroh::ALPN.to_vec()])
            .bind()
            .await
            .expect("bind iroh server endpoint");

        // Spawn accept loop
        let accept_subduction = subduction.clone();
        let accept_signer = sig;
        let accept_ep = endpoint.clone();
        let nonce_cache = NonceCache::default();
        tokio::spawn(async move {
            loop {
                match subduction_iroh::server::accept_one(
                    &accept_ep,
                    REQUEST_TIMEOUT,
                    FuturesTimerTimeout,
                    &accept_signer,
                    &nonce_cache,
                    peer_id,
                    None,
                    HANDSHAKE_MAX_DRIFT,
                )
                .await
                {
                    Ok(result) => {
                        tokio::spawn(result.listener_task);
                        tokio::spawn(result.sender_task);
                        if let Err(e) = accept_subduction.register(result.authenticated).await {
                            tracing::error!("register error: {e}");
                        }
                    }
                    Err(e) => {
                        tracing::warn!("accept error (may be shutdown): {e}");
                        break;
                    }
                }
            }
        });

        Self {
            subduction,
            endpoint,
            peer_id,
        }
    }
}

struct TestClient {
    subduction: TestSubduction,
    peer_id: PeerId,
}

impl TestClient {
    async fn connect(seed: u8, server: &TestServer) -> Self {
        let sig = signer(seed);
        let peer_id = PeerId::from(sig.verifying_key());

        let (subduction, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
            None,
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

        let client_ep = iroh::Endpoint::builder()
            .bind()
            .await
            .expect("bind iroh client endpoint");

        let server_addr = server.endpoint.addr();

        let result = subduction_iroh::client::connect(
            &client_ep,
            server_addr,
            REQUEST_TIMEOUT,
            FuturesTimerTimeout,
            &sig,
            Audience::known(server.peer_id),
        )
        .await
        .expect("client connect");

        tokio::spawn(result.listener_task);
        tokio::spawn(result.sender_task);
        subduction
            .register(result.authenticated)
            .await
            .expect("register");

        Self {
            subduction,
            peer_id,
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn handshake_connects_peers() -> TestResult {
    init_tracing();

    let server = TestServer::start(0).await;
    let client = TestClient::connect(1, &server).await;

    // Allow connection to settle
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_peers = server.subduction.connected_peer_ids().await;
    let client_peers = client.subduction.connected_peer_ids().await;

    assert!(
        server_peers.contains(&client.peer_id),
        "server should see client; server_peers = {server_peers:?}"
    );
    assert!(
        client_peers.contains(&server.peer_id),
        "client should see server; client_peers = {client_peers:?}"
    );

    Ok(())
}

#[tokio::test]
async fn client_to_server_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(10).await;
    let client = TestClient::connect(11, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([42u8; 32]);
    client
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;

    let (had_success, _stats, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "io errors: {io_errs:?}");
    assert!(had_success, "sync should succeed");

    // Allow propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server.subduction.get_commits(sed_id).await;
    assert!(
        server_commits.is_some(),
        "server should have the commit after sync"
    );

    Ok(())
}

#[tokio::test]
async fn bidirectional_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(20).await;
    let client = TestClient::connect(21, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client adds data
    let sed_id = SedimentreeId::new([99u8; 32]);
    client
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;

    // Client syncs to server
    let (had_success, _, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "io errors: {io_errs:?}");
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Server adds more data to the same sedimentree
    server
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;

    // Client syncs again (should pull server's new data)
    let _result = client
        .subduction
        .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
        .await?;

    // Check both sides converge
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server.subduction.get_commits(sed_id).await;
    let client_commits = client.subduction.get_commits(sed_id).await;

    let server_set: BTreeSet<_> = server_commits
        .into_iter()
        .flatten()
        .map(|c| c.commit_id())
        .collect();
    let client_set: BTreeSet<_> = client_commits
        .into_iter()
        .flatten()
        .map(|c| c.commit_id())
        .collect();

    assert_eq!(
        server_set, client_set,
        "both sides should have the same commits"
    );
    assert!(
        server_set.len() >= 2,
        "should have at least 2 commits, got {}",
        server_set.len()
    );

    Ok(())
}

#[tokio::test]
async fn multiple_concurrent_clients() -> TestResult {
    init_tracing();

    let server = TestServer::start(30).await;
    let client_a = TestClient::connect(31, &server).await;
    let client_b = TestClient::connect(32, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_peers = server.subduction.connected_peer_ids().await;
    assert!(
        server_peers.contains(&client_a.peer_id),
        "server should see client A"
    );
    assert!(
        server_peers.contains(&client_b.peer_id),
        "server should see client B"
    );

    // Both clients add data and sync
    let sed_id = SedimentreeId::new([77u8; 32]);
    client_a
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;
    client_b
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;

    let (ok_a, _, errs_a, io_a) = client_a.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    let (ok_b, _, errs_b, io_b) = client_b.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;

    assert!(errs_a.is_empty(), "client A call errors: {errs_a:?}");
    assert!(io_a.is_empty(), "client A io errors: {io_a:?}");
    assert!(ok_a);

    assert!(errs_b.is_empty(), "client B call errors: {errs_b:?}");
    assert!(io_b.is_empty(), "client B io errors: {io_b:?}");
    assert!(ok_b);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server.subduction.get_commits(sed_id).await;
    let count = server_commits.map_or(0, |c| c.len());
    assert!(
        count >= 2,
        "server should have at least 2 commits, got {count}"
    );

    Ok(())
}
