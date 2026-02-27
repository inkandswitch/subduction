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

/// Like `TestServer` but with discovery mode enabled.
struct TestServerDiscover {
    subduction: TestSubduction,
    endpoint: iroh::Endpoint,
    peer_id: PeerId,
}

impl TestServerDiscover {
    async fn start(seed: u8, service_name: &str) -> Self {
        let sig = signer(seed);
        let peer_id = PeerId::from(sig.verifying_key());

        let discovery_id = DiscoveryId::new(service_name.as_bytes());
        let discovery_audience = Some(Audience::discover_id(discovery_id));

        let (subduction, listener_fut, manager_fut): (TestSubduction, _, _) = Subduction::new(
            Some(discovery_id),
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
                    discovery_audience,
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

impl TestClient {
    /// Connect to a discovery-mode server using `Audience::discover`.
    async fn connect_discover(seed: u8, server: &TestServerDiscover, service_name: &str) -> Self {
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
            Audience::discover(service_name.as_bytes()),
        )
        .await
        .expect("client connect (discover)");

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

// ─── Server-to-Client Sync ──────────────────────────────────────────────────

/// Server adds a commit, client pulls it via `sync_all`.
#[tokio::test]
async fn server_to_client_sync() -> TestResult {
    init_tracing();

    let server = TestServer::start(40).await;
    let client = TestClient::connect(41, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([2u8; 32]);
    let blob = Blob::new(b"hello from server".to_vec());
    server
        .subduction
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await?;

    // sync_all (not full_sync) because the client doesn't know this tree yet
    let result = client
        .subduction
        .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
        .await?;

    let had_success = result.values().any(|(success, _, _)| *success);
    let call_errs: Vec<_> = result
        .values()
        .flat_map(|(_, _, errs)| errs.iter())
        .collect();

    assert!(call_errs.is_empty(), "sync_all call errors: {call_errs:?}");
    assert!(had_success, "sync_all should succeed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client_commits = client.subduction.get_commits(sed_id).await;
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

// ─── Large Message Handling ─────────────────────────────────────────────────

/// 1 MB blob traverses the QUIC stream and arrives intact.
#[tokio::test]
async fn large_message_handling() -> TestResult {
    init_tracing();

    let server = TestServer::start(50).await;
    let client = TestClient::connect(51, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([60u8; 32]);

    let mut large_data = vec![0u8; 1_000_000];
    rand::rngs::OsRng.fill_bytes(&mut large_data);
    let blob = Blob::new(large_data);

    client
        .subduction
        .add_commit(sed_id, BTreeSet::new(), blob)
        .await?;

    let (had_success, _stats, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "IO errors: {io_errs:?}");
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server
        .subduction
        .get_commits(sed_id)
        .await
        .expect("server should have commits");
    assert!(!server_commits.is_empty());

    // Verify blob data is intact on server
    let server_blobs = server
        .subduction
        .get_blobs(sed_id)
        .await?
        .expect("server should have blobs");
    assert!(
        server_blobs.iter().any(|b| b.as_slice().len() == 1_000_000),
        "server should have the 1 MB blob"
    );

    Ok(())
}

// ─── Message Ordering ───────────────────────────────────────────────────────

/// Five sequential commits all arrive at the server.
#[tokio::test]
async fn message_ordering() -> TestResult {
    init_tracing();

    let server = TestServer::start(60).await;
    let client = TestClient::connect(61, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([70u8; 32]);

    for i in 0..5u8 {
        let mut data = b"ordered-commit-".to_vec();
        data.push(i);
        client
            .subduction
            .add_commit(sed_id, BTreeSet::new(), Blob::new(data))
            .await?;
    }

    let (had_success, _stats, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "IO errors: {io_errs:?}");
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server
        .subduction
        .get_commits(sed_id)
        .await
        .expect("server should have commits");
    assert_eq!(server_commits.len(), 5, "server should have all 5 commits");

    Ok(())
}

// ─── Disconnect and Reconnect ───────────────────────────────────────────────

/// Client disconnects, server adds data while disconnected, new client
/// reconnects and pulls all data.
#[tokio::test]
async fn disconnect_and_reconnect() -> TestResult {
    init_tracing();

    let server = TestServer::start(70).await;

    // First client connects
    let client1 = TestClient::connect(71, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([80u8; 32]);
    client1
        .subduction
        .add_commit(
            sed_id,
            BTreeSet::new(),
            Blob::new(b"before-disconnect".to_vec()),
        )
        .await?;

    let (had_success, _, call_errs, io_errs) =
        client1.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty());
    assert!(io_errs.is_empty());
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Disconnect
    client1.subduction.disconnect_all().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Server adds a commit while client is disconnected
    server
        .subduction
        .add_commit(
            sed_id,
            BTreeSet::new(),
            Blob::new(b"while-disconnected".to_vec()),
        )
        .await?;

    // Second client (same signer seed => same PeerId) reconnects
    let client2 = TestClient::connect(71, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Pull all data
    let result = client2
        .subduction
        .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
        .await?;

    let had_success = result.values().any(|(success, _, _)| *success);
    assert!(had_success, "sync should succeed after reconnect");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client_commits = client2
        .subduction
        .get_commits(sed_id)
        .await
        .expect("client should have commits after reconnect");
    assert!(
        client_commits.len() >= 2,
        "client should have >= 2 commits (original + server's), got {}",
        client_commits.len()
    );

    Ok(())
}

// ─── Discovery Mode Handshake ───────────────────────────────────────────────

/// Client connects using `Audience::discover` instead of `Audience::known`.
#[tokio::test]
async fn discovery_mode_handshake() -> TestResult {
    init_tracing();

    let service_name = "test.iroh.discover";
    let server = TestServerDiscover::start(80, service_name).await;
    let client = TestClient::connect_discover(81, &server, service_name).await;

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

    // Verify data flows over the discovery-mode connection
    let sed_id = SedimentreeId::new([88u8; 32]);
    client
        .subduction
        .add_commit(
            sed_id,
            BTreeSet::new(),
            Blob::new(b"discover-mode-data".to_vec()),
        )
        .await?;

    let (had_success, _stats, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "IO errors: {io_errs:?}");
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server.subduction.get_commits(sed_id).await;
    assert!(
        server_commits.is_some(),
        "server should have the commit via discovery-mode connection"
    );

    Ok(())
}

// ─── Discovery Mode – Wrong Service Name Rejected ───────────────────────────

/// Client uses a different service name than the server. Handshake should fail.
#[tokio::test]
async fn discovery_wrong_service_name_rejected() -> TestResult {
    init_tracing();

    let server = TestServerDiscover::start(90, "service-alpha").await;

    let sig = signer(91);

    let client_ep = iroh::Endpoint::builder()
        .bind()
        .await
        .expect("bind client endpoint");

    let server_addr = server.endpoint.addr();

    let result = subduction_iroh::client::connect(
        &client_ep,
        server_addr,
        REQUEST_TIMEOUT,
        FuturesTimerTimeout,
        &sig,
        Audience::discover(b"service-beta"), // wrong service name
    )
    .await;

    assert!(
        result.is_err(),
        "connection with wrong service name should fail"
    );

    Ok(())
}

// ─── Multiple Clients – Full Convergence ────────────────────────────────────

/// Three clients sync commits through a server and all converge to the same
/// commit set.
#[tokio::test]
async fn multiple_concurrent_clients_full_convergence() -> TestResult {
    init_tracing();

    let server = TestServer::start(100).await;
    let sed_id = SedimentreeId::new([50u8; 32]);

    // Server adds an initial commit
    server
        .subduction
        .add_commit(sed_id, BTreeSet::new(), random_blob(64))
        .await?;

    let num_clients = 3;
    let mut clients = Vec::new();
    for i in 0..num_clients {
        let seed: u8 = 101 + u8::try_from(i).expect("< 256 clients");
        let client = TestClient::connect(seed, &server).await;
        clients.push(client);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify server sees all clients
    assert_eq!(
        server.subduction.connected_peer_ids().await.len(),
        num_clients,
        "server should see all {num_clients} clients"
    );

    // Phase 1: Each client syncs to learn about the server's initial commit
    for client in &clients {
        client
            .subduction
            .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
            .await?;
    }

    // Phase 2: Each client adds its own commit
    for (i, client) in clients.iter().enumerate() {
        let mut data = b"client-commit-".to_vec();
        data.push(u8::try_from(i).expect("< 256 clients"));
        client
            .subduction
            .add_commit(sed_id, BTreeSet::new(), Blob::new(data))
            .await?;
    }

    // Phase 3: All clients sync their commits to the server
    for client in &clients {
        client
            .subduction
            .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
            .await?;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Phase 4: Verify server has all commits (1 server + 3 clients)
    let expected = num_clients + 1;
    let server_commits = server
        .subduction
        .get_commits(sed_id)
        .await
        .expect("server should have commits");
    assert_eq!(
        server_commits.len(),
        expected,
        "server should have all {expected} commits"
    );

    // Phase 5: All clients sync again to pull other clients' commits via server
    for client in &clients {
        client
            .subduction
            .sync_all(sed_id, true, Some(REQUEST_TIMEOUT))
            .await?;
    }

    // Phase 6: Verify all clients converged
    for (i, client) in clients.iter().enumerate() {
        let commits = client
            .subduction
            .get_commits(sed_id)
            .await
            .expect("client should have commits");
        assert_eq!(
            commits.len(),
            expected,
            "client {i} should have all {expected} commits, got {}",
            commits.len()
        );
    }

    Ok(())
}

// ─── Bidirectional Sync – Multiple Commits ──────────────────────────────────

/// Server adds 3 commits, client adds 3 commits, both converge to >= 6.
#[tokio::test]
async fn bidirectional_sync_multiple_commits() -> TestResult {
    init_tracing();

    let server = TestServer::start(110).await;
    let client = TestClient::connect(111, &server).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sed_id = SedimentreeId::new([3u8; 32]);

    // Server adds 3 commits
    for i in 0..3u8 {
        let mut data = b"server-commit-".to_vec();
        data.push(i);
        server
            .subduction
            .add_commit(sed_id, BTreeSet::new(), Blob::new(data))
            .await?;
    }

    // Client adds 3 commits
    for i in 0..3u8 {
        let mut data = b"client-commit-".to_vec();
        data.push(i);
        client
            .subduction
            .add_commit(sed_id, BTreeSet::new(), Blob::new(data))
            .await?;
    }

    // Client syncs (pushes its commits, pulls server's commits)
    let (had_success, _stats, call_errs, io_errs) =
        client.subduction.full_sync(Some(REQUEST_TIMEOUT)).await;
    assert!(call_errs.is_empty(), "full_sync call errors: {call_errs:?}");
    assert!(io_errs.is_empty(), "full_sync IO errors: {io_errs:?}");
    assert!(had_success);

    tokio::time::sleep(Duration::from_millis(500)).await;

    let server_commits = server
        .subduction
        .get_commits(sed_id)
        .await
        .expect("server should have commits");
    let client_commits = client
        .subduction
        .get_commits(sed_id)
        .await
        .expect("client should have commits");

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

// ─── Endpoint Shutdown Stops Accept ─────────────────────────────────────────

/// Closing the server endpoint prevents new connections.
#[tokio::test]
async fn endpoint_shutdown_stops_accept() -> TestResult {
    init_tracing();

    let sig = signer(120);
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
        .expect("bind");

    let server_addr = endpoint.addr();

    // Spawn accept loop
    let accept_subduction = subduction.clone();
    let accept_signer = sig;
    let accept_ep = endpoint.clone();
    let nonce_cache = NonceCache::default();
    tokio::spawn(async move {
        while let Ok(result) = subduction_iroh::server::accept_one(
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
            tokio::spawn(result.listener_task);
            tokio::spawn(result.sender_task);
            accept_subduction.register(result.authenticated).await.ok();
        }
    });

    // Shut down the endpoint
    endpoint.close().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Try to connect — should fail
    let client_sig = signer(121);
    let client_ep = iroh::Endpoint::builder()
        .bind()
        .await
        .expect("bind client endpoint");

    let result = subduction_iroh::client::connect(
        &client_ep,
        server_addr,
        Duration::from_secs(2),
        FuturesTimerTimeout,
        &client_sig,
        Audience::known(peer_id),
    )
    .await;

    assert!(
        result.is_err(),
        "should not be able to connect to a shut-down endpoint"
    );

    Ok(())
}
