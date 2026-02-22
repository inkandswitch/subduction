//! Comprehensive tests for tokio WebSocket client and server

use future_form::Sendable;
use rand::RngCore;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use std::{collections::BTreeSet, net::SocketAddr, sync::OnceLock, time::Duration};
use subduction_core::{
    connection::{
        authenticated::Authenticated, handshake::Audience, message::Message,
        nonce_cache::NonceCache, Connection, Reconnect,
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::tokio::{
    client::TokioWebSocketClient, server::TokioWebSocketServer, TimeoutTokio, TokioSpawn,
};
use testresult::TestResult;
use tungstenite::http::Uri;

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    });
}

/// Maximum clock drift for handshake tests.
const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);

fn test_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn random_blob() -> Blob {
    let mut bytes = [0u8; 64];
    rand::thread_rng().fill_bytes(&mut bytes);
    Blob::new(bytes.to_vec())
}

fn random_commit() -> (BTreeSet<Digest<LooseCommit>>, Blob) {
    let blob = random_blob();
    (BTreeSet::new(), blob)
}

#[tokio::test]
async fn client_reconnect() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;

    let (mut client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let initial_peer_id = client_ws.peer_id();

    // Send a message to verify connection works
    let test_msg = Message::BlobsRequest {
        id: sedimentree_core::id::SedimentreeId::new([0u8; 32]),
        digests: vec![],
    };
    client_ws.send(&test_msg).await?;

    // Trigger reconnect
    client_ws.reconnect().await?;

    // Verify peer ID is preserved after reconnect
    assert_eq!(client_ws.peer_id(), initial_peer_id);

    // Verify connection still works after reconnect
    client_ws.send(&test_msg).await?;

    Ok(())
}

#[tokio::test]
async fn server_graceful_shutdown() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let mut server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    // Connect a client
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (_client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer.clone(),
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    // Stop the server
    server.stop();

    // Give it a moment to shut down
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Attempting to connect should fail
    let result = TokioWebSocketClient::new(
        format!("ws://{}:{}", bound.ip(), bound.port()).parse()?,
        TimeoutTokio,
        Duration::from_secs(1),
        test_signer(2),
        Audience::known(server_peer_id),
    )
    .await;

    assert!(
        result.is_err(),
        "Should not be able to connect to stopped server"
    );

    Ok(())
}

/// Test multiple concurrent clients syncing with a server.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn multiple_concurrent_clients() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    // Add initial commit to server
    let (parents1, blob1) = random_commit();
    server_subduction
        .add_commit(sed_id, parents1, blob1)
        .await?;

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();
    let _uri: Uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;

    // Create multiple clients
    let num_clients = 3;
    let mut clients = Vec::new();

    for i in 0..num_clients {
        let client_signer = test_signer(u8::try_from(i)? + 10);
        let client_storage = MemoryStorage::default();
        let (client, listener_fut, actor_fut) = Subduction::<
            Sendable,
            MemoryStorage,
            TokioWebSocketClient<MemorySigner, TimeoutTokio>,
            OpenPolicy,
            MemorySigner,
        >::new(
            None,
            client_signer.clone(),
            client_storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

        tokio::spawn(actor_fut);
        tokio::spawn(listener_fut);

        let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
        let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
            uri,
            TimeoutTokio,
            Duration::from_secs(5),
            client_signer,
            Audience::known(server_peer_id),
        )
        .await?;

        tokio::spawn(async {
            listener_fut.await?;
            Ok::<(), eyre::Report>(())
        });

        tokio::spawn(async {
            sender_fut.await?;
            Ok::<(), eyre::Report>(())
        });

        client
            .register(Authenticated::new_for_test(client_ws))
            .await?;

        clients.push(client);

        tokio::spawn({
            #[allow(clippy::expect_used)]
            let inner_client = clients.get(i).expect("client should exist").clone();
            async move {
                inner_client.listen().await?;
                Ok::<(), eyre::Report>(())
            }
        });
    }

    // Verify server sees all clients
    assert_eq!(
        server_subduction.connected_peer_ids().await.len(),
        num_clients
    );

    // Expected total: 1 server commit + num_clients client commits
    let expected_commits = num_clients + 1;

    // Phase 1: Each client syncs to get the server's initial commit and subscribe
    for client in &clients {
        client
            .sync_all(sed_id, true, Some(Duration::from_secs(5)))
            .await?;
    }

    // Phase 2: Each client adds its own commit
    for client in &clients {
        let (parents, blob) = random_commit();
        client.add_commit(sed_id, parents, blob).await?;
    }

    // Phase 3: All clients sync again to push their commits to the server
    for client in &clients {
        client
            .sync_all(sed_id, true, Some(Duration::from_secs(5)))
            .await?;
    }

    // Small delay to let server process all incoming commits
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 4: Verify server has all commits
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree should exist on server")?;
    assert_eq!(
        server_commits.len(),
        expected_commits,
        "Server should have all {expected_commits} commits (1 server + {num_clients} clients)"
    );

    // Phase 5: All clients sync again to pull commits from other clients via server
    for client in &clients {
        client
            .sync_all(sed_id, true, Some(Duration::from_secs(5)))
            .await?;
    }

    // Phase 6: Verify all clients have all commits
    for (i, client) in clients.iter().enumerate() {
        let client_commits = client
            .get_commits(sed_id)
            .await
            .ok_or("sedimentree should exist on client")?;
        assert_eq!(
            client_commits.len(),
            expected_commits,
            "Client {i} should have all {expected_commits} commits"
        );
    }

    Ok(())
}

#[tokio::test]
async fn request_with_delayed_response() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    // Add a commit so there's something to sync
    let (parents, blob) = random_commit();
    server_subduction.add_commit(sed_id, parents, blob).await?;

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >::new(
        None,
        client_signer.clone(),
        client_storage,
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    client
        .register(Authenticated::new_for_test(client_ws))
        .await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), eyre::Report>(())
        }
    });

    // Make a sync request with a very short timeout
    let _result = client.full_sync(Some(Duration::from_millis(1))).await;

    // This might succeed if the network is fast, or fail with timeout
    // The test is to verify the system handles short timeouts gracefully
    // If it succeeds, that's fine - network was fast enough
    // If it times out, that's also fine - we're just testing timeout handling

    Ok(())
}

#[tokio::test]
async fn connection_to_invalid_address() -> TestResult {
    init_tracing();

    let client_signer = test_signer(1);
    let fake_server_peer_id = PeerId::from(test_signer(0).verifying_key());

    // Try to connect to an address that's not listening
    let uri = "ws://127.0.0.1:9".parse()?; // Port 9 is discard protocol, unlikely to have WS server

    let result = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(1),
        client_signer,
        Audience::known(fake_server_peer_id),
    )
    .await;

    assert!(result.is_err(), "Should fail to connect to invalid address");

    Ok(())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn large_message_handling() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(10),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >::new(
        None,
        client_signer.clone(),
        client_storage,
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(10),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    client
        .register(Authenticated::new_for_test(client_ws))
        .await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), eyre::Report>(())
        }
    });

    // Create a large blob (1MB)
    let large_data = vec![42u8; 1024 * 1024];
    let large_blob = Blob::new(large_data);

    // Add large commit
    client
        .add_commit(sed_id, BTreeSet::new(), large_blob)
        .await?;

    // Sync with server
    client.full_sync(Some(Duration::from_secs(5))).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify server received the large commit
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(server_commits.len(), 1);

    Ok(())
}

/// Test that commits added in sequence are synced to the server.
///
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn message_ordering() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        server_storage.clone(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >::new(
        None,
        client_signer.clone(),
        client_storage,
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    );

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    client
        .register(Authenticated::new_for_test(client_ws))
        .await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), eyre::Report>(())
        }
    });

    // Add multiple commits in order
    for _ in 0..5 {
        let (parents, blob) = random_commit();
        client.add_commit(sed_id, parents, blob).await?;
    }

    // Sync all commits to server
    client.full_sync(Some(Duration::from_secs(5))).await;

    // Small delay for server to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify server has all 5 commits
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree should exist")?;
    assert_eq!(server_commits.len(), 5, "server should have all 5 commits");

    Ok(())
}

#[tokio::test]
async fn server_try_connect_known_peer() -> TestResult {
    init_tracing();

    let server1_signer = test_signer(0);
    let server2_signer = test_signer(1);
    let server2_peer_id = PeerId::from(server2_signer.verifying_key());

    let addr1: SocketAddr = "127.0.0.1:0".parse()?;
    let server1 = TokioWebSocketServer::setup(
        addr1,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server1_signer,
        None, // No discovery mode
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let addr2: SocketAddr = "127.0.0.1:0".parse()?;
    let server2 = TokioWebSocketServer::setup(
        addr2,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server2_signer,
        None, // No discovery mode
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let server2_addr = server2.address();
    let uri: Uri = format!("ws://{}:{}", server2_addr.ip(), server2_addr.port()).parse()?;

    let connected_peer_id = server1
        .try_connect(uri, TimeoutTokio, Duration::from_secs(5), server2_peer_id)
        .await?;

    assert_eq!(connected_peer_id, server2_peer_id);

    let peers = server1.subduction().connected_peer_ids().await;
    assert!(peers.contains(&server2_peer_id));

    Ok(())
}

#[tokio::test]
async fn server_try_connect_discover() -> TestResult {
    init_tracing();

    let server1_signer = test_signer(0);
    let server2_signer = test_signer(1);
    let server2_peer_id = PeerId::from(server2_signer.verifying_key());

    let service_name = "test.subduction.local";

    let addr1: SocketAddr = "127.0.0.1:0".parse()?;
    let server1 = TokioWebSocketServer::setup(
        addr1,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server1_signer,
        Some(service_name),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let addr2: SocketAddr = "127.0.0.1:0".parse()?;
    let server2 = TokioWebSocketServer::setup(
        addr2,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server2_signer,
        Some(service_name),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let server2_addr = server2.address();
    let uri: Uri = format!("ws://{}:{}", server2_addr.ip(), server2_addr.port()).parse()?;

    let connected_peer_id = server1
        .try_connect_discover(uri, TimeoutTokio, Duration::from_secs(5), service_name)
        .await?;

    assert_eq!(connected_peer_id, server2_peer_id);

    let peers = server1.subduction().connected_peer_ids().await;
    assert!(peers.contains(&server2_peer_id));

    Ok(())
}

#[tokio::test]
async fn server_try_connect_discover_wrong_service_name() -> TestResult {
    init_tracing();

    let server1_signer = test_signer(0);
    let server2_signer = test_signer(1);

    let addr1: SocketAddr = "127.0.0.1:0".parse()?;
    let server1 = TokioWebSocketServer::setup(
        addr1,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server1_signer,
        Some("service-a.local"),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let addr2: SocketAddr = "127.0.0.1:0".parse()?;
    let server2 = TokioWebSocketServer::setup(
        addr2,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        server2_signer,
        Some("service-b.local"),
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await?;

    let server2_addr = server2.address();
    let uri: Uri = format!("ws://{}:{}", server2_addr.ip(), server2_addr.port()).parse()?;

    let result = server1
        .try_connect_discover(uri, TimeoutTokio, Duration::from_secs(5), "service-a.local")
        .await;

    assert!(
        result.is_err(),
        "Should fail to connect with mismatched service name"
    );

    Ok(())
}
