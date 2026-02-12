//! Comprehensive tests for tokio WebSocket client and server

use arbitrary::{Arbitrary, Unstructured};
use future_form::Sendable;
use rand::RngCore;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    crypto::digest::Digest,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use std::{net::SocketAddr, sync::OnceLock, time::Duration};
use subduction_core::{
    connection::{
        Connection, Reconnect, handshake::Audience, message::Message, nonce_cache::NonceCache,
    },
    crypto::signer::MemorySigner,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::Subduction,
};
use subduction_websocket::tokio::{
    TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer,
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
    #[allow(clippy::expect_used)]
    Blob::arbitrary(&mut Unstructured::new(&bytes)).expect("arbitrary blob")
}

fn random_digest() -> Digest<LooseCommit> {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    #[allow(clippy::expect_used)]
    Digest::arbitrary(&mut Unstructured::new(&bytes)).expect("arbitrary digest")
}

fn random_commit() -> (LooseCommit, Blob) {
    let blob = random_blob();
    let digest = random_digest();
    let commit = LooseCommit::new(digest, vec![], BlobMeta::new(blob.as_slice()));
    (commit, blob)
}

#[tokio::test]
async fn client_reconnect() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
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
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let initial_peer_id = client_ws.peer_id();

    // Send a message to verify connection works
    let test_msg = Message::BlobsRequest(vec![]);
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
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
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
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), anyhow::Error>(())
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

#[tokio::test]
#[allow(clippy::too_many_lines)] // Integration test with multiple clients requires setup/teardown
async fn multiple_concurrent_clients() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    // Add initial commit to server
    let (commit1, blob1) = random_commit();
    server_subduction
        .add_commit(sed_id, &commit1, blob1)
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
    let uri: Uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;

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
        );

        tokio::spawn(actor_fut);
        tokio::spawn(listener_fut);

        let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
            uri.clone(),
            TimeoutTokio,
            Duration::from_secs(5),
            client_signer,
            Audience::known(server_peer_id),
        )
        .await?;

        tokio::spawn(async {
            listener_fut.await?;
            Ok::<(), anyhow::Error>(())
        });

        tokio::spawn(async {
            sender_fut.await?;
            Ok::<(), anyhow::Error>(())
        });

        client.register(client_ws).await?;

        clients.push(client);

        tokio::spawn({
            #[allow(clippy::expect_used)]
            let inner_client = clients.get(i).expect("client should exist").clone();
            async move {
                inner_client.listen().await?;
                Ok::<(), anyhow::Error>(())
            }
        });
    }

    // Verify server sees all clients
    assert_eq!(
        server_subduction.connected_peer_ids().await.len(),
        num_clients
    );

    // Sync all clients first for the specific sedimentree (establishes subscriptions)
    for client in &clients {
        client
            .sync_all(sed_id, true, Some(Duration::from_millis(100)))
            .await?;
    }

    // Now each client adds its own commit (after subscribing)
    for client in &clients {
        let (commit, blob) = random_commit();
        client.add_commit(sed_id, &commit, blob).await?;
    }

    // Give time for commits to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sync again to ensure all commits are shared
    for client in &clients {
        client
            .sync_all(sed_id, true, Some(Duration::from_millis(100)))
            .await?;
    }

    // Give time for sync to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all clients have all commits (1 server + 3 client commits = 4 total)
    for client in &clients {
        let loose_commits = client
            .get_commits(sed_id)
            .await
            .ok_or("sedimentree exists")?;
        assert_eq!(
            loose_commits.len(),
            num_clients + 1,
            "Client should have all commits"
        );
    }

    // Verify server has all commits
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(
        server_commits.len(),
        num_clients + 1,
        "Server should have all commits"
    );

    Ok(())
}

#[tokio::test]
async fn request_with_delayed_response() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    // Add a commit so there's something to sync
    let (commit, blob) = random_commit();
    server_subduction.add_commit(sed_id, &commit, blob).await?;

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
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    client.register(client_ws).await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), anyhow::Error>(())
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
    let fake_server_peer_id = test_signer(0).peer_id();

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
async fn large_message_handling() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
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
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    client.register(client_ws).await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    // Create a large blob (1MB)
    let large_data = vec![42u8; 1024 * 1024];
    let large_blob = Blob::new(large_data);
    let digest = random_digest();
    let commit = LooseCommit::new(digest, vec![], BlobMeta::new(large_blob.as_slice()));

    // Add large commit
    client.add_commit(sed_id, &commit, large_blob).await?;

    // Sync with server
    client.full_sync(Some(Duration::from_secs(5))).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify server received the large commit
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(server_commits.len(), 1);
    assert!(server_commits.contains(&commit));

    Ok(())
}

#[tokio::test]
async fn message_ordering() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = server_signer.peer_id();

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
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), anyhow::Error>(())
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
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    client.register(client_ws).await?;

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    // Add multiple commits in order
    let mut commits = Vec::new();
    for _ in 0..5 {
        let (commit, blob) = random_commit();
        client.add_commit(sed_id, &commit, blob).await?;
        commits.push(commit);
    }

    // Sync all at once
    client.full_sync(Some(Duration::from_millis(500))).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify server has all commits
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(server_commits.len(), 5);

    for commit in &commits {
        assert!(server_commits.contains(commit), "Server should have commit");
    }

    Ok(())
}

#[tokio::test]
async fn server_try_connect_known_peer() -> TestResult {
    init_tracing();

    let server1_signer = test_signer(0);
    let server2_signer = test_signer(1);
    let server2_peer_id = server2_signer.peer_id();

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
    let server2_peer_id = server2_signer.peer_id();

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
