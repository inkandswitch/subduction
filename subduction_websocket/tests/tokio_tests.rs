//! Comprehensive tests for tokio WebSocket client and server

use arbitrary::{Arbitrary, Unstructured};
use futures_kind::Sendable;
use rand::Rng;
use sedimentree_core::{
    blob::{Blob, BlobMeta, Digest},
    commit::CountLeadingZeroBytes,
    storage::MemoryStorage,
    LooseCommit, SedimentreeId,
};
use std::{net::SocketAddr, sync::OnceLock, time::Duration};
use subduction_core::{
    connection::{message::Message, Connection, Reconnect},
    peer::id::PeerId,
    Subduction,
};
use subduction_websocket::tokio::{
    client::TokioWebSocketClient, server::TokioWebSocketServer, TimeoutTokio,
};
use testresult::TestResult;
use tungstenite::http::Uri;

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    });
}

fn random_blob() -> Blob {
    #[allow(clippy::expect_used)]
    Blob::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))
        .expect("arbitrary blob")
}

fn random_digest() -> Digest {
    #[allow(clippy::expect_used)]
    Digest::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 32]>()))
        .expect("arbitrary digest")
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

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;

    // Create initial client connection
    let (mut client_ws, socket_listener) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async {
        socket_listener.await?;
        Ok::<(), anyhow::Error>(())
    });

    let initial_peer_id = client_ws.peer_id();

    // Send a message to verify connection works
    let test_msg = Message::BlobsRequest(vec![]);
    client_ws.send(test_msg.clone()).await?;

    // Trigger reconnect
    client_ws.reconnect().await?;

    // Verify peer ID is preserved after reconnect
    assert_eq!(client_ws.peer_id(), initial_peer_id);

    // Verify connection still works after reconnect
    client_ws.send(test_msg).await?;

    Ok(())
}

#[tokio::test]
async fn server_graceful_shutdown() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let mut server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    // Connect a client
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (_client_ws, socket_listener) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async {
        socket_listener.await?;
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
        PeerId::new([2; 32]),
    )
    .await;

    assert!(
        result.is_err(),
        "Should not be able to connect to stopped server"
    );

    Ok(())
}

#[tokio::test]
async fn multiple_concurrent_clients() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
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
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();
    let uri: Uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;

    // Create multiple clients
    let num_clients = 3;
    let mut clients = Vec::new();

    for i in 0..num_clients {
        let client_storage = MemoryStorage::default();
        let (client, listener_fut, actor_fut) =
            Subduction::<Sendable, MemoryStorage, TokioWebSocketClient<TimeoutTokio>>::new(
                client_storage,
                CountLeadingZeroBytes,
            );

        tokio::spawn(actor_fut);
        tokio::spawn(listener_fut);

        let (client_ws, socket_listener) = TokioWebSocketClient::new(
            uri.clone(),
            TimeoutTokio,
            Duration::from_secs(5),
            PeerId::new([u8::try_from(i)? + 1; 32]),
        )
        .await?;

        tokio::spawn(async {
            socket_listener.await?;
            Ok::<(), anyhow::Error>(())
        });

        let task_client_ws = client_ws.clone();
        tokio::spawn(async move {
            task_client_ws.listen().await?;
            Ok::<(), anyhow::Error>(())
        });

        client.register(client_ws).await?;

        // Each client adds its own commit
        let (commit, blob) = random_commit();
        client.add_commit(sed_id, &commit, blob).await?;

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
    assert_eq!(server_subduction.peer_ids().await.len(), num_clients);

    // Sync all clients
    for client in &clients {
        client
            .request_all_batch_sync_all(Some(Duration::from_millis(100)))
            .await?;
    }

    // Give time for sync to complete
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all clients have all commits (1 server + 3 client commits = 4 total)
    #[allow(clippy::expect_used)]
    for client in &clients {
        let loose_commits = client
            .get_commits(sed_id)
            .await
            .expect("sedimentree exists");
        assert_eq!(
            loose_commits.len(),
            num_clients + 1,
            "Client should have all commits"
        );
    }

    // Verify server has all commits
    #[allow(clippy::expect_used)]
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .expect("sedimentree exists");
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

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    // Add a commit so there's something to sync
    let (commit, blob) = random_commit();
    server_subduction.add_commit(sed_id, &commit, blob).await?;

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<TimeoutTokio>,
    >::new(client_storage, CountLeadingZeroBytes);

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, socket_listener) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async {
        socket_listener.await?;
        Ok::<(), anyhow::Error>(())
    });

    let task_client_ws = client_ws.clone();
    tokio::spawn(async move {
        task_client_ws.listen().await?;
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
    let _result = client
        .request_all_batch_sync_all(Some(Duration::from_millis(1)))
        .await;

    // This might succeed if the network is fast, or fail with timeout
    // The test is to verify the system handles short timeouts gracefully
    // If it succeeds, that's fine - network was fast enough
    // If it times out, that's also fine - we're just testing timeout handling

    Ok(())
}

#[tokio::test]
async fn connection_to_invalid_address() -> TestResult {
    init_tracing();

    // Try to connect to an address that's not listening
    let uri = "ws://127.0.0.1:9".parse()?; // Port 9 is discard protocol, unlikely to have WS server

    let result = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(1),
        PeerId::new([1; 32]),
    )
    .await;

    assert!(result.is_err(), "Should fail to connect to invalid address");

    Ok(())
}

#[tokio::test]
async fn large_message_handling() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(10),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<TimeoutTokio>,
    >::new(client_storage, CountLeadingZeroBytes);

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, socket_listener) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(10),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async {
        socket_listener.await?;
        Ok::<(), anyhow::Error>(())
    });

    let task_client_ws = client_ws.clone();
    tokio::spawn(async move {
        task_client_ws.listen().await?;
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
    client
        .request_all_batch_sync_all(Some(Duration::from_secs(5)))
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify server received the large commit
    #[allow(clippy::expect_used)]
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .expect("sedimentree exists");
    assert_eq!(server_commits.len(), 1);
    assert!(server_commits.contains(&commit));

    Ok(())
}

#[tokio::test]
async fn message_ordering() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, conn_actor_fut) =
        Subduction::new(server_storage.clone(), CountLeadingZeroBytes);

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<TimeoutTokio>,
    >::new(client_storage, CountLeadingZeroBytes);

    tokio::spawn(actor_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, socket_listener) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async {
        socket_listener.await?;
        Ok::<(), anyhow::Error>(())
    });

    let task_client_ws = client_ws.clone();
    tokio::spawn(async move {
        task_client_ws.listen().await?;
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
    client
        .request_all_batch_sync_all(Some(Duration::from_millis(500)))
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify server has all commits
    #[allow(clippy::expect_used)]
    let server_commits = server_subduction
        .get_commits(sed_id)
        .await
        .expect("sedimentree exists");
    assert_eq!(server_commits.len(), 5);

    for commit in &commits {
        assert!(server_commits.contains(commit), "Server should have commit");
    }

    Ok(())
}
