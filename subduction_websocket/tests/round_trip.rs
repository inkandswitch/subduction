//! Tests for round-trip communication between Subduction peers using `WebSocket`s.

use std::{net::SocketAddr, sync::OnceLock, time::Duration};
use testresult::TestResult;

use arbitrary::{Arbitrary, Unstructured};
use futures_kind::Sendable;
use rand::Rng;
use sedimentree_core::{
    blob::{Blob, BlobMeta, Digest},
    commit::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    storage::MemoryStorage,
};
use subduction_core::{
    connection::{message::Message, Connection},
    peer::id::PeerId,
    sharded_map::ShardedMap,
    Subduction,
};
use subduction_websocket::tokio::{
    client::TokioWebSocketClient, server::TokioWebSocketServer, TimeoutTokio,
};

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    });
}

#[tokio::test]
async fn rend_receive() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let memory_storage = MemoryStorage::default();
    let (suduction, listener_fut, conn_actor_fut) = Subduction::new(
        memory_storage.clone(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
    );

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    let task_subduction = suduction.clone();
    let server_ws = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        task_subduction,
    )
    .await?;

    let bound = server_ws.address();
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, actor) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([1; 32]),
    )
    .await?;

    tokio::spawn(async move {
        actor.await?;
        Ok::<(), anyhow::Error>(())
    });

    let ws = client_ws.clone();
    tokio::spawn(async move {
        ws.listen().await?;
        Ok::<(), anyhow::Error>(())
    });

    let expected = Message::BlobsRequest(Vec::new());
    client_ws.send(&expected).await?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn batch_sync() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    let blob1 = Blob::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 64]>()))?;
    let blob2 = Blob::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 64]>()))?;
    let blob3 = Blob::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 64]>()))?;

    let commit_digest1 =
        Digest::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 32]>()))?;
    let commit1 = LooseCommit::new(commit_digest1, vec![], BlobMeta::new(blob1.as_slice()));

    let commit_digest2 =
        Digest::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 32]>()))?;
    let commit2 = LooseCommit::new(commit_digest2, vec![], BlobMeta::new(blob2.as_slice()));

    let commit_digest3 =
        Digest::arbitrary(&mut Unstructured::new(&rand::thread_rng().random::<[u8; 32]>()))?;
    let commit3 = LooseCommit::new(commit_digest3, vec![], BlobMeta::new(blob3.as_slice()));

    ///////////////////
    // SERVER SETUP //
    ///////////////////

    let server_storage = MemoryStorage::default();
    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, listener_fut, conn_actor_fut) = Subduction::new(
        server_storage.clone(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
    );
    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    tokio::spawn(async move {
        conn_actor_fut.await?;
        Ok::<(), anyhow::Error>(())
    });

    server_subduction
        .add_commit(sed_id, &commit1, blob1)
        .await?;

    let inserted = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(inserted.len(), 1);

    let server = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        PeerId::new([0; 32]),
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    ///////////////////
    // CLIENT SETUP //
    ///////////////////

    // let client_tree = Sedimentree::new(vec![], vec![commit2.clone(), commit3.clone()]);
    let client_storage = MemoryStorage::default();
    let (client, listener_fut, actor_fut) = Subduction::<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<TimeoutTokio>,
    >::new(client_storage, CountLeadingZeroBytes, ShardedMap::with_key(0, 0));

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

    assert_eq!(client.peer_ids().await.len(), 0);
    client.register(client_ws).await?;
    assert_eq!(client.peer_ids().await.len(), 1);

    client.add_commit(sed_id, &commit2, blob2).await?;
    client.add_commit(sed_id, &commit3, blob3).await?;

    assert_eq!(server_subduction.peer_ids().await.len(), 1);

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    ///////////
    // SYNC //
    //////////

    assert_eq!(client.peer_ids().await.len(), 1);
    assert_eq!(server_subduction.peer_ids().await.len(), 1);

    client
        .request_all_batch_sync_all(Some(Duration::from_millis(100)))
        .await?;

    let server_updated = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;

    assert_eq!(server_updated.len(), 3);
    assert!(server_updated.contains(&commit1));
    assert!(server_updated.contains(&commit2));
    assert!(server_updated.contains(&commit3));

    let client_updated = client
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;

    assert_eq!(client_updated.len(), 3);
    assert!(client_updated.contains(&commit1));
    assert!(client_updated.contains(&commit2));
    assert!(client_updated.contains(&commit3));

    Ok(())
}
