use async_tungstenite::tokio::accept_async;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, sync::OnceLock, time::Duration};
use testresult::TestResult;

use arbitrary::{Arbitrary, Unstructured};
use rand::Rng;
use sedimentree_core::{
    future::Sendable,
    storage::{MemoryStorage, Storage},
    Blob, BlobMeta, Digest, LooseCommit, Sedimentree,
};
use subduction_core::{
    connection::{message::Message, Connection},
    peer::id::PeerId,
    Subduction,
};
use subduction_websocket::tokio::{client::TokioWebSocketClient, server::TokioWebSocketServer};
use tokio::{net::TcpListener, sync::oneshot};

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
    let listener = TcpListener::bind(addr).await?;
    let bound: SocketAddr = listener.local_addr()?;
    let (tx, rx) = oneshot::channel();

    tokio::spawn({
        async move {
            let (tcp, _peer) = listener.accept().await?;
            let ws_stream = accept_async(tcp).await?;

            let server_ws = TokioWebSocketServer::new(
                bound,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
                ws_stream,
            )
            .start();

            let msg = server_ws.recv().await?;
            tracing::info!("Server received: {msg:?}");
            tx.send(msg).unwrap();

            Ok::<(), anyhow::Error>(())
        }
    });

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let client_ws = TokioWebSocketClient::new(uri, Duration::from_secs(5), PeerId::new([1; 32]))
        .await?
        .start();

    let expected = Message::BlobsRequest(Vec::new());
    client_ws.send(expected).await?;
    assert!(rx.await.is_ok());

    Ok(())
}

#[tokio::test]
async fn batch_sync() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = TcpListener::bind(addr).await?;
    let bound: SocketAddr = listener.local_addr()?;

    let blob1 = Blob::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;
    let blob2 = Blob::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;
    let blob3 = Blob::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;

    let commit_digest1 =
        Digest::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 32]>()))?;
    let commit1 = LooseCommit::new(commit_digest1, vec![], BlobMeta::new(blob1.as_slice()));

    let commit_digest2 =
        Digest::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 32]>()))?;
    let commit2 = LooseCommit::new(commit_digest2, vec![], BlobMeta::new(blob2.as_slice()));

    let commit_digest3 =
        Digest::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 32]>()))?;
    let commit3 = LooseCommit::new(commit_digest3, vec![], BlobMeta::new(blob3.as_slice()));

    let server_storage = MemoryStorage::default();
    <MemoryStorage as Storage<Sendable>>::save_loose_commit(&server_storage, commit1.clone())
        .await?;
    <MemoryStorage as Storage<Sendable>>::save_blob(&server_storage, blob1.clone()).await?;

    let server_tree = Sedimentree::new(vec![], vec![commit1.clone()]);
    let sed_id = sedimentree_core::SedimentreeId::new([0u8; 32]);

    let server = Arc::new(
        Subduction::<Sendable, MemoryStorage, TokioWebSocketServer>::new(
            HashMap::from_iter([(sed_id, server_tree)]),
            server_storage,
            HashMap::new(),
        ),
    );

    let (tx, rx) = oneshot::channel();
    tokio::spawn({
        let inner_server = server.clone();
        async move {
            let (tcp, _peer) = listener.accept().await?;
            let ws_stream = accept_async(tcp).await?;

            let server_ws = TokioWebSocketServer::new(
                bound,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
                ws_stream,
            )
            .start();

            inner_server.register(server_ws).await?;
            tx.send(()).unwrap();
            inner_server.run().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    let client_tree = Sedimentree::new(vec![], vec![commit2.clone(), commit3.clone()]);
    let client_sed_id = sedimentree_core::SedimentreeId::new([0u8; 32]);

    let client_storage = MemoryStorage::default();
    <MemoryStorage as Storage<Sendable>>::save_loose_commit(&client_storage, commit2.clone())
        .await?;
    <MemoryStorage as Storage<Sendable>>::save_blob(&client_storage, blob2.clone()).await?;
    <MemoryStorage as Storage<Sendable>>::save_loose_commit(&client_storage, commit3.clone())
        .await?;
    <MemoryStorage as Storage<Sendable>>::save_blob(&client_storage, blob3.clone()).await?;

    let client = Arc::new(Subduction::new(
        HashMap::from_iter([(client_sed_id, client_tree)]),
        client_storage,
        HashMap::new(),
    ));

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let client_ws = TokioWebSocketClient::new(uri, Duration::from_secs(5), PeerId::new([1; 32]))
        .await?
        .start();

    client.register(client_ws).await?;
    rx.await.unwrap();

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.run().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    assert_eq!(client.peer_ids().await.len(), 1);
    assert_eq!(server.peer_ids().await.len(), 1);

    client.request_all_batch_sync_all(None).await?;

    let server_updated = server
        .get_commits(sed_id)
        .await
        .expect("sedimentree exists");

    assert_eq!(server_updated.len(), 3);
    assert!(server_updated.contains(&commit1));
    assert!(server_updated.contains(&commit2));
    assert!(server_updated.contains(&commit3));

    let client_updated = client
        .get_commits(client_sed_id)
        .await
        .expect("sedimentree exists");

    assert_eq!(client_updated.len(), 3);
    assert!(client_updated.contains(&commit1));
    assert!(client_updated.contains(&commit2));
    assert!(client_updated.contains(&commit3));

    Ok(())
}
