use async_tungstenite::tokio::accept_async;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, sync::OnceLock, time::Duration};
use testresult::TestResult;

use arbitrary::{Arbitrary, Unstructured};
use rand::Rng;
use sedimentree_core::{storage::MemoryStorage, LooseCommit, Sedimentree};
use sedimentree_sync_core::{connection::id::ConnectionId, peer::id::PeerId, SedimentreeSync};
use sedimentree_sync_websocket::tokio::{
    client::TokioWebSocketClient, server::TokioWebSocketServer,
};
use tokio::{net::TcpListener, sync::oneshot};
use tracing_subscriber;

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("debug").init();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn it_does_the_thing() -> TestResult {
    init_tracing();

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let listener = TcpListener::bind(addr).await?;
    let bound: SocketAddr = listener.local_addr()?;

    let commit1: LooseCommit =
        LooseCommit::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;
    let commit2: LooseCommit =
        LooseCommit::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;
    let commit3: LooseCommit =
        LooseCommit::arbitrary(&mut Unstructured::new(&rand::rng().random::<[u8; 64]>()))?;

    let server_tree = Sedimentree::new(vec![], vec![commit1.clone()]);
    let server_sed_id = sedimentree_core::SedimentreeId::new([0u8; 32]);

    let server = Arc::new(SedimentreeSync::<MemoryStorage, TokioWebSocketServer>::new(
        HashMap::from_iter([(server_sed_id, server_tree)]),
        MemoryStorage::default(),
        HashMap::new(),
    ));

    let (tx, rx) = oneshot::channel();
    let server_task = tokio::spawn({
        let inner_server = server.clone();
        async move {
            let (tcp, _peer) = listener.accept().await?;
            let ws_stream = accept_async(tcp).await?;

            let server_ws = TokioWebSocketServer::new(
                bound,
                Duration::from_secs(5),
                PeerId::new([0; 32]),
                ConnectionId::from(0),
                ws_stream,
            );

            inner_server.register(server_ws).await?;
            tx.send(()).unwrap();
            inner_server.run().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    let client_tree = Sedimentree::new(vec![], vec![commit2.clone(), commit3.clone()]);
    let client_sed_id = sedimentree_core::SedimentreeId::new([0u8; 32]);

    let client = Arc::new(SedimentreeSync::new(
        HashMap::from_iter([(client_sed_id, client_tree)]),
        MemoryStorage::default(),
        HashMap::new(),
    ));

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let client_ws =
        TokioWebSocketClient::new(uri, Duration::from_secs(5), PeerId::new([1; 32]), 0.into())
            .await?;

    client.register(client_ws).await?;
    rx.await.unwrap();

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.run().await?;
            Ok::<(), anyhow::Error>(())
        }
    });

    let batch = client.request_all_batch_sync_all(None).await?;

    let server_sed = server.sedimentree_snapshot(server_sed_id).await.unwrap();
    let server_updated = server_sed.loose_commits().cloned().collect::<Vec<_>>();
    assert_eq!(server_updated.len(), 3);
    assert!(server_updated.contains(&commit1));
    assert!(server_updated.contains(&commit2));
    assert!(server_updated.contains(&commit3));

    let client_sed = client.sedimentree_snapshot(server_sed_id).await.unwrap();
    let client_updated = client_sed.loose_commits().cloned().collect::<Vec<_>>();
    assert_eq!(client_updated.len(), 3);
    assert!(client_updated.contains(&commit1));
    assert!(client_updated.contains(&commit2));
    assert!(client_updated.contains(&commit3));

    Ok(())
}
