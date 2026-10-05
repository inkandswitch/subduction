//! Real WebSocket integration and low-level disconnect regression.
#![cfg(feature = "websocket")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    depth::CountLeadingZeroBytes,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{future::IntoFuture, time::Duration};
use subduction_core::{
    handshake::audience::Audience, nonce_cache::NonceCache, peer::id::PeerId,
    policy::open::OpenPolicy, storage::memory::MemoryStorage, transport::Transport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_tokio::{CallTimeout, ExpectedPeer, TokioClient, websocket::WebSocketConnector};
use subduction_websocket::tokio::{
    TimeoutTokio, client::TokioWebSocketClient, server::TokioWebSocketServer,
};

const BUDGET: Duration = Duration::from_secs(5);
#[tokio::test]
async fn managed_websocket_receives_history_and_drains() {
    let signer = MemorySigner::from_bytes(&[11; 32]);
    let peer = PeerId::from(signer.verifying_key());
    let mut server = TokioWebSocketServer::setup(
        ([127, 0, 0, 1], 0).into(),
        TimeoutTokio,
        Duration::from_secs(60),
        1024 * 1024,
        signer,
        None,
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await
    .unwrap();
    let id = SedimentreeId::new([1; 32]);
    let blob = Blob::new(b"websocket".to_vec());
    let commit = LooseCommit::new(
        id,
        CommitId::new([2; 32]),
        std::collections::BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    server
        .subduction()
        .store_built_batch(id, vec![(commit, blob.clone())], vec![])
        .await
        .unwrap();
    let client = TokioClient::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(MemorySigner::from_bytes(&[12; 32]))
        .start()
        .unwrap();
    let connection = client
        .handle()
        .connect(
            WebSocketConnector::new(format!("ws://{}", server.address())),
            ExpectedPeer::Known(peer),
        )
        .await
        .unwrap();
    let result = client
        .handle()
        .sync_with_peer(peer, id, false, CallTimeout::Default)
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(client.handle().get_blobs(id).await.unwrap(), vec![blob]);
    connection.close().await.unwrap();
    let report = tokio::time::timeout(BUDGET, client.close()).await.unwrap();
    assert!(!report.forced, "{report:?}");
    assert!(report.failure.is_none(), "{report:?}");
    tokio::time::timeout(BUDGET, server.stop_and_drain())
        .await
        .unwrap();
}

#[tokio::test]
async fn low_level_disconnect_wakes_io_and_keepalive_with_clones_retained() {
    let signer = MemorySigner::from_bytes(&[21; 32]);
    let peer = PeerId::from(signer.verifying_key());
    let mut server = TokioWebSocketServer::setup(
        ([127, 0, 0, 1], 0).into(),
        TimeoutTokio,
        Duration::from_secs(60),
        1024 * 1024,
        signer,
        None,
        MemoryStorage::default(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
    )
    .await
    .unwrap();
    let (connection, listener, sender, keepalive) = TokioWebSocketClient::new(
        format!("ws://{}", server.address()).parse().unwrap(),
        MemorySigner::from_bytes(&[22; 32]),
        Audience::Known(peer),
    )
    .await
    .unwrap();
    let listener = tokio::spawn(listener.into_future());
    let sender = tokio::spawn(sender.into_future());
    let keepalive = tokio::spawn(keepalive.into_future());
    let retained = connection.clone();
    let recv =
        tokio::spawn(async move { Transport::<Sendable>::recv_bytes(retained.inner()).await });
    tokio::time::timeout(
        BUDGET,
        Transport::<Sendable>::disconnect(connection.inner()),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(BUDGET, async {
        listener.await.unwrap().unwrap();
        sender.await.unwrap().unwrap();
        let _outcome = keepalive.await.unwrap();
        assert!(recv.await.unwrap().is_err());
    })
    .await
    .unwrap();
    assert!(
        Transport::<Sendable>::send_bytes(connection.inner(), b"closed")
            .await
            .is_err()
    );
    tokio::time::timeout(BUDGET, server.stop_and_drain())
        .await
        .unwrap();
}
