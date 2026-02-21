//! Tests for round-trip communication between Subduction peers using `WebSocket`s.

use std::{collections::BTreeSet, net::SocketAddr, sync::OnceLock, time::Duration};
use testresult::TestResult;

use arbitrary::{Arbitrary, Unstructured};
use future_form::Sendable;
use rand::RngCore;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_core::{
    connection::{
        Connection, authenticated::Authenticated, handshake::Audience, message::Message,
        nonce_cache::NonceCache,
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::tokio::{
    TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer,
};

static TRACING: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING.get_or_init(|| {
        tracing_subscriber::fmt().with_env_filter("warn").init();
    });
}

const HANDSHAKE_MAX_DRIFT: Duration = Duration::from_secs(60);

fn test_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

#[tokio::test]
async fn rend_receive() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let memory_storage = MemoryStorage::default();
    let (suduction, listener_fut, manager_fut) = Subduction::new(
        None,
        server_signer,
        memory_storage.clone(),
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

    let task_subduction = suduction.clone();
    let server_ws = TokioWebSocketServer::new(
        addr,
        TimeoutTokio,
        Duration::from_secs(5),
        HANDSHAKE_MAX_DRIFT,
        task_subduction,
    )
    .await?;

    let bound = server_ws.address();
    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, listener_fut, sender_fut) = TokioWebSocketClient::new(
        uri,
        TimeoutTokio,
        Duration::from_secs(5),
        client_signer,
        Audience::known(server_peer_id),
    )
    .await?;

    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    let ws = client_ws.clone();
    tokio::spawn(async move {
        ws.listen().await?;
        Ok::<(), eyre::Report>(())
    });

    let expected = Message::BlobsRequest {
        id: sedimentree_core::id::SedimentreeId::new([0u8; 32]),
        digests: Vec::new(),
    };
    client_ws.send(&expected).await?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn batch_sync() -> TestResult {
    init_tracing();

    let server_signer = test_signer(0);
    let client_signer = test_signer(1);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    let mut blob_bytes1 = [0u8; 64];
    let mut blob_bytes2 = [0u8; 64];
    let mut blob_bytes3 = [0u8; 64];
    let mut digest_bytes1 = [0u8; 32];
    let mut digest_bytes2 = [0u8; 32];
    let mut digest_bytes3 = [0u8; 32];

    rand::thread_rng().fill_bytes(&mut blob_bytes1);
    rand::thread_rng().fill_bytes(&mut blob_bytes2);
    rand::thread_rng().fill_bytes(&mut blob_bytes3);
    rand::thread_rng().fill_bytes(&mut digest_bytes1);
    rand::thread_rng().fill_bytes(&mut digest_bytes2);
    rand::thread_rng().fill_bytes(&mut digest_bytes3);

    let blob1 = Blob::arbitrary(&mut Unstructured::new(&blob_bytes1))?;
    let blob2 = Blob::arbitrary(&mut Unstructured::new(&blob_bytes2))?;
    let blob3 = Blob::arbitrary(&mut Unstructured::new(&blob_bytes3))?;

    let digest1: Digest<LooseCommit> = Digest::arbitrary(&mut Unstructured::new(&digest_bytes1))?;
    let digest2: Digest<LooseCommit> = Digest::arbitrary(&mut Unstructured::new(&digest_bytes2))?;
    let digest3: Digest<LooseCommit> = Digest::arbitrary(&mut Unstructured::new(&digest_bytes3))?;

    ///////////////////
    // SERVER SETUP //
    ///////////////////

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

    server_subduction
        .add_commit(sed_id, digest1, BTreeSet::new(), blob1)
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
        HANDSHAKE_MAX_DRIFT,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    ///////////////////
    // CLIENT SETUP //
    ///////////////////

    let client_storage = MemoryStorage::default();
    let (client, listener_fut, client_manager_fut) = Subduction::<
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

    tokio::spawn(client_manager_fut);
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

    assert_eq!(client.connected_peer_ids().await.len(), 0);
    client
        .register(Authenticated::new_for_test(client_ws))
        .await?;
    assert_eq!(client.connected_peer_ids().await.len(), 1);

    client
        .add_commit(sed_id, digest2, BTreeSet::new(), blob2)
        .await?;
    client
        .add_commit(sed_id, digest3, BTreeSet::new(), blob3)
        .await?;

    assert_eq!(server_subduction.connected_peer_ids().await.len(), 1);

    tokio::spawn({
        let inner_client = client.clone();
        async move {
            inner_client.listen().await?;
            Ok::<(), eyre::Report>(())
        }
    });

    ///////////
    // SYNC //
    //////////

    assert_eq!(client.connected_peer_ids().await.len(), 1);
    assert_eq!(server_subduction.connected_peer_ids().await.len(), 1);

    client.full_sync(Some(Duration::from_millis(100))).await;

    let server_updated = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;

    assert_eq!(server_updated.len(), 3);
    assert!(server_updated.iter().any(|c| c.digest() == digest1));
    assert!(server_updated.iter().any(|c| c.digest() == digest2));
    assert!(server_updated.iter().any(|c| c.digest() == digest3));

    let client_updated = client
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;

    assert_eq!(client_updated.len(), 3);
    assert!(client_updated.iter().any(|c| c.digest() == digest1));
    assert!(client_updated.iter().any(|c| c.digest() == digest2));
    assert!(client_updated.iter().any(|c| c.digest() == digest3));

    Ok(())
}
