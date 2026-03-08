//! Tests for round-trip communication between Subduction peers using `WebSocket`s.

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::Duration,
};
use testresult::TestResult;

use future_form::Sendable;
use rand::RngCore;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
};
use subduction_core::{
    connection::handshake::Audience,
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_websocket::{
    DEFAULT_MAX_MESSAGE_SIZE,
    tokio::{TimeoutTokio, TokioSpawn, client::TokioWebSocketClient, server::TokioWebSocketServer},
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

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
    >,
>;

type TestHandler = Arc<
    SyncHandler<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        CountLeadingZeroBytes,
    >,
>;

#[allow(clippy::type_complexity)]
fn setup_client_subduction(
    signer: MemorySigner,
) -> (
    TestSubduction,
    TestHandler,
    subduction_core::subduction::ListenerFuture<
        'static,
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner, TimeoutTokio>,
        OpenPolicy,
        MemorySigner,
        CountLeadingZeroBytes,
    >,
    subduction_core::connection::manager::ManagerFuture<Sendable>,
) {
    SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .build::<Sendable, TokioWebSocketClient<MemorySigner, TimeoutTokio>>()
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

    rand::thread_rng().fill_bytes(&mut blob_bytes1);
    rand::thread_rng().fill_bytes(&mut blob_bytes2);
    rand::thread_rng().fill_bytes(&mut blob_bytes3);

    let blob1 = Blob::new(blob_bytes1.to_vec());
    let blob2 = Blob::new(blob_bytes2.to_vec());
    let blob3 = Blob::new(blob_bytes3.to_vec());

    ///////////////////
    // SERVER SETUP //
    ///////////////////

    let sed_id = SedimentreeId::new([0u8; 32]);

    let (server_subduction, _server_handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(server_signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .build::<Sendable, subduction_websocket::tokio::unified::UnifiedWebSocket<TimeoutTokio>>();
    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    server_subduction
        .add_commit(sed_id, BTreeSet::new(), blob1)
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
        DEFAULT_MAX_MESSAGE_SIZE,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    ///////////////////
    // CLIENT SETUP //
    ///////////////////

    let (client, client_handler, listener_fut, client_manager_fut) =
        setup_client_subduction(client_signer.clone());

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
    client.register(client_ws).await?;
    assert_eq!(client.connected_peer_ids().await.len(), 1);

    client.add_commit(sed_id, BTreeSet::new(), blob2).await?;
    client.add_commit(sed_id, BTreeSet::new(), blob3).await?;

    assert_eq!(server_subduction.connected_peer_ids().await.len(), 1);

    tokio::spawn({
        let inner_client = client.clone();
        let handler = client_handler.clone();
        async move {
            inner_client.listen(handler).await?;
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

    // Verify both sides have all 3 commits after sync
    assert_eq!(
        server_updated.len(),
        3,
        "server should have 3 commits after sync"
    );

    let client_updated = client
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;

    assert_eq!(
        client_updated.len(),
        3,
        "client should have 3 commits after sync"
    );

    // Verify the digests match between server and client
    let server_digests: BTreeSet<_> = server_updated.iter().map(Digest::hash).collect();
    let client_digests: BTreeSet<_> = client_updated.iter().map(Digest::hash).collect();
    assert_eq!(
        server_digests, client_digests,
        "server and client should have the same commits"
    );

    Ok(())
}
