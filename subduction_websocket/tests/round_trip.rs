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
    loose_commit::id::CommitId,
};
use subduction_core::{
    handler::sync::SyncHandler,
    handshake::audience::Audience,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
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

fn random_commit_id() -> CommitId {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    CommitId::new(bytes)
}

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner>,
        SyncHandler<
            Sendable,
            MemoryStorage,
            TokioWebSocketClient<MemorySigner>,
            OpenPolicy,
            CountLeadingZeroBytes,
        >,
        OpenPolicy,
        MemorySigner,
        TimeoutTokio,
    >,
>;

type TestHandler = Arc<
    SyncHandler<
        Sendable,
        MemoryStorage,
        TokioWebSocketClient<MemorySigner>,
        OpenPolicy,
        CountLeadingZeroBytes,
    >,
>;

type ClientSyncHandler = SyncHandler<
    Sendable,
    MemoryStorage,
    TokioWebSocketClient<MemorySigner>,
    OpenPolicy,
    CountLeadingZeroBytes,
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
        TokioWebSocketClient<MemorySigner>,
        ClientSyncHandler,
        OpenPolicy,
        MemorySigner,
        TimeoutTokio,
        CountLeadingZeroBytes,
    >,
    subduction_core::connection::manager::ManagerFuture<Sendable>,
) {
    SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<Sendable, TokioWebSocketClient<MemorySigner>>()
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
        .timer(TimeoutTokio)
        .build::<Sendable, MessageTransport<subduction_websocket::tokio::unified::UnifiedWebSocket>>();
    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    server_subduction
        .add_commit(sed_id, random_commit_id(), BTreeSet::new(), blob1)
        .await?;

    let inserted = server_subduction
        .get_commits(sed_id)
        .await
        .ok_or("sedimentree exists")?;
    assert_eq!(inserted.len(), 1);

    let server = TokioWebSocketServer::new(
        addr,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        server_subduction.clone(),
    )
    .await?;

    let bound = server.address();

    ///////////////////
    // CLIENT SETUP //
    ///////////////////

    let (client, _client_handler, listener_fut, client_manager_fut) =
        setup_client_subduction(client_signer.clone());

    tokio::spawn(client_manager_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, listener_fut, sender_fut) =
        TokioWebSocketClient::new(uri, client_signer, Audience::known(server_peer_id)).await?;

    tokio::spawn(async {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    tokio::spawn(async {
        sender_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    assert_eq!(client.connected_peer_ids().await.len(), 0);
    client.add_connection(client_ws).await?;
    assert_eq!(client.connected_peer_ids().await.len(), 1);

    client
        .add_commit(sed_id, random_commit_id(), BTreeSet::new(), blob2)
        .await?;
    client
        .add_commit(sed_id, random_commit_id(), BTreeSet::new(), blob3)
        .await?;

    assert_eq!(server_subduction.connected_peer_ids().await.len(), 1);

    // NOTE: listener_fut (spawned above) already runs Subduction::listen().
    // Do NOT spawn a second client.listen() — two listeners on the same
    // msg_queue would race and cause flaky behavior.

    ///////////
    // SYNC //
    //////////

    assert_eq!(client.connected_peer_ids().await.len(), 1);
    assert_eq!(server_subduction.connected_peer_ids().await.len(), 1);

    client
        .full_sync_with_all_peers(Some(Duration::from_millis(100)))
        .await;

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

/// Two-round convergence: after a bidirectional sync, a second sync round
/// should transfer zero items. This is a black-box test — it only observes
/// the public sync stats, not internal resolver or fingerprint state.
///
/// This serves as a regression test that a full initial sync does not
/// silently drop items that would later need to be re-sent: if that
/// happened, the second round would still show non-zero diffs.
/// Internal mechanisms such as `FingerprintResolver` snapshots and
/// `minimize_tree` pruning are not asserted here directly; they are only
/// indirectly exercised through the end-to-end sync behavior.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn second_sync_round_is_empty() -> TestResult {
    init_tracing();

    let server_signer = test_signer(10);
    let client_signer = test_signer(11);
    let server_peer_id = PeerId::from(server_signer.verifying_key());

    let addr: SocketAddr = "127.0.0.1:0".parse()?;

    // Use distinct random blobs for each peer
    let mut rng = rand::thread_rng();
    let mut blob_bytes = [[0u8; 64]; 4];
    for b in &mut blob_bytes {
        rng.fill_bytes(b);
    }

    let sed_id = SedimentreeId::new([1u8; 32]);

    // --- Server setup ---

    let (server, _server_handler, listener_fut, manager_fut) = SubductionBuilder::new()
        .signer(server_signer)
        .storage(MemoryStorage::default(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TimeoutTokio)
        .build::<Sendable, MessageTransport<subduction_websocket::tokio::unified::UnifiedWebSocket>>();
    tokio::spawn(async move {
        listener_fut.await?;
        Ok::<(), eyre::Report>(())
    });
    tokio::spawn(async move {
        manager_fut.await?;
        Ok::<(), eyre::Report>(())
    });

    // Server has 2 commits
    server
        .add_commit(
            sed_id,
            random_commit_id(),
            BTreeSet::new(),
            Blob::new(blob_bytes[0].to_vec()),
        )
        .await?;
    server
        .add_commit(
            sed_id,
            random_commit_id(),
            BTreeSet::new(),
            Blob::new(blob_bytes[1].to_vec()),
        )
        .await?;

    let ws_server = TokioWebSocketServer::new(
        addr,
        HANDSHAKE_MAX_DRIFT,
        DEFAULT_MAX_MESSAGE_SIZE,
        server.clone(),
    )
    .await?;
    let bound = ws_server.address();

    // --- Client setup ---

    let (client, _client_handler, listener_fut, client_manager_fut) =
        setup_client_subduction(client_signer.clone());
    tokio::spawn(client_manager_fut);
    tokio::spawn(listener_fut);

    let uri = format!("ws://{}:{}", bound.ip(), bound.port()).parse()?;
    let (client_ws, ws_listener, ws_sender) =
        TokioWebSocketClient::new(uri, client_signer, Audience::known(server_peer_id)).await?;
    tokio::spawn(async { ws_listener.await.map_err(|e| eyre::eyre!("{e:?}")) });
    tokio::spawn(async { ws_sender.await.map_err(|e| eyre::eyre!("{e:?}")) });

    client.add_connection(client_ws).await?;

    // Client has 2 different commits
    client
        .add_commit(
            sed_id,
            random_commit_id(),
            BTreeSet::new(),
            Blob::new(blob_bytes[2].to_vec()),
        )
        .await?;
    client
        .add_commit(
            sed_id,
            random_commit_id(),
            BTreeSet::new(),
            Blob::new(blob_bytes[3].to_vec()),
        )
        .await?;

    // NOTE: listener_fut (spawned above) already runs Subduction::listen().
    // Do NOT spawn a second client.listen() — two listeners on the same
    // msg_queue would race and cause flaky behavior.

    // --- Round 1: exchange data ---

    let (round1_ok, round1_stats, _, _) = client
        .full_sync_with_all_peers(Some(Duration::from_millis(500)))
        .await;
    assert!(round1_ok, "first sync round should succeed");
    assert!(
        round1_stats.total_received() > 0 || round1_stats.total_sent() > 0,
        "first round should transfer data (received={}, sent={})",
        round1_stats.total_received(),
        round1_stats.total_sent(),
    );

    // Both sides should have all 4 commits
    let server_commits = server
        .get_commits(sed_id)
        .await
        .ok_or("server should have sedimentree")?;
    let client_commits = client
        .get_commits(sed_id)
        .await
        .ok_or("client should have sedimentree")?;
    assert_eq!(server_commits.len(), 4, "server should have all 4 commits");
    assert_eq!(client_commits.len(), 4, "client should have all 4 commits");

    // --- Round 2: should be a no-op ---

    let (round2_ok, round2_stats, _, _) = client
        .full_sync_with_all_peers(Some(Duration::from_millis(500)))
        .await;
    assert!(round2_ok, "second sync round should succeed");
    assert!(
        round2_stats.is_empty(),
        "second sync round should transfer zero items \
         (commits_received={}, fragments_received={}, \
         commits_sent={}, fragments_sent={})",
        round2_stats.commits_received,
        round2_stats.fragments_received,
        round2_stats.commits_sent,
        round2_stats.fragments_sent,
    );

    Ok(())
}
