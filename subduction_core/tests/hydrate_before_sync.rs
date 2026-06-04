//! Outgoing batch sync must hydrate per-doc from storage before building
//! fingerprint summaries, so a cold in-memory tree does not advertise an
//! empty state to peers when local storage already has data.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use core::time::Duration;
use std::sync::Arc;

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, SyncMessage},
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

fn make_unique_blob(seed: u8) -> Blob {
    Blob::new(vec![seed; 32])
}

/// Populate shared storage via one Subduction instance, then sync from a
/// second instance whose in-memory sedimentree map starts empty.
#[tokio::test]
async fn sync_with_all_peers_hydrates_from_storage_before_fingerprint() -> TestResult {
    let storage = MemoryStorage::new();
    let policy = Arc::new(OpenPolicy);

    let (writer, _handler, _listener, _actor) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(storage.clone(), policy.clone())
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let sed_id = SedimentreeId::new([0xAA; 32]);
    const NUM_COMMITS: u8 = 5;

    for i in 0..NUM_COMMITS {
        writer
            .add_commit(
                sed_id,
                CommitId::new([i + 1; 32]),
                Default::default(),
                make_unique_blob(i),
            )
            .await?;
    }

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Cold node: same storage, empty in-memory sedimentrees (SW restart shape).
    let empty_trees = Arc::new(ShardedMap::new());
    let (cold, _handler, listener, actor2) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(storage, policy)
        .sedimentrees(empty_trees)
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    assert!(
        cold.sedimentrees().get_cloned(&sed_id).await.is_none(),
        "cold node should start with no in-memory sedimentree"
    );

    let peer_id = PeerId::new([0xBB; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    cold.add_connection(conn.authenticated()).await?;

    tokio::spawn(listener);
    tokio::spawn(actor2);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sync_task = tokio::spawn(async move {
        cold.sync_with_all_peers(sed_id, false, Some(Duration::from_secs(5)))
            .await
    });

    let msg = tokio::time::timeout(Duration::from_secs(5), handle.outbound_rx.recv())
        .await
        .expect("timed out waiting for BatchSyncRequest")
        .expect("peer closed channel");

    let SyncMessage::BatchSyncRequest(BatchSyncRequest {
        fingerprint_summary,
        ..
    }) = msg
    else {
        panic!("expected BatchSyncRequest, got {msg:?}");
    };

    assert_eq!(
        fingerprint_summary.commit_fingerprints().len(),
        usize::from(NUM_COMMITS),
        "fingerprint should reflect storage-backed commits, not an empty in-memory tree"
    );

    sync_task.abort();

    Ok(())
}

/// `get_blobs` should read from storage even when the sedimentree is not yet
/// in memory (after per-doc hydration).
#[tokio::test]
async fn get_blobs_hydrates_from_storage() -> TestResult {
    let storage = MemoryStorage::new();
    let policy = Arc::new(OpenPolicy);

    let (writer, _handler, _listener, _actor) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(storage.clone(), policy.clone())
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let sed_id = SedimentreeId::new([0xCC; 32]);
    writer
        .add_commit(
            sed_id,
            CommitId::new([1; 32]),
            Default::default(),
            make_unique_blob(42),
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(20)).await;

    let empty_trees = Arc::new(ShardedMap::new());
    let (cold, _handler, _listener, _actor) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(storage, policy)
        .sedimentrees(empty_trees)
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let blobs = cold
        .get_blobs(sed_id)
        .await?
        .expect("expected blobs from storage");

    assert_eq!(blobs.len(), 1);

    Ok(())
}
