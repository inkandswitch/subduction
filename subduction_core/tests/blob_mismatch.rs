//! Tests for blob metadata mismatch detection.
//!
//! These tests verify that commits and fragments with mismatched blob metadata
//! are rejected before being stored. This prevents poisoned metadata from
//! referencing non-existent blobs.

use std::collections::BTreeSet;
use std::sync::Arc;

use async_lock::Mutex;
use core::future::Future;
use core::time::Duration;
use futures::future::Aborted;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::Map,
    commit::CountLeadingZeroBytes,
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_core::{
    connection::{
        message::Message,
        nonce_cache::NonceCache,
        test_utils::{ChannelMockConnection, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    subduction::{
        Subduction,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
    },
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

/// Create a commit with matching blob metadata.
async fn make_valid_commit(id: &SedimentreeId, data: &[u8]) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(*id, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

/// Create a commit with mismatched blob metadata.
/// The commit claims the blob has one digest, but the actual blob has different content.
async fn make_mismatched_commit(id: &SedimentreeId) -> (Signed<LooseCommit>, Blob) {
    // Commit claims blob contains "claimed data"
    let claimed_data = b"claimed data";
    let claimed_blob = Blob::new(claimed_data.to_vec());
    let blob_meta = BlobMeta::new(&claimed_blob);
    let commit = LooseCommit::new(*id, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;

    // But actual blob contains different data
    let actual_data = b"actual different data";
    let actual_blob = Blob::new(actual_data.to_vec());

    (verified.into_signed(), actual_blob)
}

/// Create a fragment with matching blob metadata.
async fn make_valid_fragment(id: &SedimentreeId, data: &[u8]) -> (Signed<Fragment>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    // Use arbitrary bytes for test fixture digests
    let head = Digest::force_from_bytes([1u8; 32]);
    let boundary = BTreeSet::from([Digest::force_from_bytes([2u8; 32])]);
    let fragment = Fragment::new(*id, head, boundary, &[], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    (verified.into_signed(), blob)
}

/// Create a fragment with mismatched blob metadata.
async fn make_mismatched_fragment(id: &SedimentreeId) -> (Signed<Fragment>, Blob) {
    // Fragment claims blob contains "claimed fragment data"
    let claimed_data = b"claimed fragment data";
    let claimed_blob = Blob::new(claimed_data.to_vec());
    let blob_meta = BlobMeta::new(&claimed_blob);
    // Use arbitrary bytes for test fixture digests
    let head = Digest::force_from_bytes([1u8; 32]);
    let boundary = BTreeSet::from([Digest::force_from_bytes([2u8; 32])]);
    let fragment = Fragment::new(*id, head, boundary, &[], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;

    // But actual blob contains different data
    let actual_data = b"actual different fragment data";
    let actual_blob = Blob::new(actual_data.to_vec());

    (verified.into_signed(), actual_blob)
}

#[allow(clippy::type_complexity)]
fn make_subduction() -> (
    Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            ChannelMockConnection,
            OpenPolicy,
            subduction_crypto::signer::memory::MemorySigner,
            CountLeadingZeroBytes,
        >,
    >,
    impl Future<Output = Result<(), Aborted>>,
    impl Future<Output = Result<(), Aborted>>,
) {
    let sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
    let connections = Arc::new(Mutex::new(Map::new()));
    let subscriptions = Arc::new(Mutex::new(Map::new()));
    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    let pending = Arc::new(Mutex::new(PendingBlobRequests::new(
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )));

    let handler = Arc::new(SyncHandler::new(
        sedimentrees.clone(),
        connections.clone(),
        subscriptions.clone(),
        storage.clone(),
        pending.clone(),
        CountLeadingZeroBytes,
    ));

    Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
        handler,
        None,
        test_signer(),
        sedimentrees,
        connections,
        subscriptions,
        storage,
        pending,
        NonceCache::default(),
        CountLeadingZeroBytes,
        TokioSpawn,
    )
}

#[tokio::test]
async fn recv_commit_rejects_mismatched_blob() -> TestResult {
    let (subduction, listener_fut, actor_fut) = make_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (commit, blob) = make_mismatched_commit(&sedimentree_id).await;

    // Send the mismatched commit
    handle
        .inbound_tx
        .send(Message::LooseCommit {
            id: sedimentree_id,
            commit,
            blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // The commit should NOT be stored
    let commits = subduction.get_commits(sedimentree_id).await;
    assert!(
        commits.as_ref().is_none_or(Vec::is_empty),
        "Mismatched commit should not be stored, found: {commits:?}"
    );

    // The sedimentree should not exist
    let ids = subduction.sedimentree_ids().await;
    assert!(
        !ids.contains(&sedimentree_id),
        "Sedimentree should not be created for mismatched commit"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn recv_fragment_rejects_mismatched_blob() -> TestResult {
    let (subduction, listener_fut, actor_fut) = make_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (fragment, blob) = make_mismatched_fragment(&sedimentree_id).await;

    // Send the mismatched fragment
    handle
        .inbound_tx
        .send(Message::Fragment {
            id: sedimentree_id,
            fragment,
            blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // The sedimentree should not exist (no data stored)
    let ids = subduction.sedimentree_ids().await;
    assert!(
        !ids.contains(&sedimentree_id),
        "Sedimentree should not be created for mismatched fragment"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn recv_commit_accepts_valid_blob() -> TestResult {
    let (subduction, listener_fut, actor_fut) = make_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (commit, blob) = make_valid_commit(&sedimentree_id, b"valid commit data").await;

    // Send the valid commit
    handle
        .inbound_tx
        .send(Message::LooseCommit {
            id: sedimentree_id,
            commit,
            blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // The commit SHOULD be stored
    let commits = subduction.get_commits(sedimentree_id).await;
    assert_eq!(
        commits.map(|c| c.len()),
        Some(1),
        "Valid commit should be stored"
    );

    // The sedimentree should exist
    let ids = subduction.sedimentree_ids().await;
    assert!(
        ids.contains(&sedimentree_id),
        "Sedimentree should be created for valid commit"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn recv_fragment_accepts_valid_blob() -> TestResult {
    let (subduction, listener_fut, actor_fut) = make_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (fragment, blob) = make_valid_fragment(&sedimentree_id, b"valid fragment data").await;

    // Send the valid fragment
    handle
        .inbound_tx
        .send(Message::Fragment {
            id: sedimentree_id,
            fragment,
            blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // The sedimentree should exist
    let ids = subduction.sedimentree_ids().await;
    assert!(
        ids.contains(&sedimentree_id),
        "Sedimentree should be created for valid fragment"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn mismatched_commit_does_not_affect_subsequent_valid_commits() -> TestResult {
    let (subduction, listener_fut, actor_fut) = make_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);

    // First: send a mismatched commit (should be rejected)
    let (bad_commit, bad_blob) = make_mismatched_commit(&sedimentree_id).await;
    handle
        .inbound_tx
        .send(Message::LooseCommit {
            id: sedimentree_id,
            commit: bad_commit,
            blob: bad_blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(30)).await;

    // Second: send a valid commit (should be accepted)
    let (good_commit, good_blob) = make_valid_commit(&sedimentree_id, b"good commit").await;
    handle
        .inbound_tx
        .send(Message::LooseCommit {
            id: sedimentree_id,
            commit: good_commit,
            blob: good_blob,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Only the valid commit should be stored
    let commits = subduction.get_commits(sedimentree_id).await;
    assert_eq!(
        commits.map(|c| c.len()),
        Some(1),
        "Only the valid commit should be stored"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}
