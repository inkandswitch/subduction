//! Tests for blob metadata mismatch detection.
//!
//! These tests verify that commits and fragments with mismatched blob metadata
//! are rejected before being stored. This prevents poisoned metadata from
//! referencing non-existent blobs.

use alloc::collections::BTreeSet;

use super::common::{test_signer, TokioSpawn};
use crate::{
    connection::{message::Message, nonce_cache::NonceCache, test_utils::ChannelMockConnection},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
};
use core::time::Duration;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

/// Create a commit with matching blob metadata.
async fn make_valid_commit(data: &[u8]) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(data);
    let digest = Digest::<LooseCommit>::hash_bytes(data);
    let commit = LooseCommit::new(digest, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

/// Create a commit with mismatched blob metadata.
/// The commit claims the blob has one digest, but the actual blob has different content.
async fn make_mismatched_commit() -> (Signed<LooseCommit>, Blob) {
    // Commit claims blob contains "claimed data"
    let claimed_data = b"claimed data";
    let blob_meta = BlobMeta::new(claimed_data);
    let digest = Digest::<LooseCommit>::hash_bytes(claimed_data);
    let commit = LooseCommit::new(digest, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;

    // But actual blob contains different data
    let actual_data = b"actual different data";
    let blob = Blob::new(actual_data.to_vec());

    (verified.into_signed(), blob)
}

/// Create a fragment with matching blob metadata.
async fn make_valid_fragment(data: &[u8]) -> (Signed<Fragment>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(data);
    let head = Digest::<LooseCommit>::hash_bytes(b"head");
    let boundary = BTreeSet::from([Digest::<LooseCommit>::hash_bytes(b"boundary")]);
    let fragment = Fragment::new(head, boundary, &[], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    (verified.into_signed(), blob)
}

/// Create a fragment with mismatched blob metadata.
async fn make_mismatched_fragment() -> (Signed<Fragment>, Blob) {
    // Fragment claims blob contains "claimed fragment data"
    let claimed_data = b"claimed fragment data";
    let blob_meta = BlobMeta::new(claimed_data);
    let head = Digest::<LooseCommit>::hash_bytes(b"head");
    let boundary = BTreeSet::from([Digest::<LooseCommit>::hash_bytes(b"boundary")]);
    let fragment = Fragment::new(head, boundary, &[], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;

    // But actual blob contains different data
    let actual_data = b"actual different fragment data";
    let blob = Blob::new(actual_data.to_vec());

    (verified.into_signed(), blob)
}

#[tokio::test]
async fn recv_commit_rejects_mismatched_blob() -> TestResult {
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (commit, blob) = make_mismatched_commit().await;

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
        commits.is_none() || commits.as_ref().map(|c| c.is_empty()).unwrap_or(true),
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
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (fragment, blob) = make_mismatched_fragment().await;

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
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (commit, blob) = make_valid_commit(b"valid commit data").await;

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
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);
    let (fragment, blob) = make_valid_fragment(b"valid fragment data").await;

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
    let storage = MemoryStorage::new();
    let (subduction, listener_fut, actor_fut) =
        Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            CountLeadingZeroBytes,
            ShardedMap::with_key(0, 0),
            TokioSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sedimentree_id = SedimentreeId::new([42u8; 32]);

    // First: send a mismatched commit (should be rejected)
    let (bad_commit, bad_blob) = make_mismatched_commit().await;
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
    let (good_commit, good_blob) = make_valid_commit(b"good commit").await;
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
