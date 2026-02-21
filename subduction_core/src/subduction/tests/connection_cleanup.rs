//! Tests for connection cleanup on send failure.

use super::common::{TestSpawn, test_signer};
use crate::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{FailingSendMockConnection, MockConnection},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use alloc::collections::BTreeSet;

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

fn make_commit_parts() -> (Digest<LooseCommit>, BTreeSet<Digest<LooseCommit>>, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents);
    let digest = Digest::<LooseCommit>::from_bytes([0u8; 32]);
    (digest, BTreeSet::new(), blob)
}

async fn make_signed_test_commit(id: &SedimentreeId) -> (Signed<LooseCommit>, Blob) {
    let (digest, parents, blob) = make_commit_parts();
    let blob_meta = BlobMeta::new(blob.as_slice());
    let commit = LooseCommit::new(digest, parents, blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit, id).await;
    (verified.into_signed(), blob)
}

#[allow(clippy::type_complexity)]
fn make_fragment_parts() -> (
    Digest<LooseCommit>,
    BTreeSet<Digest<LooseCommit>>,
    Vec<Digest<LooseCommit>>,
    Blob,
) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents);
    let head = Digest::<LooseCommit>::from_bytes([1u8; 32]);
    let boundary = BTreeSet::from([Digest::<LooseCommit>::from_bytes([2u8; 32])]);
    let checkpoints = vec![Digest::<LooseCommit>::from_bytes([3u8; 32])];
    (head, boundary, checkpoints, blob)
}

async fn make_signed_test_fragment(id: &SedimentreeId) -> (Signed<Fragment>, Blob) {
    let (head, boundary, checkpoints, blob) = make_fragment_parts();
    let blob_meta = BlobMeta::new(blob.as_slice());
    let fragment = Fragment::new(head, boundary, &checkpoints, blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment, id).await;
    (verified.into_signed(), blob)
}

#[tokio::test]
async fn test_add_commit_unregisters_connection_on_send_failure() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Add a commit - the send will fail
    let id = SedimentreeId::new([1u8; 32]);
    let (digest, parents, blob) = make_commit_parts();

    let _ = subduction.add_commit(id, digest, parents, blob).await;

    // Connection should be unregistered after send failure
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        0,
        "Connection should be unregistered after send failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_add_fragment_unregisters_connection_on_send_failure() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Add a fragment - the send will fail
    let id = SedimentreeId::new([1u8; 32]);
    let (head, boundary, checkpoints, blob) = make_fragment_parts();

    let _ = subduction
        .add_fragment(id, head, boundary, &checkpoints, blob)
        .await;

    // Connection should be unregistered after send failure
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        0,
        "Connection should be unregistered after send failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_recv_commit_unregisters_connection_on_send_failure() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register a failing connection with a different peer ID than the sender
    let sender_peer_id = PeerId::new([1u8; 32]);
    let other_peer_id = PeerId::new([2u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
    let _fresh = subduction.register(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Subscribe other_peer to the sedimentree so forwarding will be attempted
    let id = SedimentreeId::new([1u8; 32]);
    subduction.add_subscription(other_peer_id, id).await;

    // Receive a commit from a different peer - the propagation send will fail
    let (signed_commit, blob) = make_signed_test_commit(&id).await;

    let _ = subduction
        .recv_commit(&sender_peer_id, id, &signed_commit, blob)
        .await;

    // Connection should be unregistered after send failure during propagation
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        0,
        "Connection should be unregistered after send failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_recv_fragment_unregisters_connection_on_send_failure() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register a failing connection with a different peer ID than the sender
    let sender_peer_id = PeerId::new([1u8; 32]);
    let other_peer_id = PeerId::new([2u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
    let _fresh = subduction.register(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Subscribe other_peer to the sedimentree so forwarding will be attempted
    let id = SedimentreeId::new([1u8; 32]);
    subduction.add_subscription(other_peer_id, id).await;

    // Receive a fragment from a different peer - the propagation send will fail
    let (signed_fragment, blob) = make_signed_test_fragment(&id).await;

    let _ = subduction
        .recv_fragment(&sender_peer_id, id, &signed_fragment, blob)
        .await;

    // Connection should be unregistered after send failure during propagation
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        0,
        "Connection should be unregistered after send failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_request_blobs_unregisters_connection_on_send_failure() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Request blobs - the send will fail
    let digests = vec![Digest::<Blob>::from_bytes([1u8; 32])];
    subduction
        .request_blobs(SedimentreeId::new([42u8; 32]), digests)
        .await;

    // Connection should be unregistered after send failure
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        0,
        "Connection should be unregistered after send failure"
    );

    Ok(())
}

#[tokio::test]
async fn test_multiple_connections_only_failing_ones_removed() -> TestResult {
    let storage = MemoryStorage::new();
    let depth_metric = CountLeadingZeroBytes;

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
            None,
            test_signer(),
            storage,
            OpenPolicy,
            NonceCache::default(),
            depth_metric,
            ShardedMap::with_key(0, 0),
            TestSpawn,
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        );

    // Register two connections that will succeed
    let peer_id1 = PeerId::new([1u8; 32]);
    let peer_id2 = PeerId::new([2u8; 32]);
    let conn1 = MockConnection::with_peer_id(peer_id1);
    let conn2 = MockConnection::with_peer_id(peer_id2);

    subduction.register(conn1.authenticated()).await?;
    subduction.register(conn2.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 2);

    // Add a commit - sends will succeed
    let id = SedimentreeId::new([1u8; 32]);
    let (digest, parents, blob) = make_commit_parts();

    let _ = subduction.add_commit(id, digest, parents, blob).await;

    // Both connections should still be registered (sends succeeded)
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        2,
        "Both connections should remain registered when sends succeed"
    );

    Ok(())
}
