//! Tests for connection cleanup on send failure.

use super::common::{test_signer, TestSpawn};
use crate::connection::nonce_cache::NonceCache;
use crate::connection::test_utils::{FailingSendMockConnection, MockConnection};
use crate::crypto::signed::Signed;
use crate::peer::id::PeerId;
use crate::policy::OpenPolicy;
use crate::sharded_map::ShardedMap;
use crate::storage::MemoryStorage;
use crate::Subduction;
use future_form::Sendable;
use sedimentree_core::{
    blob::{Blob, BlobMeta, Digest},
    commit::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
};
use testresult::TestResult;

fn make_test_commit() -> (LooseCommit, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents.clone());
    let blob_meta = BlobMeta::new(&contents);
    let digest = Digest::from([0u8; 32]);
    let commit = LooseCommit::new(digest, vec![], blob_meta);
    (commit, blob)
}

async fn make_signed_test_commit() -> (Signed<LooseCommit>, Blob) {
    let (commit, blob) = make_test_commit();
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

fn make_test_fragment() -> (Fragment, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents.clone());
    let blob_meta = BlobMeta::new(&contents);
    let head = Digest::from([1u8; 32]);
    let boundary = vec![Digest::from([2u8; 32])];
    let checkpoints = vec![Digest::from([3u8; 32])];
    let fragment = Fragment::new(head, boundary, checkpoints, blob_meta);
    (fragment, blob)
}

async fn make_signed_test_fragment() -> (Signed<Fragment>, Blob) {
    let (fragment, blob) = make_test_fragment();
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
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
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Add a commit - the send will fail
    let id = SedimentreeId::new([1u8; 32]);
    let (commit, blob) = make_test_commit();

    let _ = subduction.add_commit(id, &commit, blob).await;

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
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Add a fragment - the send will fail
    let id = SedimentreeId::new([1u8; 32]);
    let (fragment, blob) = make_test_fragment();

    let _ = subduction.add_fragment(id, &fragment, blob).await;

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
        );

    // Register a failing connection with a different peer ID than the sender
    let sender_peer_id = PeerId::new([1u8; 32]);
    let other_peer_id = PeerId::new([2u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Subscribe other_peer to the sedimentree so forwarding will be attempted
    let id = SedimentreeId::new([1u8; 32]);
    subduction.add_subscription(other_peer_id, id).await;

    // Receive a commit from a different peer - the propagation send will fail
    let (signed_commit, blob) = make_signed_test_commit().await;

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
        );

    // Register a failing connection with a different peer ID than the sender
    let sender_peer_id = PeerId::new([1u8; 32]);
    let other_peer_id = PeerId::new([2u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Subscribe other_peer to the sedimentree so forwarding will be attempted
    let id = SedimentreeId::new([1u8; 32]);
    subduction.add_subscription(other_peer_id, id).await;

    // Receive a fragment from a different peer - the propagation send will fail
    let (signed_fragment, blob) = make_signed_test_fragment().await;

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
        );

    // Register a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.register(conn).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Request blobs - the send will fail
    let digests = vec![Digest::from([1u8; 32])];
    subduction.request_blobs(digests).await;

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
        );

    // Register two connections that will succeed
    let peer_id1 = PeerId::new([1u8; 32]);
    let peer_id2 = PeerId::new([2u8; 32]);
    let conn1 = MockConnection::with_peer_id(peer_id1);
    let conn2 = MockConnection::with_peer_id(peer_id2);

    subduction.register(conn1).await?;
    subduction.register(conn2).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 2);

    // Add a commit - sends will succeed
    let id = SedimentreeId::new([1u8; 32]);
    let (commit, blob) = make_test_commit();

    let _ = subduction.add_commit(id, &commit, blob).await;

    // Both connections should still be registered (sends succeeded)
    assert_eq!(
        subduction.connected_peer_ids().await.len(),
        2,
        "Both connections should remain registered when sends succeed"
    );

    Ok(())
}
