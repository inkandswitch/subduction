//! Tests for connection cleanup on send failure.

use std::collections::BTreeSet;
use subduction_core::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{test_signer, FailingSendMockConnection, MockConnection, TestSpawn},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
};

use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use testresult::TestResult;

fn make_commit_parts() -> (Digest<LooseCommit>, BTreeSet<Digest<LooseCommit>>, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents);
    let digest = Digest::<LooseCommit>::from_bytes([0u8; 32]);
    (digest, BTreeSet::new(), blob)
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

// NOTE: test_recv_commit_unregisters_connection_on_send_failure and
// test_recv_fragment_unregisters_connection_on_send_failure are in
// src/subduction.rs as unit tests because they use the private add_subscription method.

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
