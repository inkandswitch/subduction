//! Tests for connection cleanup on send failure.

use core::time::Duration;
use std::{collections::BTreeSet, sync::Arc};

use async_lock::Mutex;
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, collections::Map, commit::CountLeadingZeroBytes, crypto::digest::Digest,
    id::SedimentreeId, loose_commit::id::CommitId,
};
use subduction_core::{
    connection::test_utils::{FailingSendMockConnection, InstantTimeout, TestSpawn, test_signer},
    handler::sync::SyncHandler,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    subduction::{
        Subduction,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
    },
};
use testresult::TestResult;

fn make_commit_parts() -> (CommitId, BTreeSet<CommitId>, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents);
    (CommitId::new([0xCC; 32]), BTreeSet::new(), blob)
}

#[allow(clippy::type_complexity)]
fn make_fragment_parts() -> (CommitId, BTreeSet<CommitId>, Vec<CommitId>, Blob) {
    let contents = vec![0u8; 32];
    let blob = Blob::new(contents);
    let head = CommitId::new([1u8; 32]);
    let boundary = BTreeSet::from([CommitId::new([2u8; 32])]);
    let checkpoints = vec![CommitId::new([3u8; 32])];
    (head, boundary, checkpoints, blob)
}

#[tokio::test]
async fn test_add_commit_unregisters_connection_on_send_failure() -> TestResult {
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

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            PeerCounter::default(),
            NonceCache::default(),
            InstantTimeout,
            Duration::from_secs(30),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    // Add a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.add_connection(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Add a commit - the send will fail
    let id = SedimentreeId::new([1u8; 32]);
    let (head, parents, blob) = make_commit_parts();

    let _ = subduction.add_commit(id, head, parents, blob).await;

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

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            PeerCounter::default(),
            NonceCache::default(),
            InstantTimeout,
            Duration::from_secs(30),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    // Add a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.add_connection(conn.authenticated()).await?;
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

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            PeerCounter::default(),
            NonceCache::default(),
            InstantTimeout,
            Duration::from_secs(30),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    // Add a failing connection
    let peer_id = PeerId::new([1u8; 32]);
    let conn = FailingSendMockConnection::with_peer_id(peer_id);
    let _fresh = subduction.add_connection(conn.authenticated()).await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 1);

    // Request blobs - the send will fail
    let digests = vec![Digest::<Blob>::force_from_bytes([1u8; 32])];
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

    let (subduction, _listener_fut, _actor_fut) =
        Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
            handler,
            None,
            test_signer(),
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending,
            PeerCounter::default(),
            NonceCache::default(),
            InstantTimeout,
            Duration::from_secs(30),
            CountLeadingZeroBytes,
            TestSpawn,
        );

    // Register a MIX: peer 1's connection fails on send, peer 2's succeeds.
    // (Same connection type with a per-instance failure flag, so both can
    // live in this monomorphic `Subduction<FailingSendMockConnection>`.)
    let failing_peer = PeerId::new([1u8; 32]);
    let healthy_peer = PeerId::new([2u8; 32]);
    let failing_conn = FailingSendMockConnection::with_peer_id_failing(failing_peer, true);
    let healthy_conn = FailingSendMockConnection::with_peer_id_failing(healthy_peer, false);

    subduction
        .add_connection(failing_conn.authenticated())
        .await?;
    subduction
        .add_connection(healthy_conn.authenticated())
        .await?;
    assert_eq!(subduction.connected_peer_ids().await.len(), 2);

    // Broadcast a commit: the send to `failing_peer` errors, to `healthy_peer`
    // succeeds.
    let id = SedimentreeId::new([1u8; 32]);
    let (head, parents, blob) = make_commit_parts();
    let _ = subduction.add_commit(id, head, parents, blob).await;

    // ONLY the failing connection should have been unregistered; the healthy
    // one must remain. Assert by peer identity, not just count.
    let remaining = subduction.connected_peer_ids().await;
    assert!(
        !remaining.contains(&failing_peer),
        "the connection whose send failed must be unregistered; remaining={remaining:?}"
    );
    assert!(
        remaining.contains(&healthy_peer),
        "the connection whose send succeeded must remain; remaining={remaining:?}"
    );
    assert_eq!(
        remaining.len(),
        1,
        "exactly one (the healthy) connection should remain"
    );

    Ok(())
}
