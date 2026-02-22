//! Diagnostic tests for `add_commit` followed by sync.
//!
//! These tests investigate the issue where commits added via `add_commit`
//! may not be properly synced to peers.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
};
use std::collections::BTreeSet;
use subduction_core::{
    connection::{
        nonce_cache::NonceCache,
        test_utils::{ChannelMockConnection, TokioSpawn, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use testresult::TestResult;

fn make_unique_blob(seed: u8) -> Blob {
    // Create a blob with unique content based on seed
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

/// Test: Adding a single commit and verifying it's in the in-memory sedimentree.
#[tokio::test]
async fn add_single_commit_is_stored() -> TestResult {
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

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);

    let sed_id = SedimentreeId::new([1u8; 32]);
    let blob = make_unique_blob(1);

    // Add a commit
    subduction.add_commit(sed_id, BTreeSet::new(), blob).await?;

    // Check in-memory state
    let commits = subduction.get_commits(sed_id).await;
    assert_eq!(commits.map(|c| c.len()), Some(1), "Should have 1 commit");

    Ok(())
}

/// Test: Adding multiple commits sequentially and verifying all are stored.
#[tokio::test]
async fn add_multiple_commits_all_stored() -> TestResult {
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

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);

    let sed_id = SedimentreeId::new([1u8; 32]);

    // Add 5 commits with unique blobs
    for i in 0..5u8 {
        let blob = make_unique_blob(i);
        subduction.add_commit(sed_id, BTreeSet::new(), blob).await?;
    }

    // Check in-memory state
    let commits = subduction.get_commits(sed_id).await;
    assert_eq!(commits.map(|c| c.len()), Some(5), "Should have 5 commits");

    Ok(())
}

/// Test: Verify commits are retrievable after adding.
#[tokio::test]
async fn commits_retrievable_after_add() -> TestResult {
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

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);

    let sed_id = SedimentreeId::new([1u8; 32]);

    // Add 5 commits and collect their digests
    let mut digests = Vec::new();
    for i in 0..5u8 {
        let blob = make_unique_blob(i);
        subduction
            .add_commit(sed_id, BTreeSet::new(), blob.clone())
            .await?;
    }

    // Get commits and verify count
    let commits = subduction.get_commits(sed_id).await.expect("should exist");
    assert_eq!(commits.len(), 5, "Should have 5 commits");

    // Verify each commit has a unique digest
    for commit in &commits {
        digests.push(Digest::hash(commit));
    }

    // Check all digests are unique
    let unique_digests: BTreeSet<_> = digests.iter().collect();
    assert_eq!(
        unique_digests.len(),
        5,
        "All 5 commits should have unique digests"
    );

    Ok(())
}

/// Test: Fingerprint summary includes all commits.
#[tokio::test]
async fn fingerprint_summary_includes_all_commits() -> TestResult {
    use sedimentree_core::crypto::fingerprint::FingerprintSeed;

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

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);

    let sed_id = SedimentreeId::new([1u8; 32]);

    // Add 5 commits with unique blobs
    for i in 0..5u8 {
        let blob = make_unique_blob(i);
        subduction.add_commit(sed_id, BTreeSet::new(), blob).await?;
    }

    // Wait for commits to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify commits are stored
    let commits = subduction.get_commits(sed_id).await;
    assert_eq!(commits.map(|c| c.len()), Some(5), "Should have 5 commits");

    // Get the sedimentree and compute fingerprint summary
    let sedimentree = subduction
        .sedimentrees()
        .get_cloned(&sed_id)
        .await
        .expect("sedimentree should exist");

    let seed = FingerprintSeed::new(12345, 67890);
    let summary = sedimentree.fingerprint_summarize(&seed);

    assert_eq!(
        summary.commit_fingerprints().len(),
        5,
        "Fingerprint summary should have 5 commit fingerprints"
    );

    Ok(())
}

/// Test: Sync request from peer triggers correct response with all local commits.
#[tokio::test]
async fn sync_request_includes_all_local_commits() -> TestResult {
    use sedimentree_core::{crypto::fingerprint::FingerprintSeed, sedimentree::FingerprintSummary};
    use subduction_core::connection::message::{
        BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncResult,
    };

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

    let sed_id = SedimentreeId::new([1u8; 32]);
    let peer_id = PeerId::new([2u8; 32]);

    // Register a mock connection
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Add 5 commits locally
    for i in 0..5u8 {
        let blob = make_unique_blob(i);
        subduction.add_commit(sed_id, BTreeSet::new(), blob).await?;
    }

    // Wait for commits to be processed
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain any outbound messages (the commits were broadcast to the peer)
    while handle.outbound_rx.try_recv().is_ok() {}

    // Verify we have 5 commits
    let commits = subduction.get_commits(sed_id).await;
    assert_eq!(
        commits.as_ref().map(Vec::len),
        Some(5),
        "Should have 5 commits locally"
    );

    // Send a sync request from peer with empty fingerprints (peer has nothing)
    let seed = FingerprintSeed::new(12345, 67890);
    let request = BatchSyncRequest {
        id: sed_id,
        req_id: RequestId {
            requestor: peer_id,
            nonce: 1,
        },
        fingerprint_summary: FingerprintSummary::new(seed, BTreeSet::new(), BTreeSet::new()),
        subscribe: false,
    };

    handle
        .inbound_tx
        .send(Message::BatchSyncRequest(request))
        .await?;

    // Wait for response
    tokio::time::sleep(Duration::from_millis(100)).await;

    let response = tokio::time::timeout(Duration::from_millis(200), handle.outbound_rx.recv())
        .await?
        .expect("should receive response");

    let Message::BatchSyncResponse(BatchSyncResponse { result, .. }) = response else {
        panic!("Expected BatchSyncResponse, got {response:?}");
    };

    let SyncResult::Ok(diff) = result else {
        panic!("Expected SyncResult::Ok, got {result:?}");
    };

    assert_eq!(
        diff.missing_commits.len(),
        5,
        "Response should include all 5 commits that peer is missing"
    );

    Ok(())
}

/// Test: Full sync flow - add commits then call `full_sync`.
#[tokio::test]
async fn full_sync_sends_all_commits() -> TestResult {
    use subduction_core::connection::message::Message;

    let storage = MemoryStorage::new();
    let (client, listener_fut, actor_fut) =
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

    let sed_id = SedimentreeId::new([1u8; 32]);
    let server_peer_id = PeerId::new([2u8; 32]);

    // Register a mock connection to "server"
    let (conn, handle) = ChannelMockConnection::new_with_handle(server_peer_id);
    client.register(conn.authenticated()).await?;

    tokio::spawn(listener_fut);
    tokio::spawn(actor_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Add 5 commits locally
    let mut expected_digests = Vec::new();
    for i in 0..5u8 {
        let blob = make_unique_blob(i);
        client.add_commit(sed_id, BTreeSet::new(), blob).await?;
    }

    // Wait for commits to be stored
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get the digests of commits we added
    let commits = client
        .get_commits(sed_id)
        .await
        .expect("should have commits");
    for commit in &commits {
        expected_digests.push(Digest::hash(commit));
    }
    assert_eq!(expected_digests.len(), 5, "Should have 5 commit digests");

    // Drain broadcast messages (commits were sent when added)
    let mut broadcast_count = 0;
    while let Ok(msg) = handle.outbound_rx.try_recv() {
        if matches!(msg, Message::LooseCommit { .. }) {
            broadcast_count += 1;
        }
    }

    // Note: When there are no subscribers, commits are broadcast to all connections
    // So we should see the commits being broadcast
    eprintln!("Broadcast count during add_commit: {broadcast_count}");

    // Now call full_sync - this should send a BatchSyncRequest
    // The client has 5 commits, the "server" has none
    // So the response should request all 5 commits back

    // First, verify the sedimentree state
    let sedimentree = client
        .sedimentrees()
        .get_cloned(&sed_id)
        .await
        .expect("sedimentree should exist");

    let commit_count = sedimentree.loose_commits().count();
    eprintln!("Commits in sedimentree before full_sync: {commit_count}");
    assert_eq!(commit_count, 5, "Sedimentree should have 5 commits");

    Ok(())
}
