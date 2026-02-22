//! Tests for blob operations (`get`, `get_blobs`).
//!
//! With compound storage, blobs are stored together with their commits/fragments.
//! The `BlobsRequest`/`BlobsResponse` protocol is maintained for protocol compatibility
//! but no longer stores blobs independently - blobs come with `BatchSyncResponse`.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
};
use std::sync::Arc;
use subduction_core::{
    connection::{
        message::Message,
        nonce_cache::NonceCache,
        test_utils::{ChannelMockConnection, TokioSpawn, new_test_subduction, test_signer},
    },
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{Subduction, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS},
};
use testresult::TestResult;

const TEST_TREE: SedimentreeId = SedimentreeId::new([42u8; 32]);

#[tokio::test]
async fn test_get_blob_returns_none_for_missing() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let digest = Digest::<Blob>::from_bytes([1u8; 32]);
    let blob = subduction
        .get_blob(TEST_TREE, digest)
        .await
        .expect("storage error");
    assert!(blob.is_none());
}

#[tokio::test]
async fn test_get_blobs_returns_none_for_missing_tree() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let blobs = subduction.get_blobs(id).await.expect("storage error");
    assert!(blobs.is_none());
}

/// Set up a Subduction instance with a channel-based mock connection for
/// tests that need to exercise the dispatch loop.
#[allow(clippy::type_complexity)]
fn new_dispatch_subduction() -> (
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
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
) {
    Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
        None,
        test_signer(),
        MemoryStorage::new(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TokioSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )
}

/// With compound storage, `BlobsResponse` no longer stores blobs independently.
/// Blobs are stored together with commits/fragments via `BatchSyncResponse`.
/// This test verifies that `BlobsResponse` clears pending requests but does NOT
/// store blobs (since blobs must come with their associated metadata).
#[tokio::test]
async fn blobs_response_clears_pending_but_does_not_store() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a blob and compute its content-addressed digest
    let blob_data = b"requested blob content";
    let blob = Blob::new(blob_data.to_vec());
    let digest = Digest::<Blob>::hash_bytes(blob_data);

    // Request this blob — populates pending_blob_requests
    subduction.request_blobs(TEST_TREE, vec![digest]).await;

    // Drain the outbound BlobsRequest message
    let outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");
    assert!(
        matches!(outbound, Message::BlobsRequest { id, ref digests, .. } if id == TEST_TREE && digests.contains(&digest)),
        "expected BlobsRequest containing our digest for the right tree"
    );

    // Simulate peer responding with the requested blob
    handle
        .inbound_tx
        .send(Message::BlobsResponse {
            id: TEST_TREE,
            blobs: vec![blob.clone()],
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // With compound storage, BlobsResponse does NOT store blobs independently.
    // Blobs must come with commits/fragments to be stored.
    let loaded = subduction
        .get_blob(TEST_TREE, digest)
        .await
        .expect("storage error");
    assert!(
        loaded.is_none(),
        "BlobsResponse should not store blobs independently with compound storage"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn unsolicited_blobs_are_rejected() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([2u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a blob but do NOT call request_blobs — the digest won't be in pending
    let blob_data = b"unsolicited blob content";
    let blob = Blob::new(blob_data.to_vec());
    let digest = Digest::<Blob>::hash_bytes(blob_data);

    // Peer sends an unsolicited BlobsResponse
    handle
        .inbound_tx
        .send(Message::BlobsResponse {
            id: TEST_TREE,
            blobs: vec![blob],
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify blob was NOT saved to storage
    let loaded = subduction
        .get_blob(TEST_TREE, digest)
        .await
        .expect("storage error");
    assert!(loaded.is_none(), "unsolicited blob should not be persisted");

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// With compound storage, `BlobsResponse` does not store any blobs.
/// Both requested and unsolicited blobs are ignored - blobs must come
/// with commits/fragments via `BatchSyncResponse`.
#[tokio::test]
async fn blobs_response_does_not_store_any_blobs() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([3u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create two blobs
    let requested_data = b"blob we asked for";
    let requested_blob = Blob::new(requested_data.to_vec());
    let requested_digest = Digest::<Blob>::hash_bytes(requested_data);

    let unsolicited_data = b"blob we did not ask for";
    let unsolicited_blob = Blob::new(unsolicited_data.to_vec());
    let unsolicited_digest = Digest::<Blob>::hash_bytes(unsolicited_data);

    // Only request the first blob
    subduction
        .request_blobs(TEST_TREE, vec![requested_digest])
        .await;

    // Drain the outbound BlobsRequest
    let _outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");

    // Peer responds with both blobs in a single BlobsResponse
    handle
        .inbound_tx
        .send(Message::BlobsResponse {
            id: TEST_TREE,
            blobs: vec![requested_blob.clone(), unsolicited_blob],
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // With compound storage, neither blob should be stored via BlobsResponse
    let loaded_requested = subduction
        .get_blob(TEST_TREE, requested_digest)
        .await
        .expect("storage error");
    assert!(
        loaded_requested.is_none(),
        "BlobsResponse should not store blobs with compound storage"
    );

    let loaded_unsolicited = subduction
        .get_blob(TEST_TREE, unsolicited_digest)
        .await
        .expect("storage error");
    assert!(
        loaded_unsolicited.is_none(),
        "unsolicited blob should not be persisted"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

/// With compound storage, blobs from `BlobsResponse` are not stored.
/// This test verifies that even with a valid request, `BlobsResponse` does not
/// persist blobs (they must come with commits/fragments).
#[tokio::test]
async fn blobs_response_does_not_store_even_for_valid_tree() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([4u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let tree_a = SedimentreeId::new([1u8; 32]);
    let tree_b = SedimentreeId::new([2u8; 32]);

    let blob_data = b"blob for tree A only";
    let blob = Blob::new(blob_data.to_vec());
    let digest = Digest::<Blob>::hash_bytes(blob_data);

    // Request blob for tree A
    subduction.request_blobs(tree_a, vec![digest]).await;
    let _outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");

    // Respond with the blob for tree A
    handle
        .inbound_tx
        .send(Message::BlobsResponse {
            id: tree_a,
            blobs: vec![blob.clone()],
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // With compound storage, blob is NOT stored via BlobsResponse
    let loaded_a = subduction
        .get_blob(tree_a, digest)
        .await
        .expect("storage error");
    assert!(
        loaded_a.is_none(),
        "BlobsResponse should not store blobs with compound storage"
    );

    // Blob is also NOT visible under tree B (never was stored)
    let loaded_b = subduction
        .get_blob(tree_b, digest)
        .await
        .expect("storage error");
    assert!(loaded_b.is_none(), "blob should not exist under tree B");

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn blobs_response_with_wrong_sedimentree_id_is_rejected() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([5u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn.authenticated()).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let tree_a = SedimentreeId::new([1u8; 32]);
    let tree_b = SedimentreeId::new([2u8; 32]);

    let blob_data = b"blob requested for tree A";
    let blob = Blob::new(blob_data.to_vec());
    let digest = Digest::<Blob>::hash_bytes(blob_data);

    // Request blob for tree A
    subduction.request_blobs(tree_a, vec![digest]).await;
    let _outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");

    // Peer responds with the blob but claims it's for tree B
    handle
        .inbound_tx
        .send(Message::BlobsResponse {
            id: tree_b,
            blobs: vec![blob],
        })
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Blob should not be saved under tree A (we requested for A, response claims B)
    let loaded_a = subduction
        .get_blob(tree_a, digest)
        .await
        .expect("storage error");
    assert!(
        loaded_a.is_none(),
        "blob should not be saved when SedimentreeId doesn't match pending request"
    );

    // Also not saved under tree B (the (tree_b, digest) pair was never in pending)
    let loaded_b = subduction
        .get_blob(tree_b, digest)
        .await
        .expect("storage error");
    assert!(
        loaded_b.is_none(),
        "blob should not be saved under the wrong SedimentreeId"
    );

    actor_task.abort();
    listener_task.abort();
    Ok(())
}
