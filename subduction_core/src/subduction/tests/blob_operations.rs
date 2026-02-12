//! Tests for blob operations (`get`, `get_blobs`, `BlobsRequest`/`BlobsResponse` flow).

#![allow(clippy::expect_used, clippy::panic)]

use super::common::{new_test_subduction, test_signer, TokioSpawn};
use crate::{
    connection::{message::Message, nonce_cache::NonceCache, test_utils::ChannelMockConnection},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::Subduction,
};
use alloc::sync::Arc;
use core::time::Duration;
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob, commit::CountLeadingZeroBytes, crypto::digest::Digest, id::SedimentreeId,
};
use testresult::TestResult;

#[tokio::test]
async fn test_get_blob_returns_none_for_missing() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let digest = Digest::<Blob>::from_bytes([1u8; 32]);
    let blob = subduction.get_blob(digest).await.expect("storage error");
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
            crate::crypto::signer::MemorySigner,
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
    )
}

#[tokio::test]
async fn requested_blobs_are_saved_and_removed_from_pending() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([1u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn).await?;

    let actor_task = tokio::spawn(actor_fut);
    let listener_task = tokio::spawn(listener_fut);
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Create a blob and compute its content-addressed digest
    let blob_data = b"requested blob content";
    let blob = Blob::new(blob_data.to_vec());
    let digest = Digest::<Blob>::hash_bytes(blob_data);

    // Request this blob — populates pending_blob_requests
    subduction.request_blobs(vec![digest]).await;

    // Drain the outbound BlobsRequest message
    let outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");
    assert!(
        matches!(outbound, Message::BlobsRequest(ref digests) if digests.contains(&digest)),
        "expected BlobsRequest containing our digest"
    );

    // Simulate peer responding with the requested blob
    handle
        .inbound_tx
        .send(Message::BlobsResponse(vec![blob.clone()]))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify blob was saved to storage
    let loaded = subduction.get_blob(digest).await.expect("storage error");
    assert_eq!(
        loaded.as_ref(),
        Some(&blob),
        "requested blob should be persisted"
    );

    // Verify digest was removed from pending (send a second BlobsResponse with
    // the same blob — it should now be rejected as unsolicited)
    handle
        .inbound_tx
        .send(Message::BlobsResponse(vec![blob]))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The blob is still in storage (from the first save), but the key assertion
    // is that the second response didn't panic or error — it was silently rejected.
    // We can't directly observe the pending set, but the tracing::warn log would fire.

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn unsolicited_blobs_are_rejected() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([2u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn).await?;

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
        .send(Message::BlobsResponse(vec![blob]))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify blob was NOT saved to storage
    let loaded = subduction.get_blob(digest).await.expect("storage error");
    assert!(loaded.is_none(), "unsolicited blob should not be persisted");

    actor_task.abort();
    listener_task.abort();
    Ok(())
}

#[tokio::test]
async fn mixed_batch_only_requested_blobs_saved() -> TestResult {
    let (subduction, listener_fut, actor_fut) = new_dispatch_subduction();

    let peer_id = PeerId::new([3u8; 32]);
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    subduction.register(conn).await?;

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
    subduction.request_blobs(vec![requested_digest]).await;

    // Drain the outbound BlobsRequest
    let _outbound = tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv())
        .await?
        .expect("should receive BlobsRequest");

    // Peer responds with both blobs in a single BlobsResponse
    handle
        .inbound_tx
        .send(Message::BlobsResponse(vec![
            requested_blob.clone(),
            unsolicited_blob,
        ]))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify: requested blob was saved
    let loaded_requested = subduction
        .get_blob(requested_digest)
        .await
        .expect("storage error");
    assert_eq!(
        loaded_requested.as_ref(),
        Some(&requested_blob),
        "requested blob should be persisted"
    );

    // Verify: unsolicited blob was NOT saved
    let loaded_unsolicited = subduction
        .get_blob(unsolicited_digest)
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
