//! Tests for local blob reads (`get_blob`, `get_blobs`).
//!
//! With compound storage, blobs are stored together with their
//! commits/fragments and are read back locally via `get_blob` / `get_blobs`.

#![allow(clippy::expect_used, clippy::panic)]

use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};
use subduction_core::connection::test_utils::new_test_subduction;

const TEST_TREE: SedimentreeId = SedimentreeId::new([42u8; 32]);

#[tokio::test]
async fn test_get_blob_returns_none_for_missing() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let digest = Digest::<Blob>::force_from_bytes([1u8; 32]);
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
