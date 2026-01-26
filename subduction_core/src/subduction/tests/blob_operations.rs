//! Tests for blob operations (`get`, `get_blobs`).

use super::common::new_test_subduction;
use sedimentree_core::{blob::Blob, digest::Digest, id::SedimentreeId};

#[tokio::test]
async fn test_get_blob_returns_none_for_missing() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let digest = Digest::<Blob>::from_bytes([1u8; 32]);
    #[allow(clippy::unwrap_used)]
    let blob = subduction.get_blob(digest).await.unwrap();
    assert!(blob.is_none());
}

#[tokio::test]
async fn test_get_blobs_returns_none_for_missing_tree() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    #[allow(clippy::unwrap_used)]
    let blobs = subduction.get_blobs(id).await.unwrap();
    assert!(blobs.is_none());
}
