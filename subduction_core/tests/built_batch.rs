//! Tests for `add_built_batch` and `add_built_batch_locally`.
//!
//! Coverage:
//! - empty input is a true no-op (no sedimentree row, no panics)
//! - happy path with mixed commits + fragments stores everything
//! - blob-mismatch is rejected as `WriteError::Io(IoError::BlobMismatch(_))`

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::test_utils::new_test_subduction,
    subduction::error::{IoError, WriteError},
};
use testresult::TestResult;

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

/// Build a `(LooseCommit, Blob)` pair whose `BlobMeta` matches its blob.
fn make_commit(id: SedimentreeId, head: u8, blob_seed: u8) -> (LooseCommit, Blob) {
    let blob = make_blob(blob_seed);
    let blob_meta = BlobMeta::new(&blob);
    let commit = LooseCommit::new(id, CommitId::new([head; 32]), BTreeSet::new(), blob_meta);
    (commit, blob)
}

/// Build a `(Fragment, Blob)` pair whose `BlobMeta` matches its blob.
fn make_fragment(
    id: SedimentreeId,
    head: u8,
    boundary_byte: u8,
    blob_seed: u8,
) -> (Fragment, Blob) {
    let blob = make_blob(blob_seed);
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new([head; 32]);
    let boundary = BTreeSet::from([CommitId::new([boundary_byte; 32])]);
    let fragment = Fragment::new(id, head, boundary, &[], blob_meta);
    (fragment, blob)
}

#[tokio::test]
async fn add_built_batch_locally_empty_is_noop() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([1u8; 32]);

    sd.add_built_batch_locally(sed_id, Vec::new(), Vec::new())
        .await?;

    assert!(
        sd.get_commits(sed_id).await.is_none(),
        "empty batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_built_batch_empty_is_noop_no_broadcast() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([2u8; 32]);

    // No connected peers, no work to do — should be a clean no-op.
    sd.add_built_batch(sed_id, Vec::new(), Vec::new()).await?;

    assert!(
        sd.get_commits(sed_id).await.is_none(),
        "empty batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_built_batch_locally_stores_commits_and_fragments() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([3u8; 32]);

    let commits: Vec<(LooseCommit, Blob)> =
        (0..5).map(|i| make_commit(sed_id, i + 100, i)).collect();
    let fragments: Vec<(Fragment, Blob)> = (0..3)
        .map(|i| make_fragment(sed_id, i + 200, i + 220, i + 240))
        .collect();

    sd.add_built_batch_locally(sed_id, commits, fragments)
        .await?;

    let stored_commits = sd.get_commits(sed_id).await;
    assert_eq!(
        stored_commits.as_ref().map(Vec::len),
        Some(5),
        "all commits should be stored"
    );

    let stored_fragments = sd.get_fragments(sed_id).await;
    assert_eq!(
        stored_fragments.as_ref().map(Vec::len),
        Some(3),
        "all fragments should be stored"
    );

    Ok(())
}

#[tokio::test]
async fn add_built_batch_locally_rejects_mismatched_commit_blob() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([4u8; 32]);

    // Commit's BlobMeta is for the "claimed" blob; we attach a different blob.
    let claimed = make_blob(0xAA);
    let blob_meta = BlobMeta::new(&claimed);
    let commit = LooseCommit::new(
        sed_id,
        CommitId::new([0xC1u8; 32]),
        BTreeSet::new(),
        blob_meta,
    );
    let actual = make_blob(0xBB); // different bytes -> different digest

    let result = sd
        .add_built_batch_locally(sed_id, vec![(commit, actual)], Vec::new())
        .await;

    assert!(
        matches!(result, Err(WriteError::Io(IoError::BlobMismatch(_)))),
        "mismatched blob should produce BlobMismatch, got: {result:?}"
    );

    // Storage must remain untouched (no partial writes from a rejected batch).
    assert!(
        sd.get_commits(sed_id).await.is_none(),
        "rejected batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_built_batch_locally_rejects_mismatched_fragment_blob() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();
    let sed_id = SedimentreeId::new([5u8; 32]);

    let claimed = make_blob(0xCC);
    let blob_meta = BlobMeta::new(&claimed);
    let head = CommitId::new([0xF1u8; 32]);
    let boundary = BTreeSet::from([CommitId::new([0xF2u8; 32])]);
    let fragment = Fragment::new(sed_id, head, boundary, &[], blob_meta);
    let actual = make_blob(0xDD);

    let result = sd
        .add_built_batch_locally(sed_id, Vec::new(), vec![(fragment, actual)])
        .await;

    assert!(
        matches!(result, Err(WriteError::Io(IoError::BlobMismatch(_)))),
        "mismatched blob should produce BlobMismatch, got: {result:?}"
    );

    Ok(())
}
