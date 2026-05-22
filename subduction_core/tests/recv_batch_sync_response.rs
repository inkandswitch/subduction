//! Tests for [`Subduction::recv_batch_sync_response`].
//!
//! These tests directly drive `recv_batch_sync_response` with a populated
//! [`SyncDiff`] and assert that the missing commits and fragments end up
//! in local storage. They specifically guard against putter-cache
//! short-circuit bugs in `subduction_core/src/subduction/ingest.rs` — if
//! either of the `if !putter_cache.contains_key(...)` branches around
//! lines 92 and 136 inverts its sense, no data is ingested and these
//! tests fail.
//!
//! `bidirectional_sync.rs` covers the request/response wire shape but
//! does not assert that the requestor actually persists the response.

#![allow(clippy::expect_used, clippy::panic)]

use core::{future::Future, time::Duration};
use std::{collections::BTreeSet, sync::Arc};

use future_form::Sendable;
use futures::future::Aborted;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::{
    connection::{
        message::{RequestedData, SyncDiff, SyncMessage},
        test_utils::{ChannelMockConnection, InstantTimeout, TokioSpawn, test_signer},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
};
use subduction_crypto::signed::Signed;
use testresult::TestResult;

#[allow(clippy::type_complexity)]
fn make_subduction() -> (
    Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            ChannelMockConnection<SyncMessage>,
            SyncHandler<
                Sendable,
                MemoryStorage,
                ChannelMockConnection<SyncMessage>,
                OpenPolicy,
                CountLeadingZeroBytes,
            >,
            OpenPolicy,
            subduction_crypto::signer::memory::MemorySigner,
            InstantTimeout,
            CountLeadingZeroBytes,
        >,
    >,
    impl Future<Output = Result<(), Aborted>>,
    impl Future<Output = Result<(), Aborted>>,
) {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    (sd, listener, manager)
}

async fn make_test_commit(id: &SedimentreeId, data: &[u8]) -> (Signed<LooseCommit>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new({
        let mut bytes = [0u8; 32];
        let n = data.len().min(32);
        bytes[..n].copy_from_slice(&data[..n]);
        bytes
    });
    let commit = LooseCommit::new(*id, head, BTreeSet::new(), blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
    (verified.into_signed(), blob)
}

async fn make_test_fragment(id: &SedimentreeId, data: &[u8]) -> (Signed<Fragment>, Blob) {
    let blob = Blob::new(data.to_vec());
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new([data.first().copied().unwrap_or(0); 32]);
    let fragment = Fragment::new(*id, head, BTreeSet::new(), &[], blob_meta);
    let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
    (verified.into_signed(), blob)
}

/// `recv_batch_sync_response` must persist `missing_commits` to storage.
///
/// Regression test for the putter-cache branch at
/// `subduction_core/src/subduction/ingest.rs:92`. If the `!` in
/// `if !putter_cache.contains_key(&author_id)` is removed, the cache is
/// never populated and the commit is silently dropped.
#[tokio::test]
async fn recv_batch_sync_response_persists_missing_commits() -> TestResult {
    let (sd, listener, manager) = make_subduction();
    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sed_id = SedimentreeId::new([7u8; 32]);
    let (signed_commit, blob) = make_test_commit(&sed_id, b"recv-batch-commit").await;
    let from = PeerId::new([42u8; 32]);

    let diff = SyncDiff {
        missing_commits: vec![(signed_commit, blob)],
        missing_fragments: Vec::new(),
        requesting: RequestedData::default(),
    };

    sd.recv_batch_sync_response(&from, sed_id, diff).await?;

    let stored = sd.get_commits(sed_id).await;
    assert!(
        stored.is_some(),
        "sedimentree should exist after recv_batch_sync_response"
    );
    let commits = stored.expect("checked above");
    assert_eq!(
        commits.len(),
        1,
        "exactly one commit should have been ingested, got {}",
        commits.len()
    );

    listener_task.abort();
    manager_task.abort();
    Ok(())
}

/// `recv_batch_sync_response` must persist `missing_fragments` to storage.
///
/// Regression test for the putter-cache branch at
/// `subduction_core/src/subduction/ingest.rs:136`. Symmetric to the
/// commits test above.
#[tokio::test]
async fn recv_batch_sync_response_persists_missing_fragments() -> TestResult {
    let (sd, listener, manager) = make_subduction();
    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sed_id = SedimentreeId::new([9u8; 32]);
    let (signed_fragment, blob) = make_test_fragment(&sed_id, b"recv-batch-fragment").await;
    let from = PeerId::new([42u8; 32]);

    let diff = SyncDiff {
        missing_commits: Vec::new(),
        missing_fragments: vec![(signed_fragment, blob)],
        requesting: RequestedData::default(),
    };

    sd.recv_batch_sync_response(&from, sed_id, diff).await?;

    // After ingest, `recv_batch_sync_response` re-minimizes the tree. A
    // single fragment with no covering siblings should still be present.
    let stored = sd.get_fragments(sed_id).await;
    assert!(
        stored.is_some(),
        "sedimentree should exist after recv_batch_sync_response"
    );
    let fragments = stored.expect("checked above");
    assert_eq!(
        fragments.len(),
        1,
        "exactly one fragment should have been ingested, got {}",
        fragments.len()
    );

    listener_task.abort();
    manager_task.abort();
    Ok(())
}

/// Mixed-content diffs must ingest both commits and fragments in one call.
///
/// Covers both putter-cache branches simultaneously and the
/// per-author batching path that flushes via `save_batch`.
#[tokio::test]
async fn recv_batch_sync_response_persists_mixed_diff() -> TestResult {
    let (sd, listener, manager) = make_subduction();
    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sed_id = SedimentreeId::new([11u8; 32]);
    let (signed_commit, commit_blob) = make_test_commit(&sed_id, b"mixed-commit").await;
    let (signed_fragment, fragment_blob) = make_test_fragment(&sed_id, b"mixed-fragment").await;
    let from = PeerId::new([42u8; 32]);

    let diff = SyncDiff {
        missing_commits: vec![(signed_commit, commit_blob)],
        missing_fragments: vec![(signed_fragment, fragment_blob)],
        requesting: RequestedData::default(),
    };

    sd.recv_batch_sync_response(&from, sed_id, diff).await?;

    let commits = sd
        .get_commits(sed_id)
        .await
        .expect("sedimentree should exist");
    let fragments = sd
        .get_fragments(sed_id)
        .await
        .expect("sedimentree should exist");

    assert_eq!(commits.len(), 1, "commit should be ingested");
    assert_eq!(fragments.len(), 1, "fragment should be ingested");

    listener_task.abort();
    manager_task.abort();
    Ok(())
}

/// Empty diffs are no-ops and must not error.
///
/// This is the trivial-baseline case; it does not catch the cache
/// mutants but documents that an empty diff is a valid input.
#[tokio::test]
async fn recv_batch_sync_response_empty_diff_is_noop() -> TestResult {
    let (sd, listener, manager) = make_subduction();
    let listener_task = tokio::spawn(listener);
    let manager_task = tokio::spawn(manager);
    tokio::time::sleep(Duration::from_millis(10)).await;

    let sed_id = SedimentreeId::new([13u8; 32]);
    let from = PeerId::new([42u8; 32]);

    let diff = SyncDiff {
        missing_commits: Vec::new(),
        missing_fragments: Vec::new(),
        requesting: RequestedData::default(),
    };

    sd.recv_batch_sync_response(&from, sed_id, diff).await?;

    // No data was ingested, so the sedimentree should not be created.
    assert!(
        sd.get_commits(sed_id).await.is_none(),
        "empty diff should not create a sedimentree"
    );

    listener_task.abort();
    manager_task.abort();
    Ok(())
}
