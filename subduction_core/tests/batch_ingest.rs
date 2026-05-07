//! Tests for `add_commits_batch` and `add_fragments_batch`.

use std::collections::BTreeSet;

use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::{connection::test_utils::new_test_subduction, subduction::FragmentBatchItem};
use testresult::TestResult;

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

#[tokio::test]
async fn add_commits_batch_stores_all_commits() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([1u8; 32]);
    let commit_count = 10;

    let commits: Vec<(CommitId, BTreeSet<CommitId>, Blob)> = (0..commit_count)
        .map(|i| (CommitId::new([i + 100; 32]), BTreeSet::new(), make_blob(i)))
        .collect();

    sd.add_commits_batch(sed_id, commits).await?;

    let stored = sd.get_commits(sed_id).await;
    assert!(
        stored.is_some(),
        "sedimentree should exist after batch insert"
    );

    let count = stored.as_ref().map(Vec::len);
    assert_eq!(
        count,
        Some(commit_count as usize),
        "all commits should be stored"
    );

    Ok(())
}

#[tokio::test]
async fn add_commits_batch_empty_is_noop() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([2u8; 32]);

    sd.add_commits_batch(sed_id, Vec::new()).await?;

    let stored = sd.get_commits(sed_id).await;
    assert!(
        stored.is_none(),
        "empty batch should not create a sedimentree"
    );

    Ok(())
}

#[tokio::test]
async fn add_fragments_batch_stores_all_fragments() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([3u8; 32]);
    let fragment_count = 7;

    let fragments: Vec<FragmentBatchItem> = (0..fragment_count)
        .map(|i| FragmentBatchItem {
            head: CommitId::new([i + 50; 32]),
            boundary: BTreeSet::from([CommitId::new([i + 150; 32])]),
            checkpoints: Vec::new(),
            blob: make_blob(i),
        })
        .collect();

    sd.add_fragments_batch(sed_id, fragments).await?;

    let stored = sd.get_fragments(sed_id).await;
    assert!(
        stored.is_some(),
        "sedimentree should exist after fragment batch insert"
    );

    let count = stored.as_ref().map(Vec::len);
    assert_eq!(
        count,
        Some(fragment_count as usize),
        "all fragments should be stored"
    );

    Ok(())
}

#[tokio::test]
async fn add_fragments_batch_empty_is_noop() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([4u8; 32]);

    sd.add_fragments_batch(sed_id, Vec::new()).await?;

    let stored = sd.get_fragments(sed_id).await;
    assert!(
        stored.is_none(),
        "empty fragment batch should not create a sedimentree"
    );

    Ok(())
}
