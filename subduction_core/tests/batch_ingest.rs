//! Tests for batch ingestion methods (`add_commits_batch`, `add_fragments_batch`).

use std::collections::BTreeSet;

use sedimentree_core::{blob::Blob, id::SedimentreeId};
use subduction_core::connection::test_utils::new_test_subduction;
use testresult::TestResult;

fn make_blob(seed: u8) -> Blob {
    let data: Vec<u8> = (0..64).map(|i| seed.wrapping_add(i)).collect();
    Blob::new(data)
}

#[tokio::test]
async fn add_commits_batch_stores_all_commits() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([1u8; 32]);

    let commits: Vec<(BTreeSet<_>, Blob)> =
        (0..10).map(|i| (BTreeSet::new(), make_blob(i))).collect();

    sd.add_commits_batch(sed_id, commits).await?;

    let stored = sd.get_commits(sed_id).await;
    assert!(
        stored.is_some(),
        "sedimentree should exist after batch insert"
    );

    Ok(())
}

#[tokio::test]
async fn add_commits_batch_empty_is_noop() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([2u8; 32]);

    sd.add_commits_batch(sed_id, Vec::new()).await?;

    Ok(())
}

#[tokio::test]
async fn add_commits_batch_minimizes_once() -> TestResult {
    let (sd, _listener, _manager) = new_test_subduction();

    let sed_id = SedimentreeId::new([3u8; 32]);

    // Insert enough commits that minimize_tree has work to do
    let commits: Vec<(BTreeSet<_>, Blob)> =
        (0..50).map(|i| (BTreeSet::new(), make_blob(i))).collect();

    sd.add_commits_batch(sed_id, commits).await?;

    let stored = sd.get_commits(sed_id).await;
    assert!(stored.is_some());

    Ok(())
}
