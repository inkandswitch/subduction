//! Tests for sedimentree operations (add, get, remove).

use sedimentree_core::{id::SedimentreeId, sedimentree::Sedimentree};
use subduction_core::connection::test_utils::new_test_subduction;
use testresult::TestResult;

#[tokio::test]
async fn test_sedimentree_ids_returns_empty_initially() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let ids = subduction.sedimentree_ids().await;
    assert_eq!(ids.len(), 0);
}

#[tokio::test]
async fn test_add_sedimentree_increases_count() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    subduction.add_sedimentree(id, tree, blobs).await?;

    let ids = subduction.sedimentree_ids().await;
    assert_eq!(ids.len(), 1);
    assert!(ids.contains(&id));

    Ok(())
}

#[tokio::test]
async fn test_get_commits_returns_none_for_missing_tree() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let commits = subduction.get_commits(id).await;
    assert!(commits.is_none());
}

#[tokio::test]
async fn test_get_commits_returns_empty_for_empty_tree() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    subduction.add_sedimentree(id, tree, blobs).await?;

    let commits = subduction.get_commits(id).await;
    assert_eq!(commits, Some(Vec::new()));

    Ok(())
}

#[tokio::test]
async fn test_get_fragments_returns_none_for_missing_tree() {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let fragments = subduction.get_fragments(id).await;
    assert!(fragments.is_none());
}

#[tokio::test]
async fn test_get_fragments_returns_empty_for_empty_tree() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    subduction.add_sedimentree(id, tree, blobs).await?;

    let fragments = subduction.get_fragments(id).await;
    assert_eq!(fragments, Some(Vec::new()));

    Ok(())
}

#[tokio::test]
async fn test_remove_sedimentree_removes_from_ids() -> TestResult {
    let (subduction, _listener_fut, _actor_fut) = new_test_subduction();

    let id = SedimentreeId::new([1u8; 32]);
    let tree = Sedimentree::default();
    let blobs = Vec::new();

    subduction.add_sedimentree(id, tree, blobs).await?;
    assert_eq!(subduction.sedimentree_ids().await.len(), 1);

    subduction.remove_sedimentree(id).await?;
    assert_eq!(subduction.sedimentree_ids().await.len(), 0);

    Ok(())
}
