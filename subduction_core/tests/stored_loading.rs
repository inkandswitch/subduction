//! Local-first loading and scoped peer barriers on the shared stored stream.
#![allow(clippy::expect_used, clippy::panic)]
mod stored_loading_support;
use futures::StreamExt;
use sedimentree_core::{blob::Blob, id::SedimentreeId, loose_commit::id::CommitId};
use std::{collections::BTreeSet, time::Duration};
use subduction_core::{
    connection::test_utils::new_test_subduction,
    stored::{LoadError, StoredEvent, StoredItem, StoredUpdates},
};
async fn next(stream: &mut StoredUpdates) -> StoredEvent {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("event deadline")
        .expect("ongoing stream")
        .expect("event")
}
#[tokio::test]
async fn cached_loading_uses_local_contents_without_peer_markers() {
    let (node, _listener, _manager) = new_test_subduction();
    let id = SedimentreeId::new([20; 32]);
    node.store_commit(
        id,
        CommitId::new([1; 32]),
        BTreeSet::new(),
        Blob::new(vec![1]),
    )
    .await
    .expect("seed");
    let mut stream = node.load_and_watch(id);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::Item(StoredItem::Commit(_))
    ));
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    node.store_fragment(
        id,
        CommitId::new([2; 32]),
        BTreeSet::new(),
        &[],
        Blob::new(vec![2]),
    )
    .await
    .expect("ongoing write");
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::Item(StoredItem::Fragment(_))
    ));
    node.shutdown();
    assert!(matches!(stream.next().await, Some(Err(LoadError::Closed))));
}
#[tokio::test]
async fn empty_storage_keeps_waiting_until_cancelled() {
    let (node, _listener, _manager) = new_test_subduction();
    let mut stream = node.load_and_watch(SedimentreeId::new([21; 32]));
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    stream.close();
    assert!(stream.next().await.is_none());
    node.shutdown();
}
