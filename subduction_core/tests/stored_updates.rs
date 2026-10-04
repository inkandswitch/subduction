//! Storage-only observation does not depend on a listener or peer lifecycle.
#![allow(clippy::expect_used, clippy::panic)]
use future_form::Sendable;
use futures::{FutureExt, StreamExt};
use sedimentree_core::{
    blob::Blob,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use subduction_core::{
    connection::{
        message::{RequestedData, SyncDiff},
        test_utils::{
            InstantTimeout, MockConnection, TokioSpawn, new_test_subduction, test_signer,
        },
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    stored::{LoadError, StoredEvent, StoredItem, StoredUpdates},
    subduction::Subduction,
};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
type Node = Subduction<
    'static,
    Sendable,
    MemoryStorage,
    MockConnection,
    SyncHandler<
        Sendable,
        MemoryStorage,
        MockConnection,
        OpenPolicy,
        CountLeadingZeroBytes,
        TokioSpawn,
    >,
    OpenPolicy,
    MemorySigner,
    InstantTimeout,
    TokioSpawn,
    CountLeadingZeroBytes,
>;
const ID: SedimentreeId = SedimentreeId::new([81; 32]);
fn node() -> Arc<Node> {
    let (node, _listener, _manager) = new_test_subduction();
    node
}
async fn store(node: &Node, n: u64, size: usize) {
    let mut head = [1; 32];
    head[..8].copy_from_slice(&n.to_le_bytes());
    node.store_commit(
        ID,
        CommitId::new(head),
        BTreeSet::new(),
        Blob::new(vec![1; size]),
    )
    .await
    .expect("store");
}
async fn next(stream: &mut StoredUpdates) -> StoredEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("driver wake")
        .expect("ongoing stream")
        .expect("event")
}
async fn checked(stream: &mut StoredUpdates) -> usize {
    let mut count = 0;
    loop {
        match next(stream).await {
            StoredEvent::Item(_) => count += 1,
            StoredEvent::StorageChecked => return count,
            StoredEvent::RescanRequired => count = 0,
            StoredEvent::PeerSynced { .. } => panic!("storage-only watching initiates no rounds"),
        }
    }
}
#[tokio::test]
async fn catch_up_then_live_updates_share_one_ongoing_stream() {
    let node = node();
    store(&node, 1, 3).await;
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 1);
    store(&node, 2, 4).await;
    let StoredEvent::Item(StoredItem::Commit(value)) = next(&mut stream).await else {
        panic!("live commit")
    };
    assert_eq!(value.blob().as_slice(), &[1; 4]);
    assert!(
        stream.next().now_or_never().is_none(),
        "markers do not end observation"
    );
}
#[tokio::test]
async fn empty_catch_up_does_not_register_storage_and_later_writes_arrive() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    assert!(node.get_commits(ID).await.is_none());
    store(&node, 1, 1).await;
    assert!(matches!(next(&mut stream).await, StoredEvent::Item(_)));
}
#[tokio::test]
async fn registration_before_scheduling_covers_an_immediate_write() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    store(&node, 1, 1).await;
    // Either initial replay or queued live delivery covers this write.
    loop {
        match next(&mut stream).await {
            StoredEvent::Item(_) => break,
            StoredEvent::StorageChecked | StoredEvent::RescanRequired => {}
            StoredEvent::PeerSynced { .. } => panic!("no peer work"),
        }
    }
}
#[tokio::test]
async fn accepted_peer_response_items_use_the_same_stream() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    let blob = Blob::new(vec![3; 4]);
    let commit =
        LooseCommit::new(ID, CommitId::new([3; 32]), BTreeSet::new(), blob.meta()).expect("commit");
    let commit = Signed::seal::<Sendable, _>(&test_signer(), commit)
        .await
        .into_signed();
    let fragment_blob = Blob::new(vec![4; 5]);
    let fragment = Fragment::new(
        ID,
        CommitId::new([4; 32]),
        BTreeSet::new(),
        &[],
        fragment_blob.meta(),
    )
    .expect("fragment");
    let fragment = Signed::seal::<Sendable, _>(&test_signer(), fragment)
        .await
        .into_signed();
    node.recv_batch_sync_response(
        &PeerId::new([99; 32]),
        ID,
        SyncDiff {
            missing_commits: vec![(commit, blob)],
            missing_fragments: vec![(fragment, fragment_blob)],
            requesting: RequestedData::default(),
        },
    )
    .await
    .expect("accepted response");
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::Item(StoredItem::Commit(_))
    ));
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::Item(StoredItem::Fragment(_))
    ));
}
#[tokio::test]
async fn deletion_recovers_an_empty_view_and_keeps_watching() {
    let node = node();
    store(&node, 1, 1).await;
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 1);
    node.remove_sedimentree(ID).await.expect("delete");
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::RescanRequired
    ));
    assert_eq!(checked(&mut stream).await, 0);
    store(&node, 2, 1).await;
    assert!(matches!(next(&mut stream).await, StoredEvent::Item(_)));
}
#[tokio::test]
async fn a_slow_consumer_recovers_after_count_overflow() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    // One occupied delivery slot plus 256 pending items cannot retain these.
    for n in 0..260 {
        store(&node, n, 1).await;
    }
    loop {
        match next(&mut stream).await {
            StoredEvent::RescanRequired => break,
            StoredEvent::Item(_) => {}
            StoredEvent::StorageChecked | StoredEvent::PeerSynced { .. } => {
                panic!("recovery must precede its marker")
            }
        }
    }
    assert_eq!(checked(&mut stream).await, 260);
}
#[tokio::test]
async fn oversized_live_items_recover_without_being_dropped_from_the_view() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    store(&node, 1, 4 * 1024 * 1024 + 1).await;
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::RescanRequired
    ));
    let StoredEvent::Item(StoredItem::Commit(value)) = next(&mut stream).await else {
        panic!("snapshot allows large items")
    };
    assert_eq!(value.blob().as_slice().len(), 4 * 1024 * 1024 + 1);
    assert!(matches!(
        next(&mut stream).await,
        StoredEvent::StorageChecked
    ));
}
#[tokio::test]
async fn explicit_close_is_local_to_one_watcher() {
    let node = node();
    let mut first = node.watch_stored(ID);
    let mut second = node.watch_stored(ID);
    assert_eq!(checked(&mut first).await, 0);
    assert_eq!(checked(&mut second).await, 0);
    first.close();
    assert!(first.next().await.is_none());
    store(&node, 1, 1).await;
    assert!(matches!(next(&mut second).await, StoredEvent::Item(_)));
}
#[tokio::test]
async fn shutdown_wakes_pending_reads_and_rejects_new_watchers() {
    let node = node();
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    assert!(stream.next().now_or_never().is_none());
    node.shutdown();
    assert!(matches!(stream.next().await, Some(Err(LoadError::Closed))));
    assert!(stream.next().await.is_none());
    assert!(matches!(
        node.watch_stored(ID).next().await,
        Some(Err(LoadError::Closed))
    ));
}
#[tokio::test]
async fn storage_only_watchers_do_not_keep_the_node_alive() {
    let node = node();
    let weak = Arc::downgrade(&node);
    let mut stream = node.watch_stored(ID);
    assert_eq!(checked(&mut stream).await, 0);
    drop(node);
    assert!(weak.upgrade().is_none());
    assert!(matches!(stream.next().await, Some(Err(LoadError::Closed))));
}
