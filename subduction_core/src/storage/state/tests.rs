//! Deterministic handoff and queue-budget checks against the shared tree state.
#![allow(clippy::expect_used, clippy::panic)]
use super::*;
use alloc::collections::BTreeSet;
use future_form::Sendable;
use sedimentree_core::{
    blob::Blob,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
const ID: SedimentreeId = SedimentreeId::new([71; 32]);
async fn item(size: usize) -> StoredItem {
    let blob = Blob::new(alloc::vec![1; size]);
    let commit = LooseCommit::new(ID, CommitId::new([1; 32]), BTreeSet::new(), blob.meta())
        .expect("metadata");
    let signed =
        Signed::seal::<Sendable, _>(&crate::connection::test_utils::test_signer(), commit).await;
    StoredItem::Commit(Arc::new(VerifiedMeta::new(signed, blob).expect("blob")))
}
fn publish(tree: &Arc<TreeState>, items: Option<&[StoredItem]>) {
    tree.begin().finish(items);
}

#[tokio::test]
async fn overlapping_mutations_cannot_clear_a_dirty_handoff() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let old = watch.scan_start().expect("stable");
    let mut guard = tree.begin();
    assert!(watch.scan_start().is_none());
    assert!(!watch.scan_finish(old));
    guard.finish(None);
    drop(guard);
    assert!(!watch.scan_finish(old));
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(
        matches!(watch.take(), Some(StoredEvent::RescanRequired)),
        "invalidation is sticky"
    );
    assert!(watch.scan_finish(watch.scan_start().expect("settled")));
    assert!(watch.take().is_none());
}
#[tokio::test]
async fn count_overflow_is_sticky_and_releases_notification_data() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let item = item(1).await;
    for _ in 0..=MAX_PENDING_ITEMS {
        publish(&tree, Some(core::slice::from_ref(&item)));
    }
    let pending = watch.queue.pending.lock();
    assert!(pending.dirty);
    assert!(pending.items.is_empty());
    assert_eq!(pending.bytes, 0);
    drop(pending);
    publish(&tree, Some(core::slice::from_ref(&item)));
    assert!(
        watch.queue.pending.lock().items.is_empty(),
        "later writes cannot clear invalidation"
    );
    assert!(watch.scan_finish(watch.scan_start().expect("stable")));
    publish(&tree, Some(core::slice::from_ref(&item)));
    assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
}
#[tokio::test]
async fn byte_budget_and_oversized_batches_invalidate_without_notification_clones() {
    use crate::storage::mutation::Operation;
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let huge = item(MAX_PENDING_BYTES + 1).await;
    let StoredItem::Commit(verified) = huge else {
        panic!("commit")
    };
    let operation = Operation::Commit((*verified).clone());
    assert!(operation.items().is_none());
    publish(&tree, operation.items().as_deref());
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    let StoredItem::Commit(small) = item(1).await else {
        panic!("commit")
    };
    assert!(
        Operation::Batch(
            alloc::vec![(*small).clone(); MAX_PENDING_ITEMS + 1],
            Vec::new()
        )
        .items()
        .is_none()
    );
    assert_eq!(watch.queue.pending.lock().bytes, 0);
    assert!(watch.scan_finish(watch.scan_start().expect("stable")));
    let medium = item(MAX_PENDING_BYTES / 2).await;
    publish(&tree, Some(core::slice::from_ref(&medium)));
    assert_eq!(watch.queue.pending.lock().items.len(), 1);
    publish(&tree, Some(core::slice::from_ref(&medium)));
    assert!(
        matches!(watch.take(), Some(StoredEvent::RescanRequired)),
        "byte overflow below the count limit"
    );
    assert!(watch.queue.pending.lock().items.is_empty());
}
#[tokio::test]
async fn watchers_share_items_and_drop_unregisters_and_releases_queues() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let first = state.watch(ID);
    let second = state.watch(ID);
    let item = item(1).await;
    let StoredItem::Commit(value) = &item else {
        panic!("commit")
    };
    publish(&tree, Some(core::slice::from_ref(&item)));
    assert_eq!(
        Arc::strong_count(value),
        3,
        "one metadata/blob allocation across watchers"
    );
    let weak = Arc::downgrade(&first.queue);
    drop(first);
    assert!(weak.upgrade().is_none());
    assert_eq!(Arc::strong_count(value), 2);
    assert_eq!(tree.revision.lock().watchers.len(), 1);
    drop(second);
    assert_eq!(Arc::strong_count(value), 1);
    assert!(tree.revision.lock().watchers.is_empty());
}
#[tokio::test]
async fn peer_markers_follow_covered_items_and_survive_invalidation() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let first = crate::peer::id::PeerId::new([1; 32]);
    let second = crate::peer::id::PeerId::new([2; 32]);
    let item = item(1).await;
    publish(&tree, Some(core::slice::from_ref(&item)));
    watch.peer_synced(first);
    assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
    assert!(matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == first));
    watch.peer_synced(second);
    publish(&tree, None);
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(watch.scan_finish(watch.scan_start().expect("settled recovery")));
    assert!(matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == second));
}

#[tokio::test]
async fn unsettled_driver_loss_closes_all_watchers_without_certifying_a_scan() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let _first = state.watch(ID);
    let _second = state.watch(SedimentreeId::new([72; 32]));
    drop(tree.begin());
    assert!(state.closed());
    state.wait_closed().await;
}
