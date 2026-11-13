//! Deterministic handoff and queue-budget checks against the shared tree state.
#![allow(clippy::expect_used, clippy::panic)]
use super::*;
use alloc::collections::BTreeSet;
use future_form::Sendable;
use futures::FutureExt;
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
fn publish(tree: &TreeCoordination, items: Option<&[StoredItem]>) {
    tree.track_mutation().complete(items);
}

#[tokio::test]
async fn overlapping_mutations_cannot_clear_a_rescan_request() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let old = watch.prepare_read().await;
    let mut guard = tree.track_mutation();
    assert!(watch.prepare_read().now_or_never().is_none());
    assert!(!watch.finish_read(&old));
    guard.complete(None);
    drop(guard);
    assert!(!watch.finish_read(&old));
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(
        matches!(watch.take(), Some(StoredEvent::RescanRequired)),
        "invalidation is sticky"
    );
    assert!(watch.finish_read(&watch.prepare_read().await));
    assert!(watch.take().is_none());
}
#[tokio::test]
async fn another_trees_read_attempt_cannot_clear_notifications() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let other = state.tree(SedimentreeId::new([92; 32]));
    // Both counters have the same value, but only this tree's attempt is valid.
    publish(&tree, None);
    publish(&other, None);
    let foreign = other.prepare_read().await;
    assert!(!watch.finish_read(&foreign));
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(watch.finish_read(&watch.prepare_read().await));
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
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    let StoredItem::Commit(value) = &item else {
        panic!("commit")
    };
    assert_eq!(
        Arc::strong_count(value),
        1,
        "overflow releases all queued items"
    );
    publish(&tree, Some(core::slice::from_ref(&item)));
    assert!(
        matches!(watch.take(), Some(StoredEvent::RescanRequired)),
        "later writes cannot clear invalidation"
    );
    assert!(watch.finish_read(&watch.prepare_read().await));
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
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(watch.finish_read(&watch.prepare_read().await));
    let medium = item(MAX_PENDING_BYTES / 2).await;
    publish(&tree, Some(core::slice::from_ref(&medium)));
    {
        let state = watch.tree.inner.lock();
        let Pending::Updates(updates) = &state
            .watchers
            .get(&watch.id)
            .expect("registered watch")
            .pending
        else {
            panic!("updates")
        };
        assert_eq!(updates.items.len(), 1);
        assert_eq!(updates.bytes, medium.byte_len());
    }
    publish(&tree, Some(core::slice::from_ref(&medium)));
    assert!(
        matches!(watch.take(), Some(StoredEvent::RescanRequired)),
        "byte overflow below the count limit"
    );
    let StoredItem::Commit(value) = &medium else {
        panic!("commit")
    };
    assert_eq!(Arc::strong_count(value), 1, "rescan retains no backlog");
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
    let first_id = first.id;
    drop(first);
    assert!(!tree.state.inner.lock().watchers.contains_key(&first_id));
    assert_eq!(Arc::strong_count(value), 2);
    assert_eq!(tree.state.inner.lock().watchers.len(), 1);
    drop(second);
    assert_eq!(Arc::strong_count(value), 1);
    assert!(tree.state.inner.lock().watchers.is_empty());
}
#[tokio::test]
async fn queued_wakeups_do_not_keep_a_dropped_watchs_data_alive() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let watch = state.watch(ID);
    let item = item(1).await;
    let StoredItem::Commit(value) = &item else {
        panic!("commit")
    };
    let mut mutation = tree.track_mutation();
    mutation.complete(Some(core::slice::from_ref(&item)));
    assert_eq!(Arc::strong_count(value), 2);
    assert!(
        watch.notified().now_or_never().is_none(),
        "notification is deferred"
    );
    drop(watch);
    assert_eq!(
        Arc::strong_count(value),
        1,
        "pending wake-up retains only a sender"
    );
    assert!(tree.state.inner.lock().watchers.is_empty());
    drop(mutation);
    assert!(!state.closed());
}

#[tokio::test]
async fn slow_watch_recovery_does_not_clear_another_watchs_updates() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let slow = state.watch(ID);
    let fast = state.watch(ID);
    let item = item(1).await;
    for _ in 0..=MAX_PENDING_ITEMS {
        publish(&tree, Some(core::slice::from_ref(&item)));
        assert!(matches!(fast.take(), Some(StoredEvent::Item(_))));
        assert!(fast.take().is_none());
    }
    assert!(matches!(slow.take(), Some(StoredEvent::RescanRequired)));
    publish(&tree, Some(core::slice::from_ref(&item)));
    assert!(slow.finish_read(&slow.prepare_read().await));
    assert!(slow.take().is_none());
    assert!(
        matches!(fast.take(), Some(StoredEvent::Item(_))),
        "only the accepting watch clears its queue"
    );
}

#[tokio::test]
async fn consumed_items_release_their_byte_budget() {
    let item = item(MAX_PENDING_BYTES / 2).await;
    let mut pending = Pending::default();
    for _ in 0..3 {
        pending.publish(0, Some(core::slice::from_ref(&item)));
        assert!(matches!(pending.take(), Some(StoredEvent::Item(_))));
        let Pending::Updates(updates) = &pending else {
            panic!("updates")
        };
        assert!(updates.items.is_empty());
        assert_eq!(updates.bytes, 0);
    }
}

#[tokio::test]
async fn watch_id_wrap_does_not_replace_a_live_watch() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let first = state.watch(ID);
    tree.state.inner.lock().next_watch_id = u64::MAX;
    let last = state.watch(ID);
    let wrapped = state.watch(ID);
    assert_ne!(first.id, wrapped.id);
    assert_ne!(last.id, wrapped.id);
    let item = item(1).await;
    publish(&tree, Some(core::slice::from_ref(&item)));
    for watch in [first, last, wrapped] {
        assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
    }
    assert!(tree.state.inner.lock().watchers.is_empty());
}

#[tokio::test]
async fn peer_markers_separate_writes_before_and_after_a_round() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let first = PeerId::new([1; 32]);
    let second = PeerId::new([2; 32]);
    let item = item(1).await;
    publish(&tree, Some(core::slice::from_ref(&item)));
    watch.peer_synced(first);
    publish(&tree, Some(core::slice::from_ref(&item)));
    watch.peer_synced(second);
    assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
    assert!(matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == first));
    assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
    assert!(matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == second));
    assert!(watch.take().is_none());
}

#[tokio::test]
async fn peer_markers_survive_overflow_and_precede_later_updates_after_recovery() {
    let state = Arc::new(StorageState::default());
    let watch = state.watch(ID);
    let tree = state.tree(ID);
    let first = PeerId::new([1; 32]);
    let second = PeerId::new([2; 32]);
    let item = item(1).await;
    watch.peer_synced(first);
    for _ in 0..=MAX_PENDING_ITEMS {
        publish(&tree, Some(core::slice::from_ref(&item)));
    }
    watch.peer_synced(second);
    assert!(matches!(watch.take(), Some(StoredEvent::RescanRequired)));
    assert!(watch.finish_read(&watch.prepare_read().await));
    publish(&tree, Some(core::slice::from_ref(&item)));
    for expected in [first, second] {
        assert!(
            matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == expected)
        );
    }
    assert!(matches!(watch.take(), Some(StoredEvent::Item(_))));
    assert!(watch.take().is_none());
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
    assert!(watch.finish_read(&watch.prepare_read().await));
    assert!(matches!(watch.take(), Some(StoredEvent::PeerSynced { peer_id }) if peer_id == second));
}

#[tokio::test]
async fn unsettled_driver_loss_closes_all_watchers_without_certifying_a_scan() {
    let state = Arc::new(StorageState::default());
    let tree = state.tree(ID);
    let _first = state.watch(ID);
    let _second = state.watch(SedimentreeId::new([72; 32]));
    drop(tree.track_mutation());
    assert!(state.closed());
    state.wait_closed().await;
}

#[tokio::test]
async fn a_mutation_invalidates_an_attempt_even_after_completion() {
    let coordination = Arc::new(StorageState::default()).tree(ID);
    let attempt = coordination.prepare_read().await;
    assert!(attempt.is_current());

    let mut mutation = coordination.track_mutation();
    assert!(!attempt.is_current(), "a write is now in progress");
    mutation.complete(None);
    drop(mutation);
    assert!(
        !attempt.is_current(),
        "completion cannot make the old read valid"
    );
    assert!(coordination.prepare_read().await.is_current());
}

#[tokio::test]
async fn preparation_waits_for_all_mutations_and_wakes_every_reader() {
    let coordination = Arc::new(StorageState::default()).tree(ID);
    let mut first = coordination.track_mutation();
    let mut second = coordination.track_mutation();
    let read = coordination.prepare_read();
    let other = coordination.prepare_read();
    futures::pin_mut!(read, other);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(other.as_mut().now_or_never().is_none());

    first.complete(None);
    drop(first);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(other.as_mut().now_or_never().is_none());

    second.complete(None);
    drop(second);
    assert!(
        read.now_or_never()
            .expect("first reader wakes")
            .is_current()
    );
    assert!(
        other
            .now_or_never()
            .expect("second reader wakes")
            .is_current()
    );
}

#[tokio::test]
async fn completion_before_first_poll_does_not_leave_preparation_waiting() {
    let coordination = Arc::new(StorageState::default()).tree(ID);
    let mut mutation = coordination.track_mutation();
    let read = coordination.prepare_read();
    mutation.complete(None);
    drop(mutation);
    assert!(
        read.now_or_never()
            .expect("no mutation is pending")
            .is_current()
    );
}

#[tokio::test]
async fn completion_is_idempotent_and_notifies_after_the_handle_is_dropped() {
    let coordination = Arc::new(StorageState::default()).tree(ID);
    let mut mutation = coordination.track_mutation();
    let waiting = coordination.prepare_read();
    futures::pin_mut!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());

    mutation.complete(None);
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "not notified under the cache lock"
    );
    let current = coordination.prepare_read().await;
    mutation.complete(None);
    assert!(
        current.is_current(),
        "completing twice must not change the revision"
    );
    drop(mutation);
    assert!(current.is_current());
    assert!(
        waiting
            .now_or_never()
            .expect("notified on drop")
            .is_current()
    );
}

#[tokio::test]
async fn an_attempt_keeps_its_tree_registered_and_ignores_other_trees() {
    let registry = Arc::new(StorageState::default());
    // The temporary coordination handle is dropped, but the attempt must keep
    // the same tree state alive for subsequent handles from this registry.
    let attempt = registry.tree(ID).prepare_read().await;
    let mut unrelated = registry.tree(SedimentreeId::new([92; 32])).track_mutation();
    assert!(attempt.is_current());
    unrelated.complete(None);
    drop(unrelated);
    assert!(attempt.is_current());

    let mut mutation = registry.tree(ID).track_mutation();
    assert!(!attempt.is_current());
    mutation.complete(None);
    drop(mutation);
    assert!(!attempt.is_current());
}

#[tokio::test]
async fn fetcher_clones_and_put_capabilities_share_coordination() {
    use crate::{
        connection::test_utils::TokioSpawn, minimized_tree_store::memory::MemoryMinimizedTreeStore,
        policy::open::OpenPolicy, storage::memory::MemoryStorage, subduction::StorageTasks,
    };
    use future_form::Sendable;

    let storage = <Sendable as StorageTasks<
        MemoryStorage,
        OpenPolicy,
        MemoryMinimizedTreeStore<1>,
    >>::storage_powerbox(
        MemoryStorage::new(),
        Arc::new(OpenPolicy),
        MemoryMinimizedTreeStore::new(),
        TokioSpawn,
    );
    let fetcher = storage.local_fetcher(ID);
    let clone = fetcher.clone();
    let attempt = fetcher.coordination().prepare_read().await;
    drop(fetcher);
    let mut mutation = storage
        .local_putter(ID)
        .as_fetcher()
        .coordination()
        .track_mutation();
    assert!(!attempt.is_current());
    assert!(clone.coordination().prepare_read().now_or_never().is_none());
    mutation.complete(None);
    drop(mutation);
    assert!(clone.coordination().prepare_read().await.is_current());
    assert!(!attempt.is_current());
}
