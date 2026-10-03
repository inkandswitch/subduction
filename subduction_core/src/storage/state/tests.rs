//! The coordination API owns revision checks and notification ordering.
#![allow(clippy::expect_used)]

use super::*;
use futures::FutureExt;

const ID: SedimentreeId = SedimentreeId::new([91; 32]);

#[tokio::test]
async fn a_mutation_invalidates_an_attempt_even_after_completion() {
    let coordination = StorageState::default().tree(ID);
    let attempt = coordination.prepare_read().await;
    assert!(attempt.is_current());

    let mut mutation = coordination.track_mutation();
    assert!(!attempt.is_current(), "a write is now in progress");
    mutation.complete();
    drop(mutation);
    assert!(
        !attempt.is_current(),
        "completion cannot make the old read valid"
    );
    assert!(coordination.prepare_read().await.is_current());
}

#[tokio::test]
async fn preparation_waits_for_all_mutations_and_wakes_every_reader() {
    let coordination = StorageState::default().tree(ID);
    let mut first = coordination.track_mutation();
    let mut second = coordination.track_mutation();
    let read = coordination.prepare_read();
    let other = coordination.prepare_read();
    futures::pin_mut!(read, other);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(other.as_mut().now_or_never().is_none());

    first.complete();
    drop(first);
    assert!(read.as_mut().now_or_never().is_none());
    assert!(other.as_mut().now_or_never().is_none());

    second.complete();
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
    let coordination = StorageState::default().tree(ID);
    let mut mutation = coordination.track_mutation();
    let read = coordination.prepare_read();
    mutation.complete();
    drop(mutation);
    assert!(
        read.now_or_never()
            .expect("no mutation is pending")
            .is_current()
    );
}

#[tokio::test]
async fn completion_is_idempotent_and_notifies_after_the_handle_is_dropped() {
    let coordination = StorageState::default().tree(ID);
    let mut mutation = coordination.track_mutation();
    let waiting = coordination.prepare_read();
    futures::pin_mut!(waiting);
    assert!(waiting.as_mut().now_or_never().is_none());

    mutation.complete();
    assert!(
        waiting.as_mut().now_or_never().is_none(),
        "not notified under the cache lock"
    );
    let current = coordination.prepare_read().await;
    mutation.complete();
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
    let registry = StorageState::default();
    // The temporary coordination handle is dropped, but the attempt must keep
    // the same tree state alive for subsequent handles from this registry.
    let attempt = registry.tree(ID).prepare_read().await;
    let mut unrelated = registry.tree(SedimentreeId::new([92; 32])).track_mutation();
    assert!(attempt.is_current());
    unrelated.complete();
    drop(unrelated);
    assert!(attempt.is_current());

    let mut mutation = registry.tree(ID).track_mutation();
    assert!(!attempt.is_current());
    mutation.complete();
    drop(mutation);
    assert!(!attempt.is_current());
}

#[tokio::test]
async fn fetcher_clones_and_put_capabilities_share_coordination() {
    use crate::{
        policy::open::OpenPolicy,
        storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    };
    use future_form::Sendable;

    let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
    let fetcher = storage.local_fetcher::<Sendable>(ID);
    let clone = fetcher.clone();
    let attempt = fetcher.coordination().prepare_read().await;
    drop(fetcher);
    let mut mutation = storage
        .local_putter::<Sendable>(ID)
        .as_fetcher()
        .coordination()
        .track_mutation();
    assert!(!attempt.is_current());
    assert!(clone.coordination().prepare_read().now_or_never().is_none());
    mutation.complete();
    drop(mutation);
    assert!(clone.coordination().prepare_read().await.is_current());
    assert!(!attempt.is_current());
}
