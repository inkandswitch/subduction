//! Every stock minimized tree store passes the conformance checks.
//!
//! This uses only the public API, as an implementation in another crate
//! would.

use core::{
    convert::Infallible,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use std::sync::Arc;

use future_form::{FutureForm, Sendable};
use futures::{executor::block_on, future::BoxFuture};
use sedimentree_core::{
    id::SedimentreeId,
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_core::{
    connection::test_utils::InstantTimeout,
    minimized_tree_store::{
        Lookup, MinimizedTreeStore, Populate, SharedMinimizedTreeStore,
        conformance::{self, Op, TREES},
        memory::MemoryMinimizedTreeStore,
        noop::NoopMinimizedTreeStore,
        resilient::ResilientMinimizedTreeStore,
        shared::SharedMemoryMinimizedTreeStore,
    },
};

const IDS: [SedimentreeId; TREES] = [
    SedimentreeId::new([0xC6; 32]),
    SedimentreeId::new([0xC7; 32]),
    SedimentreeId::new([0xC8; 32]),
];

/// Tree ids no check has used yet.
fn fresh_ids() -> impl FnMut() -> SedimentreeId {
    let mut next = 0u8;
    move || {
        next += 1;
        SedimentreeId::new([next; 32])
    }
}

fn resilient_shared() -> ResilientMinimizedTreeStore<SharedMemoryMinimizedTreeStore, InstantTimeout>
{
    ResilientMinimizedTreeStore::new(
        SharedMemoryMinimizedTreeStore::new(),
        InstantTimeout,
        Duration::from_secs(1),
    )
}

/// Run every check on `store`, through two clones of one handle.
fn check_shared<Store: SharedMinimizedTreeStore<Sendable>>(store: &Store) {
    block_on(conformance::check_shared_store::<Sendable, _, _>(
        store,
        &store.clone(),
        fresh_ids(),
    ));
}

/// Run generated operation sequences, each on a fresh store from `fresh`.
fn check_generated_ops<Store: SharedMinimizedTreeStore<Sendable>>(fresh: fn() -> Store) {
    bolero::check!()
        .with_arbitrary::<Vec<Op>>()
        .for_each(|ops| {
            let a = fresh();
            block_on(conformance::check_ops::<Sendable, _>(
                &a,
                &a.clone(),
                IDS,
                ops,
            ));
        });
}

#[test]
fn memory_store_conforms() {
    block_on(conformance::check_store::<Sendable, _, _>(
        &MemoryMinimizedTreeStore::<256>::new(),
        fresh_ids(),
    ));
}

#[test]
fn noop_store_conforms() {
    block_on(conformance::check_store::<Sendable, _, _>(
        &NoopMinimizedTreeStore,
        fresh_ids(),
    ));
}

#[test]
fn shared_store_conforms() {
    check_shared(&SharedMemoryMinimizedTreeStore::new());
}

#[test]
fn shared_store_matches_the_model() {
    check_generated_ops(SharedMemoryMinimizedTreeStore::new);
}

#[test]
fn resilient_store_conforms() {
    check_shared(&resilient_shared());
}

#[test]
fn resilient_store_matches_the_model() {
    check_generated_ops(resilient_shared);
}

// Fewer slots than `check_ops` has trees, so most sequences sweep: slots are
// reclaimed, the absent generation moves, and waiters restart on retired
// slots.
fn bounded_shared() -> SharedMemoryMinimizedTreeStore {
    SharedMemoryMinimizedTreeStore::with_capacity(2)
}

#[test]
fn bounded_shared_store_conforms() {
    check_shared(&bounded_shared());
}

#[test]
fn bounded_shared_store_matches_the_model() {
    check_generated_ops(bounded_shared);
}

#[derive(Debug, thiserror::Error)]
#[error("injected store outage")]
struct Down;

/// A store whose every call fails while switched on, and which forgets each
/// failure: it does nothing to keep a stale tree out of use. On its own it
/// breaks the error contract; wrapped in `ResilientMinimizedTreeStore`, it
/// meets it.
#[derive(Debug, Clone)]
struct Faulty<Store> {
    inner: Store,
    failing: Arc<AtomicBool>,
}

impl<Store> Faulty<Store> {
    fn new(inner: Store) -> Self {
        Self {
            inner,
            failing: Arc::default(),
        }
    }

    fn switch(&self) -> impl Fn(bool) + use<Store> {
        let failing = Arc::clone(&self.failing);
        move |on| failing.store(on, Ordering::SeqCst)
    }

    fn down(&self) -> bool {
        self.failing.load(Ordering::SeqCst)
    }
}

impl<Store> MinimizedTreeStore<Sendable> for Faulty<Store>
where
    Store: MinimizedTreeStore<Sendable, Error = Infallible> + Send + Sync,
    Store::Generation: Send,
{
    type Error = Down;
    type Generation = Store::Generation;
    type Entry = Store::Entry;

    fn get(
        &self,
        id: SedimentreeId,
    ) -> BoxFuture<'_, Result<Lookup<Store::Entry, Store::Generation>, Down>> {
        Sendable::from_future(async move {
            if self.down() {
                return Err(Down);
            }
            Ok(self
                .inner
                .get(id)
                .await
                .unwrap_or_else(|never| match never {}))
        })
    }

    fn populate(
        &self,
        id: SedimentreeId,
        tree: MinimizedSedimentree,
        seen: Store::Generation,
    ) -> BoxFuture<'_, Result<Populate, Down>> {
        Sendable::from_future(async move {
            if self.down() {
                return Err(Down);
            }
            Ok(self
                .inner
                .populate(id, tree, seen)
                .await
                .unwrap_or_else(|never| match never {}))
        })
    }

    fn merge(&self, id: SedimentreeId, update: Sedimentree) -> BoxFuture<'_, Result<(), Down>> {
        Sendable::from_future(async move {
            if self.down() {
                return Err(Down);
            }
            self.inner
                .merge(id, update)
                .await
                .unwrap_or_else(|never| match never {});
            Ok(())
        })
    }

    fn remove(&self, id: SedimentreeId) -> BoxFuture<'_, Result<(), Down>> {
        Sendable::from_future(async move {
            if self.down() {
                return Err(Down);
            }
            self.inner
                .remove(id)
                .await
                .unwrap_or_else(|never| match never {});
            Ok(())
        })
    }
}

#[test]
fn resilient_store_fails_safe() {
    let faulty = Faulty::new(SharedMemoryMinimizedTreeStore::new());
    let switch = faulty.switch();
    let store = ResilientMinimizedTreeStore::new(faulty, InstantTimeout, Duration::from_secs(1));
    block_on(conformance::check_failing_store::<Sendable, _, _, _>(
        &store,
        switch,
        fresh_ids(),
    ));
}

/// The error-contract checks catch a store that forgets its failures.
#[test]
#[should_panic(expected = "after a failed merge")]
fn a_store_that_forgets_failures_is_caught() {
    let store = Faulty::new(SharedMemoryMinimizedTreeStore::new());
    let switch = store.switch();
    block_on(conformance::check_failing_store::<Sendable, _, _, _>(
        &store,
        switch,
        fresh_ids(),
    ));
}
