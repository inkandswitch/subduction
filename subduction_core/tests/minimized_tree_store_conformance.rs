//! Every stock minimized tree store passes the conformance checks.
//!
//! This uses only the public API, as an implementation in another crate
//! would.

use core::time::Duration;

use future_form::Sendable;
use futures::executor::block_on;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    connection::test_utils::InstantTimeout,
    minimized_tree_store::{
        SharedMinimizedTreeStore,
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
