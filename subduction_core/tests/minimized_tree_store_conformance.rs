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
        conformance::{self, Op, TREES},
        memory::MemoryMinimizedTreeStore,
        noop::NoopMinimizedTreeStore,
        resilient::ResilientMinimizedTreeStore,
        shared::SharedMemoryMinimizedTreeStore,
    },
};

const ID: SedimentreeId = SedimentreeId::new([0xC5; 32]);
const IDS: [SedimentreeId; TREES] = [
    SedimentreeId::new([0xC6; 32]),
    SedimentreeId::new([0xC7; 32]),
    SedimentreeId::new([0xC8; 32]),
];

fn resilient_shared() -> ResilientMinimizedTreeStore<SharedMemoryMinimizedTreeStore, InstantTimeout>
{
    ResilientMinimizedTreeStore::new(
        SharedMemoryMinimizedTreeStore::new(),
        InstantTimeout,
        Duration::from_secs(1),
    )
}

/// The checks every store must pass, each on a fresh store.
macro_rules! every_store {
    ($name:ident, $store:expr) => {
        mod $name {
            use super::*;

            #[test]
            fn a_populated_tree_is_served_as_populated() {
                block_on(
                    conformance::assert_a_populated_tree_is_served_as_populated::<Sendable, _>(
                        &$store, ID,
                    ),
                );
            }

            #[test]
            fn a_merge_reaches_a_resident_tree() {
                block_on(conformance::assert_a_merge_reaches_a_resident_tree::<
                    Sendable,
                    _,
                >(&$store, ID));
            }

            #[test]
            fn a_merge_on_a_miss_leaves_the_tree_absent() {
                block_on(
                    conformance::assert_a_merge_on_a_miss_leaves_the_tree_absent::<Sendable, _>(
                        &$store, ID,
                    ),
                );
            }

            #[test]
            fn remove_drops_the_tree() {
                block_on(conformance::assert_remove_drops_the_tree::<Sendable, _>(
                    &$store, ID,
                ));
            }
        }
    };
}

/// The further checks a shared store must pass, through two handles to one
/// fresh store.
macro_rules! shared_store {
    ($name:ident, $store:expr) => {
        mod $name {
            use super::*;

            #[test]
            fn a_populate_after_a_merge_on_a_miss_is_stale() {
                let a = $store;
                block_on(
                    conformance::assert_a_populate_after_a_merge_on_a_miss_is_stale::<Sendable, _>(
                        &a,
                        &a.clone(),
                        ID,
                    ),
                );
            }

            #[test]
            fn a_populate_after_a_remove_is_stale() {
                let a = $store;
                block_on(conformance::assert_a_populate_after_a_remove_is_stale::<
                    Sendable,
                    _,
                >(&a, &a.clone(), ID));
            }

            #[test]
            fn a_populate_after_a_merge_into_a_resident_tree_is_stale() {
                let a = $store;
                block_on(
                    conformance::assert_a_populate_after_a_merge_into_a_resident_tree_is_stale::<
                        Sendable,
                        _,
                    >(&a, &a.clone(), ID),
                );
            }

            #[test]
            fn generations_do_not_repeat() {
                let a = $store;
                block_on(
                    conformance::assert_generations_do_not_repeat::<Sendable, _>(
                        &a,
                        &a.clone(),
                        ID,
                    ),
                );
            }

            #[test]
            fn a_merge_is_visible_to_every_handle() {
                let a = $store;
                block_on(conformance::assert_a_merge_is_visible_to_every_handle::<
                    Sendable,
                    _,
                >(&a, &a.clone(), ID));
            }

            #[test]
            fn generated_operation_sequences_match_the_model() {
                bolero::check!()
                    .with_arbitrary::<Vec<Op>>()
                    .for_each(|ops| {
                        let a = $store;
                        block_on(conformance::check_ops::<Sendable, _>(
                            &a,
                            &a.clone(),
                            IDS,
                            ops,
                        ));
                    });
            }
        }
    };
}

every_store!(memory, MemoryMinimizedTreeStore::<256>::new());
every_store!(noop, NoopMinimizedTreeStore);
every_store!(shared, SharedMemoryMinimizedTreeStore::new());
every_store!(resilient, resilient_shared());

shared_store!(shared_handles, SharedMemoryMinimizedTreeStore::new());
shared_store!(resilient_handles, resilient_shared());
