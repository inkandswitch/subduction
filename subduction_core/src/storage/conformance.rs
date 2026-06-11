//! Generic conformance checks for [`Storage`] implementations.
//!
//! The [`Storage`] trait carries cross-method contracts that the type
//! system cannot enforce — most importantly that *persisting any item
//! registers its sedimentree id*. A backend that forgets this compiles
//! fine and works in-tree (the core write paths call
//! [`save_sedimentree_id`](Storage::save_sedimentree_id) explicitly) but
//! silently breaks direct trait consumers.
//!
//! Every backend's test suite should call these helpers so the contract
//! cannot be missed:
//!
//! ```ignore
//! let commit: VerifiedMeta<LooseCommit> = /* seal one for a fresh tree */;
//! conformance::assert_commit_save_registers_tree_id(&storage, commit).await;
//! ```

use future_form::FutureForm;
use sedimentree_core::{fragment::Fragment, loose_commit::LooseCommit};
use subduction_crypto::verified_meta::VerifiedMeta;

use super::traits::Storage;

/// Assert that [`Storage::save_loose_commit`] registers the commit's
/// sedimentree id (visible via both
/// [`contains_sedimentree_id`](Storage::contains_sedimentree_id) and
/// [`load_all_sedimentree_ids`](Storage::load_all_sedimentree_ids)).
///
/// `commit` should target a tree id that has **not** been registered on
/// `storage` yet, otherwise the check is vacuous.
///
/// # Panics
///
/// Panics when the implementation violates the contract (or errors).
pub async fn assert_commit_save_registers_tree_id<Async, Store>(
    storage: &Store,
    commit: VerifiedMeta<LooseCommit>,
) where
    Async: FutureForm,
    Store: Storage<Async>,
{
    let id = commit.payload().sedimentree_id();

    #[allow(clippy::expect_used)]
    {
        assert!(
            !storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "precondition: tree must be unregistered before the save (the check is vacuous otherwise)"
        );

        storage
            .save_loose_commit(id, commit)
            .await
            .expect("save_loose_commit failed");

        assert!(
            storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "contract violation: save_loose_commit must register the sedimentree id"
        );
        assert!(
            storage
                .load_all_sedimentree_ids()
                .await
                .expect("load_all_sedimentree_ids failed")
                .contains(&id),
            "contract violation: a saved commit's tree id must appear in load_all_sedimentree_ids"
        );
    }
}

/// Assert that [`Storage::save_fragment`] registers the fragment's
/// sedimentree id — the fragment-side twin of
/// [`assert_commit_save_registers_tree_id`].
///
/// # Panics
///
/// Panics when the implementation violates the contract (or errors).
pub async fn assert_fragment_save_registers_tree_id<Async, Store>(
    storage: &Store,
    fragment: VerifiedMeta<Fragment>,
) where
    Async: FutureForm,
    Store: Storage<Async>,
{
    let id = fragment.payload().sedimentree_id();

    #[allow(clippy::expect_used)]
    {
        assert!(
            !storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "precondition: tree must be unregistered before the save (the check is vacuous otherwise)"
        );

        storage
            .save_fragment(id, fragment)
            .await
            .expect("save_fragment failed");

        assert!(
            storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "contract violation: save_fragment must register the sedimentree id"
        );
        assert!(
            storage
                .load_all_sedimentree_ids()
                .await
                .expect("load_all_sedimentree_ids failed")
                .contains(&id),
            "contract violation: a saved fragment's tree id must appear in load_all_sedimentree_ids"
        );
    }
}
