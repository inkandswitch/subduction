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

use alloc::{collections::BTreeSet, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    crypto::digest::Digest, fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit,
};
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

/// Assert that [`Storage::save_batch`] registers the batch's sedimentree
/// id — the batch-side counterpart of the single-item helpers.
///
/// Every item in `commits` / `fragments` must target `id`, which must
/// **not** be registered on `storage` yet (the check is vacuous
/// otherwise). At least one item must be supplied so the assertion is
/// non-vacuous: an empty batch could not distinguish "registered because
/// of the items" from a no-op. (The trait contract *does* require an
/// empty batch to register `id` too — that case is exercised separately,
/// not by this helper.)
///
/// # Panics
///
/// Panics when the implementation violates the contract (or errors).
pub async fn assert_batch_save_registers_tree_id<Async, Store>(
    storage: &Store,
    id: SedimentreeId,
    commits: Vec<VerifiedMeta<LooseCommit>>,
    fragments: Vec<VerifiedMeta<Fragment>>,
) where
    Async: FutureForm,
    Store: Storage<Async>,
{
    assert!(
        !commits.is_empty() || !fragments.is_empty(),
        "precondition: the batch must contain at least one item"
    );

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
            .save_batch(id, commits, fragments)
            .await
            .expect("save_batch failed");

        assert!(
            storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "contract violation: save_batch must register the sedimentree id"
        );
    }
}

/// Assert that a *failing* [`Storage::save_loose_commit`] does **not**
/// register the commit's sedimentree id in the backend's *observable* view
/// — the negative half of the registration contract. A failed write that
/// leaves a registered-but-empty tree behind misleads every consumer of
/// [`load_all_sedimentree_ids`](Storage::load_all_sedimentree_ids).
///
/// This checks the observable view (queried immediately, no reopen); see
/// [`save_loose_commit`](Storage::save_loose_commit) for the across-reopen
/// caveat that applies to layout-derived backends.
///
/// The caller must hand over a `storage` that has been *poisoned* so the
/// save is guaranteed to fail (e.g. a roadblock file where the backend
/// needs a directory, or a corrupted blob store). Backends with no
/// reachable failure path (e.g. a pure in-memory store) have nothing to
/// check here.
///
/// # Panics
///
/// Panics when the save unexpectedly succeeds, or when the failed save
/// leaked a registration.
pub async fn assert_failed_commit_save_does_not_register_tree_id<Async, Store>(
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
            "precondition: tree must be unregistered before the failed save"
        );

        assert!(
            storage.save_loose_commit(id, commit).await.is_err(),
            "precondition: the save must fail (storage was not poisoned correctly)"
        );

        assert!(
            !storage
                .contains_sedimentree_id(id)
                .await
                .expect("contains_sedimentree_id failed"),
            "contract violation: a failed save must not register the sedimentree id"
        );
    }
}

/// Assert that the metadata-only loads
/// ([`load_loose_commit_metas`](Storage::load_loose_commit_metas) /
/// [`load_fragment_metas`](Storage::load_fragment_metas)) return exactly the
/// same payload set as the full loads — the contract the hydration path
/// relies on. Compared by content digest, so it holds under Byzantine
/// equivocation (multiple payloads per id).
///
/// `id`'s commits and fragments must already be stored; mix inline and
/// external (large) blobs to exercise both blob paths.
///
/// # Panics
///
/// Panics when the metadata-only load disagrees with the full load (or errors).
pub async fn assert_metas_match_full_load<Async, Store>(storage: &Store, id: SedimentreeId)
where
    Async: FutureForm,
    Store: Storage<Async>,
{
    #[allow(clippy::expect_used)]
    {
        let full_commits: BTreeSet<Digest<LooseCommit>> = storage
            .load_loose_commits(id)
            .await
            .expect("load_loose_commits failed")
            .iter()
            .map(|vm| Digest::hash(vm.payload()))
            .collect();
        let meta_commits: BTreeSet<Digest<LooseCommit>> = storage
            .load_loose_commit_metas(id)
            .await
            .expect("load_loose_commit_metas failed")
            .iter()
            .map(Digest::hash)
            .collect();
        assert_eq!(
            meta_commits, full_commits,
            "load_loose_commit_metas must return the same payload set as load_loose_commits"
        );

        let full_fragments: BTreeSet<Digest<Fragment>> = storage
            .load_fragments(id)
            .await
            .expect("load_fragments failed")
            .iter()
            .map(|vm| Digest::hash(vm.payload()))
            .collect();
        let meta_fragments: BTreeSet<Digest<Fragment>> = storage
            .load_fragment_metas(id)
            .await
            .expect("load_fragment_metas failed")
            .iter()
            .map(Digest::hash)
            .collect();
        assert_eq!(
            meta_fragments, full_fragments,
            "load_fragment_metas must return the same payload set as load_fragments"
        );
    }
}
