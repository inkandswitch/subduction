//! Storage capability: the driver's custody + durability duties.
//!
//! The node's [`StorageOp`](subduction_protocol::storage::StorageOp)s speak
//! blob _refs_; the driver resolves refs to bytes before anything reaches a
//! backend, so backends only ever see whole items. Signature verification
//! is not a backend duty (remote items were verified in the connection
//! machine), and neither is authorization (the effect executor consults
//! [`Policy`](crate::policy::Policy) first — see [`crate::policy`]).
//!
//! Errors cross this boundary as [`StorageFailure`] (retryable vs
//! permanent): the machine cannot meaningfully distinguish finer backend
//! causes, and the driver's telemetry sees the raw error before it is
//! coarsened.

use std::{rc::Rc, sync::Arc};

use future_form::FutureForm;
use sedimentree_core::{
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_crypto::signed::Signed;
use subduction_protocol::storage::StorageFailure;

/// Durable item storage, byte-world.
pub trait Storage<Async: FutureForm> {
    /// Persist verified items with their blobs; returns the stored count.
    fn persist_items(
        &self,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
        fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
    ) -> Async::Future<'_, Result<u32, StorageFailure>>;

    /// Load specific items; `Ok(None)` when the tree is unknown. Missing
    /// items are absent from the result.
    fn fetch_items(
        &self,
        tree: SedimentreeId,
        commit_ids: Vec<CommitId>,
        fragment_heads: Vec<CommitId>,
    ) -> Async::Future<'_, Result<Option<FetchedItems>, StorageFailure>>;

    /// Delete a tree and all its data.
    fn delete_tree(&self, tree: SedimentreeId) -> Async::Future<'_, Result<(), StorageFailure>>;
}

macro_rules! delegate_storage {
    ($pointer:ident) => {
        impl<Async: FutureForm, S: Storage<Async>> Storage<Async> for $pointer<S> {
            fn persist_items(
                &self,
                tree: SedimentreeId,
                commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,
                fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
            ) -> Async::Future<'_, Result<u32, StorageFailure>> {
                S::persist_items(self, tree, commits, fragments)
            }

            fn fetch_items(
                &self,
                tree: SedimentreeId,
                commit_ids: Vec<CommitId>,
                fragment_heads: Vec<CommitId>,
            ) -> Async::Future<'_, Result<Option<FetchedItems>, StorageFailure>> {
                S::fetch_items(self, tree, commit_ids, fragment_heads)
            }

            fn delete_tree(
                &self,
                tree: SedimentreeId,
            ) -> Async::Future<'_, Result<(), StorageFailure>> {
                S::delete_tree(self, tree)
            }
        }
    };
}

delegate_storage!(Rc);
delegate_storage!(Arc);

/// Items loaded by [`Storage::fetch_items`], blobs as bytes.
#[derive(Debug, Clone, Default)]
pub struct FetchedItems {
    /// Requested commits that were found.
    pub commits: Vec<(Signed<LooseCommit>, Vec<u8>)>,

    /// Requested fragments that were found.
    pub fragments: Vec<(Signed<Fragment>, Vec<u8>)>,
}
