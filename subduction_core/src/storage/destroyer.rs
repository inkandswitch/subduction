//! Destroyer capability for local-only delete operations.
//!
//! The [`Destroyer`] is a capability that grants the ability to delete data
//! from a sedimentree. Unlike [`Putter`], this capability is never handed out
//! to connected peers — it's strictly for local cleanup operations like
//! compaction and garbage collection.

use core::marker::PhantomData;

use alloc::sync::Arc;

use future_form::FutureForm;
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

use super::traits::Storage;

/// A capability granting delete access to a specific sedimentree's data.
///
/// This type is for local-only operations and should never be sent to peers.
/// Use cases include:
/// - Compaction (deleting loose commits after they're merged into fragments)
/// - Garbage collection (cleaning up orphaned data)
/// - Administrative cleanup
///
/// Created via [`StoragePowerbox::local_destroyer`][crate::storage::powerbox::StoragePowerbox::local_destroyer].
pub struct Destroyer<Async: FutureForm, Store: Storage<Async>> {
    storage: Arc<Store>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<Async>,
}

impl<Async: FutureForm, Store: Storage<Async>> Destroyer<Async, Store> {
    /// Create a new destroyer capability.
    ///
    /// This should only be called for local operations, never for peer requests.
    pub(crate) const fn new(storage: Arc<Store>, sedimentree_id: SedimentreeId) -> Self {
        Self {
            storage,
            sedimentree_id,
            _marker: PhantomData,
        }
    }

    /// Get the sedimentree ID this capability grants access to.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.sedimentree_id
    }

    /// Delete a single loose commit and its blob by [`CommitId`].
    #[must_use]
    pub fn delete_loose_commit(
        &self,
        commit_id: CommitId,
    ) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage
            .delete_loose_commit(self.sedimentree_id, commit_id)
    }

    /// Delete all loose commits and their blobs for this sedimentree.
    #[must_use]
    pub fn delete_loose_commits(&self) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage.delete_loose_commits(self.sedimentree_id)
    }

    /// Delete a fragment and its blob by fragment head [`CommitId`].
    #[must_use]
    pub fn delete_fragment(
        &self,
        fragment_head: CommitId,
    ) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage
            .delete_fragment(self.sedimentree_id, fragment_head)
    }

    /// Delete all fragments and their blobs for this sedimentree.
    #[must_use]
    pub fn delete_fragments(&self) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage.delete_fragments(self.sedimentree_id)
    }

    /// Remove this sedimentree's id from the durable id index.
    ///
    /// Existence is tracked by the id index independently of having
    /// commits/fragments, so removing a tree must delete its id (not just
    /// its data) for it to disappear from
    /// [`load_all_sedimentree_ids`](crate::storage::traits::Storage::load_all_sedimentree_ids).
    #[must_use]
    pub fn delete_sedimentree_id(&self) -> Async::Future<'_, Result<(), Store::Error>> {
        self.storage.delete_sedimentree_id(self.sedimentree_id)
    }
}

impl<Async: FutureForm, Store: Storage<Async>> Clone for Destroyer<Async, Store> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<Async: FutureForm, Store: Storage<Async>> core::fmt::Debug for Destroyer<Async, Store> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Destroyer")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
