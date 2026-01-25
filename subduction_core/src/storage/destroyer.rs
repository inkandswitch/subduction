//! Destroyer capability for local-only delete operations.
//!
//! The [`Destroyer`] is a capability that grants the ability to delete data
//! from a sedimentree. Unlike [`Putter`], this capability is never handed out
//! to connected peers — it's strictly for local cleanup operations like
//! compaction and garbage collection.

use core::marker::PhantomData;

use alloc::sync::Arc;

use futures_kind::FutureKind;
use sedimentree_core::{blob::Digest, id::SedimentreeId, storage::Storage};

/// A capability granting delete access to a specific sedimentree's data.
///
/// This type is for local-only operations and should never be sent to peers.
/// Use cases include:
/// - Compaction (deleting loose commits after they're merged into fragments)
/// - Garbage collection (cleaning up orphaned data)
/// - Administrative cleanup
///
/// Created via [`StoragePowerbox::local_destroyer`][crate::storage::powerbox::StoragePowerbox::local_destroyer].
pub struct Destroyer<K: FutureKind, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureKind, S: Storage<K>> Destroyer<K, S> {
    /// Create a new destroyer capability.
    ///
    /// This should only be called for local operations, never for peer requests.
    pub(crate) const fn new(storage: Arc<S>, sedimentree_id: SedimentreeId) -> Self {
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

    /// Delete all loose commits for this sedimentree.
    #[must_use]
    pub fn delete_loose_commits(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_loose_commits(self.sedimentree_id)
    }

    /// Delete all fragments for this sedimentree.
    #[must_use]
    pub fn delete_fragments(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_fragments(self.sedimentree_id)
    }

    /// Delete a blob by its digest.
    #[must_use]
    pub fn delete_blob(&self, digest: Digest) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_blob(digest)
    }
}

impl<K: FutureKind, S: Storage<K>> Clone for Destroyer<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureKind, S: Storage<K>> core::fmt::Debug for Destroyer<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Destroyer")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
