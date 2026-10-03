//! Trusted access to the local storage index.
//!
//! Tree-scoped reads use [`Fetcher`](super::fetcher::Fetcher), whether minted
//! after peer authorization or internally for local reads. This wrapper retains
//! global ID enumeration and direct access for trusted integrations.

use alloc::sync::Arc;
use future_form::FutureForm;
use sedimentree_core::{collections::Set, id::SedimentreeId};

use super::traits::Storage;

/// Trusted local storage access, independent of any peer's authorization.
/// Tree contents are read through a tree-scoped fetch capability instead.
#[derive(Debug)]
pub struct LocalStorageAccess<Store> {
    storage: Arc<Store>,
}

impl<Store> LocalStorageAccess<Store> {
    /// Create trusted access to the local index.
    pub const fn new(storage: Arc<Store>) -> Self {
        Self { storage }
    }

    /// Get the underlying storage Arc for trusted integrations.
    #[must_use]
    pub const fn storage(&self) -> &Arc<Store> {
        &self.storage
    }

    /// Load all registered sedimentree IDs.
    #[must_use]
    pub fn load_all_sedimentree_ids<Async: FutureForm>(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Store::Error>>
    where
        Store: Storage<Async>,
    {
        self.storage.load_all_sedimentree_ids()
    }

    /// Whether an ID is registered, including registered-but-empty trees.
    #[must_use]
    pub fn contains_sedimentree_id<Async: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<bool, Store::Error>>
    where
        Store: Storage<Async>,
    {
        self.storage.contains_sedimentree_id(sedimentree_id)
    }
}

impl<Store> Clone for LocalStorageAccess<Store> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}
