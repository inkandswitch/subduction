//! Local storage access for trusted internal operations.
//!
//! [`LocalStorageAccess`] provides direct storage access for operations that don't
//! fit the capability model:
//!
//! - **Hydration**: Loading our own data at startup
//!
//! This is separate from [`StoragePowerbox`] which only mints capabilities for
//! peer-authorized access.

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    collections::Set, fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

use super::traits::Storage;

/// Direct storage access for trusted local operations.
///
/// Use this for:
/// - Hydration (loading our own data at startup)
/// - Internal sync operations
///
/// This bypasses the capability model — only use from trusted code paths.
#[derive(Debug)]
pub struct LocalStorageAccess<Store> {
    storage: Arc<Store>,
}

impl<Store> LocalStorageAccess<Store> {
    /// Create a new local storage access wrapper.
    pub const fn new(storage: Arc<Store>) -> Self {
        Self { storage }
    }

    /// Get the underlying storage Arc.
    ///
    /// This is useful when you need to pass the storage to other components.
    #[must_use]
    pub const fn storage(&self) -> &Arc<Store> {
        &self.storage
    }

    // ==================== Hydration Operations ====================

    /// Load all sedimentree IDs from storage.
    #[must_use]
    pub fn load_all_sedimentree_ids<Async: FutureForm>(
        &self,
    ) -> Async::Future<'_, Result<Set<SedimentreeId>, Store::Error>>
    where
        Store: Storage<Async>,
    {
        self.storage.load_all_sedimentree_ids()
    }

    /// Whether a sedimentree ID is registered (single-key existence check).
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

    /// Load all loose commits with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup.
    #[must_use]
    pub fn load_loose_commits<Async: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Store::Error>>
    where
        Store: Storage<Async>,
    {
        self.storage.load_loose_commits(sedimentree_id)
    }

    /// Load all fragments with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup.
    #[must_use]
    pub fn load_fragments<Async: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, Store::Error>>
    where
        Store: Storage<Async>,
    {
        self.storage.load_fragments(sedimentree_id)
    }
}

impl<Store> Clone for LocalStorageAccess<Store> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}
