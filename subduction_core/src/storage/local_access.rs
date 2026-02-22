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
pub struct LocalStorageAccess<S> {
    storage: Arc<S>,
}

impl<S> LocalStorageAccess<S> {
    /// Create a new local storage access wrapper.
    pub const fn new(storage: Arc<S>) -> Self {
        Self { storage }
    }

    /// Get the underlying storage Arc.
    ///
    /// This is useful when you need to pass the storage to other components.
    #[must_use]
    pub const fn storage(&self) -> &Arc<S> {
        &self.storage
    }

    // ==================== Hydration Operations ====================

    /// Load all sedimentree IDs from storage.
    #[must_use]
    pub fn load_all_sedimentree_ids<K: FutureForm>(
        &self,
    ) -> K::Future<'_, Result<Set<SedimentreeId>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_all_sedimentree_ids()
    }

    /// Load all loose commits with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup.
    #[must_use]
    pub fn load_loose_commits<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_loose_commits(sedimentree_id)
    }

    /// Load all fragments with their blobs for a sedimentree.
    ///
    /// Used for hydration at startup.
    #[must_use]
    pub fn load_fragments<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_fragments(sedimentree_id)
    }
}

impl<S> Clone for LocalStorageAccess<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}
