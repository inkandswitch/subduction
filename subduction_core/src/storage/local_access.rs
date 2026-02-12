//! Local storage access for trusted internal operations.
//!
//! [`LocalStorageAccess`] provides direct storage access for operations that don't
//! fit the capability model:
//!
//! - **Blobs**: Content-addressed, shared across sedimentrees
//! - **Hydration**: Loading our own data at startup
//!
//! This is separate from [`StoragePowerbox`] which only mints capabilities for
//! peer-authorized access.

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::traits::Storage;
use crate::crypto::signed::Signed;

/// Direct storage access for trusted local operations.
///
/// Use this for:
/// - Blob operations (content-addressed, not sedimentree-scoped)
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

    // ==================== Blob Operations ====================

    /// Save a blob under a sedimentree, returning its digest.
    #[must_use]
    pub fn save_blob<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> K::Future<'_, Result<Digest<Blob>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.save_blob(sedimentree_id, blob)
    }

    /// Load blobs by their digests within a sedimentree.
    ///
    /// Returns only the blobs that were found. Missing digests are silently skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to load the blobs.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_blobs<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
        digests: &[Digest<Blob>],
    ) -> K::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_blobs(sedimentree_id, digests)
    }

    /// Load a single blob by its digest within a sedimentree.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails to load the blob.
    pub async fn load_blob<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Blob>,
    ) -> Result<Option<Blob>, S::Error>
    where
        S: Storage<K>,
    {
        self.storage.load_blob(sedimentree_id, digest).await
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

    /// Load loose commits for a sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_loose_commits<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, S::Error>>
    where
        S: Storage<K>,
    {
        self.storage.load_loose_commits(sedimentree_id)
    }

    /// Load fragments for a sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_fragments<K: FutureForm>(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, S::Error>>
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
