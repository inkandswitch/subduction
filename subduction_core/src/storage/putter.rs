//! Put capability for authorized write access to sedimentrees.

use core::marker::PhantomData;

use alloc::sync::Arc;
use alloc::vec::Vec;

use futures_kind::FutureKind;
use sedimentree_core::{
    blob::{Blob, Digest},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    storage::Storage,
};

use super::fetcher::Fetcher;

/// A capability granting put access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that put access was authorized
/// - The storage backend to write to
///
/// Created via [`Subduction::authorize_put`][crate::subduction::Subduction].
///
/// A `Putter` also grants fetch access (put implies fetch).
pub struct Putter<K: FutureKind, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureKind, S: Storage<K>> Putter<K, S> {
    /// Create a new putter capability.
    ///
    /// This should only be called after authorization has been verified.
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

    /// Downgrade to a fetch-only capability.
    ///
    /// This is useful when you have put access but only need to fetch.
    #[must_use]
    pub fn as_fetcher(&self) -> Fetcher<K, S> {
        Fetcher::new(self.storage.clone(), self.sedimentree_id)
    }

    /// Load all loose commits for this sedimentree.
    #[must_use]
    pub fn load_loose_commits(&self) -> K::Future<'_, Result<Vec<LooseCommit>, S::Error>> {
        self.storage.load_loose_commits(self.sedimentree_id)
    }

    /// Load all fragments for this sedimentree.
    #[must_use]
    pub fn load_fragments(&self) -> K::Future<'_, Result<Vec<Fragment>, S::Error>> {
        self.storage.load_fragments(self.sedimentree_id)
    }

    /// Load a blob by its digest.
    #[must_use]
    pub fn load_blob(&self, digest: Digest) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(digest)
    }

    /// Save a loose commit to this sedimentree.
    #[must_use]
    pub fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), S::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, loose_commit)
    }

    /// Save a fragment to this sedimentree.
    #[must_use]
    pub fn save_fragment(&self, fragment: Fragment) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_fragment(self.sedimentree_id, fragment)
    }

    /// Save a blob and return its digest.
    ///
    /// Note: Blob storage is content-addressed and not per-sedimentree.
    #[must_use]
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, S::Error>> {
        self.storage.save_blob(blob)
    }

    /// Register this sedimentree ID as having data stored.
    ///
    /// This is bookkeeping to track which sedimentrees exist.
    #[must_use]
    pub fn save_sedimentree_id(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_sedimentree_id(self.sedimentree_id)
    }
}

impl<K: FutureKind, S: Storage<K>> Clone for Putter<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureKind, S: Storage<K>> core::fmt::Debug for Putter<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
