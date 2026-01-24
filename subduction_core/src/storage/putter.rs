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

use crate::policy::Generation;
use super::fetcher::Fetcher;

/// A capability granting put access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that put access was authorized
/// - The storage backend to write to
/// - The generation counter for revocation checking
///
/// Created via [`Subduction::authorize_put`][crate::subduction::Subduction].
///
/// A `Putter` also grants fetch access (put implies fetch).
pub struct Putter<K: FutureKind, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    generation: Generation,
    _marker: PhantomData<K>,
}

impl<K: FutureKind, S: Storage<K>> Putter<K, S> {
    /// Create a new putter capability.
    ///
    /// This should only be called after authorization has been verified.
    pub(crate) fn new(storage: Arc<S>, sedimentree_id: SedimentreeId, generation: Generation) -> Self {
        Self {
            storage,
            sedimentree_id,
            generation,
            _marker: PhantomData,
        }
    }

    /// Get the sedimentree ID this capability grants access to.
    #[must_use]
    pub const fn sedimentree_id(&self) -> SedimentreeId {
        self.sedimentree_id
    }

    /// Get the generation counter when this capability was issued.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    /// Downgrade to a fetch-only capability.
    ///
    /// This is useful when you have put access but only need to fetch.
    #[must_use]
    pub fn as_fetcher(&self) -> Fetcher<K, S> {
        Fetcher::new(self.storage.clone(), self.sedimentree_id, self.generation)
    }

    // === Fetch operations (put implies fetch) ===

    /// Load all loose commits for this sedimentree.
    pub fn load_loose_commits(&self) -> K::Future<'_, Result<Vec<LooseCommit>, S::Error>> {
        self.storage.load_loose_commits(self.sedimentree_id)
    }

    /// Load all fragments for this sedimentree.
    pub fn load_fragments(&self) -> K::Future<'_, Result<Vec<Fragment>, S::Error>> {
        self.storage.load_fragments(self.sedimentree_id)
    }

    /// Load a blob by its digest.
    pub fn load_blob(&self, digest: Digest) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(digest)
    }

    // === Put operations ===

    /// Save a loose commit to this sedimentree.
    pub fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> K::Future<'_, Result<(), S::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, loose_commit)
    }

    /// Save a fragment to this sedimentree.
    pub fn save_fragment(&self, fragment: Fragment) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_fragment(self.sedimentree_id, fragment)
    }

    /// Save a blob and return its digest.
    ///
    /// Note: Blob storage is content-addressed and not per-sedimentree.
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest, S::Error>> {
        self.storage.save_blob(blob)
    }

    /// Delete all loose commits for this sedimentree.
    pub fn delete_loose_commits(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_loose_commits(self.sedimentree_id)
    }

    /// Delete all fragments for this sedimentree.
    pub fn delete_fragments(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_fragments(self.sedimentree_id)
    }

    /// Delete a blob by its digest.
    pub fn delete_blob(&self, digest: Digest) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.delete_blob(digest)
    }
}

impl<K: FutureKind, S: Storage<K>> Clone for Putter<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            generation: self.generation,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureKind, S: Storage<K>> core::fmt::Debug for Putter<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
