//! Put capability for authorized write access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Set, digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::{fetcher::Fetcher, traits::Storage};
use crate::crypto::{signed::Signed, verified::Verified};

/// A capability granting put access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that put access was authorized
/// - The storage backend to write to
///
/// Created via [`Subduction::authorize_put`][crate::subduction::Subduction].
///
/// A `Putter` also grants fetch access (put implies fetch).
pub struct Putter<K: FutureForm, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureForm, S: Storage<K>> Putter<K, S> {
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

    // ==================== Commits ====================

    /// Save a verified loose commit, returning its digest.
    ///
    /// Takes [`Verified<LooseCommit>`] to enforce that only verified data is stored.
    /// The underlying [`Signed`] is extracted and persisted.
    #[must_use]
    pub fn save_loose_commit(
        &self,
        verified: Verified<LooseCommit>,
    ) -> K::Future<'_, Result<Digest<LooseCommit>, S::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, verified.into_signed())
    }

    /// Load a loose commit by its digest.
    #[must_use]
    pub fn load_loose_commit(
        &self,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<Signed<LooseCommit>>, S::Error>> {
        self.storage.load_loose_commit(self.sedimentree_id, digest)
    }

    /// List all commit digests for this sedimentree.
    #[must_use]
    pub fn list_commit_digests(&self) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, S::Error>> {
        self.storage.list_commit_digests(self.sedimentree_id)
    }

    /// Load all loose commits for this sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_loose_commits(
        &self,
    ) -> K::Future<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, S::Error>> {
        self.storage.load_loose_commits(self.sedimentree_id)
    }

    // ==================== Fragments ====================

    /// Save a verified fragment, returning its digest.
    ///
    /// Takes [`Verified<Fragment>`] to enforce that only verified data is stored.
    /// The underlying [`Signed`] is extracted and persisted.
    #[must_use]
    pub fn save_fragment(
        &self,
        verified: Verified<Fragment>,
    ) -> K::Future<'_, Result<Digest<Fragment>, S::Error>> {
        self.storage
            .save_fragment(self.sedimentree_id, verified.into_signed())
    }

    /// Load a fragment by its digest.
    #[must_use]
    pub fn load_fragment(
        &self,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<Signed<Fragment>>, S::Error>> {
        self.storage.load_fragment(self.sedimentree_id, digest)
    }

    /// List all fragment digests for this sedimentree.
    #[must_use]
    pub fn list_fragment_digests(&self) -> K::Future<'_, Result<Set<Digest<Fragment>>, S::Error>> {
        self.storage.list_fragment_digests(self.sedimentree_id)
    }

    /// Load all fragments for this sedimentree.
    ///
    /// Returns digests alongside signed data for efficient indexing.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_fragments(
        &self,
    ) -> K::Future<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, S::Error>> {
        self.storage.load_fragments(self.sedimentree_id)
    }

    // ==================== Blobs ====================

    /// Save a blob and return its digest.
    ///
    /// Note: Blob storage is content-addressed and not per-sedimentree.
    #[must_use]
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest<Blob>, S::Error>> {
        self.storage.save_blob(blob)
    }

    /// Load a blob by its digest.
    #[must_use]
    pub fn load_blob(&self, digest: Digest<Blob>) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(digest)
    }

    // ==================== Bookkeeping ====================

    /// Register this sedimentree ID as having data stored.
    ///
    /// This is bookkeeping to track which sedimentrees exist.
    #[must_use]
    pub fn save_sedimentree_id(&self) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_sedimentree_id(self.sedimentree_id)
    }
}

impl<K: FutureForm, S: Storage<K>> Clone for Putter<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureForm, S: Storage<K>> core::fmt::Debug for Putter<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Putter")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
