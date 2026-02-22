//! Put capability for authorized write access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

use super::{fetcher::Fetcher, traits::Storage};

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
    pub(super) const fn new(storage: Arc<S>, sedimentree_id: SedimentreeId) -> Self {
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

    /// Save a commit with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<LooseCommit>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// The commit and blob are stored atomically.
    #[must_use]
    pub fn save_commit(
        &self,
        verified: VerifiedMeta<LooseCommit>,
    ) -> K::Future<'_, Result<(), S::Error>> {
        self.storage
            .save_loose_commit(self.sedimentree_id, verified)
    }

    /// Load a loose commit with its blob by digest.
    ///
    /// Returns `None` if no commit exists with the given digest.
    #[must_use]
    pub fn load_loose_commit(
        &self,
        digest: Digest<LooseCommit>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<LooseCommit>>, S::Error>> {
        self.storage.load_loose_commit(self.sedimentree_id, digest)
    }

    /// List all commit digests for this sedimentree.
    #[must_use]
    pub fn list_commit_digests(&self) -> K::Future<'_, Result<Set<Digest<LooseCommit>>, S::Error>> {
        self.storage.list_commit_digests(self.sedimentree_id)
    }

    /// Load all loose commits with their blobs for this sedimentree.
    #[must_use]
    pub fn load_loose_commits(
        &self,
    ) -> K::Future<'_, Result<Vec<VerifiedMeta<LooseCommit>>, S::Error>> {
        self.storage.load_loose_commits(self.sedimentree_id)
    }

    // ==================== Fragments ====================

    /// Save a fragment with verified blob metadata.
    ///
    /// Takes [`VerifiedMeta<Fragment>`] to enforce at compile time that:
    /// 1. The signature has been verified
    /// 2. The blob content matches the claimed metadata
    ///
    /// The fragment and blob are stored atomically.
    #[must_use]
    pub fn save_fragment(
        &self,
        verified: VerifiedMeta<Fragment>,
    ) -> K::Future<'_, Result<(), S::Error>> {
        self.storage.save_fragment(self.sedimentree_id, verified)
    }

    /// Load a fragment with its blob by digest.
    ///
    /// Returns `None` if no fragment exists with the given digest.
    #[must_use]
    pub fn load_fragment(
        &self,
        digest: Digest<Fragment>,
    ) -> K::Future<'_, Result<Option<VerifiedMeta<Fragment>>, S::Error>> {
        self.storage.load_fragment(self.sedimentree_id, digest)
    }

    /// List all fragment digests for this sedimentree.
    #[must_use]
    pub fn list_fragment_digests(&self) -> K::Future<'_, Result<Set<Digest<Fragment>>, S::Error>> {
        self.storage.list_fragment_digests(self.sedimentree_id)
    }

    /// Load all fragments with their blobs for this sedimentree.
    #[must_use]
    pub fn load_fragments(&self) -> K::Future<'_, Result<Vec<VerifiedMeta<Fragment>>, S::Error>> {
        self.storage.load_fragments(self.sedimentree_id)
    }

    // ==================== Batch Operations ====================

    /// Save a batch of commits and fragments with their blobs.
    ///
    /// Returns the count of items saved.
    #[must_use]
    pub fn save_batch(
        &self,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> K::Future<'_, Result<usize, S::Error>> {
        self.storage
            .save_batch(self.sedimentree_id, commits, fragments)
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
