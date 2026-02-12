//! Fetch capability for authorized read access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    blob::Blob, collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};

use super::traits::Storage;
use crate::crypto::signed::Signed;

/// A capability granting fetch access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that fetch access was authorized
/// - The storage backend to fetch from
///
/// Created via [`Subduction::authorize_fetch`][crate::subduction::Subduction].
pub struct Fetcher<K: FutureForm, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureForm, S: Storage<K>> Fetcher<K, S> {
    /// Create a new fetcher capability.
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

    // ==================== Commits ====================

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

    /// Load a blob by its digest.
    ///
    /// Load a blob by its digest within this sedimentree.
    #[must_use]
    pub fn load_blob(&self, digest: Digest<Blob>) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(self.sedimentree_id, digest)
    }

    /// Load multiple blobs by their digests within this sedimentree.
    ///
    /// Returns only the blobs that were found. Missing digests are silently skipped.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn load_blobs(
        &self,
        digests: &[Digest<Blob>],
    ) -> K::Future<'_, Result<Vec<(Digest<Blob>, Blob)>, S::Error>> {
        self.storage.load_blobs(self.sedimentree_id, digests)
    }
}

impl<K: FutureForm, S: Storage<K>> Clone for Fetcher<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureForm, S: Storage<K>> core::fmt::Debug for Fetcher<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Fetcher")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
