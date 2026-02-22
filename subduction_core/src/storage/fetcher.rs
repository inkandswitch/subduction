//! Fetch capability for authorized read access to sedimentrees.

use core::marker::PhantomData;

use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;
use sedimentree_core::{
    collections::Set, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_crypto::verified_meta::VerifiedMeta;

use super::traits::Storage;

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
