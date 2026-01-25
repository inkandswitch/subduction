//! Fetch capability for authorized read access to sedimentrees.

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

/// A capability granting fetch access to a specific sedimentree's data.
///
/// This type bundles:
/// - Proof that fetch access was authorized
/// - The storage backend to fetch from
///
/// Created via [`Subduction::authorize_fetch`][crate::subduction::Subduction].
pub struct Fetcher<K: FutureKind, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _marker: PhantomData<K>,
}

impl<K: FutureKind, S: Storage<K>> Fetcher<K, S> {
    /// Create a new fetcher capability.
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
    ///
    /// Note: Blob storage is content-addressed and not per-sedimentree,
    /// but this capability implies access to blobs referenced by the sedimentree's commits.
    #[must_use]
    pub fn load_blob(&self, digest: Digest) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(digest)
    }
}

impl<K: FutureKind, S: Storage<K>> Clone for Fetcher<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _marker: PhantomData,
        }
    }
}

impl<K: FutureKind, S: Storage<K>> core::fmt::Debug for Fetcher<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Fetcher")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
