//! Per-sedimentree blob storage access.
//!
//! [`BlobAccess`] provides blob I/O scoped to a specific sedimentree,
//! without exposing commit, fragment, or sedimentree ID operations.
//!
//! [`Fetcher`]: super::fetcher::Fetcher
//! [`Putter`]: super::putter::Putter

use alloc::{sync::Arc, vec::Vec};
use core::marker::PhantomData;

use future_form::FutureForm;
use sedimentree_core::{blob::Blob, crypto::digest::Digest, id::SedimentreeId};

use super::traits::Storage;

/// Per-sedimentree blob storage access.
///
/// Provides blob I/O scoped to a specific sedimentree. For commit/fragment
/// access, use [`Fetcher`](super::fetcher::Fetcher) or
/// [`Putter`](super::putter::Putter).
pub struct BlobAccess<K: FutureForm, S: Storage<K>> {
    storage: Arc<S>,
    sedimentree_id: SedimentreeId,
    _phantom: PhantomData<K>,
}

impl<K: FutureForm, S: Storage<K>> BlobAccess<K, S> {
    pub(super) fn new(storage: Arc<S>, sedimentree_id: SedimentreeId) -> Self {
        Self {
            storage,
            sedimentree_id,
            _phantom: PhantomData,
        }
    }

    /// Save a blob, returning its content-addressed digest.
    #[must_use]
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest<Blob>, S::Error>> {
        self.storage.save_blob(self.sedimentree_id, blob)
    }

    /// Load a single blob by its digest.
    #[must_use]
    pub fn load_blob(&self, digest: Digest<Blob>) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(self.sedimentree_id, digest)
    }

    /// Load multiple blobs by their digests.
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

impl<K: FutureForm, S: Storage<K>> Clone for BlobAccess<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            sedimentree_id: self.sedimentree_id,
            _phantom: PhantomData,
        }
    }
}

impl<K: FutureForm, S: Storage<K>> core::fmt::Debug for BlobAccess<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlobAccess")
            .field("sedimentree_id", &self.sedimentree_id)
            .finish_non_exhaustive()
    }
}
