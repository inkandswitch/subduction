//! Blob-only storage access.
//!
//! Blobs are content-addressed and not scoped to a sedimentree, so they
//! don't fit the [`Fetcher`]/[`Putter`] capability model (which requires a
//! [`SedimentreeId`]).
//!
//! [`BlobAccess`] provides the minimal interface for blob I/O without
//! exposing commit, fragment, or sedimentree ID operations.
//!
//! [`Fetcher`]: super::fetcher::Fetcher
//! [`Putter`]: super::putter::Putter
//! [`SedimentreeId`]: sedimentree_core::id::SedimentreeId

use alloc::{sync::Arc, vec::Vec};
use core::marker::PhantomData;

use future_form::FutureForm;
use sedimentree_core::{blob::Blob, digest::Digest};

use super::traits::Storage;

/// Blob-only storage access.
///
/// Content-addressed blob operations that don't require a sedimentree scope.
/// This is intentionally limited to blobs — for commit/fragment access, use
/// [`Fetcher`](super::fetcher::Fetcher) or [`Putter`](super::putter::Putter).
pub struct BlobAccess<K: FutureForm, S: Storage<K>> {
    storage: Arc<S>,
    _phantom: PhantomData<K>,
}

impl<K: FutureForm, S: Storage<K>> BlobAccess<K, S> {
    pub(super) const fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            _phantom: PhantomData,
        }
    }

    /// Save a blob, returning its content-addressed digest.
    #[must_use]
    pub fn save_blob(&self, blob: Blob) -> K::Future<'_, Result<Digest<Blob>, S::Error>> {
        self.storage.save_blob(blob)
    }

    /// Load a single blob by its digest.
    #[must_use]
    pub fn load_blob(&self, digest: Digest<Blob>) -> K::Future<'_, Result<Option<Blob>, S::Error>> {
        self.storage.load_blob(digest)
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
        self.storage.load_blobs(digests)
    }
}

impl<K: FutureForm, S: Storage<K>> Clone for BlobAccess<K, S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K: FutureForm, S: Storage<K>> core::fmt::Debug for BlobAccess<K, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BlobAccess").finish_non_exhaustive()
    }
}
