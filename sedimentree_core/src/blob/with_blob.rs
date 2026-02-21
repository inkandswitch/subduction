//! A payload bundled with its blob, guaranteeing metadata matches by construction.

use super::{Blob, BlobMeta, HasBlobMeta};

/// A payload bundled with its blob, guaranteeing metadata matches by construction.
///
/// This provides compile-time assurance that `T` was created from this blob's
/// metadata, avoiding runtime checks when the relationship is known statically.
///
/// # Example
///
/// ```ignore
/// let with_blob = WithBlob::new(blob, |meta| {
///     LooseCommit::new(digest, parents, meta)
/// });
/// let verified_meta = VerifiedMeta::seal(&signer, with_blob).await;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WithBlob<T: HasBlobMeta> {
    inner: T,
    blob: Blob,
}

impl<T: HasBlobMeta> WithBlob<T> {
    /// Create a `WithBlob` from a constructor function.
    ///
    /// The constructor receives [`BlobMeta`] computed from the blob and must
    /// return a `T` that incorporates that metadata.
    pub fn new<F>(blob: Blob, f: F) -> Self
    where
        F: FnOnce(BlobMeta) -> T,
    {
        let meta = blob.meta();
        Self {
            inner: f(meta),
            blob,
        }
    }

    /// Get the metadata (commit or fragment).
    #[must_use]
    pub fn metadata(&self) -> &T {
        &self.inner
    }

    /// Get the blob content.
    #[must_use]
    pub fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Consume and return parts.
    #[must_use]
    pub fn into_parts(self) -> (T, Blob) {
        (self.inner, self.blob)
    }
}
