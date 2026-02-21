//! A blob bundled with its metadata, guaranteeing they match by construction.

use super::{has_meta::HasBlobMeta, Blob, BlobMeta};

/// A blob bundled with metadata, guaranteeing they match by construction.
///
/// This provides compile-time assurance that `T` was created from this blob's
/// metadata, avoiding runtime checks when the relationship is known statically.
///
/// # Example
///
/// ```ignore
/// let blob_with_meta = BlobWithMeta::new(blob, |meta| {
///     LooseCommit::new(digest, parents, meta)
/// });
/// let verified_meta = VerifiedMeta::seal(&signer, blob_with_meta).await;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobWithMeta<T: HasBlobMeta> {
    meta: T,
    blob: Blob,
}

impl<T: HasBlobMeta> BlobWithMeta<T> {
    /// Create a `BlobWithMeta` from a constructor function.
    ///
    /// The constructor receives [`BlobMeta`] computed from the blob and must
    /// return a `T` that incorporates that metadata.
    pub fn new<F>(blob: Blob, f: F) -> Self
    where
        F: FnOnce(BlobMeta) -> T,
    {
        let blob_meta = blob.meta();
        Self {
            meta: f(blob_meta),
            blob,
        }
    }

    /// Get the metadata (commit or fragment).
    #[must_use]
    pub fn meta(&self) -> &T {
        &self.meta
    }

    /// Get the blob content.
    #[must_use]
    pub fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Consume and return parts.
    #[must_use]
    pub fn into_parts(self) -> (T, Blob) {
        (self.meta, self.blob)
    }
}
