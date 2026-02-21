//! A blob with verified metadata.

use super::{Blob, BlobMeta};

/// A blob with verified metadata.
///
/// Guarantees `meta` was computed from `blob` by construction.
#[derive(Debug, Clone)]
pub struct VerifiedBlobMeta {
    meta: BlobMeta,
    blob: Blob,
}

impl VerifiedBlobMeta {
    /// Create by computing metadata from the blob.
    #[must_use]
    pub fn new(blob: Blob) -> Self {
        Self {
            meta: blob.meta(),
            blob,
        }
    }

    /// The verified metadata.
    #[must_use]
    pub const fn meta(&self) -> BlobMeta {
        self.meta
    }

    /// The blob content.
    #[must_use]
    pub const fn blob(&self) -> &Blob {
        &self.blob
    }

    /// Consume and return parts.
    #[must_use]
    pub fn into_parts(self) -> (BlobMeta, Blob) {
        (self.meta, self.blob)
    }
}
