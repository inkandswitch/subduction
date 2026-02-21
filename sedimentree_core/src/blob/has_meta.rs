//! Trait for types that have associated blob metadata.

use super::BlobMeta;

/// Types that have associated blob metadata.
///
/// This trait is implemented by types that reference a blob and declare its
/// size and content hash. Used by `VerifiedMeta` (in `subduction_crypto`) to
/// verify that blob content matches the declared metadata.
pub trait HasBlobMeta {
    /// Returns the blob metadata for this type.
    fn blob_meta(&self) -> BlobMeta;
}
