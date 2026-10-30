//! Trait for types that have associated blob metadata.

use super::BlobMeta;

/// Types that have associated blob metadata.
///
/// This trait is implemented by types that reference a blob and declare its
/// size and content hash.
pub trait HasBlobMeta {
    /// Arguments needed to construct this type (besides `BlobMeta`).
    type Args;

    /// Construction failed because the metadata is invalid.
    type Error: core::error::Error;

    /// Returns the blob metadata for this type.
    fn blob_meta(&self) -> BlobMeta;

    /// Construct from arguments and blob metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the arguments violate the type's wire-format limits.
    fn from_args(args: Self::Args, blob_meta: BlobMeta) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
