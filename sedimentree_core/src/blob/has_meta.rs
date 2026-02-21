//! Trait for types that have associated blob metadata.

use super::BlobMeta;

/// Types that have associated blob metadata.
///
/// This trait is implemented by types that reference a blob and declare its
/// size and content hash.
pub trait HasBlobMeta {
    /// Arguments needed to construct this type (besides `BlobMeta`).
    type Args;

    /// Returns the blob metadata for this type.
    fn blob_meta(&self) -> BlobMeta;

    /// Construct from arguments and blob metadata.
    fn from_args(args: Self::Args, blob_meta: BlobMeta) -> Self;
}
