//! Loose commit metadata for Sedimentree.

use alloc::vec::Vec;

use crate::blob::{BlobMeta, Digest};

/// The smallest unit of metadata in a Sedimentree.
///
/// It includes the digest of the data, plus pointers to any (causal) parents.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LooseCommit {
    #[n(0)]
    digest: Digest,

    #[n(1)]
    parents: Vec<Digest>,

    #[n(2)]
    blob_meta: BlobMeta,
}

impl LooseCommit {
    /// Constructor for a [`LooseCommit`].
    #[must_use]
    pub const fn new(digest: Digest, parents: Vec<Digest>, blob_meta: BlobMeta) -> Self {
        Self {
            digest,
            parents,
            blob_meta,
        }
    }

    /// The unique [`Digest`] of this [`LooseCommit`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }

    /// The (possibly empty) list of parent commits.
    #[must_use]
    pub const fn parents(&self) -> &Vec<Digest> {
        &self.parents
    }

    /// Metadata about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> &BlobMeta {
        &self.blob_meta
    }
}
