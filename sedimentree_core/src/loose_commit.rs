//! Loose commit metadata for Sedimentree.

pub mod id;

use alloc::collections::BTreeSet;
use id::CommitId;

use crate::{
    blob::{BlobMeta, ClaimsBlobMeta},
    crypto::digest::Digest,
};

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
    digest: Digest<LooseCommit>,

    #[n(1)]
    parents: BTreeSet<Digest<LooseCommit>>,

    #[n(2)]
    blob_meta: BlobMeta,
}

impl LooseCommit {
    /// Extract the causal identity of this commit.
    #[must_use]
    pub const fn commit_id(&self) -> CommitId {
        CommitId::new(self.digest)
    }

    /// Constructor for a [`LooseCommit`].
    #[must_use]
    pub const fn new(
        digest: Digest<LooseCommit>,
        parents: BTreeSet<Digest<LooseCommit>>,
        blob_meta: BlobMeta,
    ) -> Self {
        Self {
            digest,
            parents,
            blob_meta,
        }
    }

    /// The unique [`Digest`] of this [`LooseCommit`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest<LooseCommit> {
        self.digest
    }

    /// The (possibly empty) set of parent commits.
    #[must_use]
    pub const fn parents(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.parents
    }

    /// Metadata about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> &BlobMeta {
        &self.blob_meta
    }
}

impl ClaimsBlobMeta for LooseCommit {
    fn claimed_blob_meta(&self) -> BlobMeta {
        *self.blob_meta()
    }
}
