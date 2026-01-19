//! Fragment types for Sedimentree data partitioning.

use alloc::vec::Vec;

use crate::{
    blob::{BlobMeta, Digest},
    collections::Set,
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
};

/// A portion of a Sedimentree that includes a set of checkpoints.
///
/// This is created by breaking up (fragmenting) a larger document or log
/// into smaller pieces (a "fragment"). Since Sedimentree is not able to
/// read the content in a particular fragment (e.g. because it's in
/// an arbitrary format or is encrypted), it maintains some basic
/// metadata about the the content to aid in deduplication and synchronization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fragment {
    summary: FragmentSummary,
    checkpoints: Vec<Digest>,
    digest: Digest,
}

impl Fragment {
    /// Constructor for a [`Fragment`].
    #[must_use]
    pub fn new(
        head: Digest,
        boundary: Vec<Digest>,
        checkpoints: Vec<Digest>,
        blob_meta: BlobMeta,
    ) -> Self {
        let digest = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(head.as_bytes());

            for end in &boundary {
                hasher.update(end.as_bytes());
            }
            hasher.update(blob_meta.digest().as_bytes());

            for checkpoint in &checkpoints {
                hasher.update(checkpoint.as_bytes());
            }

            Digest::from(*hasher.finalize().as_bytes())
        };

        Self {
            summary: FragmentSummary {
                head,
                boundary,
                blob_meta,
            },
            checkpoints,
            digest,
        }
    }

    /// Returns true if this fragment supports the given fragment summary.
    #[must_use]
    pub fn supports<M: DepthMetric>(&self, other: &FragmentSummary, hash_metric: &M) -> bool {
        if &self.summary == other {
            return true;
        }

        if self.depth(hash_metric) < other.depth(hash_metric) {
            return false;
        }

        if self.summary.head == other.head
            && self
                .checkpoints
                .iter()
                .collect::<Set<_>>()
                .is_superset(&other.boundary.iter().collect::<Set<_>>())
        {
            return true;
        }

        if self.checkpoints.contains(&other.head)
            && other
                .boundary
                .iter()
                .all(|end| self.checkpoints.contains(end))
        {
            return true;
        }

        if self.checkpoints.contains(&other.head)
            && self
                .summary
                .boundary
                .iter()
                .collect::<Set<_>>()
                .is_superset(&other.boundary.iter().collect::<Set<_>>())
        {
            return true;
        }

        false
    }

    /// Returns true if this [`Fragment`] covers the given [`Digest`].
    #[must_use]
    pub fn supports_block(&self, fragment_end: Digest) -> bool {
        self.checkpoints.contains(&fragment_end) || self.summary.boundary.contains(&fragment_end)
    }

    /// Convert to a [`FragmentSummary`].
    #[must_use]
    pub const fn summary(&self) -> &FragmentSummary {
        &self.summary
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth<M: DepthMetric>(&self, hash_metric: &M) -> Depth {
        self.summary.depth(hash_metric)
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.summary.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &[Digest] {
        self.summary.boundary.as_slice()
    }

    /// The inner checkpoints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest> {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Fragment`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// The minimal data for a [`Fragment`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentSummary {
    head: Digest,
    boundary: Vec<Digest>,
    blob_meta: BlobMeta,
}

impl FragmentSummary {
    /// Constructor for a [`FragmentSummary`].
    #[must_use]
    pub const fn new(head: Digest, boundary: Vec<Digest>, blob_meta: BlobMeta) -> Self {
        Self {
            head,
            boundary,
            blob_meta,
        }
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &[Digest] {
        self.boundary.as_slice()
    }

    /// Basic information about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> BlobMeta {
        self.blob_meta
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth<M: DepthMetric>(&self, hash_metric: &M) -> Depth {
        hash_metric.to_depth(self.head)
    }
}

/// The barest information needed to identify a fragment.
#[derive(Debug, Clone)]
pub struct FragmentSpec {
    id: SedimentreeId,
    head: Digest,
    checkpoints: Vec<Digest>,
    boundary: Vec<Digest>,
}

impl FragmentSpec {
    /// Constructor for a [`FragmentSpec`].
    #[must_use]
    pub const fn new(
        id: SedimentreeId,
        head: Digest,
        checkpoints: Vec<Digest>,
        boundary: Vec<Digest>,
    ) -> Self {
        Self {
            id,
            head,
            checkpoints,
            boundary,
        }
    }

    /// The sedimentree ID.
    #[must_use]
    pub const fn id(&self) -> SedimentreeId {
        self.id
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &[Digest] {
        self.boundary.as_slice()
    }

    /// The inner checkpoints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest> {
        &self.checkpoints
    }
}
