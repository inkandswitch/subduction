//! Fragment types for Sedimentree data partitioning.

pub mod id;

use alloc::{collections::BTreeSet, vec::Vec};
use id::FragmentId;

use crate::{
    blob::BlobMeta,
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};

/// A portion of a Sedimentree that includes a set of checkpoints.
///
/// This is created by breaking up (fragmenting) a larger document or log
/// into smaller pieces (a "fragment"). Since Sedimentree is not able to
/// read the content in a particular fragment (e.g. because it's in
/// an arbitrary format or is encrypted), it maintains some basic
/// metadata about the the content to aid in deduplication and synchronization.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fragment {
    #[n(0)]
    summary: FragmentSummary,
    #[n(1)]
    checkpoints: Vec<Digest<LooseCommit>>,
    #[n(2)]
    digest: Digest<Fragment>,
}

impl Fragment {
    /// Constructor for a [`Fragment`].
    #[must_use]
    pub fn new(
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: Vec<Digest<LooseCommit>>,
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

            Digest::from_bytes(*hasher.finalize().as_bytes())
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
            && other
                .boundary
                .iter()
                .all(|end| self.checkpoints.contains(end))
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
            && self.summary.boundary.is_superset(&other.boundary)
        {
            return true;
        }

        false
    }

    /// Returns true if this [`Fragment`] covers the given [`Digest`].
    #[must_use]
    pub fn supports_block(&self, fragment_end: Digest<LooseCommit>) -> bool {
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
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.summary.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.summary.boundary
    }

    /// The inner checkpoints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest<LooseCommit>> {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Fragment`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest<Fragment> {
        self.digest
    }
}

/// A [`Fragment`] paired with its precomputed [`FragmentId`].
///
/// The [`FragmentId`] is computed once at construction and cached for fast
/// lookup during fingerprinting and sync operations. This is an in-memory
/// optimization — `IndexedFragment` is never serialized. Use
/// [`IndexedFragment::new`] to wrap a decoded [`Fragment`].
#[derive(Debug, Clone)]
pub struct IndexedFragment {
    fragment: Fragment,
    id: FragmentId,
}

impl IndexedFragment {
    /// Wrap a [`Fragment`] with its precomputed [`FragmentId`].
    #[must_use]
    pub fn new(fragment: Fragment) -> Self {
        let id = FragmentId::new(fragment.head(), fragment.boundary());
        Self { fragment, id }
    }

    /// The precomputed causal identity of this fragment.
    #[must_use]
    pub const fn fragment_id(&self) -> FragmentId {
        self.id
    }

    /// The inner [`Fragment`].
    #[must_use]
    pub const fn fragment(&self) -> &Fragment {
        &self.fragment
    }

    /// Consume the wrapper and return the inner [`Fragment`].
    #[must_use]
    pub fn into_fragment(self) -> Fragment {
        self.fragment
    }
}

impl core::ops::Deref for IndexedFragment {
    type Target = Fragment;

    fn deref(&self) -> &Fragment {
        &self.fragment
    }
}

impl PartialEq for IndexedFragment {
    fn eq(&self, other: &Self) -> bool {
        self.fragment == other.fragment
    }
}

impl Eq for IndexedFragment {}

impl PartialOrd for IndexedFragment {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexedFragment {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.fragment.cmp(&other.fragment)
    }
}

impl core::hash::Hash for IndexedFragment {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.fragment.hash(state);
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for IndexedFragment {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::new(Fragment::arbitrary(u)?))
    }
}

/// The minimal data for a [`Fragment`].
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentSummary {
    #[n(0)]
    head: Digest<LooseCommit>,
    #[n(1)]
    boundary: BTreeSet<Digest<LooseCommit>>,
    #[n(2)]
    blob_meta: BlobMeta,
}

impl FragmentSummary {
    /// Constructor for a [`FragmentSummary`].
    #[must_use]
    pub const fn new(
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        blob_meta: BlobMeta,
    ) -> Self {
        Self {
            head,
            boundary,
            blob_meta,
        }
    }

    /// The head of the fragment.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.boundary
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
    head: Digest<LooseCommit>,
    seed: FingerprintSeed,
    checkpoints: BTreeSet<Fingerprint<CommitId>>,
    boundary: BTreeSet<Digest<LooseCommit>>,
}

impl FragmentSpec {
    /// Constructor for a [`FragmentSpec`].
    #[must_use]
    pub const fn new(
        id: SedimentreeId,
        head: Digest<LooseCommit>,
        seed: FingerprintSeed,
        checkpoints: BTreeSet<Fingerprint<CommitId>>,
        boundary: BTreeSet<Digest<LooseCommit>>,
    ) -> Self {
        Self {
            id,
            head,
            seed,
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
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.head
    }

    /// The (possibly ragged) end(s) of the fragment.
    #[must_use]
    pub const fn boundary(&self) -> &BTreeSet<Digest<LooseCommit>> {
        &self.boundary
    }

    /// The fingerprint seed used for checkpoint fingerprints.
    #[must_use]
    pub const fn seed(&self) -> &FingerprintSeed {
        &self.seed
    }

    /// The inner checkpoint fingerprints of the fragment.
    #[must_use]
    pub const fn checkpoints(&self) -> &BTreeSet<Fingerprint<CommitId>> {
        &self.checkpoints
    }
}
