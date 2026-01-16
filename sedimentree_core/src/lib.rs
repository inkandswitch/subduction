//! The core of the [Sedimentree] data partitioning scheme.
//!
//! This core library only defines the metadata tracking featrues of Sedimentree.
//! We assume that the actual data described by this metadata is not legible to the Sedimentree
//! (regardless of whether or not it's encrypted).
//!
//! Sedimentree is a way of organizing data into a series of layers, or strata, each of which
//! contains a set of checkpoints (hashes) that represent some fragment of a larger file or log.
//! For example, an Automerge document might be partitioned, and each fragment encrypted.
//! Sedimentree tracks just enough metadata to allow efficient diffing and synchronization
//! of these fragments.
//!
//! [Sedimentree]: https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

use alloc::{
    fmt::Formatter,
    str::FromStr,
    string::{String, ToString},
    vec, vec::Vec,
};
use collections::{Map, Set};

use blob::{BlobMeta, Digest};
use depth::{Depth, DepthMetric, MAX_STRATA_DEPTH};
use thiserror::Error;

pub mod blob;
pub mod commit;
mod commit_dag;
pub mod depth;
pub mod hex;
pub mod collections;
pub mod storage;

/// A unique identifier for some data managed by Sedimentree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SedimentreeId([u8; 32]);

impl SedimentreeId {
    /// Constructor for a [`SedimentreeId`].
    #[must_use]
    pub const fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// The bytes of this [`SedimentreeId`].
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The bytes of this [`SedimentreeId`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// An error indicating that a [`SedimentreeId`] could not be parsed from a string.
#[derive(Debug, Clone, Error)]
pub enum BadSedimentreeId {
    /// The provided string has an odd length.
    #[error("SedimentreeId length is not even: {0}")]
    LengthNotEven(String),

    /// The provided string contains invalid hex characters.
    #[error("SedimentreeId contains invalid hex characters: {0}")]
    InvalidHex(String),

    /// The provided string has an invalid length.
    #[error("SedimentreeId must be 32 bytes (64 hex characters): {0}")]
    InvalidLength(String),
}

impl FromStr for SedimentreeId {
    type Err = BadSedimentreeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.len().is_multiple_of(2) {
            return Err(BadSedimentreeId::LengthNotEven(s.to_string()));
        }

        let bytes = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| BadSedimentreeId::InvalidHex(s.to_string())))
            .collect::<Result<Vec<u8>, BadSedimentreeId>>()?;

        if bytes.len() == 32 {
            let mut arr = [0; 32];
            arr.copy_from_slice(&bytes);
            Ok(SedimentreeId(arr))
        } else {
            Err(BadSedimentreeId::InvalidLength(s.to_string()))
        }
    }
}

impl core::fmt::Debug for SedimentreeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl core::fmt::Display for SedimentreeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A less detailed representation of a Sedimentree that omits strata checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SedimentreeSummary {
    fragment_summaries: Set<FragmentSummary>,
    commits: Set<LooseCommit>,
}

impl SedimentreeSummary {
    /// Constructor for a [`SedimentreeSummary`].
    #[must_use]
    pub const fn new(
        fragment_summaries: Set<FragmentSummary>,
        commits: Set<LooseCommit>,
    ) -> SedimentreeSummary {
        SedimentreeSummary {
            fragment_summaries,
            commits,
        }
    }

    /// The set of fragment summaries in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn fragment_summaries(&self) -> &Set<FragmentSummary> {
        &self.fragment_summaries
    }

    /// The set of loose commits in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn loose_commits(&self) -> &Set<LooseCommit> {
        &self.commits
    }

    /// Create a [`RemoteDiff`] with empty local fragments and commits.
    #[must_use]
    pub fn as_remote_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_fragment_summaries: self.fragment_summaries.iter().collect(),
            remote_commits: self.commits.iter().collect(),
            local_fragments: Vec::new(),
            local_commits: Vec::new(),
        }
    }
}

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

/// The smallest unit of metadata in a Sedimentree.
///
/// It includes the digest of the data, plus pointers to any (causal) parents.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LooseCommit {
    digest: Digest,
    parents: Vec<Digest>,
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

/// The difference between two [`Sedimentree`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff<'a> {
    /// Fragments present in the right tree but not the left.
    pub left_missing_fragments: Vec<&'a Fragment>,

    /// Commits present in the right tree but not the left.
    pub left_missing_commits: Vec<&'a LooseCommit>,

    /// Fragments present in the left tree but not the right.
    pub right_missing_fragments: Vec<&'a Fragment>,

    /// Commits present in the left tree but not the right.
    pub right_missing_commits: Vec<&'a LooseCommit>,
}

/// The difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDiff<'a> {
    /// Fragments present in the remote tree but not the local.
    pub remote_fragment_summaries: Vec<&'a FragmentSummary>,

    /// Commits present in the remote tree but not the local.
    pub remote_commits: Vec<&'a LooseCommit>,

    /// Fragments present in the local tree but not the remote.
    pub local_fragments: Vec<&'a Fragment>,

    /// Commits present in the local tree but not the remote.
    pub local_commits: Vec<&'a LooseCommit>,
}

/// All of the Sedimentree metadata about all the fragments for a series of payload.
#[derive(Default, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(PartialOrd, Ord, Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Sedimentree {
    fragments: Set<Fragment>,
    commits: Set<LooseCommit>,
}

impl Sedimentree {
    /// Constructor for a [`Sedimentree`].
    #[must_use]
    pub fn new(fragments: Vec<Fragment>, commits: Vec<LooseCommit>) -> Self {
        Self {
            fragments: fragments.into_iter().collect(),
            commits: commits.into_iter().collect(),
        }
    }

    /// Merge another [`Sedimentree`] into this one.
    pub fn merge(&mut self, other: Sedimentree) {
        self.fragments.extend(other.fragments);
        self.commits.extend(other.commits);
    }

    /// The minimal ordered hash of this [`Sedimentree`].
    #[must_use]
    pub fn minimal_hash<M: DepthMetric>(&self, depth_metric: &M) -> MinimalTreeHash {
        let minimal = self.minimize(depth_metric);
        let mut hashes = minimal
            .fragments()
            .flat_map(|s| {
                core::iter::once(s.head())
                    .chain(s.boundary().iter().copied())
                    .chain(s.checkpoints().iter().copied())
            })
            .chain(minimal.commits.iter().map(LooseCommit::digest))
            .collect::<Vec<_>>();
        hashes.sort();

        let mut h = blake3::Hasher::new();
        for hash in hashes {
            h.update(hash.as_bytes());
        }
        MinimalTreeHash(*h.finalize().as_bytes())
    }

    /// Add a loose commit to the [`Sedimentree`].
    ///
    /// Returns `true` if the commit was not already present
    pub fn add_commit(&mut self, commit: LooseCommit) -> bool {
        self.commits.insert(commit)
    }

    /// Add a fragment to the [`Sedimentree`].
    ///
    /// Returns `true` if the stratum was not already present
    pub fn add_fragment(&mut self, fragment: Fragment) -> bool {
        self.fragments.insert(fragment)
    }

    /// Compute the difference between two local [`Sedimentree`]s.
    #[must_use]
    pub fn diff<'a>(&'a self, other: &'a Sedimentree) -> Diff<'a> {
        let our_fragments = self.fragments.iter().collect::<Set<_>>();
        let their_fragments = other.fragments.iter().collect();
        let left_missing_fragments = our_fragments.difference(&their_fragments);
        let right_missing_fragments = their_fragments.difference(&our_fragments);

        let our_commits = self.commits.iter().collect::<Set<_>>();
        let their_commits = other.commits.iter().collect();
        let left_missing_commits = our_commits.difference(&their_commits);
        let right_missing_commits = their_commits.difference(&our_commits);

        Diff {
            left_missing_fragments: left_missing_fragments.into_iter().copied().collect(),
            left_missing_commits: left_missing_commits.into_iter().copied().collect(),
            right_missing_fragments: right_missing_fragments.into_iter().copied().collect(),
            right_missing_commits: right_missing_commits.into_iter().copied().collect(),
        }
    }

    /// Compute the difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
    #[must_use]
    pub fn diff_remote<'a, M: DepthMetric>(
        &'a self,
        remote: &'a SedimentreeSummary,
        hash_metric: &M,
    ) -> RemoteDiff<'a> {
        let our_fragments_meta = self
            .fragments
            .iter()
            .map(|s| &s.summary)
            .collect::<Set<&FragmentSummary>>();
        let their_fragments = remote.fragment_summaries.iter().collect::<Set<_>>();
        let mut local_fragments = Vec::new();
        for m in our_fragments_meta.difference(&their_fragments) {
            for s in &self.fragments {
                if s.head() == m.head
                    && *s.boundary() == m.boundary
                    && s.depth(hash_metric) == m.depth(hash_metric)
                {
                    local_fragments.push(s);
                    break;
                }
            }
        }
        let remote_fragments = their_fragments.difference(&our_fragments_meta);

        let our_commits = self.commits.iter().collect::<Set<&LooseCommit>>();
        let their_commits = remote.commits.iter().collect();
        let local_commits = our_commits.difference(&their_commits);
        let remote_commits = their_commits.difference(&our_commits);

        RemoteDiff {
            remote_fragment_summaries: remote_fragments.into_iter().copied().collect(),
            remote_commits: remote_commits.into_iter().copied().collect(),
            local_fragments,
            local_commits: local_commits.into_iter().copied().collect(),
        }
    }

    /// Iterate over all fragments in this [`Sedimentree`].
    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.iter()
    }

    /// Iterate over all loose commits in this [`Sedimentree`].
    pub fn loose_commits(&self) -> impl Iterator<Item = &LooseCommit> {
        self.commits.iter()
    }

    /// Returns true if this [`Sedimentree`] has a fragment with the given digest.
    #[must_use]
    pub fn has_loose_commit(&self, digest: Digest) -> bool {
        self.loose_commits().any(|c| c.digest() == digest)
    }

    /// Returns true if this [`Sedimentree`] has a fragment starting with the given digest.
    #[must_use]
    pub fn has_fragment_starting_with<M: DepthMetric>(
        &self,
        digest: Digest,
        depth_metric: &M,
    ) -> bool {
        self.heads(depth_metric).contains(&digest)
    }

    /// Prune a [`Sedimentree`].
    ///
    /// Minimize the [`Sedimentree`] by removing any fragments that are
    /// fully supported by other fragments, and removing any loose commits
    /// that are not needed to support the remaining fragments.
    #[must_use]
    pub fn minimize<M: DepthMetric>(&self, depth_metric: &M) -> Sedimentree {
        // First sort fragments by depth, then for each stratum below the lowest
        // level, discard that stratum if it is supported by any of the stratum
        // above it.
        let mut fragments = self.fragments.iter().collect::<Vec<_>>();
        fragments.sort_by_key(|a| a.depth(depth_metric));

        let mut minimized_fragments = Vec::<Fragment>::new();

        for fragment in fragments {
            if !minimized_fragments
                .iter()
                .any(|existing| existing.supports(&fragment.summary, depth_metric))
            {
                minimized_fragments.push(fragment.clone());
            }
        }

        // Now, form a commit graph from the loose commits and simplify it relative to the minimized fragments
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let simplified_dag = dag.simplify(&minimized_fragments, depth_metric);

        let commits = self
            .commits
            .iter()
            .filter(|&c| simplified_dag.contains_commit(&c.digest()))
            .cloned()
            .collect();

        Sedimentree::new(minimized_fragments, commits)
    }

    /// Create a [`SedimentreeSummary`] from this [`Sedimentree`].
    ///
    /// This omits the checkpoints from each fragment.
    /// It is useful for sending over the wire.
    #[must_use]
    pub fn summarize(&self) -> SedimentreeSummary {
        SedimentreeSummary {
            fragment_summaries: self
                .fragments
                .iter()
                .map(|fragment| fragment.summary.clone())
                .collect(),
            commits: self.commits.clone(),
        }
    }

    /// The heads of a Sedimentree are the end hashes of all strata which are
    /// not the start of any other strata or supported by any lower stratum
    /// and which do not appear in the [`LooseCommit`] graph, plus the heads of
    /// the loose commit graph.
    #[must_use]
    pub fn heads<M: DepthMetric>(&self, depth_metric: &M) -> Vec<Digest> {
        let minimized = self.minimize(depth_metric);
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.iter());
        let mut heads = Vec::<Digest>::new();
        for fragment in &minimized.fragments {
            if !minimized
                .fragments
                .iter()
                .any(|s| s.boundary().contains(&fragment.head()))
                && fragment
                    .boundary()
                    .iter()
                    .all(|end| !dag.contains_commit(end))
            {
                heads.extend(fragment.boundary());
            }
        }
        heads.extend(dag.heads());
        heads
    }

    /// Consume this [`Sedimentree`] and return an iterator over all its items.
    pub fn into_items(self) -> impl Iterator<Item = CommitOrFragment> {
        self.fragments
            .into_iter()
            .map(CommitOrFragment::Fragment)
            .chain(self.commits.into_iter().map(CommitOrFragment::Commit))
    }

    /// Given a [`SedimentreeId`], return the [`Fragment`]s that are missing to fill in the gaps.
    #[must_use]
    pub fn missing_fragments<M: DepthMetric>(
        &self,
        id: SedimentreeId,
        depth_metric: &M,
    ) -> Vec<FragmentSpec> {
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let mut runs_by_level = Map::<Depth, (Digest, Vec<Digest>)>::new();
        let mut all_bundles = Vec::new();
        for commit_hash in dag.canonical_sequence(self.fragments.iter(), depth_metric) {
            let level = depth_metric.to_depth(commit_hash);
            for (run_level, (_start, checkpoints)) in &mut runs_by_level {
                if run_level < &level {
                    checkpoints.push(commit_hash);
                }
            }
            if level >= crate::MAX_STRATA_DEPTH && let Some((head, checkpoints)) = runs_by_level.remove(&level) {
                if self.fragments.iter().any(|s| s.supports_block(commit_hash)) {
                    runs_by_level.insert(level, (commit_hash, Vec::new()));
                } else {
                    all_bundles.push(FragmentSpec {
                        id,
                        head,
                        boundary: vec![commit_hash],
                        checkpoints: checkpoints.clone(),
                    });
                }
            }
        }
        all_bundles
    }

    /// Create a [`RemoteDiff`] with empty remote fragments and commits.
    #[must_use]
    pub fn as_local_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_fragment_summaries: Vec::new(),
            remote_commits: Vec::new(),
            local_fragments: self.fragments.iter().collect(),
            local_commits: self.commits.iter().collect(),
        }
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

/// An enum over either a [`LooseCommit`] or a [`Fragment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOrFragment {
    /// A loose commit.
    Commit(LooseCommit),

    /// A fragment.
    Fragment(Fragment),
}

impl core::fmt::Debug for Sedimentree {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Sedimentree")
            .field("fragments", &self.fragments.len())
            .field("commits", &self.commits.len())
            .finish()
    }
}

/// The minimum ordered hash of a [`Sedimentree`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct MinimalTreeHash([u8; 32]);

impl MinimalTreeHash {
    /// The bytes of this [`MinimalTreeHash`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for MinimalTreeHash {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

/// Checks if any of the given commits has a commit boundary.
pub fn has_commit_boundary<I: IntoIterator<Item = D>, D: Into<Digest>, M: DepthMetric>(
    commits: I,
    depth_metric: &M,
) -> bool {
    commits
        .into_iter()
        .any(|digest| depth_metric.to_depth(digest.into()) <= MAX_STRATA_DEPTH)
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::{blob::BlobMeta, commit::CountLeadingZeroBytes};

    use super::*;

    fn hash_with_leading_zeros(zeros_count: u32) -> Digest {
        let mut byte_arr: [u8; 32] = rand::rng().random::<[u8; 32]>();
        for slot in byte_arr.iter_mut().take(zeros_count as usize) {
            *slot = 0;
        }
        // Ensure the byte after the zeros is non-zero to prevent accidentally
        // having more leading zeros than intended
        if (zeros_count as usize) < 32 {
            let idx = zeros_count as usize;
            #[allow(clippy::indexing_slicing)]
            if byte_arr[idx] == 0 {
                byte_arr[idx] = 1; // Make it non-zero
            }
        }
        Digest::from(byte_arr)
    }

    #[test]
    fn fragment_supports_higher_levels() {
        #[derive(Debug)]
        struct Scenario {
            deeper: Fragment,
            shallower: FragmentSummary,
        }
        impl<'a> arbitrary::Arbitrary<'a> for Scenario {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                #[allow(clippy::enum_variant_names)]
                #[derive(arbitrary::Arbitrary)]
                enum ShallowerDepthType {
                    StartsAtStartBoundaryAtCheckpoint,
                    StartsAtCheckpointBoundaryAtCheckpoint,
                    StartsAtCheckpointBoundaryAtBoundary,
                }

                let start_hash = hash_with_leading_zeros(10);
                let deeper_boundary_hash = hash_with_leading_zeros(10);

                let shallower_start_hash: Digest;
                let shallower_boundary_hash: Digest;
                let mut checkpoints = Vec::<Digest>::arbitrary(u)?;
                let lower_level_type = ShallowerDepthType::arbitrary(u)?;
                match lower_level_type {
                    ShallowerDepthType::StartsAtStartBoundaryAtCheckpoint => {
                        shallower_start_hash = start_hash;
                        shallower_boundary_hash = hash_with_leading_zeros(9);
                        checkpoints.push(shallower_boundary_hash);
                    }
                    ShallowerDepthType::StartsAtCheckpointBoundaryAtCheckpoint => {
                        shallower_start_hash = hash_with_leading_zeros(9);
                        shallower_boundary_hash = hash_with_leading_zeros(9);
                        checkpoints.push(shallower_start_hash);
                        checkpoints.push(shallower_boundary_hash);
                    }
                    ShallowerDepthType::StartsAtCheckpointBoundaryAtBoundary => {
                        shallower_start_hash = hash_with_leading_zeros(9);
                        checkpoints.push(shallower_start_hash);
                        shallower_boundary_hash = deeper_boundary_hash;
                    }
                }

                let deeper = Fragment::new(
                    start_hash,
                    vec![deeper_boundary_hash],
                    checkpoints,
                    BlobMeta::arbitrary(u)?,
                );
                let shallower = FragmentSummary::new(
                    shallower_start_hash,
                    vec![shallower_boundary_hash],
                    BlobMeta::arbitrary(u)?,
                );

                Ok(Self { deeper, shallower })
            }
        }
        bolero::check!()
            .with_arbitrary::<Scenario>()
            .for_each(|Scenario { deeper, shallower }| {
                assert!(deeper.supports(shallower, &CountLeadingZeroBytes));
            });
    }

    #[test]
    fn minimized_loose_commit_dag_doesnt_change() {
        #[derive(Debug)]
        struct Scenario {
            commits: Vec<super::LooseCommit>,
        }
        impl<'a> arbitrary::Arbitrary<'a> for Scenario {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                let mut frontier: Vec<Digest> = Vec::new();
                let num_commits: u32 = u.int_in_range(1..=20)?;
                let mut result = Vec::with_capacity(num_commits as usize);
                for _ in 0..num_commits {
                    let contents = Vec::<u8>::arbitrary(u)?;
                    let blob_meta = BlobMeta::new(&contents);
                    let hash = crate::Digest::arbitrary(u)?;
                    let mut parents = Vec::new();
                    let mut num_parents = u.int_in_range(0..=frontier.len())?;
                    let mut parent_choices = frontier.iter().collect::<Vec<_>>();
                    while num_parents > 0 {
                        let parent = u.choose(&parent_choices)?;
                        parents.push(**parent);
                        #[allow(clippy::unwrap_used)]
                        parent_choices
                            .remove(parent_choices.iter().position(|p| p == parent).unwrap());
                        num_parents -= 1;
                    }
                    frontier.retain(|p| !parents.contains(p));
                    frontier.push(hash);
                    result.push(super::LooseCommit {
                        digest: hash,
                        parents,
                        blob_meta,
                    });
                }
                Ok(Scenario { commits: result })
            }
        }
        bolero::check!()
            .with_arbitrary::<Scenario>()
            .for_each(|Scenario { commits }| {
                let tree = super::Sedimentree::new(vec![], commits.clone());
                let minimized = tree.minimize(&CountLeadingZeroBytes);
                assert_eq!(tree, minimized);
            });
    }
}
