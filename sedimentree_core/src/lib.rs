//! The core of the [Sedimentree] data partitioning scheme.
//!
//! This core library only defines the metadata tracking featrues of Sedimentree.
//! We assume that the actual data described by this metadata is not legible to the Sedimentree
//! (regardless of whether or not it's encrypted).
//!
//! Sedimentree is a way of organizing data into a series of layers, or strata, each of which
//! contains a set of checkpoints (hashes) that represent some chunk of a larger file or log.
//! For example, an Automerge document might be partitioned, and each chunk encrypted.
//! Sedimentree tracks just enough metadata to allow efficient diffing and synchronization
//! of these chunks.
//!
//! [Sedimentree]: https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use nonempty::{nonempty, NonEmpty};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Formatter,
    str::FromStr,
};

mod blob;
mod commit_dag;
pub mod future;
pub mod storage;

pub use blob::*;

/// The maximum depth of strata that a [`Sedimentree`] can go to.
pub const MAX_STRATA_DEPTH: Depth = Depth(2);

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
}

/// An error indicating that a [`SedimentreeId`] could not be parsed from a string.
#[derive(Debug, Clone, Copy)]
pub struct BadSedimentreeId;

impl FromStr for SedimentreeId {
    type Err = BadSedimentreeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() % 2 != 0 {
            return Err(BadSedimentreeId);
        }

        let bytes = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| BadSedimentreeId))
            .collect::<Result<Vec<u8>, BadSedimentreeId>>()?;

        if bytes.len() == 32 {
            let mut arr = [0; 32];
            arr.copy_from_slice(&bytes);
            Ok(SedimentreeId(arr))
        } else {
            Err(BadSedimentreeId)
        }
    }
}

impl std::fmt::Debug for SedimentreeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for SedimentreeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A less detailed representation of a Sedimentree that omits strata checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SedimentreeSummary {
    chunk_summaries: BTreeSet<ChunkSummary>,
    commits: BTreeSet<LooseCommit>,
}

impl SedimentreeSummary {
    /// Constructor for a [`SedimentreeSummary`].
    #[must_use]
    pub const fn new(
        chunk_summaries: BTreeSet<ChunkSummary>,
        commits: BTreeSet<LooseCommit>,
    ) -> SedimentreeSummary {
        SedimentreeSummary {
            chunk_summaries,
            commits,
        }
    }

    /// The set of chunk summaries in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn chunk_summaries(&self) -> &BTreeSet<ChunkSummary> {
        &self.chunk_summaries
    }

    /// The set of loose commits in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn loose_commits(&self) -> &BTreeSet<LooseCommit> {
        &self.commits
    }

    /// Create a [`RemoteDiff`] with empty local chunks and commits.
    #[must_use]
    pub fn as_remote_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_chunk_summaries: self.chunk_summaries.iter().collect(),
            remote_commits: self.commits.iter().collect(),
            local_chunks: Vec::new(),
            local_commits: Vec::new(),
        }
    }
}

/// How deep in the Sedimentree a stratum is.
///
/// The greater the depth, the more leading zeros, the (probabilistically) larger,
/// and thus "lower" the stratum. They become larger due to the chunking strategy.
/// This means that the same data can appear in multiple strata, but may be chunked
/// into smaller or larger sections based on a hash hardness metric.
///
/// The depth is determined by the number of leading zeros in each hash in base 10.
/// If there's zero-or-more leading zeros, it may only live in the topmost (0th) layer.
/// If there is one leading zero (or more), it can only live in the 0th or 1st layer.
/// If there are two leading zeros (or more), it can only live in the 0th, 1st, or 2nd layer
/// (and so on).
///
/// ```diagram
///         ┌───┐ ┌───┐ ┌───┐ ┌─────────┐ ┌───┐ ┌───┐
/// Depth 0 │ 1 │ │ 1 │ │ 1 │ │    2    │ │ 1 │ │ 1 │
///         └───┘ └───┘ └───┘ └─────────┘ └───┘ └───┘
///         ┌───────────────┐ ┌─────────────────────┐
/// Depth 1 │   3 commits   │ │      4 commits      │
///         └───────────────┘ └─────────────────────┘
///         ┌───────────────────────────────────────┐
/// Depth 2 │               7 commits               │
///         └───────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Depth(pub u32);

impl<'a> From<&'a Digest> for Depth {
    fn from(hash: &'a Digest) -> Self {
        Depth(trailing_zeros_in_base(hash.as_bytes(), 10))
    }
}

impl From<Digest> for Depth {
    fn from(hash: Digest) -> Self {
        Self::from(&hash)
    }
}

impl std::fmt::Display for Depth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Depth({})", self.0)
    }
}

/// A portion of a Sedimentree that includes a set of checkpoints.
///
/// This is created by breaking up (chunking) a larger document or log
/// into smaller pieces (a "chunk"). Since Sedimentree is not able to
/// read the content in a particular chunk (e.g. because it's in
/// an arbitrary format or is encrypted), it maintains some basic
/// metadata about the the content to aid in deduplication and synchronization.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Chunk {
    summary: ChunkSummary,
    checkpoints: Vec<Digest>,
    digest: Digest,
}

impl Chunk {
    /// Constructor for a [`Chunk`].
    #[must_use]
    pub fn new(
        head: Digest,
        boundary: NonEmpty<Digest>,
        checkpoints: Vec<Digest>,
        blob_meta: BlobMeta,
    ) -> Self {
        let digest = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(head.as_bytes());

            for end in boundary.iter() {
                hasher.update(end.as_bytes());
            }
            hasher.update(blob_meta.digest().as_bytes());

            for checkpoint in &checkpoints {
                hasher.update(checkpoint.as_bytes());
            }

            Digest::from(*hasher.finalize().as_bytes())
        };

        Self {
            summary: ChunkSummary {
                head,
                boundary,
                blob_meta,
            },
            checkpoints,
            digest,
        }
    }

    /// Returns true if this chunk supports the given chunk summary.
    #[must_use]
    pub fn supports(&self, other: &ChunkSummary) -> bool {
        if &self.summary == other {
            return true;
        }

        if self.depth() < other.depth() {
            return false;
        }

        if self.summary.head == other.head
            && self
                .checkpoints
                .iter()
                .collect::<HashSet<_>>()
                .is_superset(&other.boundary.iter().collect::<HashSet<_>>())
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
                .collect::<HashSet<_>>()
                .is_superset(&other.boundary.iter().collect::<HashSet<_>>())
        {
            return true;
        }

        false
    }

    /// Returns true if this [`Chunk`] covers the given [`Digest`].
    #[must_use]
    pub fn supports_block(&self, chunk_end: Digest) -> bool {
        self.checkpoints.contains(&chunk_end) || self.summary.boundary.contains(&chunk_end)
    }

    /// Convert to a [`ChunkSummary`].
    #[must_use]
    pub const fn summary(&self) -> &ChunkSummary {
        &self.summary
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth(&self) -> Depth {
        self.summary.depth()
    }

    /// The head of the chunk.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.summary.head
    }

    /// The (possibly ragged) end(s) of the chunk.
    #[must_use]
    pub const fn boundary(&self) -> &NonEmpty<Digest> {
        &self.summary.boundary
    }

    /// The inner checkpoints of the chunk.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest> {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Chunk`], derived from its content.
    #[must_use]
    pub const fn digest(&self) -> Digest {
        self.digest
    }
}

/// The minimal data for a [`Chunk`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ChunkSummary {
    head: Digest,
    boundary: NonEmpty<Digest>,
    blob_meta: BlobMeta,
}

impl ChunkSummary {
    /// Constructor for a [`ChunkSummary`].
    #[must_use]
    pub const fn new(head: Digest, boundary: NonEmpty<Digest>, blob_meta: BlobMeta) -> Self {
        Self {
            head,
            boundary,
            blob_meta,
        }
    }

    /// The head of the chunk.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.head
    }

    /// The (possibly ragged) end(s) of the chunk.
    #[must_use]
    pub const fn boundary(&self) -> &NonEmpty<Digest> {
        &self.boundary
    }

    /// Basic information about the payload blob.
    #[must_use]
    pub const fn blob_meta(&self) -> BlobMeta {
        self.blob_meta
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    #[must_use]
    pub fn depth(&self) -> Depth {
        let level = trailing_zeros_in_base(self.head.as_bytes(), 10);
        Depth(level)
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
    blob: BlobMeta,
}

impl LooseCommit {
    /// Constructor for a [`LooseCommit`].
    #[must_use]
    pub const fn new(digest: Digest, parents: Vec<Digest>, blob: BlobMeta) -> Self {
        Self {
            digest,
            parents,
            blob,
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
    pub const fn blob(&self) -> &BlobMeta {
        &self.blob
    }
}

/// The difference between two [`Sedimentree`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff<'a> {
    /// Chunks present in the right tree but not the left.
    pub left_missing_chunks: Vec<&'a Chunk>,

    /// Commits present in the right tree but not the left.
    pub left_missing_commits: Vec<&'a LooseCommit>,

    /// Chunks present in the left tree but not the right.
    pub right_missing_chunks: Vec<&'a Chunk>,

    /// Commits present in the left tree but not the right.
    pub right_missing_commits: Vec<&'a LooseCommit>,
}

/// The difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDiff<'a> {
    /// Chunks present in the remote tree but not the local.
    pub remote_chunk_summaries: Vec<&'a ChunkSummary>,

    /// Commits present in the remote tree but not the local.
    pub remote_commits: Vec<&'a LooseCommit>,

    /// Chunks present in the local tree but not the remote.
    pub local_chunks: Vec<&'a Chunk>,

    /// Commits present in the local tree but not the remote.
    pub local_commits: Vec<&'a LooseCommit>,
}

/// All of the Sedimentree metadata about all the chunks for a series of payload.
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Sedimentree {
    chunks: BTreeSet<Chunk>,
    commits: BTreeSet<LooseCommit>,
}

impl Sedimentree {
    /// Constructor for a [`Sedimentree`].
    #[must_use]
    pub fn new(chunks: Vec<Chunk>, commits: Vec<LooseCommit>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            commits: commits.into_iter().collect(),
        }
    }

    /// The minimal ordered hash of this [`Sedimentree`].
    #[must_use]
    pub fn minimal_hash(&self) -> MinimalTreeHash {
        let minimal = self.minimize();
        let mut hashes = minimal
            .chunks()
            .flat_map(|s| {
                std::iter::once(s.head())
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

    /// Add a chunk to the [`Sedimentree`].
    ///
    /// Returns `true` if the stratum was not already present
    pub fn add_chunk(&mut self, chunk: Chunk) -> bool {
        self.chunks.insert(chunk)
    }

    /// Add a loose commit to the [`Sedimentree`].
    ///
    /// Returns `true` if the commit was not already present
    pub fn add_commit(&mut self, commit: LooseCommit) -> bool {
        self.commits.insert(commit)
    }

    /// Compute the difference between two local [`Sedimentree`]s.
    #[must_use]
    pub fn diff<'a>(&'a self, other: &'a Sedimentree) -> Diff<'a> {
        let our_chunks = self.chunks.iter().collect::<HashSet<_>>();
        let their_chunks = other.chunks.iter().collect();
        let left_missing_chunks = our_chunks.difference(&their_chunks);
        let right_missing_chunks = their_chunks.difference(&our_chunks);

        let our_commits = self.commits.iter().collect::<HashSet<_>>();
        let their_commits = other.commits.iter().collect();
        let left_missing_commits = our_commits.difference(&their_commits);
        let right_missing_commits = their_commits.difference(&our_commits);

        Diff {
            left_missing_chunks: left_missing_chunks.into_iter().copied().collect(),
            left_missing_commits: left_missing_commits.into_iter().copied().collect(),
            right_missing_chunks: right_missing_chunks.into_iter().copied().collect(),
            right_missing_commits: right_missing_commits.into_iter().copied().collect(),
        }
    }

    /// Compute the difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
    #[must_use]
    pub fn diff_remote<'a>(&'a self, remote: &'a SedimentreeSummary) -> RemoteDiff<'a> {
        let our_chunks_meta = self
            .chunks
            .iter()
            .map(|s| &s.summary)
            .collect::<HashSet<&ChunkSummary>>();
        let their_chunks = remote.chunk_summaries.iter().collect::<HashSet<_>>();
        let mut local_chunks = Vec::new();
        for m in our_chunks_meta.difference(&their_chunks) {
            for s in &self.chunks {
                if s.head() == m.head && *s.boundary() == m.boundary && s.depth() == m.depth() {
                    local_chunks.push(s);
                    break;
                }
            }
        }
        let remote_chunks = their_chunks.difference(&our_chunks_meta);

        let our_commits = self.commits.iter().collect::<HashSet<&LooseCommit>>();
        let their_commits = remote.commits.iter().collect();
        let local_commits = our_commits.difference(&their_commits);
        let remote_commits = their_commits.difference(&our_commits);

        RemoteDiff {
            remote_chunk_summaries: remote_chunks.into_iter().copied().collect(),
            remote_commits: remote_commits.into_iter().copied().collect(),
            local_chunks,
            local_commits: local_commits.into_iter().copied().collect(),
        }
    }

    /// Iterate over all chunks in this [`Sedimentree`].
    pub fn chunks(&self) -> impl Iterator<Item = &Chunk> {
        self.chunks.iter()
    }

    /// Iterate over all loose commits in this [`Sedimentree`].
    pub fn loose_commits(&self) -> impl Iterator<Item = &LooseCommit> {
        self.commits.iter()
    }

    /// Returns true if this [`Sedimentree`] has a chunk with the given digest.
    #[must_use]
    pub fn has_loose_commit(&self, digest: Digest) -> bool {
        self.loose_commits().any(|c| c.digest() == digest)
    }

    /// Returns true if this [`Sedimentree`] has a chunk starting with the given digest.
    #[must_use]
    pub fn has_chunk_starting_with(&self, digest: Digest) -> bool {
        self.heads().contains(&digest)
    }

    /// Prune a [`Sedimentree`].
    ///
    /// Minimize the [`Sedimentree`] by removing any chunks that are
    /// fully supported by other chunks, and removing any loose commits
    /// that are not needed to support the remaining chunks.
    #[must_use]
    pub fn minimize(&self) -> Sedimentree {
        // First sort chunks by depth, then for each stratum below the lowest
        // level, discard that stratum if it is supported by any of the stratum
        // above it.
        let mut chunks = self.chunks.iter().collect::<Vec<_>>();
        chunks.sort_by_key(|a| a.depth());

        let mut minimized_chunks = Vec::<Chunk>::new();

        for chunk in chunks {
            if !minimized_chunks
                .iter()
                .any(|existing| existing.supports(&chunk.summary))
            {
                minimized_chunks.push(chunk.clone());
            }
        }

        // Now, form a commit graph from the loose commits and simplify it relative to the minimized chunks
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let simplified_dag = dag.simplify(&minimized_chunks);

        let commits = self
            .commits
            .iter()
            .filter(|&c| simplified_dag.contains_commit(&c.digest()))
            .cloned()
            .collect();

        Sedimentree::new(minimized_chunks, commits)
    }

    /// Create a [`SedimentreeSummary`] from this [`Sedimentree`].
    ///
    /// This omits the checkpoints from each chunk.
    /// It is useful for sending over the wire.
    #[must_use]
    pub fn summarize(&self) -> SedimentreeSummary {
        SedimentreeSummary {
            chunk_summaries: self
                .chunks
                .iter()
                .map(|chunk| chunk.summary.clone())
                .collect(),
            commits: self.commits.clone(),
        }
    }

    /// The heads of a Sedimentree are the end hashes of all strata which are
    /// not the start of any other strata or supported by any lower stratum
    /// and which do not appear in the [`LooseCommit`] graph, plus the heads of
    /// the loose commit graph.
    #[must_use]
    pub fn heads(&self) -> Vec<Digest> {
        let minimized = self.minimize();
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.iter());
        let mut heads = Vec::<Digest>::new();
        for chunk in &minimized.chunks {
            if !minimized
                .chunks
                .iter()
                .any(|s| s.boundary().contains(&chunk.head()))
                && chunk.boundary().iter().all(|end| !dag.contains_commit(end))
            {
                heads.extend(chunk.boundary());
            }
        }
        heads.extend(dag.heads());
        heads
    }

    /// Consume this [`Sedimentree`] and return an iterator over all its items.
    pub fn into_items(self) -> impl Iterator<Item = CommitOrChunk> {
        self.chunks
            .into_iter()
            .map(CommitOrChunk::Chunk)
            .chain(self.commits.into_iter().map(CommitOrChunk::Commit))
    }

    /// Given a [`SedimentreeId`], return the [`Chunk`]s that are missing to fill in the gaps.
    #[must_use]
    pub fn missing_chunks(&self, id: SedimentreeId) -> Vec<ChunkSpec> {
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let mut runs_by_level = BTreeMap::<Depth, (Digest, Vec<Digest>)>::new();
        let mut all_bundles = Vec::new();
        for commit_hash in dag.canonical_sequence(self.chunks.iter()) {
            let level = Depth::from(commit_hash);
            for (run_level, (_start, checkpoints)) in &mut runs_by_level {
                if run_level < &level {
                    checkpoints.push(commit_hash);
                }
            }
            if level >= crate::MAX_STRATA_DEPTH {
                if let Some((head, checkpoints)) = runs_by_level.remove(&level) {
                    if self.chunks.iter().any(|s| s.supports_block(commit_hash)) {
                        runs_by_level.insert(level, (commit_hash, Vec::new()));
                    } else {
                        all_bundles.push(ChunkSpec {
                            id,
                            head,
                            boundary: nonempty![commit_hash], // FIXME could be more than one, right?
                            checkpoints: checkpoints.clone(),
                        });
                    }
                }
            }
        }
        all_bundles
    }

    /// Create a [`RemoteDiff`] with empty remote chunks and commits.
    #[must_use]
    pub fn as_local_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_chunk_summaries: Vec::new(),
            remote_commits: Vec::new(),
            local_chunks: self.chunks.iter().collect(),
            local_commits: self.commits.iter().collect(),
        }
    }
}

/// The barest information needed to identify a chunk.
#[derive(Debug, Clone)]
pub struct ChunkSpec {
    id: SedimentreeId,
    head: Digest,
    checkpoints: Vec<Digest>,
    boundary: NonEmpty<Digest>,
}

impl ChunkSpec {
    /// Constructor for a [`ChunkSpec`].
    #[must_use]
    pub const fn id(&self) -> SedimentreeId {
        self.id
    }

    /// The head of the chunk.
    #[must_use]
    pub const fn head(&self) -> Digest {
        self.head
    }

    /// The (possibly ragged) end(s) of the chunk.
    #[must_use]
    pub const fn boundary(&self) -> &NonEmpty<Digest> {
        &self.boundary
    }

    /// The inner checkpopoints of the chunk.
    #[must_use]
    pub const fn checkpoints(&self) -> &Vec<Digest> {
        &self.checkpoints
    }
}

/// An enum over either a [`LooseCommit`] or a [`Chunk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOrChunk {
    /// A loose commit.
    Commit(LooseCommit),

    /// A chunk.
    Chunk(Chunk),
}

fn trailing_zeros_in_base(arr: &[u8; 32], base: u8) -> u32 {
    assert!(base > 1, "Base must be greater than 1");
    let (_, bytes) =
        num::BigInt::from_bytes_be(num::bigint::Sign::Plus, arr).to_radix_be(base.into());

    #[allow(clippy::expect_used)]
    u32::try_from(bytes.into_iter().rev().take_while(|&i| i == 0).count())
        .expect("u32 is big enough")
}

impl std::fmt::Debug for Sedimentree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let chunk_summaries = self
            .chunks
            .iter()
            .map(|s| {
                format!(
                    "{{depth: {}, size_bytes: {}}}",
                    s.depth(),
                    s.summary().blob_meta().size_bytes()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        f.debug_struct("Sedimentree")
            .field("chunks", &chunk_summaries)
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
pub fn has_commit_boundary<I: IntoIterator<Item = D>, D: Into<Digest>>(commits: I) -> bool {
    commits
        .into_iter()
        .any(|digest| Depth::from(digest.into()) <= MAX_STRATA_DEPTH)
}

#[cfg(test)]
mod tests {
    use nonempty::nonempty;
    use num::Num;

    use super::*;

    fn hash_with_trailing_zeros(
        unstructured: &mut arbitrary::Unstructured<'_>,
        base: u32,
        trailing_zeros: u32,
    ) -> Result<Digest, arbitrary::Error> {
        assert!(base > 1, "Base must be greater than 1");
        assert!(base <= 10, "Base must be less than 10");

        let zero_str = "0".repeat(trailing_zeros as usize);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let num_digits = (256.0 / f64::from(base).log2()).floor() as u64;

        let mut num_str = zero_str;
        num_str.push('1');
        #[allow(clippy::cast_possible_truncation)]
        while num_str.len() < num_digits as usize {
            if unstructured.is_empty() {
                return Err(arbitrary::Error::NotEnoughData);
            }
            #[allow(clippy::range_minus_one)]
            let digit = unstructured.int_in_range(0..=base - 1)?;
            num_str.push_str(&digit.to_string());
        }
        // reverse the string to get the correct representation
        num_str = num_str.chars().rev().collect();
        #[allow(clippy::unwrap_used)]
        let num = num::BigInt::from_str_radix(&num_str, base).unwrap();

        let (_, mut bytes) = num.to_bytes_be();
        if bytes.len() < 32 {
            let mut padded_bytes = vec![0; 32 - bytes.len()];
            padded_bytes.extend(bytes);
            bytes = padded_bytes;
        }
        #[allow(clippy::unwrap_used)]
        let byte_arr: [u8; 32] = bytes.try_into().unwrap();
        Ok(Digest::from(byte_arr))
    }

    #[test]
    fn chunk_supports_higher_levels() {
        #[derive(Debug)]
        struct Scenario {
            deeper: Chunk,
            shallower: ChunkSummary,
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

                let start_hash = hash_with_trailing_zeros(u, 10, 10)?;
                let deeper_boundary_hash = hash_with_trailing_zeros(u, 10, 10)?;

                let shallower_start_hash: Digest;
                let shallower_boundary_hash: Digest;
                let mut checkpoints = Vec::<Digest>::arbitrary(u)?;
                let lower_level_type = ShallowerDepthType::arbitrary(u)?;
                match lower_level_type {
                    ShallowerDepthType::StartsAtStartBoundaryAtCheckpoint => {
                        shallower_start_hash = start_hash;
                        shallower_boundary_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(shallower_boundary_hash);
                    }
                    ShallowerDepthType::StartsAtCheckpointBoundaryAtCheckpoint => {
                        shallower_start_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        shallower_boundary_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(shallower_start_hash);
                        checkpoints.push(shallower_boundary_hash);
                    }
                    ShallowerDepthType::StartsAtCheckpointBoundaryAtBoundary => {
                        shallower_start_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(shallower_start_hash);
                        shallower_boundary_hash = deeper_boundary_hash;
                    }
                }

                let deeper = Chunk::new(
                    start_hash,
                    nonempty![deeper_boundary_hash],
                    checkpoints,
                    BlobMeta::arbitrary(u)?,
                );
                let shallower = ChunkSummary::new(
                    shallower_start_hash,
                    nonempty![shallower_boundary_hash],
                    BlobMeta::arbitrary(u)?,
                );

                Ok(Self { deeper, shallower })
            }
        }
        bolero::check!()
            .with_arbitrary::<Scenario>()
            .for_each(|Scenario { deeper, shallower }| {
                assert!(deeper.supports(shallower));
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
                    let blob = BlobMeta::new(&contents);
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
                        blob,
                    });
                }
                Ok(Scenario { commits: result })
            }
        }
        bolero::check!()
            .with_arbitrary::<Scenario>()
            .for_each(|Scenario { commits }| {
                let tree = super::Sedimentree::new(vec![], commits.clone());
                let minimized = tree.minimize();
                assert_eq!(tree, minimized);
            });
    }
}
