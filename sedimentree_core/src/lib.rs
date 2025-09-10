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
pub mod storage;

pub use blob::*;

/// The maximum depth of strata that a [`Sedimentree`] can go to.
pub const MAX_STRATA_DEPTH: Depth = Depth(2);

/// A unique identifier for some data managed by Sedimentree.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SedimentreeId([u8; 32]);

/// An error indicating that a SedimentreeId could not be parsed from a string.
#[derive(Debug, Clone, Copy)]
pub struct BadSedimentreeId;

impl FromStr for SedimentreeId {
    type Err = BadSedimentreeId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = bs58::decode(s)
            .with_check(None)
            .into_vec()
            .map_err(|_| BadSedimentreeId)?;

        if bytes.len() != 32 {
            Err(BadSedimentreeId)
        } else {
            let mut arr = [0; 32];
            arr.copy_from_slice(&bytes);
            Ok(SedimentreeId(arr))
        }
    }
}

impl std::fmt::Debug for SedimentreeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let as_string = bs58::encode(&self.0).with_check().into_string();
        write!(f, "{}", as_string)
    }
}

impl std::fmt::Display for SedimentreeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let as_string = bs58::encode(&self.0).with_check().into_string();
        write!(f, "{}", as_string)
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
    pub fn new(
        chunk_summaries: BTreeSet<ChunkSummary>,
        commits: BTreeSet<LooseCommit>,
    ) -> SedimentreeSummary {
        SedimentreeSummary {
            chunk_summaries,
            commits,
        }
    }

    /// The set of chunk summaries in this [`SedimentreeSummary`].
    pub fn chunk_summaries(&self) -> &BTreeSet<ChunkSummary> {
        &self.chunk_summaries
    }

    /// The set of loose commits in this [`SedimentreeSummary`].
    pub fn commits(&self) -> &BTreeSet<LooseCommit> {
        &self.commits
    }

    /// Create a [`RemoteDiff`] with empty local chunks and commits.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Depth(pub u32); // TODO why not u8?

// impl Default for Depth {
//     fn default() -> Self {
//         Self(2)
//     }
// }

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

impl PartialOrd for Depth {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Flip the ordering so that stratum with a larger number of leading zeros are
// "lower". This is mainly so that the sedimentary rock metaphor holds
impl Ord for Depth {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.0.cmp(&other.0) {
            std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => std::cmp::Ordering::Equal,
        }
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
    pub fn new(
        start: Digest,
        ends: NonEmpty<Digest>,
        checkpoints: Vec<Digest>,
        blob_meta: BlobMeta,
    ) -> Self {
        let digest = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(start.as_bytes());

            for end in ends.iter() {
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
                start,
                ends,
                blob_meta,
            },
            checkpoints,
            digest,
        }
    }

    /// Constructor for a [`Chunk`] from its raw components, including its digest.
    pub fn from_raw(summary: ChunkSummary, checkpoints: Vec<Digest>, digest: Digest) -> Self {
        Chunk {
            summary,
            checkpoints,
            digest,
        }
    }

    /// Returns true if this chunk supports the given chunk summary.
    pub fn supports(&self, other: &ChunkSummary) -> bool {
        if &self.summary == other {
            return true;
        }

        if self.depth() >= other.depth() {
            return false;
        }

        if self.summary.start == other.start
            && HashSet::<&Digest>::from_iter(self.checkpoints.iter())
                .is_superset(&HashSet::from_iter(other.ends.iter()))
        {
            return true;
        }

        if self.checkpoints.contains(&other.start)
            && other.ends.iter().all(|end| self.checkpoints.contains(end))
        {
            return true;
        }

        if self.checkpoints.contains(&other.start)
            && HashSet::<&Digest>::from_iter(self.summary.ends.iter())
                .is_superset(&HashSet::from_iter(other.ends.iter()))
        {
            return true;
        }

        false
    }

    /// Returns true if this [`Chunk`] covers the given [`Digest`].
    pub fn supports_block(&self, chunk_end: Digest) -> bool {
        self.checkpoints.contains(&chunk_end) || self.summary.ends.contains(&chunk_end)
    }

    /// Convert to a [`ChunkSummary`].
    pub fn summary(&self) -> &ChunkSummary {
        &self.summary
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    pub fn depth(&self) -> Depth {
        self.summary.depth()
    }

    /// The head of the chunk.
    pub fn start(&self) -> Digest {
        self.summary.start
    }

    /// The (possibly ragged) end(s) of the chunk.
    pub fn ends(&self) -> &NonEmpty<Digest> {
        &self.summary.ends
    }

    /// The inner checkpopoints of the chunk.
    pub fn checkpoints(&self) -> &[Digest] {
        &self.checkpoints
    }

    /// The unique [`Digest`] of this [`Chunk`], derived from its content.
    pub fn digest(&self) -> Digest {
        self.digest
    }
}

/// The minimal data for a [`Chunk`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ChunkSummary {
    start: Digest,
    ends: NonEmpty<Digest>,
    blob_meta: BlobMeta,
}

impl ChunkSummary {
    /// Constructor for a [`ChunkSummary`].
    pub fn new(start: Digest, ends: NonEmpty<Digest>, blob_meta: BlobMeta) -> Self {
        Self {
            start,
            ends,
            blob_meta,
        }
    }

    /// The head of the chunk.
    pub fn start(&self) -> Digest {
        self.start
    }

    /// The (possibly ragged) end(s) of the chunk.
    pub fn ends(&self) -> &NonEmpty<Digest> {
        &self.ends
    }

    /// Basic information about the payload blob.
    pub fn blob_meta(&self) -> BlobMeta {
        self.blob_meta
    }

    /// The depth of this stratum, determined by the number of leading zeros.
    pub fn depth(&self) -> Depth {
        // TODO at least in theory this should ALWAYS be the head, right? -BZ
        let start_level = trailing_zeros_in_base(self.start.as_bytes(), 10);
        let smallest_level = self.ends.iter().fold(start_level, |acc, end| {
            std::cmp::min(acc, trailing_zeros_in_base(end.as_bytes(), 10))
        });
        Depth(smallest_level)
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
    pub fn new(digest: Digest, parents: Vec<Digest>, blob: BlobMeta) -> Self {
        Self {
            digest,
            parents,
            blob,
        }
    }

    /// The unique [`Digest`] of this [`LooseCommit`], derived from its content.
    pub fn digest(&self) -> Digest {
        self.digest
    }

    /// The (possibly empty) list of parent commits.
    pub fn parents(&self) -> &[Digest] {
        &self.parents
    }

    /// Metadata about the payload blob.
    pub fn blob(&self) -> &BlobMeta {
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
    pub fn new(chunks: Vec<Chunk>, commits: Vec<LooseCommit>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            commits: commits.into_iter().collect(),
        }
    }

    /// The minimal ordered hash of this [`Sedimentree`].
    pub fn minimal_hash(&self) -> MinimalTreeHash {
        let minimal = self.minimize();
        let mut hashes = minimal
            .chunks()
            .flat_map(|s| {
                std::iter::once(s.start())
                    .chain(s.ends().iter().copied())
                    .chain(s.checkpoints().iter().copied())
            })
            .chain(minimal.commits.iter().map(|c| c.digest()))
            .collect::<Vec<_>>();
        hashes.sort();

        let mut hasher = blake3::Hasher::new();
        for hash in hashes {
            hasher.update(hash.as_bytes());
        }
        MinimalTreeHash(*hasher.finalize().as_bytes())
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
    pub fn diff<'a>(&'a self, other: &'a Sedimentree) -> Diff<'a> {
        let our_chunks = HashSet::<&Chunk>::from_iter(self.chunks.iter());
        let their_chunks = HashSet::from_iter(other.chunks.iter());
        let left_missing_chunks = our_chunks.difference(&their_chunks);
        let right_missing_chunks = their_chunks.difference(&our_chunks);

        let our_commits = HashSet::<&LooseCommit>::from_iter(self.commits.iter());
        let their_commits = HashSet::from_iter(other.commits.iter());
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
    pub fn diff_remote<'a>(&'a self, remote: &'a SedimentreeSummary) -> RemoteDiff<'a> {
        let our_chunks_meta =
            HashSet::<&ChunkSummary>::from_iter(self.chunks.iter().map(|s| &s.summary));
        let their_chunks = HashSet::from_iter(remote.chunk_summaries.iter());
        let local_chunks = our_chunks_meta.difference(&their_chunks).map(|m| {
            self.chunks
                .iter()
                .find(|s| s.start() == m.start && *s.ends() == m.ends && s.depth() == m.depth())
                .unwrap()
        });
        let remote_chunks = their_chunks.difference(&our_chunks_meta);

        let our_commits = HashSet::<&LooseCommit>::from_iter(self.commits.iter());
        let their_commits = HashSet::from_iter(remote.commits.iter());
        let local_commits = our_commits.difference(&their_commits);
        let remote_commits = their_commits.difference(&our_commits);

        RemoteDiff {
            remote_chunk_summaries: remote_chunks.into_iter().copied().collect(),
            remote_commits: remote_commits.into_iter().copied().collect(),
            local_chunks: local_chunks.into_iter().collect(),
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
    pub fn has_loose_commit(&self, digest: Digest) -> bool {
        self.loose_commits().any(|c| c.digest() == digest)
    }

    /// Returns true if this [`Sedimentree`] has a chunk starting with the given digest.
    pub fn has_chunk_starting_with(&self, digest: Digest) -> bool {
        self.heads().contains(&digest)
    }

    /// Prune a [`Sedimentree`].
    ///
    /// Minimize the [`Sedimentree`] by removing any chunks that are
    /// fully supported by other chunks, and removing any loose commits
    /// that are not needed to support the remaining chunks.
    pub fn minimize(&self) -> Sedimentree {
        // First sort chunks by depth, then for each stratum below the lowest
        // level, discard that stratum if it is supported by any of the stratum
        // above it.
        let mut chunks = self.chunks.iter().collect::<Vec<_>>();
        chunks.sort_by(|a, b| a.depth().cmp(&b.depth()).reverse());

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
    pub fn heads(&self) -> Vec<Digest> {
        let minimized = self.minimize();
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.iter());
        let mut heads = Vec::<Digest>::new();
        for chunk in minimized.chunks.iter() {
            if !minimized
                .chunks
                .iter()
                .any(|s| s.ends().contains(&chunk.start()))
                && chunk.ends().iter().all(|end| !dag.contains_commit(end))
            {
                heads.extend(chunk.ends());
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

    /// Given a SedimentreeId, return the [`Chunk`]s that are missing to fill in the gaps.
    pub fn missing_chunks(&self, id: SedimentreeId) -> Vec<ChunkSpec> {
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let mut runs_by_level = BTreeMap::<Depth, (Digest, Vec<Digest>)>::new();
        let mut all_bundles = Vec::new();
        for commit_hash in dag.canonical_sequence(self.chunks.iter()) {
            let level = Depth::from(commit_hash);
            for (run_level, (_start, checkpoints)) in runs_by_level.iter_mut() {
                if run_level < &level {
                    checkpoints.push(commit_hash);
                }
            }
            if level <= crate::MAX_STRATA_DEPTH {
                if let Some((start, checkpoints)) = runs_by_level.remove(&level) {
                    if !self.chunks.iter().any(|s| s.supports_block(commit_hash)) {
                        all_bundles.push(ChunkSpec {
                            id,
                            start,
                            ends: nonempty![commit_hash], // FIXME could be more than one, right?
                            checkpoints: checkpoints.clone(),
                        });
                    } else {
                        runs_by_level.insert(level, (commit_hash, Vec::new()));
                    }
                }
            }
        }
        all_bundles
    }

    /// Create a [`RemoteDiff`] with empty remote chunks and commits.
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
    start: Digest,
    checkpoints: Vec<Digest>,
    ends: NonEmpty<Digest>,
}

impl ChunkSpec {
    /// Constructor for a [`ChunkSpec`].
    pub fn id(&self) -> SedimentreeId {
        self.id
    }

    /// The head of the chunk.
    pub fn start(&self) -> Digest {
        self.start
    }

    /// The (possibly ragged) end(s) of the chunk.
    pub fn ends(&self) -> &NonEmpty<Digest> {
        &self.ends
    }

    /// The inner checkpopoints of the chunk.
    pub fn checkpoints(&self) -> &[Digest] {
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

fn trailing_zeros_in_base(arr: &[u8; 32], base: u32) -> u32 {
    assert!(base > 1, "Base must be greater than 1");
    let bytes = num::BigInt::from_bytes_be(num::bigint::Sign::Plus, arr)
        .to_radix_be(base)
        .1;
    bytes.into_iter().rev().take_while(|&i| i == 0).count() as u32
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
    pub fn as_bytes(&self) -> &[u8; 32] {
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
        let num_digits = (256.0 / (base as f64).log2()).floor() as u64;

        let mut num_str = zero_str;
        num_str.push('1');
        while num_str.len() < num_digits as usize {
            if unstructured.is_empty() {
                return Err(arbitrary::Error::NotEnoughData);
            }
            let digit = unstructured.int_in_range(0..=base - 1)?;
            num_str.push_str(&digit.to_string());
        }
        // reverse the string to get the correct representation
        num_str = num_str.chars().rev().collect();
        let num = num::BigInt::from_str_radix(&num_str, base).unwrap();

        let (_, mut bytes) = num.to_bytes_be();
        if bytes.len() < 32 {
            let mut padded_bytes = vec![0; 32 - bytes.len()];
            padded_bytes.extend(bytes);
            bytes = padded_bytes;
        }
        let byte_arr: [u8; 32] = bytes.try_into().unwrap();
        Ok(Digest::from(byte_arr))
    }

    #[test]
    fn chunk_supports_higher_levels() {
        #[derive(Debug)]
        struct Scenario {
            lower_level: Chunk,
            higher_level: ChunkSummary,
        }
        impl<'a> arbitrary::Arbitrary<'a> for Scenario {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                let start_hash = hash_with_trailing_zeros(u, 10, 10)?;
                let lower_end_hash = hash_with_trailing_zeros(u, 10, 10)?;

                #[allow(clippy::enum_variant_names)]
                #[derive(arbitrary::Arbitrary)]
                enum HigherDepthType {
                    StartsAtStartEndsAtCheckpoint,
                    StartsAtCheckpointEndsAtEnd,
                    StartsAtCheckpointEndsAtCheckpoint,
                }

                let higher_start_hash: Digest;
                let higher_end_hash: Digest;
                let mut checkpoints = Vec::<Digest>::arbitrary(u)?;
                let lower_level_type = HigherDepthType::arbitrary(u)?;
                match lower_level_type {
                    HigherDepthType::StartsAtStartEndsAtCheckpoint => {
                        higher_start_hash = start_hash;
                        higher_end_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(higher_end_hash);
                    }
                    HigherDepthType::StartsAtCheckpointEndsAtEnd => {
                        higher_start_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(higher_start_hash);
                        higher_end_hash = lower_end_hash;
                    }
                    HigherDepthType::StartsAtCheckpointEndsAtCheckpoint => {
                        higher_start_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        higher_end_hash = hash_with_trailing_zeros(u, 10, 9)?;
                        checkpoints.push(higher_start_hash);
                        checkpoints.push(higher_end_hash);
                    }
                };

                let lower_level = Chunk::new(
                    start_hash,
                    nonempty![lower_end_hash],
                    checkpoints,
                    BlobMeta::arbitrary(u)?,
                );
                let higher_level = ChunkSummary::new(
                    higher_start_hash,
                    nonempty![higher_end_hash],
                    BlobMeta::arbitrary(u)?,
                );

                Ok(Self {
                    lower_level,
                    higher_level,
                })
            }
        }
        bolero::check!().with_arbitrary::<Scenario>().for_each(
            |Scenario {
                 lower_level,
                 higher_level,
             }| {
                assert!(lower_level.supports(&higher_level));
            },
        )
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
            })
    }
}
