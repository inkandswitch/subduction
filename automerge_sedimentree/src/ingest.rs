//! Automerge document ingestion into Sedimentree.
//!
//! This module provides the complete pipeline for converting an Automerge
//! document into a [`Sedimentree`] — the core operation for migrating data
//! from automerge's native format into the sedimentree sync format.
//!
//! # Pipeline
//!
//! ```text
//! Automerge doc
//!   │
//!   ├─ get_changes_meta()    ← cheap: DAG structure only
//!   │     │
//!   │     └─ build_fragment_store()  ← O(n) BFS
//!   │           │
//!   │           ├─ fragment members   (depth > 0 commits)
//!   │           └─ uncovered digests  (depth-0 loose commits)
//!   │
//!   └─ get_changes()         ← expensive: reconstructs raw bytes
//!         │
//!         └─ group by membership → fragment blobs + loose blobs
//!
//!   → Sedimentree { fragments, loose_commits }
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use automerge::Automerge;
//! use automerge_sedimentree::ingest::ingest_automerge;
//! use sedimentree_core::id::SedimentreeId;
//!
//! let bytes = std::fs::read("document.am").unwrap();
//! let doc = Automerge::load(&bytes).unwrap();
//! let sed_id = SedimentreeId::new([0u8; 32]); // your document ID
//! let result = ingest_automerge(&doc, sed_id).unwrap();
//!
//! println!("fragments: {}", result.fragment_count);
//! println!("loose commits: {}", result.loose_count);
//! ```

use alloc::{collections::BTreeSet, vec::Vec};

use automerge::{Automerge, ChangeHash};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{
        CommitStore, CountLeadingZeroBytes, FragmentError, FragmentState, MissingCommitError,
    },
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::Sedimentree,
};

use crate::indexed::{IndexedSedimentreeAutomerge, OwnedParents};

/// Errors that can occur during automerge document ingestion.
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum IngestError {
    /// A commit required during fragment construction was missing from the store.
    #[error(transparent)]
    MissingCommit(#[from] MissingCommitError),

    /// A depth-0 commit was passed as a fragment head.
    #[error("depth-0 commit cannot be a fragment head: {0}")]
    DepthZeroHead(CommitId),
}

/// Extract the concrete error from a [`FragmentError`] produced by
/// [`IndexedSedimentreeAutomerge`], whose `LookupError` is
/// [`Infallible`](core::convert::Infallible).
#[allow(clippy::needless_pass_by_value)] // Consumes the error by destructuring
const fn extract_fragment_error(
    err: FragmentError<'static, IndexedSedimentreeAutomerge>,
) -> IngestError {
    match err {
        FragmentError::MissingCommit(m) => IngestError::MissingCommit(m),
        FragmentError::DepthZeroHead(d) => IngestError::DepthZeroHead(d),
        FragmentError::LookupError(infallible) => match infallible {},
    }
}

/// Result of ingesting an Automerge document.
///
/// Contains the [`Sedimentree`], the corresponding [`Blob`]s, and metadata.
/// Pass `sedimentree` and `blobs` to [`Subduction::add_sedimentree`] to
/// store and sync the data.
#[derive(Debug, Clone)]
pub struct IngestResult {
    /// The constructed sedimentree (fragments + loose commits with `BlobMeta`).
    pub sedimentree: Sedimentree,

    /// All blobs referenced by the sedimentree's fragments and loose commits.
    /// Matched to the sedimentree entries by blob digest.
    pub blobs: Vec<Blob>,

    /// Total number of changes in the source document.
    pub change_count: usize,

    /// Number of changes covered by fragments.
    pub covered_count: usize,

    /// Number of loose commits (depth-0, not in any fragment).
    pub loose_count: usize,

    /// Number of fragments produced.
    pub fragment_count: usize,
}

/// Ingest an Automerge document into a [`Sedimentree`].
///
/// This is the main entry point for converting an automerge document into
/// the sedimentree format. It performs a two-phase decomposition:
///
/// 1. **Metadata phase** (cheap): builds the commit index from
///    `get_changes_meta` and runs `build_fragment_store` to determine
///    fragment boundaries and membership.
///
/// 2. **Byte extraction phase** (expensive): calls `get_changes` once to
///    reconstruct raw change bytes, then groups them into fragment blobs
///    and loose commit blobs.
///
/// The `sedimentree_id` identifies the document in the sedimentree storage
/// layer.
///
/// # Errors
///
/// Returns [`IngestError::MissingCommit`] if a commit required during
/// fragment construction is absent from the document.
pub fn ingest_automerge(
    doc: &Automerge,
    sedimentree_id: SedimentreeId,
) -> Result<IngestResult, IngestError> {
    // Phase 1: metadata index + fragment decomposition.
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();

    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();
    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(extract_fragment_error)?;
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<CommitId> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();
    let covered_count = covered.len();

    // Phase 2: produce blobs.
    //
    // One `get_changes(&[])` call to get all changes with raw bytes in
    // topological (causal) order. Then for each fragment we filter to
    // member changes only and concatenate their `raw_bytes()`.
    //
    // For loose commits: just the individual change's raw_bytes() (already
    // small, no recompression needed).
    let changes = doc.get_changes(&[]);
    let index = ChangeIndex::new(&changes);

    let (fragments, fragment_blobs) = compress_fragments(&states, &index, sedimentree_id);

    let (loose_commits, loose_blobs) =
        collect_loose_commits(&changes, &covered, &states, sedimentree_id);

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
    let mut blobs = fragment_blobs;
    blobs.extend(loose_blobs);
    let sedimentree = Sedimentree::new(fragments, loose_commits);

    Ok(IngestResult {
        sedimentree,
        blobs,
        change_count,
        covered_count,
        loose_count,
        fragment_count,
    })
}

/// Parallel variant of [`ingest_automerge`].
///
/// Uses [rayon] to compress fragment blobs concurrently. Each fragment's
/// `load_incremental` + `save_after` runs on a separate thread. For a
/// document with N fragments, this gives up to N-way parallelism.
///
/// The `get_changes` call (the main bottleneck) is still sequential —
/// it's internal to automerge and single-threaded.
///
/// # Errors
///
/// Returns [`IngestError`] if fragment construction or blob compression fails.
#[cfg(feature = "rayon")]
#[cfg_attr(docsrs, doc(cfg(feature = "rayon")))]
pub fn ingest_automerge_par(
    doc: &Automerge,
    sedimentree_id: SedimentreeId,
) -> Result<IngestResult, IngestError> {
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();

    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();
    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(extract_fragment_error)?;
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<CommitId> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();
    let covered_count = covered.len();

    let changes = doc.get_changes(&[]);
    let index = ChangeIndex::new(&changes);

    let (fragments, fragment_blobs) = compress_fragments_par(&states, &index, sedimentree_id);

    let (loose_commits, loose_blobs) =
        collect_loose_commits(&changes, &covered, &states, sedimentree_id);

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
    let mut blobs = fragment_blobs;
    blobs.extend(loose_blobs);
    let sedimentree = Sedimentree::new(fragments, loose_commits);

    Ok(IngestResult {
        sedimentree,
        blobs,
        change_count,
        covered_count,
        loose_count,
        fragment_count,
    })
}

/// Pre-indexed change data for efficient per-fragment blob construction.
///
/// Built once from the full `get_changes(&[])` output, then shared across all fragment
/// compressions.
///
/// The `entries` vector stores `(ChangeHash, raw_bytes)` in **topological order** — this
/// ordering is preserved when assembling a fragment's concatenated blob so downstream
/// `load_incremental` sees changes in dependency order.
///
/// `position_by_hash` is a reverse index built from `entries` so per-fragment blob
/// construction runs in `O(|fragment members| · log |fragment members|)` instead of the
/// previous `O(|all changes|)` scan. For a document with N total changes and F fragments
/// each containing ≈ N/F members, this turns the overall compression from `O(F · N)` into
/// `O(N · log(N/F))`.
struct ChangeIndex<'a> {
    entries: Vec<(ChangeHash, &'a [u8])>,
    position_by_hash: Map<ChangeHash, usize>,
}

impl<'a> ChangeIndex<'a> {
    fn new(changes: &'a [automerge::Change]) -> Self {
        let entries: Vec<_> = changes.iter().map(|c| (c.hash(), c.raw_bytes())).collect();
        let position_by_hash: Map<ChangeHash, usize> = entries
            .iter()
            .enumerate()
            .map(|(idx, (hash, _))| (*hash, idx))
            .collect();
        Self {
            entries,
            position_by_hash,
        }
    }
}

/// Compress a single fragment: concatenate only the member changes'
/// raw bytes, preserving the topological order from `get_changes`.
///
/// Previous versions loaded boundary + members into a sub-doc and used
/// `save_after(boundary)` for compact output. This broke on concurrent
/// DAGs because `save_after` excludes changes reachable from _any_
/// boundary head — but in a concurrent DAG, member changes can be
/// ancestors of boundary commits on a different branch, causing them
/// to be silently dropped.
///
/// # Complexity
///
/// Previous implementation was `O(|all changes|)` — it scanned the full index once per
/// fragment to filter out non-members. For a document with N total changes and F fragments
/// that's `O(F · N)` overall.
///
/// Current implementation uses `ChangeIndex::position_by_hash` to look up each member's
/// position directly, then sorts those positions. That's `O(|members| · log |members|)`
/// per fragment, so `O(N · log(N/F))` overall — a win whenever F > 1.
fn compress_one_fragment(
    state: &FragmentState<OwnedParents>,
    index: &ChangeIndex<'_>,
    sedimentree_id: SedimentreeId,
) -> (sedimentree_core::fragment::Fragment, Blob) {
    let members = state.members();

    // Map each member's ChangeHash to its position in the topologically-ordered index.
    // Unknown members (not in the index) can only occur if the state refers to a commit
    // that isn't in the document — which would be a bug upstream. Skip them to match the
    // previous implementation's silent behaviour (the old `members.contains(&hash)` check
    // also simply yielded no bytes for unknowns).
    let mut positions: Vec<usize> = Vec::with_capacity(members.len());
    for member in members {
        let hash = ChangeHash(*member.as_bytes());
        if let Some(&pos) = index.position_by_hash.get(&hash) {
            positions.push(pos);
        }
    }
    positions.sort_unstable();

    // Pre-size the concatenated buffer so `extend_from_slice` doesn't repeatedly grow.
    let total_bytes: usize = positions
        .iter()
        .filter_map(|&p| index.entries.get(p).map(|(_, bytes)| bytes.len()))
        .sum();
    let mut raw = Vec::with_capacity(total_bytes);
    for pos in positions {
        if let Some((_, bytes)) = index.entries.get(pos) {
            raw.extend_from_slice(bytes);
        }
    }

    let blob = Blob::new(raw);
    let fragment = state
        .clone()
        .to_fragment(sedimentree_id, BlobMeta::new(&blob));
    (fragment, blob)
}

/// Sequential fragment compression.
fn compress_fragments(
    states: &[FragmentState<OwnedParents>],
    index: &ChangeIndex<'_>,
    sedimentree_id: SedimentreeId,
) -> (Vec<sedimentree_core::fragment::Fragment>, Vec<Blob>) {
    let mut fragments = Vec::with_capacity(states.len());
    let mut blobs = Vec::with_capacity(states.len());
    for state in states {
        let (fragment, blob) = compress_one_fragment(state, index, sedimentree_id);
        fragments.push(fragment);
        blobs.push(blob);
    }
    (fragments, blobs)
}

/// Parallel fragment compression using rayon.
#[cfg(feature = "rayon")]
fn compress_fragments_par(
    states: &[FragmentState<OwnedParents>],
    index: &ChangeIndex<'_>,
    sedimentree_id: SedimentreeId,
) -> (Vec<sedimentree_core::fragment::Fragment>, Vec<Blob>) {
    use rayon::prelude::*;

    let (fragments, blobs): (Vec<_>, Vec<_>) = states
        .par_iter()
        .map(|state| compress_one_fragment(state, index, sedimentree_id))
        .unzip();

    (fragments, blobs)
}

/// Collect loose commits (depth-0, not covered by any fragment).
///
/// Parent digests that point into a fragment's member set are remapped to the
/// fragment's head digest. This prevents `Sedimentree::minimize` from
/// considering the loose commit "covered" by the fragment and pruning it.
fn collect_loose_commits(
    changes: &[automerge::Change],
    covered: &Set<CommitId>,
    states: &[FragmentState<OwnedParents>],
    sedimentree_id: SedimentreeId,
) -> (Vec<LooseCommit>, Vec<Blob>) {
    // Build a mapping: covered member id → fragment head id.
    let mut member_to_head: Map<CommitId, CommitId> = Map::new();
    for state in states {
        let head = state.head_id();
        for member in state.members() {
            if *member != head {
                member_to_head.insert(*member, head);
            }
        }
    }

    let mut loose_commits = Vec::new();
    let mut blobs = Vec::new();
    for change in changes {
        let id = CommitId::new(change.hash().0);
        if covered.contains(&id) {
            continue;
        }
        // Remap parents: if a parent is a fragment member, point to
        // the fragment head instead so minimize doesn't prune us.
        let parents: BTreeSet<CommitId> = change
            .deps()
            .iter()
            .map(|d| {
                let dep = CommitId::new(d.0);
                member_to_head.get(&dep).copied().unwrap_or(dep)
            })
            .collect();
        let blob = Blob::new(change.raw_bytes().to_vec());
        let commit = LooseCommit::new(sedimentree_id, id, parents, BlobMeta::new(&blob));
        loose_commits.push(commit);
        blobs.push(blob);
    }
    (loose_commits, blobs)
}
