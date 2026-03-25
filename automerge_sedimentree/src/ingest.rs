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

use alloc::collections::BTreeSet;

use automerge::{Automerge, ChangeHash};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    crypto::digest::Digest,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
};

use crate::indexed::{IndexedSedimentreeAutomerge, OwnedParents};

/// Errors that can occur during automerge document ingestion.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Fragment construction failed.
    #[error("fragment construction failed: {0}")]
    Fragment(String),

    /// A fragment member's change was not found in the change set.
    #[error("change not found for hash: {0}")]
    MissingChange(ChangeHash),

    /// Failed to load or save a sub-document during blob compression.
    #[error("automerge error during blob compression: {0}")]
    Automerge(String),
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
/// Returns [`IngestError::Fragment`] if fragment construction fails (e.g.,
/// due to missing commits in a partial document).
///
/// Returns [`IngestError::MissingChange`] if a fragment member's change
/// hash is not found in the document's change set.
pub fn ingest_automerge(
    doc: &Automerge,
    sedimentree_id: SedimentreeId,
) -> Result<IngestResult, IngestError> {
    // Phase 1: metadata index + fragment decomposition.
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<Digest<LooseCommit>> = doc
        .get_heads()
        .iter()
        .map(|h| Digest::force_from_bytes(h.0))
        .collect();

    let mut known: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();
    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(|e| IngestError::Fragment(e.to_string()))?;
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<Digest<LooseCommit>> = known
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
    let heads: Vec<Digest<LooseCommit>> = doc
        .get_heads()
        .iter()
        .map(|h| Digest::force_from_bytes(h.0))
        .collect();

    let mut known: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();
    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .map_err(|e| IngestError::Fragment(e.to_string()))?;
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<Digest<LooseCommit>> = known
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
/// Built once from the full `get_changes(&[])` output, then shared
/// across all fragment compressions. Each entry stores the raw bytes
/// of a change keyed by its hash, in topological order.
struct ChangeIndex<'a> {
    /// `(ChangeHash, raw_bytes)` in topological order.
    entries: Vec<(ChangeHash, &'a [u8])>,
}

impl<'a> ChangeIndex<'a> {
    fn new(changes: &'a [automerge::Change]) -> Self {
        Self {
            entries: changes.iter().map(|c| (c.hash(), c.raw_bytes())).collect(),
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
fn compress_one_fragment(
    state: &FragmentState<OwnedParents>,
    index: &ChangeIndex<'_>,
    sedimentree_id: SedimentreeId,
) -> (sedimentree_core::fragment::Fragment, Blob) {
    let members: Set<ChangeHash> = state
        .members()
        .iter()
        .map(|d| ChangeHash(*d.as_bytes()))
        .collect();

    let mut raw = Vec::new();
    for &(hash, bytes) in &index.entries {
        if members.contains(&hash) {
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
    covered: &Set<Digest<LooseCommit>>,
    states: &[FragmentState<OwnedParents>],
    sedimentree_id: SedimentreeId,
) -> (Vec<LooseCommit>, Vec<Blob>) {
    // Build a mapping: covered member digest → fragment head digest.
    let mut member_to_head: Map<Digest<LooseCommit>, Digest<LooseCommit>> = Map::new();
    for state in states {
        let head = state.head_digest();
        for member in state.members() {
            if *member != head {
                member_to_head.insert(*member, head);
            }
        }
    }

    let mut loose_commits = Vec::new();
    let mut blobs = Vec::new();
    for change in changes {
        let digest = Digest::force_from_bytes(change.hash().0);
        if covered.contains(&digest) {
            continue;
        }
        // Remap parents: if a parent is a fragment member, point to
        // the fragment head instead so minimize doesn't prune us.
        let parents: BTreeSet<Digest<LooseCommit>> = change
            .deps()
            .iter()
            .map(|d| {
                let dep = Digest::force_from_bytes(d.0);
                member_to_head.get(&dep).copied().unwrap_or(dep)
            })
            .collect();
        let blob = Blob::new(change.raw_bytes().to_vec());
        let commit = LooseCommit::new(sedimentree_id, parents, BlobMeta::new(&blob));
        loose_commits.push(commit);
        blobs.push(blob);
    }
    (loose_commits, blobs)
}
