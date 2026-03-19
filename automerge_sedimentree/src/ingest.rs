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
    // topological (causal) order. Then for each fragment we:
    //   1. Filter to member changes, preserving topological order
    //   2. Concatenate their raw_bytes()
    //   3. Load into a fresh Automerge doc via load_incremental
    //   4. save() → compact Document format (unified columnar encoding)
    //
    // This produces blobs comparable in size to the original .am file
    // rather than the inflated per-change or Bundle formats.
    //
    // For loose commits: just the individual change's raw_bytes() (already
    // small, no recompression needed).
    let changes = doc.get_changes(&[]);

    let mut blobs = Vec::new();
    let mut fragments = Vec::with_capacity(states.len());
    for state in states {
        let members: Set<ChangeHash> = state
            .members()
            .iter()
            .map(|d| ChangeHash(*d.as_bytes()))
            .collect();
        let boundary: Set<ChangeHash> = state
            .boundary()
            .keys()
            .map(|d| ChangeHash(*d.as_bytes()))
            .collect();

        // Build a sub-document with:
        // 1. Boundary commits (causal deps — needed so member changes apply
        //    to the OpSet instead of being queued as orphans)
        // 2. Member commits
        //
        // Both in topological order from get_changes.
        let mut sub_doc = Automerge::new();
        let mut raw = Vec::new();
        for change in &changes {
            let hash = change.hash();
            if boundary.contains(&hash) || members.contains(&hash) {
                raw.extend_from_slice(change.raw_bytes());
            }
        }
        sub_doc
            .load_incremental(&raw)
            .map_err(|e| IngestError::Automerge(e.to_string()))?;

        // save_after(boundary_heads) gives us the compact Document format
        // for only the member changes (excluding the boundary base state).
        let boundary_heads: Vec<ChangeHash> = boundary.into_iter().collect();
        let compact = sub_doc.save_after(&boundary_heads);

        let blob = Blob::new(compact);
        let fragment = state.to_fragment(sedimentree_id, BlobMeta::new(&blob));
        fragments.push(fragment);
        blobs.push(blob);
    }

    // Loose commits: raw_bytes() per individual change.
    let mut loose_commits = Vec::with_capacity(changes.len() - covered_count);
    for change in &changes {
        let digest = Digest::force_from_bytes(change.hash().0);
        if covered.contains(&digest) {
            continue;
        }

        let parents: std::collections::BTreeSet<Digest<LooseCommit>> = change
            .deps()
            .iter()
            .map(|d| Digest::force_from_bytes(d.0))
            .collect();
        let blob = Blob::new(change.raw_bytes().to_vec());
        let commit = LooseCommit::new(sedimentree_id, parents, BlobMeta::new(&blob));
        loose_commits.push(commit);
        blobs.push(blob);
    }

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
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
