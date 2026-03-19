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

    /// A fragment member's change was not found in the document.
    #[error("change not found for digest: {0}")]
    MissingChange(Digest<LooseCommit>),
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

    // Phase 2: extract raw change bytes (one get_changes call).
    let changes = doc.get_changes(&[]);
    let by_hash: Map<ChangeHash, &automerge::Change> =
        changes.iter().map(|c| (c.hash(), c)).collect();

    // Build fragments: one blob per fragment state.
    let mut blobs = Vec::new();
    let mut fragments = Vec::with_capacity(states.len());
    for state in states {
        let mut raw = Vec::new();
        for member in state.members() {
            let hash = ChangeHash(*member.as_bytes());
            let change = by_hash
                .get(&hash)
                .ok_or(IngestError::MissingChange(*member))?;
            raw.extend_from_slice(change.raw_bytes());
        }
        let blob = Blob::new(raw);
        let fragment = state.to_fragment(sedimentree_id, BlobMeta::new(&blob));
        fragments.push(fragment);
        blobs.push(blob);
    }

    // Build loose commits from uncovered changes.
    let uncovered: Vec<&automerge::Change> = changes
        .iter()
        .filter(|c| !covered.contains(&Digest::force_from_bytes(c.hash().0)))
        .collect();

    let mut loose_commits = Vec::with_capacity(uncovered.len());
    for change in &uncovered {
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
