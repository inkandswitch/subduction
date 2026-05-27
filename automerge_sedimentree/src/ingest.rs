//! Automerge document ingestion into Sedimentree.
//!
//! Thin adapter over [`Automerge::fragments()`] and
//! [`Automerge::bundle_fragments()`]. Automerge computes the fragmentation
//! internally — we just map each level-1+ `automerge::Fragment` into a
//! sedimentree [`Fragment`], and each level-0 fragment into a [`LooseCommit`].

use alloc::{collections::BTreeSet, vec::Vec};

use automerge::Automerge;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

/// Result of ingesting an Automerge document.
///
/// Pass `sedimentree` and `blobs` to `Subduction::add_sedimentree` to store
/// and sync the data. Blob order is fragments first, loose commits after.
#[derive(Debug, Clone)]
pub struct IngestResult {
    /// The constructed sedimentree.
    pub sedimentree: Sedimentree,

    /// All blobs referenced by the sedimentree.
    pub blobs: Vec<Blob>,

    /// Total number of changes in the source document.
    pub change_count: usize,

    /// Number of distinct changes covered by fragments.
    pub covered_count: usize,

    /// Number of loose commits.
    pub loose_count: usize,

    /// Number of fragments produced.
    pub fragment_count: usize,
}

/// Ingest an Automerge document into a [`Sedimentree`].
#[must_use]
pub fn ingest_automerge(doc: &Automerge, sedimentree_id: SedimentreeId) -> IngestResult {
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);

    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());

    // Loose commits whose parents point at a non-head member of a cached
    // fragment are remapped to the containing fragment's head, so
    // Sedimentree::minimize doesn't treat the loose commit as covered and
    // prune it.
    let mut member_to_head: Map<CommitId, CommitId> = Map::new();
    for f in &cached {
        let head = CommitId::new(f.head.0);
        for m in &f.members {
            let id = CommitId::new(m.0);
            if id != head {
                member_to_head.insert(id, head);
            }
        }
    }

    let mut fragments = Vec::with_capacity(cached.len());
    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let mut covered: Set<CommitId> = Set::new();
    for (f, raw) in cached.iter().zip(cached_bytes) {
        for m in &f.members {
            covered.insert(CommitId::new(m.0));
        }
        let blob = Blob::new(raw);
        let boundary: BTreeSet<CommitId> = f.boundary.iter().map(|h| CommitId::new(h.0)).collect();
        let checkpoints: Vec<CommitId> = f.checkpoints.iter().map(|h| CommitId::new(h.0)).collect();
        let meta = BlobMeta::new(&blob);
        fragments.push(Fragment::new(
            sedimentree_id,
            CommitId::new(f.head.0),
            boundary,
            &checkpoints,
            meta,
        ));
        blobs.push(blob);
    }

    let mut loose_commits = Vec::with_capacity(loose.len());
    for (f, raw) in loose.iter().zip(loose_bytes) {
        let head = CommitId::new(f.head.0);
        let parents: BTreeSet<CommitId> = f
            .boundary
            .iter()
            .map(|p| {
                let dep = CommitId::new(p.0);
                member_to_head.get(&dep).copied().unwrap_or(dep)
            })
            .collect();
        let blob = Blob::new(raw);
        let meta = BlobMeta::new(&blob);
        loose_commits.push(LooseCommit::new(sedimentree_id, head, parents, meta));
        blobs.push(blob);
    }

    let fragment_count = fragments.len();
    let loose_count = loose_commits.len();
    let covered_count = covered.len();

    IngestResult {
        sedimentree: Sedimentree::new(fragments, loose_commits),
        blobs,
        fragment_count,
        loose_count,
        covered_count,
        change_count: covered_count + loose_count,
    }
}
