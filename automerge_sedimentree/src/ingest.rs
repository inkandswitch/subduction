//! Automerge document ingestion into Sedimentree.
//!
//! Thin adapter over [`Automerge::fragments()`] and
//! [`Automerge::bundle_fragments()`]. Automerge computes the fragmentation
//! internally — we just map each level-1+ `automerge::Fragment` into a
//! sedimentree [`Fragment`], and each level-0 fragment into a [`LooseCommit`].
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
//! let result = ingest_automerge(&doc, sed_id);
//!
//! println!("fragments: {}", result.fragment_count);
//! println!("loose commits: {}", result.loose_count);
//! ```

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

#[cfg(feature = "rayon")]
use rayon::prelude::*;

/// Result of ingesting an Automerge document.
///
/// Pass `sedimentree` and `blobs` to `subduction_core::Subduction::add_sedimentree`
/// (or the equivalent on whatever `Subduction` instance you hold) to store and
/// sync the data. Blob order is fragments first, loose commits after.
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

    finalize(doc, sedimentree_id, &cached, &loose, cached_bytes, loose_bytes)
}

/// Parallel variant of [`ingest_automerge`].
///
/// Uses [rayon] to parallelize the per-fragment deflate / columnize step
/// inside `bundle_fragments`. Each fragment's bundling is independent and
/// CPU-heavy, so this gives roughly N-way parallelism on documents with N
/// fragments. The metadata walk (`Automerge::fragments`) is upstream and
/// remains sequential.
///
/// # Panics
///
/// Panics if [`Automerge::bundle_fragments`] returns no bytes for a
/// fragment that was just returned by [`Automerge::fragments`] on the
/// same document — an upstream invariant the parallel path relies on
/// when bundling one fragment at a time.
///
/// [rayon]: https://docs.rs/rayon
#[cfg(feature = "rayon")]
#[cfg_attr(docsrs, doc(cfg(feature = "rayon")))]
#[must_use]
#[allow(clippy::expect_used)] // upstream-invariant violation; see # Panics
pub fn ingest_automerge_par(doc: &Automerge, sedimentree_id: SedimentreeId) -> IngestResult
where
    Automerge: Sync,
{
    let cached = doc.fragments(1..);
    let loose = doc.fragments(0..=0);

    let bundle_one = |f: &automerge::Fragment| -> Vec<u8> {
        doc.bundle_fragments(core::iter::once(f.clone()))
            .into_iter()
            .next()
            .expect("bundle_fragments returned empty for a fragment from this doc")
    };

    let cached_bytes: Vec<Vec<u8>> = cached.par_iter().map(bundle_one).collect();
    let loose_bytes: Vec<Vec<u8>> = loose.par_iter().map(bundle_one).collect();

    finalize(doc, sedimentree_id, &cached, &loose, cached_bytes, loose_bytes)
}

/// Common post-processing for the sequential and parallel ingest paths.
///
/// Both variants converge here once they have the bundled blob bytes for
/// the cached (level ≥ 1) and loose (level 0) fragment lists.
fn finalize(
    doc: &Automerge,
    sedimentree_id: SedimentreeId,
    cached: &[automerge::Fragment],
    loose: &[automerge::Fragment],
    cached_bytes: Vec<Vec<u8>>,
    loose_bytes: Vec<Vec<u8>>,
) -> IngestResult {
    // Loose commits whose parents point at a non-head member of a cached
    // fragment are remapped to the containing fragment's head, so
    // Sedimentree::minimize doesn't treat the loose commit as covered and
    // prune it.
    let mut member_to_head: Map<CommitId, CommitId> = Map::new();
    for f in cached {
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
        change_count: doc.get_changes_meta(&[]).len(),
    }
}
