//! Phase-level wall-clock breakdown of the OLD vs. NEW ingest pipelines.
//!
//! Old path = `IndexedSedimentreeAutomerge` + `build_fragment_store` + raw
//! byte concatenation (what was in `src/ingest.rs` before the rewrite).
//! New path = `Automerge::fragments` + `Automerge::bundle_fragments`.
//!
//! Each phase is timed with `Instant::now()`. Reports per-doc, per-phase ms.

#![allow(clippy::cast_precision_loss, clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::time::Instant;

use automerge::{Automerge, ChangeHash};
use automerge_sedimentree::indexed::{IndexedSedimentreeAutomerge, OwnedParents};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    crypto::digest::Digest,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

fn sed_id(bytes: &[u8]) -> SedimentreeId {
    let d: Digest<Blob> = Digest::hash(&Blob::new(bytes.to_vec()));
    SedimentreeId::new(*d.as_bytes())
}

fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[derive(Default)]
struct Phases {
    labels: Vec<&'static str>,
    times_ms: Vec<f64>,
}

impl Phases {
    fn record(&mut self, label: &'static str, dur: std::time::Duration) {
        self.labels.push(label);
        self.times_ms.push(ms(dur));
    }
    fn total(&self) -> f64 {
        self.times_ms.iter().sum()
    }
    fn print(&self, prefix: &str) {
        for (l, t) in self.labels.iter().zip(&self.times_ms) {
            eprintln!("  {prefix} {l:<28} {t:>9.2} ms");
        }
        eprintln!("  {prefix} {:<28} {:>9.2} ms", "TOTAL", self.total());
    }
}

// ============ OLD pipeline (reconstructed) ============
fn run_old(doc: &Automerge, sedimentree_id: SedimentreeId) -> Phases {
    let mut p = Phases::default();

    let t = Instant::now();
    let metadata = doc.get_changes_meta(&[]);
    p.record("get_changes_meta", t.elapsed());

    let t = Instant::now();
    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    p.record("indexed_from_metadata", t.elapsed());

    let t = Instant::now();
    let heads: Vec<CommitId> = doc
        .get_heads()
        .iter()
        .map(|h| CommitId::new(h.0))
        .collect();
    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();
    store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .expect("build_fragment_store");
    let fresh: Vec<FragmentState<OwnedParents>> = known.values().cloned().collect();
    p.record("build_fragment_store", t.elapsed());

    let t = Instant::now();
    let changes = doc.get_changes(&[]);
    p.record("get_changes", t.elapsed());

    let t = Instant::now();
    let mut index: Vec<(ChangeHash, &[u8])> = Vec::with_capacity(changes.len());
    for c in &changes {
        index.push((c.hash(), c.raw_bytes()));
    }
    p.record("build_change_index", t.elapsed());

    let t = Instant::now();
    let covered: Set<CommitId> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();
    p.record("collect_covered", t.elapsed());

    let t = Instant::now();
    let mut fragments_vec = Vec::with_capacity(fresh.len());
    let mut fragment_blobs = Vec::with_capacity(fresh.len());
    for state in &fresh {
        let members: Set<ChangeHash> = state
            .members()
            .iter()
            .map(|d| ChangeHash(*d.as_bytes()))
            .collect();
        let mut raw = Vec::new();
        for &(hash, bytes) in &index {
            if members.contains(&hash) {
                raw.extend_from_slice(bytes);
            }
        }
        let blob = Blob::new(raw);
        let fragment = state
            .clone()
            .to_fragment(sedimentree_id, BlobMeta::new(&blob));
        fragments_vec.push(fragment);
        fragment_blobs.push(blob);
    }
    p.record("compress_fragments", t.elapsed());

    let t = Instant::now();
    let mut member_to_head: Map<CommitId, CommitId> = Map::new();
    for state in &fresh {
        let head = state.head_id();
        for member in state.members() {
            if *member != head {
                member_to_head.insert(*member, head);
            }
        }
    }
    let mut loose_commits = Vec::new();
    let mut loose_blobs = Vec::new();
    for change in &changes {
        let id = CommitId::new(change.hash().0);
        if covered.contains(&id) {
            continue;
        }
        let parents: BTreeSet<CommitId> = change
            .deps()
            .iter()
            .map(|d| {
                let dep = CommitId::new(d.0);
                member_to_head.get(&dep).copied().unwrap_or(dep)
            })
            .collect();
        let blob = Blob::new(change.raw_bytes().to_vec());
        loose_commits.push(LooseCommit::new(
            sedimentree_id,
            id,
            parents,
            BlobMeta::new(&blob),
        ));
        loose_blobs.push(blob);
    }
    p.record("collect_loose_commits", t.elapsed());

    let t = Instant::now();
    let mut blobs = fragment_blobs;
    blobs.extend(loose_blobs);
    let _ = Sedimentree::new(fragments_vec, loose_commits);
    p.record("assemble_sedimentree", t.elapsed());

    p
}

// ============ NEW pipeline ============
fn run_new(doc: &Automerge, sedimentree_id: SedimentreeId) -> Phases {
    let mut p = Phases::default();

    let t = Instant::now();
    let cached = doc.fragments(1..);
    p.record("doc.fragments(1..)", t.elapsed());

    let t = Instant::now();
    let loose = doc.fragments(0..=0);
    p.record("doc.fragments(0..=0)", t.elapsed());

    let t = Instant::now();
    let cached_bytes = doc.bundle_fragments(cached.iter().cloned());
    p.record("bundle_fragments(cached)", t.elapsed());

    let t = Instant::now();
    let loose_bytes = doc.bundle_fragments(loose.iter().cloned());
    p.record("bundle_fragments(loose)", t.elapsed());

    let t = Instant::now();
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
    p.record("member_to_head_map", t.elapsed());

    let t = Instant::now();
    let mut fragments_vec = Vec::with_capacity(cached.len());
    let mut blobs = Vec::with_capacity(cached.len() + loose.len());
    let mut covered: Set<CommitId> = Set::new();
    for (f, raw) in cached.iter().zip(cached_bytes) {
        for m in &f.members {
            covered.insert(CommitId::new(m.0));
        }
        let blob = Blob::new(raw);
        let boundary: BTreeSet<CommitId> =
            f.boundary.iter().map(|h| CommitId::new(h.0)).collect();
        let checkpoints: Vec<CommitId> =
            f.checkpoints.iter().map(|h| CommitId::new(h.0)).collect();
        let meta = BlobMeta::new(&blob);
        fragments_vec.push(Fragment::new(
            sedimentree_id,
            CommitId::new(f.head.0),
            boundary,
            &checkpoints,
            meta,
        ));
        blobs.push(blob);
    }
    p.record("build_cached_fragments", t.elapsed());

    let t = Instant::now();
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
        loose_commits.push(LooseCommit::new(
            sedimentree_id,
            head,
            parents,
            BlobMeta::new(&blob),
        ));
        blobs.push(blob);
    }
    p.record("build_loose_commits", t.elapsed());

    let t = Instant::now();
    let _ = Sedimentree::new(fragments_vec, loose_commits);
    p.record("assemble_sedimentree", t.elapsed());

    p
}

#[test]
fn breakdown_a1_a2() {
    for (name, bytes) in [
        ("A1", include_bytes!("../test-vectors/A1.am").as_slice()),
        ("A2", include_bytes!("../test-vectors/A2.am").as_slice()),
    ] {
        eprintln!("\n=== {name} ===");
        let load_t = Instant::now();
        let doc = Automerge::load(bytes).expect(name);
        eprintln!(
            "  shared    Automerge::load            {:>9.2} ms",
            ms(load_t.elapsed())
        );

        let sid = sed_id(bytes);
        let old = run_old(&doc, sid);
        old.print("OLD ");
        let new = run_new(&doc, sid);
        new.print("NEW ");
    }
}

#[test]
fn breakdown_all_vectors() {
    for (name, bytes) in [
        ("S1", include_bytes!("../test-vectors/S1.am").as_slice()),
        ("S2", include_bytes!("../test-vectors/S2.am").as_slice()),
        ("S3", include_bytes!("../test-vectors/S3.am").as_slice()),
        ("A1", include_bytes!("../test-vectors/A1.am").as_slice()),
        ("A2", include_bytes!("../test-vectors/A2.am").as_slice()),
        ("C1", include_bytes!("../test-vectors/C1.am").as_slice()),
        ("C2", include_bytes!("../test-vectors/C2.am").as_slice()),
    ] {
        eprintln!("\n=== {name} ===");
        let load_t = Instant::now();
        let doc = Automerge::load(bytes).expect(name);
        let load_ms = ms(load_t.elapsed());

        let sid = sed_id(bytes);
        let old = run_old(&doc, sid);
        let new = run_new(&doc, sid);

        eprintln!("  load:                       {load_ms:>9.2} ms");
        eprintln!("  OLD ingest total:           {:>9.2} ms", old.total());
        eprintln!("  NEW ingest total:           {:>9.2} ms", new.total());
    }
}
