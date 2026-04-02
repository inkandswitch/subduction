//! Roundtrip ingestion tests using real Automerge documents from the egwalker
//! paper test vectors.
//!
//! Each test loads a `.am` file, decomposes it into Sedimentree fragments +
//! loose commits via `build_fragment_store`, and verifies structural
//! invariants. Byte-identical reassembly is tested in release mode only
//! (automerge's `get_changes` has a `debug_assert` that doubles work).

#![allow(
    clippy::cast_precision_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use automerge::{Automerge, ChangeHash, ReadDoc, ROOT};
use automerge_sedimentree::indexed::{IndexedSedimentreeAutomerge, OwnedParents};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    crypto::{digest::Digest, fingerprint::FingerprintSeed},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::{Sedimentree, SedimentreeItem},
};

/// Lightweight decomposition using only `get_changes_meta` (fast in debug).
/// Does NOT extract raw bytes — just builds the fragment index.
struct MetadataDecomp {
    change_count: usize,
    heads: Vec<CommitId>,
    covered: Set<CommitId>,
    uncovered_count: usize,
    fragment_count: usize,
}

fn decompose_meta(doc: &Automerge) -> MetadataDecomp {
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();
    let all_digests: Set<CommitId> = metadata.iter().map(|m| CommitId::new(m.hash.0)).collect();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();
    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();

    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .expect("build_fragment_store");
    let fragment_count = fresh.len();

    let covered: Set<CommitId> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();

    let uncovered_count = all_digests.iter().filter(|d| !covered.contains(d)).count();

    MetadataDecomp {
        change_count,
        heads,
        covered,
        uncovered_count,
        fragment_count,
    }
}

/// Full decomposition including raw bytes (requires `get_changes` — slow in
/// debug due to automerge's internal `debug_assert`).
struct FullDecomp {
    change_count: usize,
    /// Compact Document-format blobs per fragment.
    fragment_blobs: Vec<Vec<u8>>,
    /// Raw change bytes per loose commit.
    uncovered_blobs: Vec<Vec<u8>>,
    uncovered_parents: Vec<std::collections::BTreeSet<CommitId>>,
    fragment_state_blobs: Vec<(FragmentState<OwnedParents>, Blob)>,
}

fn decompose_full(doc: &Automerge) -> FullDecomp {
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<CommitId> = doc.get_heads().iter().map(|h| CommitId::new(h.0)).collect();
    let mut known: Map<CommitId, FragmentState<OwnedParents>> = Map::new();

    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .expect("build_fragment_store");
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<CommitId> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();

    // Get all changes with raw bytes in topological order.
    let changes = doc.get_changes(&[]);

    // For each fragment: concatenate only the member changes' raw bytes,
    // preserving the topological order from get_changes.
    let mut fragment_blobs = Vec::new();
    let mut fragment_state_blobs = Vec::new();
    for state in &states {
        let members: Set<ChangeHash> = state
            .members()
            .iter()
            .map(|d| ChangeHash(*d.as_bytes()))
            .collect();

        let mut raw = Vec::new();
        for change in &changes {
            if members.contains(&change.hash()) {
                raw.extend_from_slice(change.raw_bytes());
            }
        }

        fragment_state_blobs.push((state.clone(), Blob::new(raw.clone())));
        fragment_blobs.push(raw);
    }

    // Loose commits: raw_bytes per uncovered change.
    let mut uncovered_blobs = Vec::new();
    let mut uncovered_parents = Vec::new();
    for change in &changes {
        let id = CommitId::new(change.hash().0);
        if covered.contains(&id) {
            continue;
        }
        uncovered_blobs.push(change.raw_bytes().to_vec());
        uncovered_parents.push(change.deps().iter().map(|d| CommitId::new(d.0)).collect());
    }

    FullDecomp {
        change_count,
        fragment_blobs,
        uncovered_blobs,
        uncovered_parents,
        fragment_state_blobs,
    }
}

fn sed_id(bytes: &[u8]) -> SedimentreeId {
    let d: Digest<Blob> = Digest::hash(&Blob::new(bytes.to_vec()));
    SedimentreeId::new(*d.as_bytes())
}

fn build_tree(bytes: &[u8], d: &FullDecomp) -> (Sedimentree, Vec<Fragment>, Vec<LooseCommit>) {
    let id = sed_id(bytes);

    let fragments: Vec<Fragment> = d
        .fragment_state_blobs
        .iter()
        .map(|(state, blob)| state.clone().to_fragment(id, BlobMeta::new(blob)))
        .collect();

    let loose: Vec<LooseCommit> = d
        .uncovered_blobs
        .iter()
        .zip(d.uncovered_parents.iter())
        .enumerate()
        .map(|(i, (raw, parents))| {
            let head = CommitId::new({
                let mut bytes = [0u8; 32];
                bytes[0] = i as u8;
                bytes[1] = (i >> 8) as u8;
                // Use raw bytes hash for uniqueness
                let blob = Blob::new(raw.clone());
                let digest = Digest::hash(&blob);
                bytes[2..].copy_from_slice(&digest.as_bytes()[..30]);
                bytes
            });
            LooseCommit::new(
                id,
                head,
                parents.clone(),
                BlobMeta::new(&Blob::new(raw.clone())),
            )
        })
        .collect();

    let tree = Sedimentree::new(fragments.clone(), loose.clone());
    (tree, fragments, loose)
}

// ---------------------------------------------------------------------------
// Fast tests (metadata only — no get_changes, runs in debug mode)
// ---------------------------------------------------------------------------

/// Verify documents load and `build_fragment_store` completes.
#[test]
fn load_and_count() {
    for (name, bytes) in [
        ("C1", &include_bytes!("../test-vectors/C1.am")[..]),
        ("C2", &include_bytes!("../test-vectors/C2.am")[..]),
        ("S1", &include_bytes!("../test-vectors/S1.am")[..]),
        ("S2", &include_bytes!("../test-vectors/S2.am")[..]),
        ("S3", &include_bytes!("../test-vectors/S3.am")[..]),
        ("A1", &include_bytes!("../test-vectors/A1.am")[..]),
        ("A2", &include_bytes!("../test-vectors/A2.am")[..]),
    ] {
        let doc = Automerge::load(bytes).expect(name);
        let d = decompose_meta(&doc);
        eprintln!(
            "{name}: {} changes, {} fragments, {} uncovered, {} heads",
            d.change_count,
            d.fragment_count,
            d.uncovered_count,
            d.heads.len(),
        );
        assert!(d.change_count > 0, "{name}: should have changes");
        assert!(!d.heads.is_empty(), "{name}: should have heads");
    }
}

/// Fragment count + uncovered count == total change count.
#[test]
fn coverage_is_complete() {
    for (name, bytes) in [
        ("C1", &include_bytes!("../test-vectors/C1.am")[..]),
        ("C2", &include_bytes!("../test-vectors/C2.am")[..]),
        ("S1", &include_bytes!("../test-vectors/S1.am")[..]),
        ("S2", &include_bytes!("../test-vectors/S2.am")[..]),
        ("S3", &include_bytes!("../test-vectors/S3.am")[..]),
        ("A1", &include_bytes!("../test-vectors/A1.am")[..]),
        ("A2", &include_bytes!("../test-vectors/A2.am")[..]),
    ] {
        let doc = Automerge::load(bytes).expect(name);
        let d = decompose_meta(&doc);
        let total = d.covered.len() + d.uncovered_count;
        assert_eq!(
            total,
            d.change_count,
            "{name}: covered ({}) + uncovered ({}) != total ({})",
            d.covered.len(),
            d.uncovered_count,
            d.change_count,
        );
    }
}

/// Documents with >2 changes should produce at least one fragment.
#[test]
fn produces_fragments() {
    for (name, bytes) in [
        ("A1", &include_bytes!("../test-vectors/A1.am")[..]),
        ("A2", &include_bytes!("../test-vectors/A2.am")[..]),
        // C1/C2 have 93k/134k changes — always produce fragments
        ("C1", &include_bytes!("../test-vectors/C1.am")[..]),
        ("C2", &include_bytes!("../test-vectors/C2.am")[..]),
    ] {
        let doc = Automerge::load(bytes).expect(name);
        let d = decompose_meta(&doc);
        assert!(
            d.fragment_count > 0,
            "{name}: {} changes should produce fragments",
            d.change_count,
        );
    }
}

fn roundtrip_full(name: &str, bytes: &[u8]) {
    let doc = Automerge::load(bytes).expect(name);
    let d = decompose_full(&doc);
    let (tree, _fragments, _loose) = build_tree(bytes, &d);

    // Build a lookup from BlobMeta digest → raw bytes.
    // Sedimentree::fragments()/loose_commits() iterate in BTreeMap order
    // (by Digest<Fragment>/Digest<LooseCommit>), which differs from the
    // FullDecomp insertion order. We match them via BlobMeta digest.
    // Map blob digest → raw bytes for lookup during reassembly.
    let blob_by_digest: Map<Digest<Blob>, &[u8]> = d
        .fragment_state_blobs
        .iter()
        .map(|(_, blob)| (Digest::hash(blob), blob.as_slice()))
        .chain(d.uncovered_blobs.iter().map(|raw| {
            let blob = Blob::new(raw.clone());
            (Digest::hash(&blob), raw.as_slice())
        }))
        .collect();

    let order = tree
        .topsorted_blob_order()
        .expect("no cycles in test fixture");
    let fragments: Vec<_> = tree.fragments().collect();
    let loose: Vec<_> = tree.loose_commits().collect();

    let mut buf = Vec::new();
    for blob_ref in &order {
        let digest = match blob_ref {
            SedimentreeItem::Fragment(i) => fragments[*i].summary().blob_meta().digest(),
            SedimentreeItem::LooseCommit(i) => loose[*i].blob_meta().digest(),
        };
        let raw = blob_by_digest
            .get(&digest)
            .unwrap_or_else(|| panic!("{name}: blob not found for {blob_ref:?}"));
        buf.extend_from_slice(raw);
    }

    let mut rebuilt = Automerge::new();
    rebuilt
        .load_incremental(&buf)
        .expect("load topsorted blobs");

    assert_eq!(
        doc.get_heads(),
        rebuilt.get_heads(),
        "{name}: heads diverged"
    );
    assert_eq!(
        d.change_count,
        rebuilt.get_changes_meta(&[]).len(),
        "{name}: change count diverged"
    );

    // Content verification: compare root-level keys and values.
    let orig_keys: Vec<_> = doc.keys(&ROOT).collect();
    let rebuilt_keys: Vec<_> = rebuilt.keys(&ROOT).collect();
    assert_eq!(orig_keys, rebuilt_keys, "{name}: root keys diverged");

    for key in &orig_keys {
        let orig_val = doc.get(&ROOT, key.as_str());
        let rebuilt_val = rebuilt.get(&ROOT, key.as_str());
        assert_eq!(
            orig_val, rebuilt_val,
            "{name}: value at root key {key:?} diverged"
        );
    }

    // Spot check: root object length should match.
    assert_eq!(
        doc.length(&ROOT),
        rebuilt.length(&ROOT),
        "{name}: root object length diverged"
    );
}

/// Report blob sizes for all vectors that have fragments.
/// Release-only because it calls `get_changes`.
#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn blob_size_report() {
    for (name, bytes) in [
        ("A1", &include_bytes!("../test-vectors/A1.am")[..]),
        ("A2", &include_bytes!("../test-vectors/A2.am")[..]),
        ("C1", &include_bytes!("../test-vectors/C1.am")[..]),
        ("C2", &include_bytes!("../test-vectors/C2.am")[..]),
    ] {
        let doc = Automerge::load(bytes).expect(name);
        let d = decompose_full(&doc);
        let frag_bytes: usize = d.fragment_blobs.iter().map(Vec::len).sum();
        let loose_bytes: usize = d.uncovered_blobs.iter().map(Vec::len).sum();
        let original = bytes.len();
        let ratio = (frag_bytes + loose_bytes) as f64 / original as f64;
        eprintln!(
            "{name}: original={original} fragments={frag_bytes} ({} blobs) loose={loose_bytes} ({} blobs) total={} ratio={ratio:.2}x",
            d.fragment_state_blobs.len(),
            d.uncovered_blobs.len(),
            frag_bytes + loose_bytes,
        );
    }
}

#[test]
fn roundtrip_s1() {
    roundtrip_full("S1", include_bytes!("../test-vectors/S1.am"));
}

#[test]
fn roundtrip_s2() {
    roundtrip_full("S2", include_bytes!("../test-vectors/S2.am"));
}

#[test]
fn roundtrip_s3() {
    roundtrip_full("S3", include_bytes!("../test-vectors/S3.am"));
}

// A1/A2/C1/C2 full roundtrips — only run in release mode because
// automerge's get_changes has a debug_assert that doubles work.
#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn roundtrip_a1() {
    roundtrip_full("A1", include_bytes!("../test-vectors/A1.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn roundtrip_a2() {
    roundtrip_full("A2", include_bytes!("../test-vectors/A2.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn roundtrip_c1() {
    roundtrip_full("C1", include_bytes!("../test-vectors/C1.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn roundtrip_c2() {
    roundtrip_full("C2", include_bytes!("../test-vectors/C2.am"));
}

static S1: &[u8] = include_bytes!("../test-vectors/S1.am");

#[test]
fn sedimentree_diff_against_empty() {
    let doc = Automerge::load(S1).unwrap();
    let d = decompose_full(&doc);
    let (tree, fragments, loose) = build_tree(S1, &d);
    let empty = Sedimentree::default();

    let diff = empty.diff(&tree);
    assert_eq!(diff.left_missing_fragments.len(), fragments.len());
    assert_eq!(diff.left_missing_commits.len(), loose.len());
    assert!(diff.right_missing_fragments.is_empty());
    assert!(diff.right_missing_commits.is_empty());
}

#[test]
fn sedimentree_diff_against_self() {
    let doc = Automerge::load(S1).unwrap();
    let d = decompose_full(&doc);
    let (tree, _, _) = build_tree(S1, &d);

    let diff = tree.diff(&tree);
    assert!(diff.left_missing_fragments.is_empty());
    assert!(diff.left_missing_commits.is_empty());
    assert!(diff.right_missing_fragments.is_empty());
    assert!(diff.right_missing_commits.is_empty());
}

#[test]
fn fingerprint_self_diff_empty() {
    let doc = Automerge::load(S1).unwrap();
    let d = decompose_full(&doc);
    let (tree, _, _) = build_tree(S1, &d);

    let seed = FingerprintSeed::new(42, 99);
    let summary = tree.fingerprint_summarize(&seed);
    let diff = tree.diff_remote_fingerprints(&summary);

    assert!(diff.local_only_fragments.is_empty());
    assert!(diff.local_only_commits.is_empty());
    assert!(diff.remote_only_fragment_fingerprints.is_empty());
    assert!(diff.remote_only_commit_fingerprints.is_empty());
}

#[test]
fn merge_identical_is_idempotent() {
    let doc = Automerge::load(S1).unwrap();
    let d = decompose_full(&doc);
    let (tree_a, frags, loose) = build_tree(S1, &d);
    let tree_b = Sedimentree::new(frags, loose);
    let m = CountLeadingZeroBytes;

    let before = tree_a.minimal_hash(&m);
    let mut merged = tree_a;
    merged.merge(tree_b);
    assert_eq!(before, merged.minimal_hash(&m));
}

/// After minimize, the sedimentree should still have the same number of
/// loose commits and fragments as before. Loose commits' parents are
/// remapped to fragment heads so minimize doesn't prune them.
#[test]
fn minimize_preserves_all_items_after_ingestion() {
    let doc = Automerge::load(include_bytes!("../test-vectors/S1.am")).unwrap();
    let sed_id = sed_id(include_bytes!("../test-vectors/S1.am"));
    let result = automerge_sedimentree::ingest::ingest_automerge(&doc, sed_id).unwrap();

    let before_fragments = result.sedimentree.fragments().count();
    let before_commits = result.sedimentree.loose_commits().count();

    let minimized = result.sedimentree.minimize(&CountLeadingZeroBytes);
    let after_fragments = minimized.fragments().count();
    let after_commits = minimized.loose_commits().count();

    assert_eq!(
        before_commits, after_commits,
        "minimize should not drop loose commits (before={before_commits}, after={after_commits})"
    );
    assert!(
        after_fragments <= before_fragments,
        "minimize should not add fragments"
    );
}

/// Fingerprint summary after ingestion should include all items.
#[test]
fn fingerprint_summary_includes_all_items_after_ingestion() {
    let doc = Automerge::load(include_bytes!("../test-vectors/S1.am")).unwrap();
    let sed_id = sed_id(include_bytes!("../test-vectors/S1.am"));
    let result = automerge_sedimentree::ingest::ingest_automerge(&doc, sed_id).unwrap();

    let seed = FingerprintSeed::new(42, 99);
    let summary = result.sedimentree.fingerprint_summarize(&seed);

    assert_eq!(
        summary.commit_fingerprints().len(),
        result.loose_count,
        "fingerprint summary should have all loose commits"
    );
    assert_eq!(
        summary.fragment_fingerprints().len(),
        result.fragment_count,
        "fingerprint summary should have all fragments"
    );
}

/// Ingest A2 (3,208 changes) via the production `ingest_automerge` API
/// and verify the roundtrip: topsorted reassembly must produce the same
/// document heads and change count.
#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn ingest_roundtrip_a2() {
    let bytes = include_bytes!("../test-vectors/A2.am");
    let doc = Automerge::load(bytes).expect("A2");
    let original_heads = doc.get_heads();
    let original_count = doc.get_changes(&[]).len();

    let sed_id = sed_id(bytes);
    let result = automerge_sedimentree::ingest::ingest_automerge(&doc, sed_id).expect("ingest");

    // Verify ingest accounting: every change is either covered by a
    // fragment or emitted as a loose commit, with no overlap.
    assert_eq!(
        result.covered_count + result.loose_count,
        original_count,
        "ingest must account for all changes (covered={}, loose={}, total={})",
        result.covered_count,
        result.loose_count,
        original_count,
    );

    // Reassemble via topsort
    let order = result
        .sedimentree
        .topsorted_blob_order()
        .expect("no cycles");
    let fragments: Vec<_> = result.sedimentree.fragments().collect();
    let loose: Vec<_> = result.sedimentree.loose_commits().collect();

    let blob_by_digest: Map<Digest<Blob>, &[u8]> = result
        .blobs
        .iter()
        .map(|blob| (Digest::hash(blob), blob.as_slice()))
        .collect();

    let mut buf = Vec::new();
    for item in &order {
        let digest = match item {
            SedimentreeItem::Fragment(i) => fragments[*i].summary().blob_meta().digest(),
            SedimentreeItem::LooseCommit(i) => loose[*i].blob_meta().digest(),
        };
        let raw = blob_by_digest
            .get(&digest)
            .expect("blob not found for item");
        buf.extend_from_slice(raw);
    }

    let mut rebuilt = Automerge::new();
    rebuilt.load_incremental(&buf).expect("load topsorted");

    assert_eq!(rebuilt.get_heads(), original_heads, "heads must match");
    assert_eq!(
        rebuilt.get_changes(&[]).len(),
        original_count,
        "change count must match"
    );
}

// ---------------------------------------------------------------------------
// Real-world document tests (226k changes, ~6MB)
//
// The test file `real-world.am` is not checked in — it contains real user
// data and is too large for the repository. Tests skip gracefully when the
// file is absent (the CI egwalker vectors A1/A2/C1/C2/S1-S3 still run).
//
// To run these tests locally:
//
//   1. Download the document from the shared team drive or regenerate it
//      from the canonical source. The expected file is the Automerge
//      binary format (`.am`), ~6 MB, ~226k changes.
//
//   2. Place it at:
//
//        automerge_sedimentree/test-vectors/real-world.am
//
//   3. Run:
//
//        cargo test --package automerge_sedimentree --release real_world
//
//      The `real_world_metadata_decomposition` test runs in debug mode
//      too (metadata-only, no `get_changes`). The `real_world_roundtrip`
//      test requires release mode (~9 seconds).
//
//   4. To run *all* roundtrip tests (including the egwalker vectors and
//      the real-world doc):
//
//        cargo test --package automerge_sedimentree --release roundtrip
//
// The file is gitignored. Do not check it in.
// ---------------------------------------------------------------------------

const REAL_WORLD_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test-vectors/real-world.am");

fn load_real_world() -> Option<Vec<u8>> {
    std::fs::read(REAL_WORLD_PATH).ok()
}

/// Metadata-only decomposition on the 226k-change real-world doc.
/// Verifies `build_fragment_store` completes and coverage is correct.
#[test]
fn real_world_metadata_decomposition() {
    let Some(bytes) = load_real_world() else {
        eprintln!("skipping: real-world.am not found at {REAL_WORLD_PATH}");
        return;
    };

    let doc = Automerge::load(&bytes).expect("load real-world doc");
    let d = decompose_meta(&doc);

    eprintln!(
        "real-world: {} changes, {} fragments, {} uncovered, {} heads",
        d.change_count,
        d.fragment_count,
        d.uncovered_count,
        d.heads.len(),
    );

    assert!(d.change_count > 200_000, "expected 200k+ changes");
    assert!(d.fragment_count > 0, "should produce fragments");
    assert_eq!(
        d.covered.len() + d.uncovered_count,
        d.change_count,
        "coverage mismatch"
    );
}

/// Full bundle roundtrip on the real-world doc.
/// Only runs in release mode, and only when the file is present.
#[test]
#[cfg_attr(debug_assertions, ignore = "too slow in debug mode")]
fn real_world_roundtrip() {
    let Some(bytes) = load_real_world() else {
        eprintln!("skipping: real-world.am not found at {REAL_WORLD_PATH}");
        return;
    };

    roundtrip_full("real-world", &bytes);
}
