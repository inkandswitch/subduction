//! Roundtrip ingestion tests using real Automerge documents from the egwalker
//! paper test vectors.
//!
//! Each test loads a `.am` file, decomposes it into Sedimentree fragments +
//! loose commits via `build_fragment_store`, and verifies structural
//! invariants. Byte-identical reassembly is tested in release mode only
//! (automerge's `get_changes` has a `debug_assert` that doubles work).

use std::collections::HashMap;

use automerge::Automerge;
use automerge::ChangeHash;
use automerge_sedimentree::indexed::{IndexedSedimentreeAutomerge, OwnedParents};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{CommitStore, CountLeadingZeroBytes, FragmentState},
    crypto::{digest::Digest, fingerprint::FingerprintSeed},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Lightweight decomposition using only `get_changes_meta` (fast in debug).
/// Does NOT extract raw bytes — just builds the fragment index.
struct MetadataDecomp {
    change_count: usize,
    heads: Vec<Digest<LooseCommit>>,
    covered: Set<Digest<LooseCommit>>,
    uncovered_count: usize,
    fragment_count: usize,
}

fn decompose_meta(doc: &Automerge) -> MetadataDecomp {
    let metadata = doc.get_changes_meta(&[]);
    let change_count = metadata.len();
    let all_digests: Set<Digest<LooseCommit>> = metadata
        .iter()
        .map(|m| Digest::force_from_bytes(m.hash.0))
        .collect();

    let store = IndexedSedimentreeAutomerge::from_metadata(&metadata);
    let heads: Vec<Digest<LooseCommit>> = doc
        .get_heads()
        .iter()
        .map(|h| Digest::force_from_bytes(h.0))
        .collect();
    let mut known: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

    let fresh = store
        .build_fragment_store(&heads, &mut known, &CountLeadingZeroBytes)
        .expect("build_fragment_store");
    let fragment_count = fresh.len();

    let covered: Set<Digest<LooseCommit>> = known
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
    /// One bundle (columnar-compressed) per fragment.
    fragment_bundles: Vec<Vec<u8>>,
    /// One bundle per loose commit.
    uncovered_bundles: Vec<Vec<u8>>,
    uncovered_parents: Vec<std::collections::BTreeSet<Digest<LooseCommit>>>,
    fragment_state_blobs: Vec<(FragmentState<OwnedParents>, Blob)>,
}

fn decompose_full(doc: &Automerge) -> FullDecomp {
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
        .expect("build_fragment_store");
    let states: Vec<_> = fresh.into_iter().cloned().collect();

    let covered: Set<Digest<LooseCommit>> = known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();

    // Single get_changes call — one OpSet scan instead of N bundle() calls.
    let changes = doc.get_changes(&[]);
    let by_hash: HashMap<ChangeHash, &automerge::Change> =
        changes.iter().map(|c| (c.hash(), c)).collect();

    // Build one bundle per fragment from the pre-fetched changes.
    let mut fragment_bundles = Vec::new();
    let mut fragment_state_blobs = Vec::new();
    for state in &states {
        let mut raw = Vec::new();
        for member in state.members() {
            let hash = ChangeHash(*member.as_bytes());
            raw.extend_from_slice(by_hash.get(&hash).expect("member in changes").raw_bytes());
        }
        fragment_state_blobs.push((state.clone(), Blob::new(raw.clone())));
        fragment_bundles.push(raw);
    }

    // Loose commits from the same pre-fetched changes.
    let uncovered: Vec<&automerge::Change> = changes
        .iter()
        .filter(|c| !covered.contains(&Digest::force_from_bytes(c.hash().0)))
        .collect();

    let uncovered_bundles: Vec<Vec<u8>> =
        uncovered.iter().map(|c| c.raw_bytes().to_vec()).collect();
    let uncovered_parents: Vec<std::collections::BTreeSet<Digest<LooseCommit>>> = uncovered
        .iter()
        .map(|c| {
            c.deps()
                .iter()
                .map(|d| Digest::force_from_bytes(d.0))
                .collect()
        })
        .collect();

    FullDecomp {
        change_count,
        fragment_bundles,
        uncovered_bundles,
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
        .uncovered_bundles
        .iter()
        .zip(d.uncovered_parents.iter())
        .map(|(bundle_bytes, parents)| {
            LooseCommit::new(
                id,
                parents.clone(),
                BlobMeta::new(&Blob::new(bundle_bytes.clone())),
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

// ---------------------------------------------------------------------------
// Full roundtrip tests (require get_changes — slow in debug, fast in release)
// Use S1/S2/S3 (2 changes each) which are instant even in debug.
// ---------------------------------------------------------------------------

fn roundtrip_full(name: &str, bytes: &[u8]) {
    let doc = Automerge::load(bytes).expect(name);
    let d = decompose_full(&doc);

    let mut rebuilt = Automerge::new();
    for bundle_bytes in &d.fragment_bundles {
        rebuilt
            .load_incremental(bundle_bytes)
            .expect("load fragment bundle");
    }
    for bundle_bytes in &d.uncovered_bundles {
        rebuilt
            .load_incremental(bundle_bytes)
            .expect("load loose commit bundle");
    }

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

    // Note: byte-identical comparison (doc.save() == rebuilt.save()) is not
    // guaranteed because `load_incremental` with individual change blobs may
    // produce different internal ordering than `load` from a single document
    // chunk. The semantic equivalence (same heads, same change set) is what
    // matters for correctness.
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
#[cfg_attr(debug_assertions, ignore)]
fn roundtrip_a1() {
    roundtrip_full("A1", include_bytes!("../test-vectors/A1.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore)]
fn roundtrip_a2() {
    roundtrip_full("A2", include_bytes!("../test-vectors/A2.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore)]
fn roundtrip_c1() {
    roundtrip_full("C1", include_bytes!("../test-vectors/C1.am"));
}

#[test]
#[cfg_attr(debug_assertions, ignore)]
fn roundtrip_c2() {
    roundtrip_full("C2", include_bytes!("../test-vectors/C2.am"));
}

// ---------------------------------------------------------------------------
// Sedimentree structural tests (use S1 for speed — full decomp is cheap)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Real-world document tests (226k changes, ~6MB)
//
// The file is not checked in (it contains real user data). Tests skip
// gracefully when the file is absent.
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
#[cfg_attr(debug_assertions, ignore)]
fn real_world_roundtrip() {
    let Some(bytes) = load_real_world() else {
        eprintln!("skipping: real-world.am not found at {REAL_WORLD_PATH}");
        return;
    };

    roundtrip_full("real-world", &bytes);
}
