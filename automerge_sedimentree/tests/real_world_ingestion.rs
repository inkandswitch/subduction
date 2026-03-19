//! Roundtrip ingestion tests using real Automerge documents.
//!
//! These tests load Automerge documents from the egwalker paper test vectors,
//! decompose them into Sedimentree fragments + loose commits, then verify
//! the data can be fully reassembled into an equivalent Automerge document.
//!
//! When the `rayon` feature is enabled, parallel decomposition is also tested.

use std::collections::HashMap;

use automerge::{Automerge, ChangeHash};
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

struct TestVector {
    name: &'static str,
    bytes: &'static [u8],
}

macro_rules! include_test_vector {
    ($name:literal) => {
        TestVector {
            name: $name,
            bytes: include_bytes!(concat!("../test-vectors/", $name, ".am")),
        }
    };
}

static TEST_VECTORS: &[TestVector] = &[
    include_test_vector!("A1"),
    include_test_vector!("A2"),
    include_test_vector!("C1"),
    include_test_vector!("C2"),
    include_test_vector!("S1"),
    include_test_vector!("S2"),
    include_test_vector!("S3"),
];

fn load_doc(bytes: &[u8]) -> Automerge {
    Automerge::load(bytes).expect("failed to load automerge document")
}

fn sed_id_from_bytes(bytes: &[u8]) -> SedimentreeId {
    let blob = Blob::new(bytes.to_vec());
    let digest: Digest<Blob> = Digest::hash(&blob);
    SedimentreeId::new(*digest.as_bytes())
}

struct Decomposition {
    original_bytes: Vec<u8>,
    change_count: usize,
    heads: Vec<Digest<LooseCommit>>,
    fragment_blobs: Vec<(FragmentState<OwnedParents>, Blob)>,
    uncovered_blobs: Vec<(Digest<LooseCommit>, Blob)>,
    uncovered_parents: Vec<std::collections::BTreeSet<Digest<LooseCommit>>>,
    covered: Set<Digest<LooseCommit>>,
}

fn decompose(doc: &Automerge) -> Decomposition {
    let original_bytes = doc.save();
    let changes = doc.get_changes(&[]);
    let change_count = changes.len();

    let commit_store = IndexedSedimentreeAutomerge::from_changes(&changes);
    let heads: Vec<Digest<LooseCommit>> = doc
        .get_heads()
        .iter()
        .map(|h| Digest::force_from_bytes(h.0))
        .collect();
    let strategy = CountLeadingZeroBytes;
    let mut known_states: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

    let fresh = commit_store
        .build_fragment_store(&heads, &mut known_states, &strategy)
        .expect("fragment building should succeed");

    let fragment_states: Vec<FragmentState<OwnedParents>> = fresh.into_iter().cloned().collect();

    let changes_by_hash: HashMap<ChangeHash, &automerge::Change> =
        changes.iter().map(|c| (c.hash(), c)).collect();

    let mut fragment_blobs = Vec::new();
    for state in &fragment_states {
        let mut blob_bytes = Vec::new();
        for member in state.members() {
            let hash = ChangeHash(*member.as_bytes());
            let change = changes_by_hash
                .get(&hash)
                .expect("member should be in changes_by_hash");
            blob_bytes.extend_from_slice(change.raw_bytes());
        }
        fragment_blobs.push((state.clone(), Blob::new(blob_bytes)));
    }

    let covered: Set<Digest<LooseCommit>> = known_states
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect();

    let uncovered_changes: Vec<&automerge::Change> = changes
        .iter()
        .filter(|c| {
            let d = Digest::force_from_bytes(c.hash().0);
            !covered.contains(&d)
        })
        .collect();

    let uncovered_blobs: Vec<(Digest<LooseCommit>, Blob)> = uncovered_changes
        .iter()
        .map(|c| {
            let d = Digest::force_from_bytes(c.hash().0);
            (d, Blob::new(c.raw_bytes().to_vec()))
        })
        .collect();

    let uncovered_parents: Vec<std::collections::BTreeSet<Digest<LooseCommit>>> = uncovered_changes
        .iter()
        .map(|c| {
            c.deps()
                .iter()
                .map(|dep| Digest::force_from_bytes(dep.0))
                .collect()
        })
        .collect();

    Decomposition {
        original_bytes,
        change_count,
        heads,
        fragment_blobs,
        uncovered_blobs,
        uncovered_parents,
        covered,
    }
}

fn build_tree(
    sed_id: SedimentreeId,
    decomp: &Decomposition,
) -> (Sedimentree, Vec<Fragment>, Vec<LooseCommit>) {
    let fragments: Vec<Fragment> = decomp
        .fragment_blobs
        .iter()
        .map(|(state, blob)| state.clone().to_fragment(sed_id, BlobMeta::new(blob)))
        .collect();

    let loose_commits: Vec<LooseCommit> = decomp
        .uncovered_blobs
        .iter()
        .zip(decomp.uncovered_parents.iter())
        .map(|((_digest, blob), parents)| {
            LooseCommit::new(sed_id, parents.clone(), BlobMeta::new(blob))
        })
        .collect();

    let tree = Sedimentree::new(fragments.clone(), loose_commits.clone());
    (tree, fragments, loose_commits)
}

fn reassemble(decomp: &Decomposition) -> Automerge {
    let mut doc = Automerge::new();
    for (_state, blob) in &decomp.fragment_blobs {
        doc.load_incremental(blob.as_slice())
            .expect("fragment blob should load");
    }
    for (_digest, blob) in &decomp.uncovered_blobs {
        doc.load_incremental(blob.as_slice())
            .expect("uncovered blob should load");
    }
    doc
}

// -----------------------------------------------------------------------
// Automerge roundtrip tests
// -----------------------------------------------------------------------

#[test]
fn all_vectors_load_with_changes() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let changes = doc.get_changes(&[]);
        assert!(
            !changes.is_empty(),
            "{}: document should have changes",
            tv.name
        );
        assert!(
            !doc.get_heads().is_empty(),
            "{}: document should have heads",
            tv.name
        );
    }
}

#[test]
fn decomposition_covers_all_changes() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);

        let total = d.covered.len() + d.uncovered_blobs.len();
        assert_eq!(
            total,
            d.change_count,
            "{}: covered ({}) + uncovered ({}) != total ({})",
            tv.name,
            d.covered.len(),
            d.uncovered_blobs.len(),
            d.change_count,
        );
    }
}

#[test]
fn decomposition_produces_fragments() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);

        assert!(
            !d.fragment_blobs.is_empty(),
            "{}: should produce at least one fragment from {} changes",
            tv.name,
            d.change_count,
        );
    }
}

#[test]
fn reassembled_has_same_heads() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let reassembled = reassemble(&d);

        assert_eq!(
            doc.get_heads(),
            reassembled.get_heads(),
            "{}: heads should match after reassembly",
            tv.name,
        );
    }
}

#[test]
fn reassembled_has_same_change_count() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let reassembled = reassemble(&d);

        assert_eq!(
            d.change_count,
            reassembled.get_changes(&[]).len(),
            "{}: change count should match after reassembly",
            tv.name,
        );
    }
}

#[test]
fn reassembled_has_identical_bytes() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let reassembled = reassemble(&d);

        assert_eq!(
            d.original_bytes,
            reassembled.save(),
            "{}: saved bytes should be identical after reassembly",
            tv.name,
        );
    }
}

// -----------------------------------------------------------------------
// Sedimentree construction tests
// -----------------------------------------------------------------------

#[test]
fn sedimentree_contains_all_items() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, fragments, loose_commits) = build_tree(sed_id, &d);

        assert_eq!(
            tree.fragments().count(),
            fragments.len(),
            "{}: fragment count mismatch",
            tv.name,
        );
        assert_eq!(
            tree.loose_commits().count(),
            loose_commits.len(),
            "{}: loose commit count mismatch",
            tv.name,
        );

        for frag in &fragments {
            let digest = Digest::hash(frag);
            assert!(
                tree.fragments().any(|f| Digest::hash(f) == digest),
                "{}: missing fragment",
                tv.name,
            );
        }

        for commit in &loose_commits {
            assert!(
                tree.has_loose_commit(Digest::hash(commit)),
                "{}: missing loose commit",
                tv.name,
            );
        }
    }
}

#[test]
fn minimize_is_idempotent() {
    let strategy = CountLeadingZeroBytes;
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, _, _) = build_tree(sed_id, &d);

        let minimized = tree.minimize(&strategy);
        let double_minimized = minimized.minimize(&strategy);

        assert_eq!(
            minimized.minimal_hash(&strategy),
            double_minimized.minimal_hash(&strategy),
            "{}: minimize should be idempotent",
            tv.name,
        );
    }
}

#[test]
fn minimize_does_not_add_fragments() {
    let strategy = CountLeadingZeroBytes;
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, fragments, _) = build_tree(sed_id, &d);

        let minimized = tree.minimize(&strategy);
        assert!(
            minimized.fragments().count() <= fragments.len(),
            "{}: minimized should not have more fragments",
            tv.name,
        );
    }
}

#[test]
fn heads_include_automerge_heads() {
    let strategy = CountLeadingZeroBytes;
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, _, _) = build_tree(sed_id, &d);

        let tree_heads: Set<Digest<LooseCommit>> = tree.heads(&strategy).into_iter().collect();

        for am_head in &d.heads {
            assert!(
                tree_heads.contains(am_head),
                "{}: automerge head {:?} missing from sedimentree heads",
                tv.name,
                am_head,
            );
        }
    }
}

// -----------------------------------------------------------------------
// Diff and fingerprint tests
// -----------------------------------------------------------------------

#[test]
fn diff_against_empty_yields_everything() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, fragments, loose_commits) = build_tree(sed_id, &d);
        let empty = Sedimentree::default();

        let diff = empty.diff(&tree);

        assert_eq!(
            diff.left_missing_fragments.len(),
            fragments.len(),
            "{}: empty should be missing all fragments",
            tv.name,
        );
        assert_eq!(
            diff.left_missing_commits.len(),
            loose_commits.len(),
            "{}: empty should be missing all commits",
            tv.name,
        );
        assert!(
            diff.right_missing_fragments.is_empty(),
            "{}: full tree should not be missing fragments from empty",
            tv.name,
        );
        assert!(
            diff.right_missing_commits.is_empty(),
            "{}: full tree should not be missing commits from empty",
            tv.name,
        );
    }
}

#[test]
fn diff_against_self_yields_nothing() {
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, _, _) = build_tree(sed_id, &d);

        let diff = tree.diff(&tree);

        assert!(diff.left_missing_fragments.is_empty(), "{}", tv.name);
        assert!(diff.left_missing_commits.is_empty(), "{}", tv.name);
        assert!(diff.right_missing_fragments.is_empty(), "{}", tv.name);
        assert!(diff.right_missing_commits.is_empty(), "{}", tv.name);
    }
}

#[test]
fn fingerprint_self_diff_is_empty() {
    let seed = FingerprintSeed::new(42, 99);
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree, _, _) = build_tree(sed_id, &d);

        let summary = tree.fingerprint_summarize(&seed);
        let diff = tree.diff_remote_fingerprints(&summary);

        assert!(
            diff.local_only_fragments.is_empty(),
            "{}: no local-only fragments expected",
            tv.name,
        );
        assert!(
            diff.local_only_commits.is_empty(),
            "{}: no local-only commits expected",
            tv.name,
        );
        assert!(
            diff.remote_only_fragment_fingerprints.is_empty(),
            "{}: no remote-only fragment fingerprints expected",
            tv.name,
        );
        assert!(
            diff.remote_only_commit_fingerprints.is_empty(),
            "{}: no remote-only commit fingerprints expected",
            tv.name,
        );
    }
}

#[test]
fn merge_identical_trees_is_idempotent() {
    let strategy = CountLeadingZeroBytes;
    for tv in TEST_VECTORS {
        let doc = load_doc(tv.bytes);
        let d = decompose(&doc);
        let sed_id = sed_id_from_bytes(tv.bytes);
        let (tree_a, fragments, loose_commits) = build_tree(sed_id, &d);
        let tree_b = Sedimentree::new(fragments, loose_commits);

        let hash_before = tree_a.minimal_hash(&strategy);

        let mut merged = tree_a;
        merged.merge(tree_b);

        assert_eq!(
            hash_before,
            merged.minimal_hash(&strategy),
            "{}: merge of identical trees should be idempotent",
            tv.name,
        );
    }
}

// -----------------------------------------------------------------------
// Rayon parallel decomposition tests
// -----------------------------------------------------------------------

#[cfg(feature = "rayon")]
mod rayon_tests {
    use super::*;

    fn decompose_par(doc: &Automerge) -> Decomposition {
        let original_bytes = doc.save();
        let changes = doc.get_changes(&[]);
        let change_count = changes.len();

        let commit_store = IndexedSedimentreeAutomerge::from_changes(&changes);
        let heads: Vec<Digest<LooseCommit>> = doc
            .get_heads()
            .iter()
            .map(|h| Digest::force_from_bytes(h.0))
            .collect();
        let strategy = CountLeadingZeroBytes;
        let mut known_states: Map<Digest<LooseCommit>, FragmentState<OwnedParents>> = Map::new();

        let fresh = commit_store
            .build_fragment_store_par(&heads, &mut known_states, &strategy)
            .expect("parallel fragment building should succeed");

        let fragment_states: Vec<FragmentState<OwnedParents>> =
            fresh.into_iter().cloned().collect();

        let changes_by_hash: HashMap<ChangeHash, &automerge::Change> =
            changes.iter().map(|c| (c.hash(), c)).collect();

        let mut fragment_blobs = Vec::new();
        for state in &fragment_states {
            let mut blob_bytes = Vec::new();
            for member in state.members() {
                let hash = ChangeHash(*member.as_bytes());
                let change = changes_by_hash
                    .get(&hash)
                    .expect("member should be in changes_by_hash");
                blob_bytes.extend_from_slice(change.raw_bytes());
            }
            fragment_blobs.push((state.clone(), Blob::new(blob_bytes)));
        }

        let covered: Set<Digest<LooseCommit>> = known_states
            .values()
            .flat_map(|s| s.members().iter().copied())
            .collect();

        let uncovered_changes: Vec<&automerge::Change> = changes
            .iter()
            .filter(|c| {
                let d = Digest::force_from_bytes(c.hash().0);
                !covered.contains(&d)
            })
            .collect();

        let uncovered_blobs: Vec<(Digest<LooseCommit>, Blob)> = uncovered_changes
            .iter()
            .map(|c| {
                let d = Digest::force_from_bytes(c.hash().0);
                (d, Blob::new(c.raw_bytes().to_vec()))
            })
            .collect();

        let uncovered_parents: Vec<std::collections::BTreeSet<Digest<LooseCommit>>> =
            uncovered_changes
                .iter()
                .map(|c| {
                    c.deps()
                        .iter()
                        .map(|dep| Digest::force_from_bytes(dep.0))
                        .collect()
                })
                .collect();

        Decomposition {
            original_bytes,
            change_count,
            heads,
            fragment_blobs,
            uncovered_blobs,
            uncovered_parents,
            covered,
        }
    }

    #[test]
    fn par_decomposition_covers_all_changes() {
        for tv in TEST_VECTORS {
            let doc = load_doc(tv.bytes);
            let d = decompose_par(&doc);

            let total = d.covered.len() + d.uncovered_blobs.len();
            assert_eq!(
                total, d.change_count,
                "{} (par): covered + uncovered != total",
                tv.name,
            );
        }
    }

    #[test]
    fn par_reassembled_has_same_heads() {
        for tv in TEST_VECTORS {
            let doc = load_doc(tv.bytes);
            let d = decompose_par(&doc);
            let reassembled = reassemble(&d);

            assert_eq!(
                doc.get_heads(),
                reassembled.get_heads(),
                "{} (par): heads should match after reassembly",
                tv.name,
            );
        }
    }

    #[test]
    fn par_reassembled_has_identical_bytes() {
        for tv in TEST_VECTORS {
            let doc = load_doc(tv.bytes);
            let d = decompose_par(&doc);
            let reassembled = reassemble(&d);

            assert_eq!(
                d.original_bytes,
                reassembled.save(),
                "{} (par): saved bytes should be identical after reassembly",
                tv.name,
            );
        }
    }

    #[test]
    fn par_and_seq_produce_same_coverage() {
        for tv in TEST_VECTORS {
            let doc = load_doc(tv.bytes);
            let seq = decompose(&doc);
            let par = decompose_par(&doc);

            assert_eq!(
                seq.covered, par.covered,
                "{}: sequential and parallel should cover the same changes",
                tv.name,
            );
        }
    }

    #[test]
    fn par_and_seq_produce_same_sedimentree_hash() {
        let strategy = CountLeadingZeroBytes;
        for tv in TEST_VECTORS {
            let doc = load_doc(tv.bytes);
            let sed_id = sed_id_from_bytes(tv.bytes);

            let seq_decomp = decompose(&doc);
            let par_decomp = decompose_par(&doc);

            let (seq_tree, _, _) = build_tree(sed_id, &seq_decomp);
            let (par_tree, _, _) = build_tree(sed_id, &par_decomp);

            assert_eq!(
                seq_tree.minimal_hash(&strategy),
                par_tree.minimal_hash(&strategy),
                "{}: sequential and parallel should produce the same minimal hash",
                tv.name,
            );
        }
    }
}
