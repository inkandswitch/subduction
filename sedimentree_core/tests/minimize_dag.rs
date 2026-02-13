//! Integration tests for minimize() + simplify() interaction with complex DAGs.
//!
//! These tests verify that the minimize algorithm correctly handles
//! non-trivial commit graph structures including diamonds, multi-head
//! divergence, and nested fragment depths.

use std::collections::BTreeSet;

use sedimentree_core::{
    commit::CountLeadingZeroBytes,
    test_utils::{TestGraph, seeded_rng},
};

/// Diamond merge: A diverges to B and C, which merge at D.
///
/// ```text
///     A (depth 2)
///    / \
///   B   C  (depth 0)
///    \ /
///     D (depth 2)
/// ```
#[test]
fn diamond_merge_fragment_covers_interior() {
    let mut rng = seeded_rng(100);
    let graph = TestGraph::new(
        &mut rng,
        vec![("a", 2), ("b", 0), ("c", 0), ("d", 2)],
        vec![("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    );

    // Fragment from A to D, with B and C as checkpoints
    let fragment = graph.make_fragment("a", &["d"], &["b", "c"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment.clone()]);

    let minimized = tree.minimize(&CountLeadingZeroBytes);

    // Fragment should be kept
    assert_eq!(minimized.fragments().count(), 1);

    // Commits B and C should be pruned (covered by fragment)
    let b_hash = graph.node_hash("b");
    let c_hash = graph.node_hash("c");
    assert!(
        !minimized.has_loose_commit(b_hash),
        "commit B should be pruned (inside fragment range)"
    );
    assert!(
        !minimized.has_loose_commit(c_hash),
        "commit C should be pruned (inside fragment range)"
    );
}

/// Diamond with partial fragment coverage - only one branch covered.
///
/// ```text
///     A (depth 2)
///    / \
///   B   C  (depth 0)
///    \ /
///     D (depth 2)
/// ```
/// Fragment covers A to D but only includes B in checkpoints.
#[test]
fn diamond_partial_coverage() {
    let mut rng = seeded_rng(101);
    let graph = TestGraph::new(
        &mut rng,
        vec![("a", 2), ("b", 0), ("c", 0), ("d", 2)],
        vec![("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    );

    // Fragment from A to D, only B in checkpoints (C not included)
    let fragment = graph.make_fragment("a", &["d"], &["b"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment]);

    let minimized = tree.minimize(&CountLeadingZeroBytes);

    // Fragment should be kept
    assert_eq!(minimized.fragments().count(), 1);
}

/// Two independent branches with separate fragments.
///
/// ```text
///   A ─── B    C ─── D
/// ```
#[test]
fn independent_branches_separate_fragments() {
    let mut rng = seeded_rng(102);
    let graph = TestGraph::new(
        &mut rng,
        vec![("a", 2), ("b", 2), ("c", 2), ("d", 2)],
        vec![("a", "b"), ("c", "d")],
    );

    let fragment1 = graph.make_fragment("a", &["b"], &[]);
    let fragment2 = graph.make_fragment("c", &["d"], &[]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment1, fragment2]);

    let minimized = tree.minimize(&CountLeadingZeroBytes);

    // Both fragments should be kept (independent, neither supports the other)
    assert_eq!(minimized.fragments().count(), 2);
}

/// Deep linear chain covered by single fragment.
///
/// ```text
///   A ─── B ─── C ─── D ─── E
/// ```
#[test]
fn deep_linear_chain() {
    let mut rng = seeded_rng(103);
    let graph = TestGraph::new(
        &mut rng,
        vec![("a", 3), ("b", 0), ("c", 0), ("d", 0), ("e", 2)],
        vec![("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    // Fragment covers entire chain
    let fragment = graph.make_fragment("a", &["e"], &["b", "c", "d"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment]);

    let minimized = tree.minimize(&CountLeadingZeroBytes);

    // Single fragment kept
    assert_eq!(minimized.fragments().count(), 1);

    // All interior commits should be pruned
    for node in ["b", "c", "d"] {
        let hash = graph.node_hash(node);
        assert!(
            !minimized.has_loose_commit(hash),
            "commit {} should be pruned",
            node
        );
    }
}

/// Nested fragments at three depth levels.
///
/// Deep fragment (depth 3) contains medium (depth 2) contains shallow (depth 1).
#[test]
fn nested_fragments_three_levels() {
    // Create digests at specific depths
    let deep_head = sedimentree_core::test_utils::digest_with_depth(3, 1);
    let deep_boundary = sedimentree_core::test_utils::digest_with_depth(1, 100);
    let medium_head = sedimentree_core::test_utils::digest_with_depth(2, 2);
    let medium_boundary = sedimentree_core::test_utils::digest_with_depth(1, 101);
    let shallow_head = sedimentree_core::test_utils::digest_with_depth(1, 3);
    let shallow_boundary = sedimentree_core::test_utils::digest_with_depth(0, 102);

    // Deep fragment contains medium's head and boundary
    let deep_fragment = sedimentree_core::fragment::Fragment::new(
        deep_head,
        BTreeSet::from([deep_boundary]),
        &[medium_head, medium_boundary, shallow_head, shallow_boundary],
        sedimentree_core::blob::BlobMeta::new(&[1]),
    );

    // Medium fragment contains shallow's head and boundary
    let medium_fragment = sedimentree_core::fragment::Fragment::new(
        medium_head,
        BTreeSet::from([medium_boundary]),
        &[shallow_head, shallow_boundary],
        sedimentree_core::blob::BlobMeta::new(&[2]),
    );

    // Shallow fragment
    let shallow_fragment = sedimentree_core::fragment::Fragment::new(
        shallow_head,
        BTreeSet::from([shallow_boundary]),
        &[],
        sedimentree_core::blob::BlobMeta::new(&[3]),
    );

    let tree = sedimentree_core::sedimentree::Sedimentree::new(
        vec![
            deep_fragment.clone(),
            medium_fragment.clone(),
            shallow_fragment.clone(),
        ],
        vec![],
    );

    let minimized = tree.minimize(&CountLeadingZeroBytes);
    let fragments: Vec<_> = minimized.fragments().cloned().collect();

    // Only deepest fragment should remain
    assert_eq!(fragments.len(), 1, "only deepest fragment should remain");
    assert!(
        fragments.contains(&deep_fragment),
        "deep fragment should be kept"
    );
    assert!(
        !fragments.contains(&medium_fragment),
        "medium fragment should be pruned"
    );
    assert!(
        !fragments.contains(&shallow_fragment),
        "shallow fragment should be pruned"
    );
}

/// Overlapping fragments at same depth - neither dominates.
#[test]
fn overlapping_same_depth_both_kept() {
    // Two depth-2 fragments with different heads
    let head1 = sedimentree_core::test_utils::digest_with_depth(2, 1);
    let head2 = sedimentree_core::test_utils::digest_with_depth(2, 2);
    let boundary1 = sedimentree_core::test_utils::digest_with_depth(1, 100);
    let boundary2 = sedimentree_core::test_utils::digest_with_depth(1, 101);

    let fragment1 = sedimentree_core::fragment::Fragment::new(
        head1,
        BTreeSet::from([boundary1]),
        &[],
        sedimentree_core::blob::BlobMeta::new(&[1]),
    );

    let fragment2 = sedimentree_core::fragment::Fragment::new(
        head2,
        BTreeSet::from([boundary2]),
        &[],
        sedimentree_core::blob::BlobMeta::new(&[2]),
    );

    let tree = sedimentree_core::sedimentree::Sedimentree::new(
        vec![fragment1.clone(), fragment2.clone()],
        vec![],
    );

    let minimized = tree.minimize(&CountLeadingZeroBytes);
    let fragments: Vec<_> = minimized.fragments().cloned().collect();

    // Both should be kept (same depth, different ranges)
    assert_eq!(fragments.len(), 2);
    assert!(fragments.contains(&fragment1));
    assert!(fragments.contains(&fragment2));
}

/// Fragment boundary at merge point.
///
/// ```text
///     A (depth 2)
///    / \
///   B   C  (depth 0)
///    \ /
///     D (depth 2) <- fragment boundary
///     |
///     E (depth 0)
/// ```
#[test]
fn fragment_boundary_at_merge() {
    let mut rng = seeded_rng(105);
    let graph = TestGraph::new(
        &mut rng,
        vec![("a", 2), ("b", 0), ("c", 0), ("d", 2), ("e", 0)],
        vec![("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")],
    );

    // Fragment from A to D (boundary at merge point)
    let fragment = graph.make_fragment("a", &["d"], &["b", "c"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment]);

    let minimized = tree.minimize(&CountLeadingZeroBytes);

    // Fragment should be kept
    assert_eq!(minimized.fragments().count(), 1);

    // E should remain as loose commit (below fragment boundary)
    let e_hash = graph.node_hash("e");
    assert!(
        minimized.has_loose_commit(e_hash),
        "commit E should remain (below fragment boundary)"
    );
}

/// Multiple deep fragments collectively support a shallow one.
#[test]
fn collective_support_from_multiple_deep() {
    let shallow_head = sedimentree_core::test_utils::digest_with_depth(2, 1);
    let shallow_boundary = sedimentree_core::test_utils::digest_with_depth(1, 100);

    // Deep fragment 1 contains shallow's head
    let deep1_head = sedimentree_core::test_utils::digest_with_depth(3, 10);
    let deep1_boundary = sedimentree_core::test_utils::digest_with_depth(1, 101);
    let deep1 = sedimentree_core::fragment::Fragment::new(
        deep1_head,
        BTreeSet::from([deep1_boundary]),
        &[shallow_head],
        sedimentree_core::blob::BlobMeta::new(&[1]),
    );

    // Deep fragment 2 contains shallow's boundary
    let deep2_head = sedimentree_core::test_utils::digest_with_depth(3, 20);
    let deep2_boundary = sedimentree_core::test_utils::digest_with_depth(1, 102);
    let deep2 = sedimentree_core::fragment::Fragment::new(
        deep2_head,
        BTreeSet::from([deep2_boundary]),
        &[shallow_boundary],
        sedimentree_core::blob::BlobMeta::new(&[2]),
    );

    // Shallow fragment
    let shallow = sedimentree_core::fragment::Fragment::new(
        shallow_head,
        BTreeSet::from([shallow_boundary]),
        &[],
        sedimentree_core::blob::BlobMeta::new(&[3]),
    );

    let tree = sedimentree_core::sedimentree::Sedimentree::new(
        vec![deep1.clone(), deep2.clone(), shallow.clone()],
        vec![],
    );

    let minimized = tree.minimize(&CountLeadingZeroBytes);
    let fragments: Vec<_> = minimized.fragments().cloned().collect();

    // Shallow should be pruned (collectively supported by deep1 + deep2)
    assert_eq!(fragments.len(), 2);
    assert!(fragments.contains(&deep1));
    assert!(fragments.contains(&deep2));
    assert!(!fragments.contains(&shallow));
}
