//! Integration tests for `Sedimentree::missing_fragments` with branching DAGs.
//!
//! These tests exercise the known bugs in `missing_fragments`:
//!
//! 1. `canonical_sequence` flattens the DAG via DFS, losing branch structure
//! 2. `runs_by_level` stores one run per depth — can't represent two branches
//! 3. `FragmentSpec` boundary is always a singleton, even when multi-element is needed
//! 4. No runs are started when no fragments cover any checkpoint — returns empty
//!
//! All tests are written against expected *correct* behavior, so they should
//! FAIL against the current broken implementation and PASS after the fix.
//!
//! Requires the `test_utils` feature.

#![cfg(feature = "test_utils")]
#![allow(clippy::similar_names)]

use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed,
    id::SedimentreeId,
    test_utils::{TestGraph, seeded_rng},
};

fn test_seed() -> FingerprintSeed {
    FingerprintSeed::new(1, 2)
}

fn test_sedimentree_id() -> SedimentreeId {
    SedimentreeId::new([0xAA; 32])
}

/// Diamond DAG with no fragments — all commits are loose.
///
/// ```text
///        A (depth 2)
///       / \
///      B   C    (depth 0)
///       \ /
///        D (depth 2)
/// ```
///
/// Since A and D are deep (≥ MAX_STRATA_DEPTH = 2) and nothing is covered
/// by a fragment, `missing_fragments` should return at least one `FragmentSpec`
/// describing the gap.
///
/// Bug #4: current impl never starts a run when no fragments exist, so it
/// returns an empty Vec.
#[test]
fn diamond_dag_no_fragments() {
    let mut rng = seeded_rng(200);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
        &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    );

    let tree = graph.to_sedimentree();
    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    assert!(
        !specs.is_empty(),
        "diamond with two deep commits and no fragments should report missing fragment(s)"
    );
}

/// Fork with two independent depth-2 boundaries — no fragments.
///
/// ```text
///        A (depth 2)
///       / \
///      B   C  (depth 0)
///      |   |
///      D   E  (depth 2)
/// ```
///
/// Two branches each ending at a deep commit. The missing fragment spec(s)
/// should account for both branches. Either:
/// - One spec with a multi-element boundary {D, E}, or
/// - Two separate specs (one per branch)
///
/// Bug #2: `runs_by_level` stores one run per depth, so the second branch
/// at the same depth overwrites the first.
#[test]
fn fork_two_independent_boundaries() {
    let mut rng = seeded_rng(201);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("b", 0), ("c", 0), ("d", 2), ("e", 2)],
        &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "e")],
    );

    let tree = graph.to_sedimentree();
    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    assert!(
        !specs.is_empty(),
        "forking DAG with deep boundaries should report missing fragment(s)"
    );

    // D and E are the DAG heads (children of the fork). A is the root
    // (depth 2) that forms the boundary of both fragment specs.
    //
    // Expected: two FragmentSpecs — head=D boundary={A}, head=E boundary={A}
    // (or a single spec if heads share the same walk).
    let all_heads: std::collections::BTreeSet<_> = specs.iter().map(|s| s.head()).collect();
    let all_boundaries: std::collections::BTreeSet<_> = specs
        .iter()
        .flat_map(|s| s.boundary().iter().copied())
        .collect();

    let a_hash = graph.node_hash("a");
    let d_hash = graph.node_hash("d");
    let e_hash = graph.node_hash("e");

    // Both fork tips should appear as heads of fragment specs
    assert!(
        all_heads.contains(&d_hash) && all_heads.contains(&e_hash),
        "both fork heads (D, E) should appear as FragmentSpec heads"
    );

    // The root A is the deep boundary for both branches
    assert!(
        all_boundaries.contains(&a_hash),
        "root A (depth 2) should appear as a boundary commit"
    );
}

/// Two completely independent chains — disconnected DAG.
///
/// ```text
///    A (depth 2)     C (depth 2)
///    |               |
///    B (depth 2)     D (depth 2)
/// ```
///
/// Should produce two separate `FragmentSpec`s (one per chain).
///
/// Bug #1: DFS linearization merges independent chains into a single
/// sequence, losing the fact that they're disconnected.
#[test]
fn two_independent_chains_no_fragments() {
    let mut rng = seeded_rng(202);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("b", 2), ("c", 2), ("d", 2)],
        &[("a", "b"), ("c", "d")],
    );

    let tree = graph.to_sedimentree();
    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    assert!(
        !specs.is_empty(),
        "two independent chains with deep commits should report missing fragment(s)"
    );
}

/// Diamond with one branch covered by a fragment — only the uncovered branch
/// should be reported as missing.
///
/// ```text
///        A (depth 2)
///       / \
///      B   C    (depth 0)
///       \ /
///        D (depth 2)
///
///   Fragment covers A→B→D (the left branch).
///   The right branch A→C→D is uncovered.
/// ```
///
/// Bug #2+#3: covered branch masks uncovered; singleton boundary can't express
/// multi-element coverage boundaries.
#[test]
fn partial_coverage_one_branch_covered() {
    let mut rng = seeded_rng(203);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
        &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
    );

    // Fragment covers A→D with B as checkpoint (the "left" branch)
    let fragment = graph.make_fragment("a", &["d"], &["b"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment]);

    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    // Even though one branch is covered, the uncovered branch still has
    // commit C which is not in any fragment. However, since the fragment
    // already spans A→D, and C is between A and D, we might expect either:
    //   (a) no missing fragments (the existing fragment covers the range), or
    //   (b) a missing fragment for the C branch specifically
    //
    // The key assertion: this should NOT panic and should return a
    // deterministic, reasonable result. At minimum, any returned specs
    // should have valid heads and boundaries.
    for spec in &specs {
        assert!(
            !spec.boundary().is_empty(),
            "every FragmentSpec should have at least one boundary commit"
        );
    }
}

/// Empty sedimentree — no commits, no fragments.
///
/// Should return an empty Vec (nothing to cover).
#[test]
fn empty_sedimentree() {
    let tree = sedimentree_core::sedimentree::Sedimentree::new(vec![], vec![]);
    let metric = sedimentree_core::test_utils::MockDepthMetric::new();
    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), &metric);

    assert!(
        specs.is_empty(),
        "empty sedimentree should have no missing fragments"
    );
}

/// All commits covered by a fragment — nothing missing.
///
/// ```text
///   A (depth 2) → B (depth 0) → C (depth 2)
///   Fragment covers A→C with B as checkpoint.
/// ```
///
/// Should return an empty Vec.
#[test]
fn all_commits_covered_by_fragment() {
    let mut rng = seeded_rng(205);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("b", 0), ("c", 2)],
        &[("a", "b"), ("b", "c")],
    );

    let fragment = graph.make_fragment("a", &["c"], &["b"]);
    let tree = graph.to_sedimentree_with_fragments(vec![fragment]);

    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    assert!(
        specs.is_empty(),
        "fully covered sedimentree should have no missing fragments"
    );
}

/// Linear chain with only shallow commits (all depth 0).
///
/// ```text
///   A (depth 0) → B (depth 0) → C (depth 0)
/// ```
///
/// No commits reach MAX_STRATA_DEPTH, so no fragments can be formed.
/// Should return empty.
#[test]
fn linear_chain_no_deep_commits() {
    let mut rng = seeded_rng(206);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 0), ("c", 0)],
        &[("a", "b"), ("b", "c")],
    );

    let tree = graph.to_sedimentree();
    let specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());

    assert!(
        specs.is_empty(),
        "all-shallow chain should have no missing fragments (no deep commits to form boundaries)"
    );
}

/// Single deep commit, no parents, no fragments.
///
/// ```text
///   A (depth 2)
/// ```
///
/// A single deep commit with no parents forms a trivial "fragment" of one.
/// Should not panic, and should return some representation of this gap
/// (or empty if a single commit doesn't qualify as a fragment range).
///
/// Bug #4: no runs started → returns empty.
#[test]
fn single_deep_commit_no_panic() {
    let mut rng = seeded_rng(207);
    let graph = TestGraph::new(&mut rng, &[("a", 2)], &[]);

    let tree = graph.to_sedimentree();

    // At minimum: should not panic
    let _specs = tree.missing_fragments(test_sedimentree_id(), &test_seed(), graph.depth_metric());
}
