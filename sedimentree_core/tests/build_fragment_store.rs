//! Tests for `build_fragment_store` using deterministic DAGs with controlled
//! depth assignments via `MockDepthMetric`.
//!
//! These tests verify the core invariants of the fragment decomposition:
//! - Depth-0 commits are never fragment heads (they're loose commits)
//! - Same-depth boundaries are absorbed into the current fragment
//! - Strictly deeper commits form boundaries
//! - All non-depth-0 commits in the DAG are covered by exactly one fragment
//! - Checkpoints are interior depth boundaries (0 < depth < `head_depth`)

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use sedimentree_core::{
    collections::{Map, Set},
    commit::{CommitStore, FragmentState},
    crypto::digest::Digest,
    loose_commit::LooseCommit,
    test_utils::{TestGraph, seeded_rng},
};

type Known = Map<Digest<LooseCommit>, FragmentState<Set<Digest<LooseCommit>>>>;

/// Helper: run `build_fragment_store` on a `TestGraph` starting from named heads.
fn run_fragment_store(
    graph: &TestGraph,
    head_names: &[&str],
) -> (Vec<FragmentState<Set<Digest<LooseCommit>>>>, Known) {
    let heads: Vec<Digest<LooseCommit>> = head_names.iter().map(|n| graph.node_hash(n)).collect();
    let mut known: Known = Map::new();
    let fresh = graph
        .build_fragment_store(&heads, &mut known, graph.depth_metric())
        .expect("build_fragment_store");
    let fresh_owned: Vec<_> = fresh.into_iter().cloned().collect();
    (fresh_owned, known)
}

/// Collect all member digests across all known fragments.
fn all_covered(known: &Known) -> Set<Digest<LooseCommit>> {
    known
        .values()
        .flat_map(|s| s.members().iter().copied())
        .collect()
}

// -----------------------------------------------------------------------
// Linear chain tests
// -----------------------------------------------------------------------

/// Linear chain, all depth 0: no fragments produced, everything is loose.
///
/// ```text
/// a(0) ← b(0) ← c(0) ← d(0)
/// ```
#[test]
fn linear_all_depth_0_produces_no_fragments() {
    let mut rng = seeded_rng(1);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 0), ("c", 0), ("d", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d")],
    );

    let (fresh, known) = run_fragment_store(&graph, &["d"]);
    assert!(fresh.is_empty(), "all depth-0 should produce no fragments");
    assert!(known.is_empty());
}

/// Linear chain with one depth-1 boundary in the middle.
///
/// ```text
/// a(0) ← b(1) ← c(0) ← d(0)
/// ```
///
/// Fragment headed at b should contain {b, a}. c and d are loose.
#[test]
fn linear_single_depth_1_boundary() {
    let mut rng = seeded_rng(2);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 1), ("c", 0), ("d", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d")],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["d"]);
    assert_eq!(fresh.len(), 1, "should produce exactly 1 fragment");

    let frag = &fresh[0];
    assert_eq!(frag.head_digest(), graph.node_hash("b"));

    let members: Set<_> = frag.members().iter().copied().collect();
    assert!(
        members.contains(&graph.node_hash("b")),
        "head should be member"
    );
    assert!(
        members.contains(&graph.node_hash("a")),
        "a should be member"
    );
    assert!(
        !members.contains(&graph.node_hash("c")),
        "c is depth-0 above b"
    );
    assert!(
        !members.contains(&graph.node_hash("d")),
        "d is depth-0 above b"
    );
    assert!(frag.boundary().is_empty(), "no deeper boundary below a");
}

/// Linear chain with two depth-1 boundaries.
///
/// ```text
/// a(0) ← b(1) ← c(0) ← d(1) ← e(0)
/// ```
///
/// Both b and d have depth 1. Since fragment(d) uses `>` not `>=`,
/// it should absorb b (same depth) and produce ONE fragment {d, c, b, a}.
#[test]
fn linear_two_depth_1_boundaries_absorbed() {
    let mut rng = seeded_rng(3);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 1), ("c", 0), ("d", 1), ("e", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["e"]);
    assert_eq!(
        fresh.len(),
        1,
        "same-depth boundaries should be absorbed into one fragment"
    );

    let frag = &fresh[0];
    assert_eq!(frag.head_digest(), graph.node_hash("d"));

    let members: Set<_> = frag.members().iter().copied().collect();
    assert!(members.contains(&graph.node_hash("a")));
    assert!(members.contains(&graph.node_hash("b")));
    assert!(members.contains(&graph.node_hash("c")));
    assert!(members.contains(&graph.node_hash("d")));
    assert!(
        !members.contains(&graph.node_hash("e")),
        "e is loose (depth 0, above d)"
    );

    // b is a checkpoint: depth 1, which equals head_depth (1), so NOT a checkpoint
    // (checkpoints are 0 < depth < head_depth)
    let checkpoints: Set<_> = frag.checkpoints().iter().copied().collect();
    assert!(
        !checkpoints.contains(&graph.node_hash("b")),
        "same-depth member should not be a checkpoint"
    );
}

/// Linear chain with depth escalation: depth 1 then depth 2.
///
/// ```text
/// a(0) ← b(1) ← c(0) ← d(1) ← e(0) ← f(2) ← g(0)
/// ```
///
/// Fragment headed at f (depth 2): should absorb d, c, b, a (all depth <= 2).
/// b and d are checkpoints (depth 1, which is 0 < 1 < 2).
/// g is loose.
#[test]
fn linear_depth_escalation() {
    let mut rng = seeded_rng(4);
    let graph = TestGraph::new(
        &mut rng,
        &[
            ("a", 0),
            ("b", 1),
            ("c", 0),
            ("d", 1),
            ("e", 0),
            ("f", 2),
            ("g", 0),
        ],
        &[
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("d", "e"),
            ("e", "f"),
            ("f", "g"),
        ],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["g"]);
    assert_eq!(
        fresh.len(),
        1,
        "depth-2 head should absorb all depth-1 and depth-0 below"
    );

    let frag = &fresh[0];
    assert_eq!(frag.head_digest(), graph.node_hash("f"));

    let members: Set<_> = frag.members().iter().copied().collect();
    for name in &["a", "b", "c", "d", "e", "f"] {
        assert!(
            members.contains(&graph.node_hash(name)),
            "{name} should be member"
        );
    }
    assert!(!members.contains(&graph.node_hash("g")), "g is loose");

    let checkpoints: Set<_> = frag.checkpoints().iter().copied().collect();
    assert!(
        checkpoints.contains(&graph.node_hash("b")),
        "b (depth 1) is checkpoint in depth-2 fragment"
    );
    assert!(
        checkpoints.contains(&graph.node_hash("d")),
        "d (depth 1) is checkpoint in depth-2 fragment"
    );
    assert!(
        !checkpoints.contains(&graph.node_hash("a")),
        "a (depth 0) is not a checkpoint"
    );
}

/// Linear chain where depth-2 boundary separates two fragments.
///
/// ```text
/// a(0) ← b(2) ← c(0) ← d(1) ← e(0)
/// ```
///
/// Fragment at d (depth 1): members {d, c}, boundary {b} (depth 2 > 1).
/// Fragment at b (depth 2): members {b, a}, no boundary.
#[test]
fn linear_deeper_boundary_separates_fragments() {
    let mut rng = seeded_rng(5);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 2), ("c", 0), ("d", 1), ("e", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    let (fresh, known) = run_fragment_store(&graph, &["e"]);
    assert_eq!(
        fresh.len(),
        2,
        "should produce 2 fragments (depth-1 and depth-2)"
    );

    // Find fragments by head
    let frag_d = known.get(&graph.node_hash("d")).expect("fragment at d");
    let frag_b = known.get(&graph.node_hash("b")).expect("fragment at b");

    // Fragment at d: members {d, c}, boundary {b}
    let d_members: Set<_> = frag_d.members().iter().copied().collect();
    assert!(d_members.contains(&graph.node_hash("d")));
    assert!(d_members.contains(&graph.node_hash("c")));
    assert!(
        !d_members.contains(&graph.node_hash("b")),
        "b is boundary, not member"
    );
    assert!(frag_d.boundary().contains_key(&graph.node_hash("b")));

    // Fragment at b: members {b, a}, no boundary
    let b_members: Set<_> = frag_b.members().iter().copied().collect();
    assert!(b_members.contains(&graph.node_hash("b")));
    assert!(b_members.contains(&graph.node_hash("a")));
    assert!(frag_b.boundary().is_empty());

    // e is loose (not covered)
    let covered = all_covered(&known);
    assert!(!covered.contains(&graph.node_hash("e")));
}

// -----------------------------------------------------------------------
// DAG (branching) tests
// -----------------------------------------------------------------------

/// Diamond DAG with depth-1 head at merge point.
///
/// ```text
///     b(0)
///    /    \
/// a(0)    d(1) ← e(0)
///    \    /
///     c(0)
/// ```
///
/// Fragment at d: members {d, b, c, a}, no boundary.
/// e is loose.
#[test]
fn diamond_merge_at_depth_1() {
    let mut rng = seeded_rng(10);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 0), ("c", 0), ("d", 1), ("e", 0)],
        &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["e"]);
    assert_eq!(fresh.len(), 1);

    let frag = &fresh[0];
    let members: Set<_> = frag.members().iter().copied().collect();
    assert!(members.contains(&graph.node_hash("a")));
    assert!(members.contains(&graph.node_hash("b")));
    assert!(members.contains(&graph.node_hash("c")));
    assert!(members.contains(&graph.node_hash("d")));
    assert!(!members.contains(&graph.node_hash("e")));
}

/// Diamond with deeper boundary below merge.
///
/// ```text
///          b(0)
///         /    \
/// a(2) ← x(0)  d(1) ← e(0)
///         \    /
///          c(0)
/// ```
///
/// Fragment at d (depth 1): walks down through b/c/x, hits a (depth 2 > 1) → boundary.
/// Members: {d, b, c, x}. Boundary: {a}.
/// Fragment at a (depth 2): members {a}.
#[test]
fn diamond_with_deeper_boundary_below() {
    let mut rng = seeded_rng(11);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 2), ("x", 0), ("b", 0), ("c", 0), ("d", 1), ("e", 0)],
        &[
            ("a", "x"),
            ("x", "b"),
            ("x", "c"),
            ("b", "d"),
            ("c", "d"),
            ("d", "e"),
        ],
    );

    let (fresh, known) = run_fragment_store(&graph, &["e"]);
    assert_eq!(fresh.len(), 2);

    let frag_d = known.get(&graph.node_hash("d")).expect("fragment at d");
    let d_members: Set<_> = frag_d.members().iter().copied().collect();
    assert!(d_members.contains(&graph.node_hash("d")));
    assert!(d_members.contains(&graph.node_hash("b")));
    assert!(d_members.contains(&graph.node_hash("c")));
    assert!(d_members.contains(&graph.node_hash("x")));
    assert!(!d_members.contains(&graph.node_hash("a")));
    assert!(frag_d.boundary().contains_key(&graph.node_hash("a")));

    let frag_a = known.get(&graph.node_hash("a")).expect("fragment at a");
    let a_members: Set<_> = frag_a.members().iter().copied().collect();
    assert!(a_members.contains(&graph.node_hash("a")));
    assert!(frag_a.boundary().is_empty());
}

/// Two independent branches from the same root, each with a depth-1 head.
///
/// ```text
/// a(0) ← b(1)
/// a(0) ← c(1)
/// ```
///
/// Two fragment heads (b and c), both walk to a (depth 0).
/// Both absorb a as a member. (a appears in both fragments' member sets.)
#[test]
fn two_branches_same_root() {
    let mut rng = seeded_rng(12);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 1), ("c", 1)],
        &[("a", "b"), ("a", "c")],
    );

    let (fresh, known) = run_fragment_store(&graph, &["b", "c"]);
    assert_eq!(fresh.len(), 2, "two independent depth-1 heads");

    let frag_b = known.get(&graph.node_hash("b")).expect("fragment at b");
    let frag_c = known.get(&graph.node_hash("c")).expect("fragment at c");

    assert!(frag_b.members().contains(&graph.node_hash("a")));
    assert!(frag_c.members().contains(&graph.node_hash("a")));
}

// -----------------------------------------------------------------------
// Coverage invariant tests
// -----------------------------------------------------------------------

/// Every non-depth-0 commit in the DAG should be covered by exactly one
/// fresh fragment (when starting from all heads).
#[test]
fn coverage_includes_all_nonzero_depth_commits() {
    let mut rng = seeded_rng(20);
    // Complex DAG with mixed depths
    let graph = TestGraph::new(
        &mut rng,
        &[
            ("a", 0),
            ("b", 1),
            ("c", 0),
            ("d", 2),
            ("e", 1),
            ("f", 0),
            ("g", 0),
            ("h", 1),
        ],
        &[
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("d", "e"),
            ("e", "f"),
            ("d", "g"),
            ("g", "h"),
        ],
    );

    let (_fresh, known) = run_fragment_store(&graph, &["f", "h"]);
    let covered = all_covered(&known);

    // All depth > 0 commits should be covered
    for name in &["b", "d", "e", "h"] {
        assert!(
            covered.contains(&graph.node_hash(name)),
            "{name} (depth > 0) should be covered by a fragment"
        );
    }
}

/// `known_fragment_states` accumulates across multiple calls.
/// A second call with the same heads should produce no new fragments.
#[test]
fn second_call_produces_no_new_fragments() {
    let mut rng = seeded_rng(30);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 1), ("c", 0), ("d", 2), ("e", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    let heads: Vec<Digest<LooseCommit>> = vec![graph.node_hash("e")];
    let mut known: Known = Map::new();

    let first = graph
        .build_fragment_store(&heads, &mut known, graph.depth_metric())
        .expect("first call");
    assert!(!first.is_empty());

    let second = graph
        .build_fragment_store(&heads, &mut known, graph.depth_metric())
        .expect("second call");
    assert!(
        second.is_empty(),
        "second call should find everything already known"
    );
}
