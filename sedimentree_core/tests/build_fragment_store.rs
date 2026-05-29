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
    loose_commit::id::CommitId,
    test_utils::{TestGraph, seeded_rng},
};

type Known = Map<CommitId, FragmentState<Set<CommitId>>>;

/// Helper: run `build_fragment_store` on a `TestGraph` starting from named heads.
fn run_fragment_store(
    graph: &TestGraph,
    head_names: &[&str],
) -> (Vec<FragmentState<Set<CommitId>>>, Known) {
    let heads: Vec<CommitId> = head_names.iter().map(|n| graph.node_hash(n)).collect();
    let mut known: Known = Map::new();
    let fresh = graph
        .build_fragment_store(&heads, &mut known, graph.depth_metric())
        .expect("build_fragment_store");
    let fresh_owned: Vec<_> = fresh.into_iter().cloned().collect();
    (fresh_owned, known)
}

/// Collect all member identifiers across all known fragments.
fn all_covered(known: &Known) -> Set<CommitId> {
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
    assert_eq!(frag.head_id(), graph.node_hash("b"));

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
/// Both b and d have depth 1. Since fragment(d) uses `>=`, b becomes a
/// boundary (sibling fragment head) rather than being absorbed.
#[test]
fn linear_two_depth_1_boundaries_are_siblings() {
    let mut rng = seeded_rng(3);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 1), ("c", 0), ("d", 1), ("e", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["e"]);
    assert_eq!(
        fresh.len(),
        2,
        "same-depth commits should produce sibling fragments"
    );

    // Fragment headed at d: contains d and c, with b as boundary
    let frag_d = fresh
        .iter()
        .find(|f| f.head_id() == graph.node_hash("d"))
        .expect("should have fragment headed at d");
    let d_members: Set<_> = frag_d.members().iter().copied().collect();
    assert!(d_members.contains(&graph.node_hash("d")));
    assert!(d_members.contains(&graph.node_hash("c")));
    assert!(
        !d_members.contains(&graph.node_hash("b")),
        "b is a boundary, not a member"
    );

    // Fragment headed at b: contains b and a
    let frag_b = fresh
        .iter()
        .find(|f| f.head_id() == graph.node_hash("b"))
        .expect("should have fragment headed at b");
    let b_members: Set<_> = frag_b.members().iter().copied().collect();
    assert!(b_members.contains(&graph.node_hash("b")));
    assert!(b_members.contains(&graph.node_hash("a")));
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
    assert_eq!(frag.head_id(), graph.node_hash("f"));

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

    let heads: Vec<CommitId> = vec![graph.node_hash("e")];
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

// -----------------------------------------------------------------------
// Same-depth sibling tests

/// Two same-depth commits in a linear chain should produce sibling
/// fragments rather than one large fragment that absorbs the other.
///
/// ```text
/// a(0) ← b(2) ← c(0) ← d(2) ← e(0)
/// ```
///
/// Expected: two fragments:
///   Frag(d): head=d, members={d, c}, boundary={b}
///   Frag(b): head=b, members={b, a}, boundary={}
///
/// This ensures that same-depth boundary commits are treated as fragment
/// boundaries (siblings) rather than absorbed as interior members, which
/// is required for `minimize` to later compact the interior loose commits.
#[test]
fn linear_same_depth_produces_sibling_fragments() {
    let mut rng = seeded_rng(40);
    let graph = TestGraph::new(
        &mut rng,
        &[("a", 0), ("b", 2), ("c", 0), ("d", 2), ("e", 0)],
        &[("a", "b"), ("b", "c"), ("c", "d"), ("d", "e")],
    );

    let (fresh, _known) = run_fragment_store(&graph, &["e"]);

    assert_eq!(
        fresh.len(),
        2,
        "same-depth commits should produce sibling fragments, not one merged fragment"
    );

    // Fragment headed at d should have b as a boundary
    let frag_d = fresh
        .iter()
        .find(|f| f.head_id() == graph.node_hash("d"))
        .expect("should have fragment headed at d");
    let d_members: Set<_> = frag_d.members().iter().copied().collect();
    assert!(d_members.contains(&graph.node_hash("d")));
    assert!(d_members.contains(&graph.node_hash("c")));
    assert!(
        !d_members.contains(&graph.node_hash("b")),
        "b should be a boundary of frag(d), not a member"
    );

    // Fragment headed at b
    let frag_b = fresh
        .iter()
        .find(|f| f.head_id() == graph.node_hash("b"))
        .expect("should have fragment headed at b");
    let b_members: Set<_> = frag_b.members().iter().copied().collect();
    assert!(b_members.contains(&graph.node_hash("b")));
    assert!(b_members.contains(&graph.node_hash("a")));
}

// -----------------------------------------------------------------------
// Transitive-ancestor dedup (regression for `build_fragment_store`)
// -----------------------------------------------------------------------

/// Walk a fragment's boundary chain transitively and collect the union of
/// all reachable ancestor fragments' members.
fn transitive_ancestor_members(
    start: &FragmentState<Set<CommitId>>,
    known: &Known,
) -> Set<CommitId> {
    let mut acc: Set<CommitId> = Set::new();
    let mut seen: Set<CommitId> = Set::new();
    let mut to_visit: Vec<CommitId> = start.boundary().keys().copied().collect();

    while let Some(b) = to_visit.pop() {
        if !seen.insert(b) {
            continue;
        }
        if let Some(anc) = known.get(&b) {
            acc.extend(anc.members().iter().copied());
            to_visit.extend(anc.boundary().keys().copied());
        }
    }

    acc
}

/// Regression: in a concurrent DAG, a fragment's members must not overlap
/// with any transitively-reachable ancestor fragment's members.
///
/// The fragment-construction BFS in `CommitStore::fragment` walks parents
/// until it hits boundary-depth commits. In a concurrent DAG it can reach
/// a commit that's _also_ a member of a deeper-ancestor fragment, via a
/// path that doesn't go through the deeper-ancestor fragment's head. The
/// inline strip inside `fragment()` only sees direct boundaries, so the
/// duplicate slips through unless `build_fragment_store` does a post-pass
/// over transitive ancestors.
///
/// Minimal trigger:
/// ```text
///                   c (depth 0)
///                  / \
///        f_deep (2)   other_path (depth 0)
///             \              \
///         f_mid (depth 1)     \
///                  \           \
///                   f_shallow (depth 1)
/// ```
///
/// `fragment(f_shallow)` walks `f_shallow → {f_mid, other_path}`, absorbs
/// `other_path` (depth 0) and then `c` (depth 0) as members. `c` is
/// _also_ a member of `f_deep` (reached as `f_deep → c`). `f_deep` is
/// not in `f_shallow.boundary` — only `f_mid` is — so the direct-boundary
/// strip cannot reach it. The post-pass over `f_shallow.boundary.f_mid
/// .boundary.f_deep` is what removes `c` from `f_shallow.members`.
#[test]
fn members_disjoint_from_transitive_ancestor_members() {
    let mut rng = seeded_rng(60);
    let graph = TestGraph::new(
        &mut rng,
        &[
            ("c", 0),
            ("f_deep", 2),
            ("other_path", 0),
            ("f_mid", 1),
            ("f_shallow", 1),
        ],
        &[
            ("c", "f_deep"),
            ("c", "other_path"),
            ("f_deep", "f_mid"),
            ("f_mid", "f_shallow"),
            ("other_path", "f_shallow"),
        ],
    );

    let (_fresh, known) = run_fragment_store(&graph, &["f_shallow"]);

    let mut violations: Vec<(CommitId, Vec<CommitId>)> = Vec::new();
    for (head, state) in &known {
        let ancestor_members = transitive_ancestor_members(state, &known);
        let overlap: Vec<CommitId> = state
            .members()
            .iter()
            .filter(|m| ancestor_members.contains(m))
            .copied()
            .collect();
        if !overlap.is_empty() {
            violations.push((*head, overlap));
        }
    }

    assert!(
        violations.is_empty(),
        "fragment members overlap with transitively-reachable ancestor fragments: {violations:?}"
    );
}

/// Generalisation of the regression test above: assert the invariant
/// across all fragments produced for an arbitrary mixed-depth DAG with
/// multiple concurrent paths. Catches regressions whose specific bug
/// shape doesn't match the minimal trigger above.
#[test]
fn members_disjoint_invariant_on_complex_concurrent_dag() {
    let mut rng = seeded_rng(61);

    // Two parallel "lanes" descending from a common deep root, joined
    // again at a shallow head. Several concurrent-edit shapes per lane.
    //
    // ```text
    //              root (depth 0)
    //               /         \
    //          d_left (2)   d_right (2)
    //              |             |
    //          m_left (0)    m_right (0)
    //              |             |
    //          mid_left (1)  mid_right (1)
    //               \           /
    //                top (depth 1)
    //                    |
    //                  head (depth 0)
    // ```
    let graph = TestGraph::new(
        &mut rng,
        &[
            ("root", 0),
            ("d_left", 2),
            ("d_right", 2),
            ("m_left", 0),
            ("m_right", 0),
            ("mid_left", 1),
            ("mid_right", 1),
            ("top", 1),
            ("head", 0),
        ],
        &[
            ("root", "d_left"),
            ("root", "d_right"),
            ("d_left", "m_left"),
            ("d_right", "m_right"),
            ("m_left", "mid_left"),
            ("m_right", "mid_right"),
            ("mid_left", "top"),
            ("mid_right", "top"),
            ("top", "head"),
        ],
    );

    let (_fresh, known) = run_fragment_store(&graph, &["head"]);

    for (head, state) in &known {
        let ancestor_members = transitive_ancestor_members(state, &known);
        let overlap: Vec<CommitId> = state
            .members()
            .iter()
            .filter(|m| ancestor_members.contains(m))
            .copied()
            .collect();
        assert!(
            overlap.is_empty(),
            "fragment {head:?} members overlap with transitive ancestors: {overlap:?}",
        );
    }
}
