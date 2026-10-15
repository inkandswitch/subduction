//! # Shape
//!
//! ```text
//!   z    depth 0, root
//!   │
//!   a    depth 1   ── F's boundary (not a member of F)
//!   │
//!   c    depth 0   ┐
//!   │              ├ F: head h, boundary {a}, members {h, c}
//!   h    depth 1   ┘
//! ```
//!
//! The responder holds F plus all four commits loose. `a` and `z` are in
//! no fragment, so the minimized tree must keep them.

#![cfg(feature = "test_utils")]

use std::collections::BTreeSet;

use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed,
    loose_commit::id::CommitId,
    sedimentree::Sedimentree,
    test_utils::{TestGraph, seeded_rng},
};

fn graph() -> TestGraph {
    TestGraph::new(
        &mut seeded_rng(42),
        &[("z", 0), ("a", 1), ("c", 0), ("h", 1)],
        &[("z", "a"), ("a", "c"), ("c", "h")],
    )
}

fn responder(graph: &TestGraph) -> Sedimentree {
    graph.to_sedimentree_with_fragments(vec![graph.make_fragment("h", &["a"], &[])])
}

/// Diff directly, allowing callers to test with or without minimization.
fn sent(responder: &Sedimentree, requester: &Sedimentree) -> BTreeSet<CommitId> {
    let seed = FingerprintSeed::new(0xAAAA_BBBB_CCCC_DDDD, 0x1234_5678_9ABC_DEF0);
    let summary = requester.fingerprint_summarize(&seed);
    responder
        .diff_remote_fingerprints(&summary)
        .local_only_commits
        .iter()
        .map(|(id, _)| **id)
        .collect()
}

#[test]
fn minimize_keeps_loose_boundary_and_its_range() {
    let graph = graph();
    let minimal = responder(&graph).minimize(graph.depth_metric());
    assert!(
        minimal.has_loose_commit(graph.node_hash("a")),
        "boundary a is not a member of F"
    );
    assert!(
        minimal.has_loose_commit(graph.node_hash("z")),
        "z is below F's boundary"
    );
}

#[test]
fn minimize_drops_fragment_members() {
    // Control: the members of F are legitimately covered.
    let graph = graph();
    let minimal = responder(&graph).minimize(graph.depth_metric());
    assert!(!minimal.has_loose_commit(graph.node_hash("h")));
    assert!(!minimal.has_loose_commit(graph.node_hash("c")));
}

#[test]
fn empty_requester_receives_history_below_boundary() {
    let graph = graph();
    let minimal = responder(&graph).minimize(graph.depth_metric());
    assert_eq!(
        sent(&minimal, &Sedimentree::default()),
        BTreeSet::from([graph.node_hash("a"), graph.node_hash("z")]),
        "a and z are in no fragment"
    );
}

#[test]
fn without_fragment_boundary_history_is_kept() {
    // Control: the same loose commits with no fragment are all kept.
    let graph = graph();
    let tree = graph.to_sedimentree();
    assert_eq!(tree.minimize(graph.depth_metric()), tree);
}
