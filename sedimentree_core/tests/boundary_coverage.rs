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
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::id::CommitId,
    sedimentree::Sedimentree,
    test_utils::{TestGraph, seeded_rng},
};

const SEED: FingerprintSeed = FingerprintSeed::new(0xAAAA_BBBB_CCCC_DDDD, 0x1234_5678_9ABC_DEF0);

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
    let summary = requester.fingerprint_summarize(&SEED);
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
fn requester_holding_fragment_receives_history_below_boundary() {
    let graph = graph();
    let minimal = responder(&graph).minimize(graph.depth_metric());
    let requester = Sedimentree::new(vec![graph.make_fragment("h", &["a"], &[])], vec![]);
    assert_eq!(
        sent(&minimal, &requester),
        BTreeSet::from([graph.node_hash("a"), graph.node_hash("z")]),
        "F contains neither a nor z"
    );
}

#[test]
fn unminimized_diff_sends_boundary_missing_from_remote_fragment() {
    let graph = graph();
    let requester = Sedimentree::new(vec![graph.make_fragment("h", &["a"], &[])], vec![]);
    // Bypass minimization to isolate the fingerprint coverage failure.
    assert_eq!(
        sent(&responder(&graph), &requester),
        BTreeSet::from([graph.node_hash("a"), graph.node_hash("z")]),
        "F covers h and c, but neither its boundary a nor the older z"
    );
}

#[test]
fn without_fragment_boundary_history_is_kept() {
    // Control: the same loose commits with no fragment are all kept.
    let graph = graph();
    let tree = graph.to_sedimentree();
    assert_eq!(tree.minimize(graph.depth_metric()), tree);
}

#[test]
fn summaries_distinguish_boundaries_from_ownership() {
    let graph = graph();
    let tree = Sedimentree::new(vec![graph.make_fragment("h", &["a"], &[])], vec![]);
    let summary = tree.fingerprint_summarize(&SEED);
    let resolver = tree.fingerprint_resolver(&SEED);
    let head = Fingerprint::new(&SEED, &graph.node_hash("h"));
    let boundary = Fingerprint::new(&SEED, &graph.node_hash("a"));

    assert_eq!(&summary, resolver.summary());
    assert_eq!(summary.commit_fingerprints(), &BTreeSet::from([head]));
    assert_eq!(summary.fragment_fingerprints(), &BTreeSet::from([head]));
    assert_eq!(resolver.resolve_commit(&boundary), None);

    // Referencing a boundary does not advertise it as standalone data.
    let empty = Sedimentree::default();
    let diff = empty.diff_remote_fingerprints(&summary);
    assert_eq!(diff.remote_only_commit_fingerprints, vec![head]);
}

#[test]
fn missing_local_boundary_is_requested_from_remote() {
    let graph = graph();
    let local = Sedimentree::new(vec![graph.make_fragment("h", &["a"], &[])], vec![]);
    let remote = Sedimentree::new(vec![], graph.get_commits(&["a", "z"]));
    let resolver = remote.fingerprint_resolver(&SEED);
    let diff = local.diff_remote_fingerprints(resolver.summary());
    let requested: BTreeSet<_> = diff
        .remote_only_commit_fingerprints
        .iter()
        .filter_map(|fp| resolver.resolve_commit(fp))
        .collect();
    assert_eq!(
        requested,
        BTreeSet::from([graph.node_hash("a"), graph.node_hash("z")])
    );
}

#[test]
fn independently_held_loose_boundary_is_advertised_but_not_its_parents() {
    let graph = graph();
    let remote = Sedimentree::new(
        vec![graph.make_fragment("h", &["a"], &[])],
        graph.get_commits(&["a"]),
    );
    let resolver = remote.fingerprint_resolver(&SEED);
    let boundary = Fingerprint::new(&SEED, &graph.node_hash("a"));
    assert_eq!(resolver.summary(), &remote.fingerprint_summarize(&SEED));
    assert!(resolver.summary().commit_fingerprints().contains(&boundary));
    assert_eq!(
        resolver.resolve_commit(&boundary),
        Some(graph.node_hash("a"))
    );
    assert_eq!(
        sent(&responder(&graph), &remote),
        BTreeSet::from([graph.node_hash("z")])
    );
}

#[test]
fn boundary_that_is_another_fragment_head_starts_its_own_walk() {
    let graph = graph();
    let remote = Sedimentree::new(
        vec![
            graph.make_fragment("h", &["a"], &[]),
            graph.make_fragment("a", &[], &[]),
        ],
        vec![],
    );
    let boundary = Fingerprint::new(&SEED, &graph.node_hash("a"));
    let summary = remote.fingerprint_summarize(&SEED);
    assert!(summary.commit_fingerprints().contains(&boundary));
    // Both fragments are shared, so their boundaries are known locally.
    let local = graph.to_sedimentree_with_fragments(remote.fragments().cloned().collect());
    // The a-headed fragment independently covers both a and z.
    assert!(sent(&local, &remote).is_empty());
}

#[test]
fn unknown_remote_fragment_does_not_cover_ancestors_of_matching_loose_head() {
    let graph = graph();
    let remote = Sedimentree::new(vec![graph.make_fragment("h", &["a"], &[])], vec![]);
    assert_eq!(
        sent(&graph.to_sedimentree(), &remote),
        BTreeSet::from([
            graph.node_hash("z"),
            graph.node_hash("a"),
            graph.node_hash("c")
        ]),
        "without fragment metadata, send all unadvertised commits, even redundant c"
    );
}

#[test]
fn overlapping_shared_fragments_walk_their_own_ranges() {
    let graph = TestGraph::new(
        &mut seeded_rng(44),
        &[("z", 3), ("a", 2), ("c", 0), ("h", 2), ("g", 3)],
        &[("z", "a"), ("a", "c"), ("c", "h"), ("h", "g")],
    );
    let fragments = vec![
        graph.make_fragment("h", &["a"], &[]),
        graph.make_fragment("g", &["z"], &["a", "h"]),
    ];
    let local = graph.to_sedimentree_with_fragments(fragments.clone());
    let remote = Sedimentree::new(fragments, vec![]);
    assert_eq!(
        sent(&local, &remote),
        BTreeSet::from([graph.node_hash("z")]),
        "the g-headed walk must reach a even if h and c were already covered"
    );
}

#[test]
fn branching_walk_stops_at_every_excluded_boundary() {
    let graph = TestGraph::new(
        &mut seeded_rng(43),
        &[("z", 0), ("a", 1), ("b", 1), ("c", 0), ("h", 1)],
        &[("z", "a"), ("z", "b"), ("a", "c"), ("b", "c"), ("c", "h")],
    );
    let fragment = graph.make_fragment("h", &["a", "b"], &[]);
    let local = graph.to_sedimentree_with_fragments(vec![fragment.clone()]);
    let remote = Sedimentree::new(vec![fragment], vec![]);
    assert_eq!(
        sent(&local, &remote),
        BTreeSet::from([
            graph.node_hash("a"),
            graph.node_hash("b"),
            graph.node_hash("z")
        ]),
    );
}
