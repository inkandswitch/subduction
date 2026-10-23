//! Sync-efficiency budget for the sedimentree layer: minimization and the
//! fingerprint diff that decides what goes on the wire.
//!
//! Efficient sync is the point of the sedimentree design: once a run of
//! commits is compacted into a fragment, peers should exchange the fragment
//! and *not* the commits inside it, and an in-sync peer's summary should
//! cost `O(fragments + live tail)`, not `O(history)`.
//!
//! Every test measures structural, hash-independent quantities (loose
//! commits surviving `minimize`, items in a diff, fingerprints in a
//! summary) over the deterministic histories in
//! `sedimentree_core::test_utils::sync_budget`. See that module for the
//! ratchet / known-bug conventions.
//!
//! # Known bug C2: coverage pruning is broken for automerge-repo's layout
//!
//! automerge-repo stores a fragment's head commit only as a `Fragment`,
//! never as a `LooseCommit`. Two separate code paths find a fragment's
//! members by walking the loose-commit DAG *from the fragment head*:
//!
//! * `CommitDag::simplify`, used by `minimize`, and
//! * the "requester already holds this fragment" pruning in
//!   `diff_remote_fingerprints`, which only matters when the responder's
//!   tree is *not* already minimal.
//!
//! With no loose head neither walk starts, so nothing is ever covered:
//! trees never minimize, a cold fetch ships every change both loose *and*
//! inside its fragment, and a peer that already holds the fragment is
//! re-sent all of its members. Each path has its own target below, so a
//! fix to one cannot hide behind the other.
//!
//! The same histories in [`Layout::Full`] (head also stored loose) take the
//! working path, and those tests guard it, over both a linear history and
//! a branchy one (merges inside fragments, a depth-2 fragment with
//! checkpoints, and the depth-1 fragments it supports).
//!
//! # Known bug ORDER: minimizing as items arrive depends on their order
//!
//! A node minimizes after every batch it takes in. A commit that first
//! arrives after a child of it was already pruned as covered has no loose
//! path left to its fragment's head, so it stays as a stray loose commit
//! until the tree is rebuilt from storage. Causal arrival (commits before
//! the fragment covering them) is guarded; arbitrary arrival is the target.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stderr
)]

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::SmallRng, seq::SliceRandom};
use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed,
    depth::{CountLeadingZeroBytes, Depth},
    fragment::Fragment,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{
        CommitOrFragment, FingerprintDiff, FingerprintSummary, Sedimentree,
        minimized::MinimizedSedimentree,
    },
    test_utils::sync_budget::{self, History, Layout, known_bug},
};

const SEED: FingerprintSeed = FingerprintSeed::new(0x5EED_0000_0000_0001, 0x5EED_0000_0000_0002);

/// Ratchet `metric` against this crate's `tests/sync_budget_baselines.txt`.
#[track_caller]
fn ratchet(metric: &str, current: u64) {
    sync_budget::ratchet(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/sync_budget_baselines.txt"
        ),
        metric,
        current,
    );
}

/// History sizes. `N` gives 3 fragments + a 232-commit tail; `10 * N`
/// gives 39 fragments + a 16-commit tail.
const N: usize = 1_000;

/// Branchy history size: depth-2 fragments at commits 1023 and 2047 (each
/// supporting the three depth-1 fragments before it, recorded as
/// checkpoints), six depth-1 fragments, a merge every 16 commits, and a
/// 52-commit tail.
const BRANCHY_N: usize = 2_100;

/// The histories the full-layout (working path) guards run over.
fn guarded_histories() -> [History; 3] {
    [
        History::linear(N),
        History::linear(10 * N),
        History::branchy(BRANCHY_N),
    ]
}

fn minimized(history: &History, layout: Layout) -> Sedimentree {
    history.tree(layout).minimize(&CountLeadingZeroBytes)
}

fn loose_ids(tree: &Sedimentree) -> BTreeSet<CommitId> {
    tree.loose_commits().map(LooseCommit::head).collect()
}

/// Items in a summary: the number of 8-byte fingerprints a
/// `BatchSyncRequest` carries for this tree.
fn summary_items(summary: &FingerprintSummary) -> usize {
    summary.commit_fingerprints().len() + summary.fragment_fingerprints().len()
}

/// Loose commits in `diff` whose payload a fragment in the *same* diff
/// already carries: bytes shipped twice.
fn redundant_in_diff(history: &History, diff: &FingerprintDiff<'_>) -> BTreeSet<CommitId> {
    history.redundant(
        diff.local_only_commits.iter().map(|(id, _)| *id),
        diff.local_only_fragments.iter().map(|(id, _)| *id),
    )
}

fn label(history: &History) -> String {
    format!("{:?} n={}", history.shape, history.len())
}

/// The branchy fixture really has what the guards rely on: merges, two
/// depth-2 fragments each recording its three depth-1 checkpoints, and
/// depth-1 fragments those support (so `minimize` has fragments to drop).
#[test]
fn branchy_fixture_has_merges_and_deep_fragments() {
    let history = History::branchy(BRANCHY_N);
    let merges = history
        .commits
        .iter()
        .filter(|(c, _)| c.parents().len() > 1)
        .count();
    assert_eq!(merges, BRANCHY_N / sync_budget::MERGE_PERIOD);
    let deep: Vec<_> = history
        .fragments
        .iter()
        .filter(|(f, _)| f.depth(&CountLeadingZeroBytes) > Depth(1))
        .collect();
    assert_eq!(deep.len(), 2);
    for (f, _) in deep {
        assert_eq!(f.checkpoints().len(), 3, "{f:?}");
    }
    assert_eq!(history.fragments.len(), 8);
    assert_eq!(history.top_fragments().len(), 2);
    assert_eq!(history.tail().len(), 52);
}

// ════════════════════════════════════════════════════════════════════════
// minimize: covered loose commits must be pruned
// ════════════════════════════════════════════════════════════════════════

/// Ideal: after `minimize`, the only loose commits left are the live tail
/// (commits after the last fragment head); every fragment member is pruned,
/// and every fragment a deeper one supports is dropped. Full layout takes
/// the working path today; this guards it.
#[test]
fn full_layout_minimize_keeps_only_tail() {
    for history in guarded_histories() {
        let tree = minimized(&history, Layout::Full);
        assert_eq!(
            loose_ids(&tree),
            history.tail(),
            "{}: minimize must prune every fragment member and keep exactly the tail",
            label(&history)
        );
        let kept_fragments: BTreeSet<CommitId> = tree.fragments().map(Fragment::head).collect();
        assert_eq!(
            kept_fragments,
            history.top_fragments(),
            "{}: minimize must keep exactly the fragments no deeper one supports",
            label(&history)
        );
    }
}

/// Ideal (same as above, for the layout automerge-repo actually stores):
/// minimize keeps only the tail. Today it keeps every loose commit, because
/// no fragment head is a loose commit so `CommitDag::simplify` never
/// assigns any commit to a fragment range.
#[test]
#[should_panic(expected = "KNOWN BUG C2:")]
fn am_layout_minimize_keeps_only_tail() {
    let history = History::linear(N);
    let kept = loose_ids(&minimized(&history, Layout::AutomergeRepo));
    let covered_kept = kept.intersection(&history.covered()).count();
    known_bug(
        "C2",
        kept == history.tail(),
        &format!(
            "minimize kept {} loose commits, {covered_kept} of them inside a fragment \
             (ideal: the {} tail commits only)",
            kept.len(),
            history.tail().len()
        ),
    );
}

/// Ratchet: fragment members that survive minimize in automerge-repo's
/// layout (ideal 0; today every one of them).
#[test]
fn am_layout_minimize_covered_survivors_ratchet() {
    for n in [N, 10 * N] {
        let history = History::linear(n);
        let kept = loose_ids(&minimized(&history, Layout::AutomergeRepo));
        assert!(
            kept.is_superset(&history.tail()),
            "n={n}: minimize dropped tail commits that no fragment carries"
        );
        let covered_kept = kept.intersection(&history.covered()).count();
        ratchet(
            &format!("sedimentree.am_layout.minimize.covered_loose_survivors.n{n}"),
            covered_kept as u64,
        );
    }
}

// ════════════════════════════════════════════════════════════════════════
// diff for an empty requester (cold fetch)
// ════════════════════════════════════════════════════════════════════════

/// Ideal: a cold fetch from a (minimized) responder sends each top-level
/// fragment and the tail, and no loose commit whose payload a fragment in
/// the same response already carries. Working path (full layout); guard.
#[test]
fn full_layout_cold_diff_sends_no_covered_commits() {
    for history in guarded_histories() {
        let responder = minimized(&history, Layout::Full);
        let empty = Sedimentree::default().fingerprint_summarize(&SEED);
        let diff = responder.diff_remote_fingerprints(&empty);

        let sent_fragments: BTreeSet<CommitId> = diff
            .local_only_fragments
            .iter()
            .map(|(id, _)| **id)
            .collect();
        assert_eq!(
            sent_fragments,
            history.top_fragments(),
            "{}: cold fetch sends exactly the top-level fragments",
            label(&history)
        );
        assert!(
            redundant_in_diff(&history, &diff).is_empty(),
            "{}: cold fetch must not ship a commit both loose and inside a fragment",
            label(&history)
        );
        let sent: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
        assert_eq!(
            sent,
            history.tail(),
            "{}: cold fetch sends exactly the tail loose",
            label(&history)
        );
    }
}

/// Ideal: as above, for automerge-repo's layout. Today every fragment
/// member is shipped twice (C2: the responder's tree never minimizes).
#[test]
#[should_panic(expected = "KNOWN BUG C2:")]
fn am_layout_cold_diff_sends_no_covered_commits() {
    let history = History::linear(N);
    let responder = minimized(&history, Layout::AutomergeRepo);
    let empty = Sedimentree::default().fingerprint_summarize(&SEED);
    let diff = responder.diff_remote_fingerprints(&empty);
    let redundant = redundant_in_diff(&history, &diff);
    known_bug(
        "C2",
        redundant.is_empty(),
        &format!(
            "cold diff ships {} loose commits that a fragment in the same response already \
             carries ({} loose + {} fragments total)",
            redundant.len(),
            diff.local_only_commits.len(),
            diff.local_only_fragments.len()
        ),
    );
}

/// Ratchet: redundant loose commits and total items in a cold diff
/// (automerge-repo layout).
#[test]
fn am_layout_cold_diff_ratchet() {
    for n in [N, 10 * N] {
        let history = History::linear(n);
        let responder = minimized(&history, Layout::AutomergeRepo);
        let empty = Sedimentree::default().fingerprint_summarize(&SEED);
        let diff = responder.diff_remote_fingerprints(&empty);

        // Nothing lost: every fragment and every tail commit is still sent.
        let sent: BTreeSet<CommitId> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
        assert!(sent.is_superset(&history.tail()), "n={n}: tail not sent");
        assert_eq!(
            diff.local_only_fragments.len(),
            history.fragments.len(),
            "n={n}: fragments not sent"
        );

        ratchet(
            &format!("sedimentree.am_layout.cold_diff.redundant_loose.n{n}"),
            redundant_in_diff(&history, &diff).len() as u64,
        );
        ratchet(
            &format!("sedimentree.am_layout.cold_diff.items.n{n}"),
            (diff.local_only_commits.len() + diff.local_only_fragments.len()) as u64,
        );
    }
}

// ════════════════════════════════════════════════════════════════════════
// diff for a requester that holds the fragments but not their members
// (e.g. a client that compacted its absorbed loose commits, C4)
// ════════════════════════════════════════════════════════════════════════

/// The requester holds every fragment plus the tail, and none of the
/// covered loose commits.
fn compacted_requester(history: &History) -> FingerprintSummary {
    let tail = history.tail();
    Sedimentree::new(
        history.fragments.iter().map(|(f, _)| f.clone()).collect(),
        history
            .commits
            .iter()
            .filter(|(c, _)| tail.contains(&c.head()))
            .map(|(c, _)| c.clone())
            .collect(),
    )
    .fingerprint_summarize(&SEED)
}

/// Ideal: a requester that already holds the fragments is sent nothing at
/// all. Guarded for the full layout, both against a minimized responder and
/// against an unminimized one (which exercises the diff's own
/// "requester holds this fragment" walk rather than minimize).
#[test]
fn full_layout_fragment_holder_receives_nothing() {
    for history in guarded_histories() {
        let requester = compacted_requester(&history);
        for (which, responder) in [
            ("minimized", minimized(&history, Layout::Full)),
            ("unminimized", history.tree(Layout::Full)),
        ] {
            let diff = responder.diff_remote_fingerprints(&requester);
            assert!(
                diff.local_only_commits.is_empty() && diff.local_only_fragments.is_empty(),
                "{}: {which} responder re-sent {} commits / {} fragments to a peer that \
                 holds them",
                label(&history),
                diff.local_only_commits.len(),
                diff.local_only_fragments.len()
            );
        }
    }
}

/// Ideal: as above, for automerge-repo's layout and a *minimized* responder
/// (what every Subduction node diffs from). Today the responder re-sends
/// every fragment member, because minimize kept them all (C2; C4 is the
/// re-download a compacted client pays on every reload).
#[test]
#[should_panic(expected = "KNOWN BUG C2:")]
fn am_layout_fragment_holder_receives_nothing() {
    let history = History::linear(N);
    let requester = compacted_requester(&history);
    let responder = minimized(&history, Layout::AutomergeRepo);
    let diff = responder.diff_remote_fingerprints(&requester);
    known_bug(
        "C2",
        diff.local_only_commits.is_empty(),
        &format!(
            "minimized responder re-sent {} loose commits already carried by fragments the \
             requester holds (C4 re-download on reload)",
            diff.local_only_commits.len()
        ),
    );
}

/// Ideal: as above, against an *unminimized* responder, which isolates the
/// diff's own coverage walk in `diff_remote_fingerprints` from `minimize`.
/// Today that walk starts at `self.commits.get(&fragment.head())`, which is
/// `None` in automerge-repo's layout, so nothing is pruned (C2, diff path).
/// A fix to `minimize` alone leaves this red.
#[test]
#[should_panic(expected = "KNOWN BUG C2:")]
fn am_layout_unminimized_fragment_holder_receives_nothing() {
    let history = History::linear(N);
    let requester = compacted_requester(&history);
    let responder = history.tree(Layout::AutomergeRepo);
    let diff = responder.diff_remote_fingerprints(&requester);
    known_bug(
        "C2",
        diff.local_only_commits.is_empty(),
        &format!(
            "unminimized responder's coverage walk re-sent {} loose commits already carried by \
             fragments the requester holds",
            diff.local_only_commits.len()
        ),
    );
}

/// Ratchet: commits re-sent to a fragment holder (automerge-repo layout),
/// from a minimized responder and, separately, from an unminimized one (the
/// diff's coverage walk on its own).
#[test]
fn am_layout_fragment_holder_ratchet() {
    for n in [N, 10 * N] {
        let history = History::linear(n);
        let requester = compacted_requester(&history);
        for (which, responder) in [
            ("minimized", minimized(&history, Layout::AutomergeRepo)),
            ("unminimized", history.tree(Layout::AutomergeRepo)),
        ] {
            let diff = responder.diff_remote_fingerprints(&requester);
            ratchet(
                &format!("sedimentree.am_layout.fragment_holder.{which}.resent_loose.n{n}"),
                diff.local_only_commits.len() as u64,
            );
        }
    }
}

// ════════════════════════════════════════════════════════════════════════
// in-sync peers
// ════════════════════════════════════════════════════════════════════════

/// Ideal: two peers with identical trees exchange nothing: no items sent,
/// no fingerprints echoed back. Holds in both layouts today; guard.
#[test]
fn in_sync_diff_is_empty() {
    for history in [History::linear(N), History::branchy(BRANCHY_N)] {
        for layout in [Layout::Full, Layout::AutomergeRepo] {
            let local = minimized(&history, layout);
            let remote = minimized(&history, layout).fingerprint_summarize(&SEED);
            let diff = local.diff_remote_fingerprints(&remote);
            assert!(
                diff.local_only_commits.is_empty()
                    && diff.local_only_fragments.is_empty()
                    && diff.remote_only_commit_fingerprints.is_empty()
                    && diff.remote_only_fragment_fingerprints.is_empty(),
                "{} {layout:?}: in-sync diff must be empty, got {diff:?}",
                label(&history)
            );
        }
    }
}

/// The ideal size of an in-sync summary: one fingerprint per loose tail
/// commit, plus two per kept fragment (its head among the commit
/// fingerprints, and its fragment fingerprint).
fn ideal_summary_items(history: &History) -> usize {
    history.tail().len() + 2 * history.top_fragments().len()
}

/// Ideal: the summary a peer sends on every sync round is
/// `O(fragments + tail)`. Full layout achieves it exactly; guard.
#[test]
fn full_layout_summary_is_fragments_plus_tail() {
    for history in guarded_histories() {
        let summary = minimized(&history, Layout::Full).fingerprint_summarize(&SEED);
        assert_eq!(
            summary_items(&summary),
            ideal_summary_items(&history),
            "{}",
            label(&history)
        );
    }
}

/// Ideal: as above, for automerge-repo's layout. Today the summary is one
/// fingerprint per commit of history, so each sync round (which
/// automerge-repo runs on every throttled local save, C1) costs
/// `8 B x history` on the wire.
#[test]
#[should_panic(expected = "KNOWN BUG C1/C2:")]
fn am_layout_summary_is_fragments_plus_tail() {
    let history = History::linear(10 * N);
    let summary = minimized(&history, Layout::AutomergeRepo).fingerprint_summarize(&SEED);
    let items = summary_items(&summary);
    let ideal = ideal_summary_items(&history);
    known_bug(
        "C1/C2",
        items <= ideal,
        &format!(
            "in-sync summary for a {}-commit history has {items} fingerprints \
             ({} bytes); ideal is {ideal} (fragments + tail)",
            history.len(),
            items * 8
        ),
    );
}

/// Ratchet: in-sync summary size (fingerprint count) for automerge-repo's
/// layout at `N` and `10 * N`, and its growth factor between them
/// (x100; today ~10x).
#[test]
fn am_layout_summary_size_ratchet() {
    let small = History::linear(N);
    let large = History::linear(10 * N);
    let items = |h: &History| {
        let tree = minimized(h, Layout::AutomergeRepo);
        // Nothing lost: the summarized tree still holds the tail and every
        // fragment, so a smaller summary can only mean less redundancy.
        assert!(loose_ids(&tree).is_superset(&h.tail()), "tail lost");
        assert_eq!(tree.fragments().count(), h.fragments.len(), "fragment lost");
        summary_items(&tree.fingerprint_summarize(&SEED)) as u64
    };
    let (s, l) = (items(&small), items(&large));
    ratchet(
        &format!("sedimentree.am_layout.in_sync_summary.items.n{N}"),
        s,
    );
    ratchet(
        &format!("sedimentree.am_layout.in_sync_summary.items.n{}", 10 * N),
        l,
    );
    ratchet(
        "sedimentree.am_layout.in_sync_summary.growth_10x_history_x100",
        l * 100 / s,
    );
}

// ════════════════════════════════════════════════════════════════════════
// heads: a history that ends in one commit has exactly one head
// ════════════════════════════════════════════════════════════════════════

/// Ideal: a history that ends in a single commit has exactly one head, that
/// commit. Heads travel in every `BatchSyncResponse`, push and
/// `HeadsUpdate` (32 B each), so spurious heads are wire overhead on every
/// message. Full layout gets it right today; guard.
#[test]
fn full_layout_history_has_one_head() {
    for history in guarded_histories() {
        let heads = history.tree(Layout::Full).heads(&CountLeadingZeroBytes);
        assert_eq!(
            heads,
            vec![history.id(history.len() - 1)],
            "{}",
            label(&history)
        );
    }
}

/// Ideal: as above, for automerge-repo's layout. Today the commit just
/// before every fragment head is reported as a head too: its only child is
/// stored solely as a fragment (C2), and a fragment records its head,
/// boundary and checkpoints but not its members, so nothing references it.
#[test]
#[should_panic(expected = "KNOWN BUG C2:")]
fn am_layout_linear_history_has_one_head() {
    let history = History::linear(10 * N);
    let heads = history
        .tree(Layout::AutomergeRepo)
        .heads(&CountLeadingZeroBytes);
    known_bug(
        "C2",
        heads.len() == 1,
        &format!(
            "linear history of {} commits / {} fragments reports {} heads",
            history.len(),
            history.fragments.len(),
            heads.len()
        ),
    );
}

/// Ratchet: head count of a linear history, automerge-repo layout (ideal 1).
#[test]
fn am_layout_heads_ratchet() {
    for n in [N, 10 * N] {
        let history = History::linear(n);
        let heads = history
            .tree(Layout::AutomergeRepo)
            .heads(&CountLeadingZeroBytes);
        assert!(
            heads.contains(&history.id(n - 1)),
            "n={n}: the real head is missing"
        );
        ratchet(
            &format!("sedimentree.am_layout.linear_history.heads.n{n}"),
            heads.len() as u64,
        );
    }
}

// ════════════════════════════════════════════════════════════════════════
// re-sent items: the minimal tree must not depend on delivery history
// ════════════════════════════════════════════════════════════════════════

/// Every commit and fragment of `history` under `layout`, in a fixed order
/// (the tree's own iteration order is not), so seeded shuffles reproduce.
fn items(history: &History, layout: Layout) -> Vec<CommitOrFragment> {
    let mut items: Vec<CommitOrFragment> = history.tree(layout).into_items().collect();
    items.sort_by_key(|item| match item {
        CommitOrFragment::Commit(c) => (0, c.head()),
        CommitOrFragment::Fragment(f) => (1, f.head()),
    });
    items
}

/// Deliver one batch the way a node ingests it: add every item, then
/// minimize once.
fn deliver(resident: &mut MinimizedSedimentree, batch: &[CommitOrFragment]) {
    for item in batch {
        match item {
            CommitOrFragment::Commit(c) => {
                resident.add_commit(c.clone());
            }
            CommitOrFragment::Fragment(f) => {
                resident.add_fragment(f.clone());
            }
        }
    }
    resident.ensure_minimized(&CountLeadingZeroBytes);
}

/// Ideal: receiving items a node already holds changes nothing. Pushed one
/// at a time after the tree was minimized (a stale peer re-sending its old
/// loose commits), covered commits used to come back as strays that
/// `minimize` could never prune again, because the commits connecting them
/// to their fragment were already gone. Guarded on the working layout.
#[test]
fn resent_items_leave_the_minimal_tree_unchanged() {
    for history in [History::linear(N), History::branchy(BRANCHY_N)] {
        let full = history.tree(Layout::Full);
        let expected = full.minimize(&CountLeadingZeroBytes);
        let mut resident = MinimizedSedimentree::new(full);
        resident.ensure_minimized(&CountLeadingZeroBytes);
        for item in items(&history, Layout::Full) {
            deliver(&mut resident, core::slice::from_ref(&item));
        }
        assert_eq!(
            resident.tree(),
            &expected,
            "{}: re-sent items changed the minimal tree ({} loose commits, expected {})",
            label(&history),
            resident.loose_commits().count(),
            expected.loose_commits().count()
        );
    }
}

/// Ideal: the minimal tree depends only on when each item first arrived,
/// never on re-sends. For random delivery schedules (random order, random
/// batches, each batch also re-sending random earlier items), the tree
/// equals the one built from the same schedule with the re-sends removed.
/// Also checks nothing was lost: every delivered item is held, and every
/// commit no longer in the tree is carried by a fragment that is.
#[test]
fn minimal_tree_ignores_resends() {
    for (history, layout) in [
        (History::linear(N), Layout::Full),
        (History::branchy(BRANCHY_N), Layout::Full),
        (History::linear(N), Layout::AutomergeRepo),
    ] {
        let all = items(&history, layout);
        for seed in 0..16 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut order: Vec<&CommitOrFragment> = all.iter().collect();
            order.shuffle(&mut rng);

            let mut with_resends = MinimizedSedimentree::default();
            let mut first_only = MinimizedSedimentree::default();
            let mut delivered: Vec<&CommitOrFragment> = Vec::new();
            let mut rest = order.as_slice();
            while !rest.is_empty() {
                let (chunk, tail) = rest.split_at(rng.gen_range(1..=200).min(rest.len()));
                rest = tail;
                let fresh: Vec<CommitOrFragment> = chunk.iter().map(|i| (*i).clone()).collect();
                let mut batch = fresh.clone();
                for _ in 0..rng.gen_range(0..=delivered.len().min(100)) {
                    batch.push(delivered[rng.gen_range(0..delivered.len())].clone());
                }
                batch.shuffle(&mut rng);
                deliver(&mut with_resends, &batch);
                deliver(&mut first_only, &fresh);
                delivered.extend(chunk);
            }

            let what = format!("{} {layout:?} seed {seed}", label(&history));
            assert_eq!(
                with_resends.tree(),
                first_only.tree(),
                "{what}: re-sends changed the minimal tree"
            );
            for item in &all {
                match item {
                    CommitOrFragment::Commit(c) => {
                        assert!(
                            with_resends.holds_commit(c.head()),
                            "{what}: commit not held"
                        );
                    }
                    CommitOrFragment::Fragment(f) => {
                        assert!(
                            with_resends.holds_fragment(f.head()),
                            "{what}: fragment not held"
                        );
                    }
                }
            }
            let ids: Vec<CommitId> = history.commits.iter().map(|(c, _)| c.head()).collect();
            let kept: Vec<CommitId> = with_resends.fragments().map(Fragment::head).collect();
            let carried = history.redundant(&ids, &kept);
            for (commit, _) in history.loose(layout) {
                let id = commit.head();
                assert!(
                    with_resends.has_loose_commit(id) || carried.contains(&id),
                    "{what}: commit {id:?} dropped without a kept fragment carrying it"
                );
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════
// first deliveries: minimizing as items arrive vs. all at once
// ════════════════════════════════════════════════════════════════════════

/// Commits oldest first, each fragment at a random point after its head:
/// the order a node sees when every commit is uploaded before the fragment
/// that covers it.
fn causal_order<'a>(
    history: &History,
    all: &'a [CommitOrFragment],
    rng: &mut SmallRng,
) -> Vec<&'a CommitOrFragment> {
    let position: std::collections::BTreeMap<CommitId, usize> = history
        .commits
        .iter()
        .enumerate()
        .map(|(i, (c, _))| (c.head(), i))
        .collect();
    let mut keyed: Vec<(usize, &CommitOrFragment)> = all
        .iter()
        .map(|item| match item {
            CommitOrFragment::Commit(c) => (2 * position[&c.head()], item),
            CommitOrFragment::Fragment(f) => {
                (2 * (position[&f.head()] + rng.gen_range(0..300)) + 1, item)
            }
        })
        .collect();
    keyed.sort_by_key(|(k, _)| *k);
    keyed.into_iter().map(|(_, item)| item).collect()
}

/// Deliver each item of `order` once, in random batches, minimizing after
/// each batch. Returns how many loose commits the result keeps beyond
/// `minimize` over everything at once (0 is ideal), after checking the
/// fragments agree.
fn strays_after_delivering(
    history: &History,
    order: &[&CommitOrFragment],
    rng: &mut SmallRng,
) -> usize {
    let expected = minimized(history, Layout::Full);
    let mut resident = MinimizedSedimentree::default();
    let mut rest = order;
    while !rest.is_empty() {
        let (chunk, tail) = rest.split_at(rng.gen_range(1..=50).min(rest.len()));
        rest = tail;
        let batch: Vec<CommitOrFragment> = chunk.iter().map(|i| (*i).clone()).collect();
        deliver(&mut resident, &batch);
    }
    let kept = loose_ids(resident.tree());
    let ideal = loose_ids(&expected);
    assert!(
        ideal.is_subset(&kept),
        "{}: minimizing as items arrived dropped a commit minimize(all) keeps",
        label(history)
    );
    let fragments = |tree: &Sedimentree| -> BTreeSet<CommitId> {
        tree.fragments().map(Fragment::head).collect()
    };
    assert_eq!(
        fragments(resident.tree()),
        fragments(&expected),
        "{}: minimizing as items arrived kept different fragments",
        label(history)
    );
    kept.len() - ideal.len()
}

/// Ideal: minimizing items as they first arrive gives the tree minimizing
/// them all at once would, i.e. `minimize(minimize(a) ∪ b) ==
/// minimize(a ∪ b)`. Holds when every commit arrives before the fragment
/// that covers it, as a writer uploads them: pruning then only drops whole
/// runs whose commits have all arrived. Guard.
#[test]
fn causal_first_deliveries_minimize_like_all_at_once() {
    for history in [History::linear(N), History::branchy(BRANCHY_N)] {
        let all = items(&history, Layout::Full);
        for seed in 0..8 {
            let mut rng = SmallRng::seed_from_u64(seed);
            let order = causal_order(&history, &all, &mut rng);
            let strays = strays_after_delivering(&history, &order, &mut rng);
            assert_eq!(
                strays,
                0,
                "{} seed {seed}: stray loose commits",
                label(&history)
            );
        }
    }
}

/// Ideal: the same for any arrival order. Today (ORDER) a commit that first
/// arrives after a child of it was already pruned as covered has no loose
/// path left to its fragment's head, so it stays as a stray loose commit
/// until the node rebuilds the tree from storage (on eviction or restart,
/// the full history is minimized at once). Re-sends no longer do this
/// (`minimal_tree_ignores_resends`); only first deliveries out of causal
/// order, e.g. a commit a responder held only inside a fragment, echoed
/// and then uploaded by a stale peer.
#[test]
#[should_panic(expected = "KNOWN BUG ORDER:")]
fn any_order_first_deliveries_minimize_like_all_at_once() {
    let mut strays = Vec::new();
    for history in [History::linear(N), History::branchy(BRANCHY_N)] {
        let mut all = items(&history, Layout::Full);
        for seed in 0..8 {
            let mut rng = SmallRng::seed_from_u64(seed);
            all.shuffle(&mut rng);
            let order: Vec<&CommitOrFragment> = all.iter().collect();
            strays.push(strays_after_delivering(&history, &order, &mut rng));
        }
    }
    known_bug(
        "ORDER",
        strays.iter().all(|s| *s == 0),
        &format!(
            "stray loose commits after random-order delivery, per seed (linear n={N} x8, \
             branchy n={BRANCHY_N} x8): {strays:?}"
        ),
    );
}
