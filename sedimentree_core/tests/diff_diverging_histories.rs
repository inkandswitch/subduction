//! Tests for `Sedimentree::diff_remote_fingerprints` with **diverging
//! loose-commit-only histories**.
//!
//! The existing tests in `sedimentree.rs::tests` focus on fragment
//! scenarios (fragments dominating commits, fragment-coverage pruning,
//! etc.). The user-observed bug from the React-todo workflow involves
//! **zero fragments** and a stream of loose-commit-only updates, so this
//! file exercises that regime specifically.
//!
//! Key invariants we want to lock down:
//!
//! 1. **No overlap → both sides report their full disjoint sets.**
//!    Disjoint chains should produce `local_only_commits.len() == |local|`
//!    and `remote_only_commit_fingerprints.len() == |remote|`.
//!
//! 2. **Full overlap → empty diff.** Identical trees produce empty diffs.
//!
//! 3. **Partial overlap → only the delta.** Two chains sharing a common
//!    prefix should send only the divergent suffix.
//!
//! 4. **DAG-ancestry pruning is correct.** When the responder has commit
//!    X and X is in the requester's fingerprint set, the responder should
//!    skip sending X's ancestors. We test this is sound (doesn't drop
//!    commits the requester actually needs) and complete (does drop
//!    redundant ancestor sends).
//!
//! 5. **Forked histories.** Two chains diverging after a shared root
//!    should each send their post-divergence tail, not the shared prefix.

#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::panic)]

use std::collections::BTreeSet;

use sedimentree_core::{
    blob::{Blob, BlobMeta},
    commit::CountLeadingZeroBytes,
    crypto::fingerprint::FingerprintSeed,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

const SED_ID: SedimentreeId = SedimentreeId::new([42u8; 32]);
const SEED: FingerprintSeed = FingerprintSeed::new(0x1234_5678, 0xDEAD_BEEF);

/// Build a loose commit whose head is `[seed; 32]` and whose blob bytes
/// are also a function of `seed` (so different seeds → different blob
/// digests, avoiding accidental collisions).
fn commit(seed: u8, parents: &[CommitId]) -> LooseCommit {
    let blob = Blob::new(vec![seed; 64]);
    let blob_meta = BlobMeta::new(&blob);
    let head = CommitId::new([seed; 32]);
    LooseCommit::new(
        SED_ID,
        head,
        parents.iter().copied().collect::<BTreeSet<_>>(),
        blob_meta,
    )
}

fn head(seed: u8) -> CommitId {
    CommitId::new([seed; 32])
}

fn tree(commits: Vec<LooseCommit>) -> Sedimentree {
    Sedimentree::new(vec![], commits)
}

// ===========================================================================
// Section 1: Identity & emptiness invariants
// ===========================================================================

/// Two identical trees should produce an empty diff in both directions.
#[test]
fn identical_trees_have_empty_diff() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone()]);
    let remote = tree(vec![a, b, c]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert!(
        diff.local_only_commits.is_empty(),
        "identical trees: local_only_commits should be empty, got {}",
        diff.local_only_commits.len(),
    );
    assert!(
        diff.remote_only_commit_fingerprints.is_empty(),
        "identical trees: remote_only_commit_fingerprints should be empty, got {}",
        diff.remote_only_commit_fingerprints.len(),
    );
}

/// Local empty, remote non-empty. Local should request everything remote
/// has, send nothing.
#[test]
fn empty_local_full_remote_requests_all() {
    let local = tree(vec![]);
    let remote = tree(vec![commit(1, &[]), commit(2, &[head(1)])]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(
        diff.local_only_commits.len(),
        0,
        "empty local sends nothing"
    );
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        2,
        "empty local requests all 2 commits from remote"
    );
}

/// Local non-empty, remote empty. Local should send everything, request
/// nothing.
#[test]
fn full_local_empty_remote_sends_all() {
    let local = tree(vec![commit(1, &[]), commit(2, &[head(1)])]);
    let remote = tree(vec![]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert_eq!(
        diff.local_only_commits.len(),
        2,
        "full local sends all 2 commits"
    );
    assert!(
        diff.remote_only_commit_fingerprints.is_empty(),
        "empty remote: nothing to request, got {} fps",
        diff.remote_only_commit_fingerprints.len(),
    );
}

// ===========================================================================
// Section 2: Disjoint histories — the user's bug shape
// ===========================================================================

/// **The user's bug shape.** Two peers with completely disjoint commit
/// sets should produce `local_only.len() == |local|` and
/// `remote_only.len() == |remote|` — this exactly matches the
/// "19 missing, requesting 19" pattern from the logs IF the two browsers
/// genuinely had disjoint commits at sync time.
///
/// We assert the math here to confirm that *this specific symptom is
/// expected* given disjoint inputs — which means the user's bug is
/// **either** about the data being genuinely disjoint when it shouldn't
/// be, **or** about the system failing to converge after the disjoint
/// diff round.
#[test]
fn disjoint_histories_send_full_sets_in_both_directions() {
    // Local has chain A → B → C (seeds 10, 11, 12)
    let local = tree(vec![
        commit(10, &[]),
        commit(11, &[head(10)]),
        commit(12, &[head(11)]),
    ]);

    // Remote has chain X → Y → Z (seeds 20, 21, 22) — fully disjoint
    let remote = tree(vec![
        commit(20, &[]),
        commit(21, &[head(20)]),
        commit(22, &[head(21)]),
    ]);

    let remote_summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&remote_summary);

    // Local has 3 commits that remote doesn't have.
    assert_eq!(
        diff.local_only_commits.len(),
        3,
        "disjoint: local should report all 3 of its commits as local_only"
    );
    // Remote has 3 commits that local doesn't have.
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        3,
        "disjoint: remote_only_commit_fingerprints should be all 3 of remote's"
    );
}

/// Disjoint histories with **unequal sizes** — e.g. local has 19, remote
/// has 20. Confirms the "M missing, requesting N" pattern matches the
/// inputs exactly when there's zero overlap.
#[test]
fn disjoint_histories_unequal_sizes() {
    // Local: chain of 19 commits starting at seed 10.
    let mut local_commits = vec![commit(10, &[])];
    for i in 1..19u8 {
        local_commits.push(commit(10 + i, &[head(10 + i - 1)]));
    }
    assert_eq!(local_commits.len(), 19);
    let local = tree(local_commits);

    // Remote: chain of 20 commits starting at seed 100 (disjoint).
    let mut remote_commits = vec![commit(100, &[])];
    for i in 1..20u8 {
        remote_commits.push(commit(100 + i, &[head(100 + i - 1)]));
    }
    assert_eq!(remote_commits.len(), 20);
    let remote = tree(remote_commits);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // 19 missing (local-only, what local will send to remote)
    assert_eq!(
        diff.local_only_commits.len(),
        19,
        "local has 19 disjoint → local_only_commits is 19"
    );
    // 20 requesting (remote-only, fingerprints local will echo back)
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        20,
        "remote has 20 disjoint → remote_only_commit_fingerprints is 20"
    );
}

// ===========================================================================
// Section 3: Overlapping histories (the convergence case)
// ===========================================================================

/// Local and remote share commits A, B; local has additional C; remote
/// has additional D. After sync, both should converge.
#[test]
fn forked_after_shared_prefix() {
    // Shared prefix
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);

    // Local-only continuation
    let c = commit(3, &[head(2)]);
    // Remote-only continuation
    let d = commit(4, &[head(2)]);

    let local = tree(vec![a.clone(), b.clone(), c]);
    let remote = tree(vec![a, b, d]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // A and B are shared, ancestors_of({A, B}) = {A, B}. Local's commits
    // are {A, B, C}. local_only_commits before pruning = [C]. After
    // ancestor pruning: C is not an ancestor of shared, keep. = [C].
    assert_eq!(
        diff.local_only_commits.len(),
        1,
        "forked: local should only send its divergent suffix (1 commit)"
    );
    let sent_id = *diff.local_only_commits[0].0;
    assert_eq!(sent_id, head(3), "should send C, not A or B");

    // Remote-only: D is not in local's fp set. A and B are in both.
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        1,
        "forked: should request 1 commit (D)"
    );
}

/// Linear history, remote is a strict prefix of local. Local sends only
/// the tail; the shared prefix is pruned by DAG-ancestry analysis.
#[test]
fn remote_is_prefix_of_local() {
    // Local: A → B → C → D
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone(), d]);
    // Remote: A → B only
    let remote = tree(vec![a, b]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // Pre-pruning local_only = [C, D]. ancestors_of({A, B}) = {A, B}.
    // Neither C nor D is in there, so both survive.
    assert_eq!(
        diff.local_only_commits.len(),
        2,
        "prefix: local sends C and D"
    );
    let sent_ids: BTreeSet<CommitId> = diff
        .local_only_commits
        .iter()
        .map(|(id, _)| **id)
        .collect();
    assert!(sent_ids.contains(&head(3)) && sent_ids.contains(&head(4)));
    assert!(
        !sent_ids.contains(&head(1)) && !sent_ids.contains(&head(2)),
        "shared prefix must not be re-sent"
    );

    assert!(
        diff.remote_only_commit_fingerprints.is_empty(),
        "prefix: nothing to request"
    );
}

/// Local is a strict prefix of remote. Local requests the tail.
#[test]
fn local_is_prefix_of_remote() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone()]);
    let remote = tree(vec![a, b, c, d]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    assert!(
        diff.local_only_commits.is_empty(),
        "local prefix has nothing to send"
    );
    assert_eq!(
        diff.remote_only_commit_fingerprints.len(),
        2,
        "local prefix requests C and D"
    );
}

// ===========================================================================
// Section 4: DAG-ancestry pruning correctness
// ===========================================================================

/// The pruning step removes commits from `local_only_commits` when they're
/// ancestors of a commit the remote has. This test pins down that
/// behavior: if remote has the tip C, local doesn't re-send A and B
/// (C's ancestors).
#[test]
fn ancestry_pruning_drops_ancestors_of_shared_tip() {
    // Local: A → B → C → D (D is local-only, A,B,C shared)
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);
    let d = commit(4, &[head(3)]);

    let local = tree(vec![a.clone(), b.clone(), c.clone(), d.clone()]);

    // Remote claims to have A, B, C (but not D)
    let remote = tree(vec![a, b, c]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // Pre-pruning local_only = [D]. ancestors_of({A,B,C}) = {A,B,C}.
    // D is not in ancestors → keep.
    assert_eq!(
        diff.local_only_commits.len(),
        1,
        "ancestry: send only D (the divergent tip)"
    );
    assert_eq!(*diff.local_only_commits[0].0, head(4));
}

/// **Sound-vs-complete edge case.** If remote claims to have B but does
/// not actually have B's parent A, ancestry pruning drops A from
/// `local_only_commits`. Convergence depends on the protocol invariant
/// that "if you have a commit, you have its ancestors." This test
/// confirms our pruning DOES drop A in this case — documenting the
/// invariant and the cost of violating it.
///
/// (If a future change wants to be defensive against protocol violations,
/// this test will need updating.)
#[test]
fn ancestry_pruning_trusts_remote_has_advertised_commits_ancestors() {
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);

    let local = tree(vec![a.clone(), b.clone()]);

    // Remote claims to have only B (NOT A). This is "ill-formed" in the
    // sense that a peer should never advertise B without having A, but
    // the protocol can't enforce that.
    let remote = tree(vec![b]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // shared = {B}. ancestors_of({B}) = {B, A}. local_only pre-prune = [A].
    // After pruning: A is in ancestors → drop. Result: send nothing.
    assert!(
        diff.local_only_commits.is_empty(),
        "ancestry pruning DOES trust the remote's advertisement; A is not re-sent. \
         This is documented behavior — a peer that has B without A will not get A \
         from this responder. Result was: {} commits",
        diff.local_only_commits.len(),
    );
}

/// Multi-tip DAG: local has two parallel chains rooted at the same point.
/// Remote has only chain 1's tip. Pruning should drop chain 1's
/// ancestors but keep all of chain 2.
#[test]
fn ancestry_pruning_preserves_disjoint_branches() {
    let root = commit(1, &[]);

    // Branch 1: root → b1 → c1
    let b1 = commit(10, &[head(1)]);
    let c1 = commit(11, &[head(10)]);

    // Branch 2: root → b2 → c2
    let b2 = commit(20, &[head(1)]);
    let c2 = commit(21, &[head(20)]);

    let local = tree(vec![root.clone(), b1.clone(), c1.clone(), b2.clone(), c2.clone()]);

    // Remote has only c1's chain: root, b1, c1
    let remote = tree(vec![root, b1, c1]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // shared = {root, b1, c1}. ancestors_of(those) = {root, b1, c1}.
    // local_only pre-prune = [b2, c2]. After: both kept (neither is an
    // ancestor of shared).
    assert_eq!(
        diff.local_only_commits.len(),
        2,
        "should send branch 2 (b2 + c2) but not the shared chain"
    );
    let sent_ids: BTreeSet<CommitId> = diff
        .local_only_commits
        .iter()
        .map(|(id, _)| **id)
        .collect();
    assert!(
        sent_ids.contains(&head(20)) && sent_ids.contains(&head(21)),
        "sent set should be exactly {{b2, c2}}, got: {:?}",
        sent_ids
    );
}

/// Diamond DAG: shared root with two parallel branches that re-merge.
///
/// ```text
///       root
///      /    \
///     b1     b2
///      \    /
///       merge
/// ```
///
/// Remote has the merge commit. Pruning should drop all of root, b1, b2.
#[test]
fn ancestry_pruning_handles_diamond_dag() {
    let root = commit(1, &[]);
    let b1 = commit(2, &[head(1)]);
    let b2 = commit(3, &[head(1)]);
    let merge = commit(4, &[head(2), head(3)]);

    let local = tree(vec![root, b1, b2, merge.clone()]);

    // Remote has only the merge commit
    let remote = tree(vec![merge]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // shared = {merge}. ancestors_of({merge}) walks both parents:
    //   merge → b1 → root (added)
    //   merge → b2 → root (already visited)
    // Result: {merge, b1, b2, root}.
    // local_only pre-prune = [root, b1, b2]. All in ancestors → all dropped.
    assert!(
        diff.local_only_commits.is_empty(),
        "diamond DAG: nothing should be sent (remote has the merge tip)"
    );
}

/// Many parallel small chains, remote has only the latest commit of one.
/// Verifies pruning scales correctly with many disjoint branches.
#[test]
fn many_parallel_chains_pruning() {
    // 5 independent chains of length 3 each
    let mut all_commits = Vec::new();
    let mut chain_tips: Vec<CommitId> = Vec::new();
    for chain_idx in 0..5u8 {
        let base = 10 + chain_idx * 10;
        let c0 = commit(base, &[]);
        let c1 = commit(base + 1, &[head(base)]);
        let c2 = commit(base + 2, &[head(base + 1)]);
        chain_tips.push(head(base + 2));
        all_commits.push(c0);
        all_commits.push(c1);
        all_commits.push(c2);
    }
    let local = tree(all_commits);
    assert_eq!(
        local.loose_commits().count(),
        15,
        "5 chains × 3 commits = 15"
    );

    // Remote has just the last commit of the first chain (seed 12)
    let remote = tree(vec![commit(12, &[head(11)])]);

    let summary = remote.fingerprint_summarize(&SEED);
    let diff = local.diff_remote_fingerprints(&summary);

    // Chain 0: shared = {c2}. ancestors_of({c2}) = {c2, c1, c0}.
    //   Local's c0, c1, c2 all in ancestors → all dropped.
    // Chains 1-4: nothing shared, all 12 commits sent.
    assert_eq!(
        diff.local_only_commits.len(),
        12,
        "4 chains × 3 commits = 12 sent; chain 0 fully pruned"
    );
}

// ===========================================================================
// Section 5: The "growing forever" failure mode
// ===========================================================================

/// **Models the user's compute-grows-on-each-update symptom.** If two
/// peers keep authoring independent commits and never converge, each
/// successive sync produces a larger disjoint set. This is correct
/// behavior at the diff layer — but if the FOLLOW-UP step (ingesting
/// `missing_commits`) is broken, the next round still sees the same
/// disjointness.
///
/// We verify the math layer: round-2 of a disjoint sync should produce
/// 0 missing and 0 requesting **assuming both sides ingested correctly**.
#[test]
fn convergence_after_one_round_when_both_sides_ingest() {
    // Round 0: local has chain A, remote has chain B (disjoint).
    let local_initial = tree(vec![
        commit(10, &[]),
        commit(11, &[head(10)]),
        commit(12, &[head(11)]),
    ]);
    let remote_initial = tree(vec![
        commit(20, &[]),
        commit(21, &[head(20)]),
        commit(22, &[head(21)]),
    ]);

    // Simulate round 1: each side observes the other's diff
    let local_summary = local_initial.fingerprint_summarize(&SEED);
    let remote_summary = remote_initial.fingerprint_summarize(&SEED);

    // Materialize owned commit sets from the diffs before moving the trees.
    let from_remote_to_local: Vec<LooseCommit> = {
        let diff_at_remote = remote_initial.diff_remote_fingerprints(&local_summary);
        assert_eq!(diff_at_remote.local_only_commits.len(), 3);
        assert_eq!(diff_at_remote.remote_only_commit_fingerprints.len(), 3);
        diff_at_remote
            .local_only_commits
            .into_iter()
            .map(|(_, c)| c.clone())
            .collect()
    };

    let from_local_to_remote: Vec<LooseCommit> = {
        let diff_at_local = local_initial.diff_remote_fingerprints(&remote_summary);
        assert_eq!(diff_at_local.local_only_commits.len(), 3);
        assert_eq!(diff_at_local.remote_only_commit_fingerprints.len(), 3);
        diff_at_local
            .local_only_commits
            .into_iter()
            .map(|(_, c)| c.clone())
            .collect()
    };

    // Each side ingests the OTHER side's local_only_commits.
    let mut local_after = local_initial;
    for commit in from_remote_to_local {
        local_after.add_commit(commit);
    }
    let mut remote_after = remote_initial;
    for commit in from_local_to_remote {
        remote_after.add_commit(commit);
    }

    // After ingestion both should have all 6 commits.
    assert_eq!(local_after.loose_commits().count(), 6);
    assert_eq!(remote_after.loose_commits().count(), 6);

    // Round 2: with a fresh seed, the diff should be empty in both
    // directions. This proves the diff layer converges in ONE round
    // when both sides ingest the response correctly.
    let seed2 = FingerprintSeed::new(0xFEED_FACE, 0xCAFE_BABE);
    let local_summary2 = local_after.fingerprint_summarize(&seed2);
    let remote_summary2 = remote_after.fingerprint_summarize(&seed2);

    let round2_at_local = local_after.diff_remote_fingerprints(&remote_summary2);
    let round2_at_remote = remote_after.diff_remote_fingerprints(&local_summary2);

    assert!(
        round2_at_local.local_only_commits.is_empty()
            && round2_at_local.remote_only_commit_fingerprints.is_empty(),
        "round 2 at local should be empty (received={}, sent_back={})",
        round2_at_local.remote_only_commit_fingerprints.len(),
        round2_at_local.local_only_commits.len(),
    );
    assert!(
        round2_at_remote.local_only_commits.is_empty()
            && round2_at_remote.remote_only_commit_fingerprints.is_empty(),
        "round 2 at remote should be empty (received={}, sent_back={})",
        round2_at_remote.remote_only_commit_fingerprints.len(),
        round2_at_remote.local_only_commits.len(),
    );
}

/// **The most direct test of the user's bug-pattern as a pure math
/// property.** If sync convergence works at the diff layer, then for
/// any pair of trees `(A, B)`, after we add `A.local_only` to `B` and
/// `B.local_only` to `A`, a follow-up diff with a fresh seed is empty.
///
/// This is just an explicit, deterministic version of what the property
/// tests in `sedimentree.rs::tests::proptests` should cover for the
/// loose-commit-only case.
#[test]
fn loose_commit_only_sync_converges_in_one_round_arbitrary_overlap() {
    // Shared: A, B, C
    // Local-only: D, E
    // Remote-only: X, Y, Z (with X having parent B from the shared set)
    let a = commit(1, &[]);
    let b = commit(2, &[head(1)]);
    let c = commit(3, &[head(2)]);

    let d = commit(4, &[head(3)]);
    let e = commit(5, &[head(4)]);

    let x = commit(11, &[head(2)]); // forks off B
    let y = commit(12, &[head(11)]);
    let z = commit(13, &[head(12)]);

    let local_initial = tree(vec![a.clone(), b.clone(), c.clone(), d, e]);
    let remote_initial = tree(vec![a, b, c, x, y, z]);

    let local_summary = local_initial.fingerprint_summarize(&SEED);
    let remote_summary = remote_initial.fingerprint_summarize(&SEED);

    let from_remote_to_local: Vec<LooseCommit> = {
        let diff_at_remote = remote_initial.diff_remote_fingerprints(&local_summary);
        assert_eq!(
            diff_at_remote.local_only_commits.len(),
            3,
            "remote sends X, Y, Z (post-shared)"
        );
        diff_at_remote
            .local_only_commits
            .into_iter()
            .map(|(_, c)| c.clone())
            .collect()
    };

    let from_local_to_remote: Vec<LooseCommit> = {
        let diff_at_local = local_initial.diff_remote_fingerprints(&remote_summary);
        assert_eq!(
            diff_at_local.local_only_commits.len(),
            2,
            "local sends D, E (post-shared)"
        );
        diff_at_local
            .local_only_commits
            .into_iter()
            .map(|(_, c)| c.clone())
            .collect()
    };

    let mut local_after = local_initial;
    for commit in from_remote_to_local {
        local_after.add_commit(commit);
    }
    let mut remote_after = remote_initial;
    for commit in from_local_to_remote {
        remote_after.add_commit(commit);
    }

    assert_eq!(local_after.loose_commits().count(), 8);
    assert_eq!(remote_after.loose_commits().count(), 8);

    // Round 2 with fresh seed
    let seed2 = FingerprintSeed::new(7, 11);
    let remote_summary2 = remote_after.fingerprint_summarize(&seed2);
    let round2 = local_after.diff_remote_fingerprints(&remote_summary2);

    assert!(round2.local_only_commits.is_empty());
    assert!(round2.remote_only_commit_fingerprints.is_empty());
}

// ===========================================================================
// Section 6: Minimize interaction (loose-only side)
// ===========================================================================

/// Verifies that minimize on a loose-only tree (no fragments) doesn't
/// change the fingerprint set, so a tree minimized before fingerprinting
/// is interchangeable with the unminimized version.
#[test]
fn minimize_loose_only_preserves_fingerprints() {
    let local = tree(vec![
        commit(1, &[]),
        commit(2, &[head(1)]),
        commit(3, &[head(2)]),
    ]);

    let depth_metric = CountLeadingZeroBytes;
    let minimized = local.minimize(&depth_metric);

    let summary_raw = local.fingerprint_summarize(&SEED);
    let summary_min = minimized.fingerprint_summarize(&SEED);

    assert_eq!(
        summary_raw.commit_fingerprints(),
        summary_min.commit_fingerprints(),
        "minimize on loose-only tree must not change commit fingerprints"
    );
    assert_eq!(
        summary_raw.fragment_fingerprints(),
        summary_min.fragment_fingerprints(),
        "fragment set should be empty in both"
    );
}
