//! Property tests for `Sedimentree::diff_remote_fingerprints` and
//! `fingerprint_summarize`. The canonical "after mutual ingest, a fresh
//! diff is empty" property is the single most important sync-correctness
//! invariant — if it ever fails, sync diverges forever.

#![cfg(feature = "bolero")]
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;

use sedimentree_core::{
    collections::Set,
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};

/// Rebuild `commits` dropping any parent reference whose target isn't
/// also in the list. The diff layer's ancestry-pruning step assumes
/// "if you have a commit, you have its ancestors" — peers that violate
/// this can't be expected to converge in one round.
fn close_ancestry(commits: &[LooseCommit]) -> Vec<LooseCommit> {
    let ids: BTreeSet<CommitId> = commits.iter().map(LooseCommit::head).collect();
    commits
        .iter()
        .map(|c| {
            let pruned_parents: BTreeSet<CommitId> = c
                .parents()
                .iter()
                .copied()
                .filter(|p| ids.contains(p))
                .collect();
            LooseCommit::new(c.sedimentree_id(), c.head(), pruned_parents, *c.blob_meta())
        })
        .collect()
}

/// Topo-sort an already ancestry-closed list of commits so that every
/// commit appears after all its parents. Drops members of any cycle.
fn topo_sort(commits: Vec<LooseCommit>) -> Vec<LooseCommit> {
    let mut placed = BTreeSet::new();
    let mut result = Vec::new();
    let mut remaining = commits;

    loop {
        let before = remaining.len();
        let mut next_remaining = Vec::new();
        for c in remaining {
            if c.parents().iter().all(|p| placed.contains(p)) {
                placed.insert(c.head());
                result.push(c);
            } else {
                next_remaining.push(c);
            }
        }
        if next_remaining.len() == before {
            // No progress — remaining commits are in a cycle. Drop them.
            return result;
        }
        if next_remaining.is_empty() {
            return result;
        }
        remaining = next_remaining;
    }
}

/// Partition a topo-sorted universe into two ancestry-closed subsets
/// using element-wise boolean masks. A commit is admitted to a subset
/// only if its mask bit is set *and* all its parents are already in
/// that subset. Produces overlapping, disjoint, or empty sets depending
/// on the masks.
fn partition(
    universe: &[LooseCommit],
    a_mask: &[bool],
    b_mask: &[bool],
) -> (Vec<LooseCommit>, Vec<LooseCommit>) {
    let mut a_commits = Vec::new();
    let mut b_commits = Vec::new();
    let mut a_ids: BTreeSet<CommitId> = BTreeSet::new();
    let mut b_ids: BTreeSet<CommitId> = BTreeSet::new();

    for (i, c) in universe.iter().enumerate() {
        let in_a = a_mask.get(i).copied().unwrap_or(false);
        let in_b = b_mask.get(i).copied().unwrap_or(false);

        if in_a && c.parents().iter().all(|p| a_ids.contains(p)) {
            a_commits.push(c.clone());
            a_ids.insert(c.head());
        }
        if in_b && c.parents().iter().all(|p| b_ids.contains(p)) {
            b_commits.push(c.clone());
            b_ids.insert(c.head());
        }
    }

    (a_commits, b_commits)
}

/// **The canonical sync-correctness property.** For any two
/// ancestry-closed loose-commit trees drawn from a shared universe
/// (so they may overlap, fork, or be disjoint) and any pair of seeds,
/// after each side ingests the other's `local_only_commits` (computed
/// under the first seed), a fresh diff under the second seed is empty
/// in both directions.
#[test]
fn prop_mutual_ingest_converges_in_one_round() {
    bolero::check!()
        .with_arbitrary::<(
            Vec<LooseCommit>,
            Vec<bool>,
            Vec<bool>,
            FingerprintSeed,
            FingerprintSeed,
        )>()
        .for_each(|(universe, a_mask, b_mask, seed1, seed2)| {
            let universe = topo_sort(close_ancestry(universe));
            let (a_commits, b_commits) = partition(&universe, a_mask, b_mask);

            let a_initial = Sedimentree::new(vec![], a_commits);
            let b_initial = Sedimentree::new(vec![], b_commits);

            let a_summary1 = a_initial.fingerprint_summarize(seed1);
            let b_summary1 = b_initial.fingerprint_summarize(seed1);

            let to_a: Vec<LooseCommit> = b_initial
                .diff_remote_fingerprints(&a_summary1)
                .local_only_commits
                .into_iter()
                .map(|(_, c)| c.clone())
                .collect();
            let to_b: Vec<LooseCommit> = a_initial
                .diff_remote_fingerprints(&b_summary1)
                .local_only_commits
                .into_iter()
                .map(|(_, c)| c.clone())
                .collect();

            let mut a_after = a_initial;
            for c in to_a {
                a_after.add_commit(c);
            }
            let mut b_after = b_initial;
            for c in to_b {
                b_after.add_commit(c);
            }

            let a_summary2 = a_after.fingerprint_summarize(seed2);
            let b_summary2 = b_after.fingerprint_summarize(seed2);

            let round2_at_a = a_after.diff_remote_fingerprints(&b_summary2);
            let round2_at_b = b_after.diff_remote_fingerprints(&a_summary2);

            assert!(
                round2_at_a.local_only_commits.is_empty()
                    && round2_at_a.remote_only_commit_fingerprints.is_empty(),
                "round 2 at A non-empty: {} local-only, {} requested",
                round2_at_a.local_only_commits.len(),
                round2_at_a.remote_only_commit_fingerprints.len(),
            );
            assert!(
                round2_at_b.local_only_commits.is_empty()
                    && round2_at_b.remote_only_commit_fingerprints.is_empty(),
                "round 2 at B non-empty: {} local-only, {} requested",
                round2_at_b.local_only_commits.len(),
                round2_at_b.remote_only_commit_fingerprints.len(),
            );
        });
}

/// Every commit in a tree appears in its own fingerprint summary.
#[test]
fn prop_summary_contains_every_commits_fingerprint() {
    bolero::check!()
        .with_arbitrary::<(Vec<LooseCommit>, FingerprintSeed)>()
        .for_each(|(commits, seed)| {
            let tree = Sedimentree::new(vec![], close_ancestry(commits));
            let summary = tree.fingerprint_summarize(seed);

            for commit in tree.loose_commits() {
                let fp = Fingerprint::new(seed, &commit.head());
                assert!(
                    summary.commit_fingerprints().contains(&fp),
                    "summary missing fp of commit {:?}",
                    commit.head(),
                );
            }
        });
}

/// `diff(T, T.summarize(s))` is empty for any tree and seed.
#[test]
fn prop_self_diff_is_empty() {
    bolero::check!()
        .with_arbitrary::<(Vec<LooseCommit>, FingerprintSeed)>()
        .for_each(|(commits, seed)| {
            let tree = Sedimentree::new(vec![], close_ancestry(commits));
            let diff = tree.diff_remote_fingerprints(&tree.fingerprint_summarize(seed));

            assert!(diff.local_only_commits.is_empty());
            assert!(diff.remote_only_commit_fingerprints.is_empty());
        });
}

/// **Ancestry-pruning soundness.** The `local_only_commits` returned
/// by `diff_remote_fingerprints` is exactly
/// `(local ∖ remote_fingerprints) ∖ ancestors_of(local ∩ remote_fingerprints)`.
///
/// A commit is dropped iff it would have been in `local_only` *and* it
/// is an ancestor of some shared commit. Anything else is a bug —
/// either pruning is too aggressive (drops a commit the remote needs)
/// or too lax (re-sends a commit the remote already has via ancestry).
#[test]
fn prop_ancestry_pruning_drops_exactly_ancestors_of_shared() {
    bolero::check!()
        .with_arbitrary::<(Vec<LooseCommit>, FingerprintSummary)>()
        .for_each(|(local_commits, remote_summary)| {
            let local = Sedimentree::new(vec![], close_ancestry(local_commits));
            let seed = remote_summary.seed();

            let pre_pruning_local_only: Set<CommitId> = local
                .loose_commits()
                .filter(|c| {
                    !remote_summary
                        .commit_fingerprints()
                        .contains(&Fingerprint::new(seed, &c.head()))
                })
                .map(LooseCommit::head)
                .collect();

            let shared_ids: Set<CommitId> = local
                .loose_commits()
                .filter(|c| {
                    remote_summary
                        .commit_fingerprints()
                        .contains(&Fingerprint::new(seed, &c.head()))
                })
                .map(LooseCommit::head)
                .collect();

            let ancestors = local.ancestors_of(&shared_ids);

            let diff = local.diff_remote_fingerprints(remote_summary);
            let post_pruning_local_only: Set<CommitId> = diff
                .local_only_commits
                .iter()
                .map(|(id, _)| **id)
                .collect();

            let actual_dropped: Set<CommitId> = pre_pruning_local_only
                .difference(&post_pruning_local_only)
                .copied()
                .collect();
            let expected_dropped: Set<CommitId> = pre_pruning_local_only
                .intersection(&ancestors)
                .copied()
                .collect();

            assert_eq!(
                actual_dropped, expected_dropped,
                "pruning rule mismatch: actual={actual_dropped:?} expected={expected_dropped:?}",
            );
        });
}
