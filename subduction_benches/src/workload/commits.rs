//! `LooseCommit` generators and canonical DAG shapes.
//!
//! # DAG shapes
//!
//! | Shape            | Use case                                                            |
//! |------------------|---------------------------------------------------------------------|
//! | [`linear_chain`] | Simple sequential history; baseline for scaling                    |
//! | [`merge_heavy`]  | History with periodic merge commits; stress for DAG algorithms     |
//! | [`wide`]         | Many parallel branches; stress for concurrent-writer scenarios     |

use std::collections::BTreeSet;

use sedimentree_core::loose_commit::{id::CommitId, LooseCommit};

use super::{blobs::synthetic_blob_meta, ids};

/// Single synthetic loose commit with the given parents.
///
/// The head / blob-meta seeds are derived deterministically from `seed`.
#[must_use]
pub fn synthetic_commit(seed: u64, parents: BTreeSet<CommitId>) -> LooseCommit {
    let sedimentree_id = ids::sedimentree_id_from_seed(seed);
    let head = ids::commit_id_from_seed(seed.wrapping_add(500_000));
    let blob_meta = synthetic_blob_meta(seed.wrapping_add(1_000_000), 1024);
    LooseCommit::new(sedimentree_id, head, parents, blob_meta)
}

/// Linear chain: each commit has exactly one parent, the previous commit.
///
/// Complexity: O(count) allocation.
#[must_use]
pub fn linear_chain(count: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(count);
    let mut prev = None;

    for i in 0..count {
        let parents = prev.map(|p| BTreeSet::from([p])).unwrap_or_default();
        let commit = synthetic_commit(base_seed.wrapping_add(i as u64), parents);
        prev = Some(commit.head());
        commits.push(commit);
    }

    commits
}

/// DAG with periodic 2-parent merges.
///
/// Every `merge_frequency`-th commit has two parents (the two most recent heads); other commits
/// have one parent. A sliding window of the 10 most recent heads bounds memory.
#[must_use]
pub fn merge_heavy(count: usize, merge_frequency: usize, base_seed: u64) -> Vec<LooseCommit> {
    const RECENT_WINDOW: usize = 10;

    let merge_frequency = merge_frequency.max(1);
    let mut commits = Vec::with_capacity(count);
    let mut recent: Vec<CommitId> = Vec::with_capacity(RECENT_WINDOW);

    for i in 0..count {
        let parents = choose_parents(i, merge_frequency, &recent);
        let commit = synthetic_commit(base_seed.wrapping_add(i as u64), parents);
        recent.push(commit.head());

        if recent.len() > RECENT_WINDOW {
            recent.remove(0);
        }

        commits.push(commit);
    }

    commits
}

/// Wide DAG: `width` independent branches, each `depth_per_branch` commits long.
///
/// Useful for simulating fan-out from many concurrent writers.
#[must_use]
pub fn wide(width: usize, depth_per_branch: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(width.saturating_mul(depth_per_branch));

    for branch in 0..width {
        let branch_seed = base_seed.wrapping_add((branch as u64).wrapping_mul(10_000));
        let mut prev = None;

        for i in 0..depth_per_branch {
            let parents = prev.map(|p| BTreeSet::from([p])).unwrap_or_default();
            let commit = synthetic_commit(branch_seed.wrapping_add(i as u64), parents);
            prev = Some(commit.head());
            commits.push(commit);
        }
    }

    commits
}

/// Internal: decide parents for position `i` given the recent-heads window.
///
/// Returns an empty set for the very first commit; otherwise either the most recent head, or
/// the two most recent heads (on merge boundaries).
fn choose_parents(index: usize, merge_frequency: usize, recent: &[CommitId]) -> BTreeSet<CommitId> {
    if index == 0 {
        return BTreeSet::new();
    }

    let is_merge = index % merge_frequency == 0 && recent.len() >= 2;

    if is_merge {
        let mut last_two = recent.iter().rev().take(2).copied();
        let mut set = BTreeSet::new();

        if let Some(p) = last_two.next() {
            set.insert(p);
        }
        if let Some(p) = last_two.next() {
            set.insert(p);
        }

        return set;
    }

    recent
        .last()
        .copied()
        .map(|p| BTreeSet::from([p]))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linear_chain_has_expected_length() {
        assert_eq!(linear_chain(0, 0).len(), 0);
        assert_eq!(linear_chain(1, 0).len(), 1);
        assert_eq!(linear_chain(100, 7).len(), 100);
    }

    #[test]
    fn linear_chain_is_actually_linear() {
        let commits = linear_chain(10, 0);

        // Every commit after the first should reference exactly one parent, and that parent
        // should be the previous commit's head.
        for pair in commits.windows(2) {
            if let [prev, curr] = pair {
                assert_eq!(curr.parents().len(), 1);
                assert!(
                    curr.parents().contains(&prev.head()),
                    "curr should point at prev.head()",
                );
            }
        }
    }

    #[test]
    fn merge_heavy_produces_some_2_parent_commits() {
        let commits = merge_heavy(50, 5, 0);
        let merges = commits.iter().filter(|c| c.parents().len() == 2).count();
        assert!(merges > 0, "expected at least one merge commit, got 0");
    }

    #[test]
    fn wide_produces_width_times_depth_commits() {
        let commits = wide(5, 10, 0);
        assert_eq!(commits.len(), 50);
    }

    #[test]
    fn wide_branches_are_independent() {
        // Distinct branch seeds ⇒ distinct head sets per branch — sanity check that branches
        // don't accidentally share history.
        let commits = wide(3, 5, 0);
        let heads: BTreeSet<_> = commits.iter().map(|c| c.head()).collect();
        assert_eq!(heads.len(), 15, "all heads should be unique");
    }

    #[test]
    fn merge_frequency_zero_does_not_panic() {
        let _ = merge_heavy(5, 0, 0);
    }
}
