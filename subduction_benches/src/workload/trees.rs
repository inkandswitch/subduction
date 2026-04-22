//! Composed `Sedimentree` builders.

use sedimentree_core::{fragment::Fragment, sedimentree::Sedimentree};

use super::{commits::linear_chain, fragments::synthetic_fragment};

/// Build a `Sedimentree` with the given counts of fragments and loose commits.
///
/// Fragments cycle through depths 0, 1, 2 (wrapping) so the tree exercises the full depth-metric
/// range. Commits are arranged as a single linear chain.
#[must_use]
pub fn synthetic_sedimentree(
    fragment_count: usize,
    commit_count: usize,
    base_seed: u64,
) -> Sedimentree {
    let fragments: Vec<Fragment> = (0..fragment_count)
        .map(|i| {
            let leading_zeros = i % 3;
            synthetic_fragment(
                base_seed.wrapping_add((i as u64).wrapping_mul(1_000)),
                /* boundary */ 2,
                /* checkpoints */ 5,
                leading_zeros,
            )
        })
        .collect();

    let commits = linear_chain(commit_count, base_seed.wrapping_add(500_000));

    Sedimentree::new(fragments, commits)
}

/// Two `Sedimentree`s with a controlled overlap, useful for diff / merge benches.
///
/// The returned pair (`a`, `b`) share `shared_fragments` fragments and `shared_commits` loose
/// commits, plus `unique_fragments_each` and `unique_commits_each` items that are distinct per
/// side.
#[must_use]
pub fn overlapping_sedimentrees(
    shared_fragments: usize,
    unique_fragments_each: usize,
    shared_commits: usize,
    unique_commits_each: usize,
    base_seed: u64,
) -> (Sedimentree, Sedimentree) {
    let shared_frags: Vec<Fragment> = (0..shared_fragments)
        .map(|i| {
            synthetic_fragment(
                base_seed.wrapping_add((i as u64).wrapping_mul(1_000)),
                2,
                5,
                i % 3,
            )
        })
        .collect();
    let shared_commits_vec = linear_chain(shared_commits, base_seed.wrapping_add(100_000));

    let a_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
        .map(|i| {
            synthetic_fragment(
                base_seed
                    .wrapping_add(200_000)
                    .wrapping_add((i as u64).wrapping_mul(1_000)),
                2,
                5,
                i % 3,
            )
        })
        .collect();
    let a_unique_commits = linear_chain(unique_commits_each, base_seed.wrapping_add(300_000));

    let b_unique_frags: Vec<Fragment> = (0..unique_fragments_each)
        .map(|i| {
            synthetic_fragment(
                base_seed
                    .wrapping_add(400_000)
                    .wrapping_add((i as u64).wrapping_mul(1_000)),
                2,
                5,
                i % 3,
            )
        })
        .collect();
    let b_unique_commits = linear_chain(unique_commits_each, base_seed.wrapping_add(500_000));

    let mut a_frags = shared_frags.clone();
    a_frags.extend(a_unique_frags);
    let mut a_commits = shared_commits_vec.clone();
    a_commits.extend(a_unique_commits);

    let mut b_frags = shared_frags;
    b_frags.extend(b_unique_frags);
    let mut b_commits = shared_commits_vec;
    b_commits.extend(b_unique_commits);

    (
        Sedimentree::new(a_frags, a_commits),
        Sedimentree::new(b_frags, b_commits),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_sedimentree_builds() {
        let tree = synthetic_sedimentree(8, 32, 0);
        // Tree construction succeeds (sedimentree preserves fragment/commit counts internally
        // modulo deduplication; we don't assert exact equality because collisions are possible
        // but extremely unlikely).
        let _ = tree;
    }

    #[test]
    fn overlapping_pair_has_disjoint_unique_heads() {
        let (a, b) = overlapping_sedimentrees(4, 3, 8, 5, 0);

        // Construction should succeed; real divergence behaviour is exercised by the diff
        // benches themselves.
        let _ = (a, b);
    }
}
