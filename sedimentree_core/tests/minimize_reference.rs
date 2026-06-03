//! Naive-reference equivalence tests for [`Sedimentree::minimize`].
//!
//! Tests the production minimize algorithm against an independent naive
//! reference, plus a battery of structural-correctness properties.
//!
//! ## Properties at a glance
//!
//! - **Equivalence to naive reference**
//!   ([`loose_commit_pruning_agrees_with_naive`]): the naive impl computes
//!   the surviving commit set via per-commit DFS toward children
//!   (production uses reverse-topo accumulation from tips). Different
//!   algorithm, same answer.
//!
//! - **Coverage preservation** ([`fragment_minimization_preserves_coverage`]):
//!   the combined `head ∪ boundary ∪ checkpoints` `Checkpoint`-set of
//!   kept fragments equals that of the input fragments. Minimization
//!   doesn't lose data via the fragment side.
//!
//! - **No data loss via reachability**
//!   ([`minimize_preserves_known_commit_ids`]): every `CommitId` known
//!   to the original tree (as commit head, fragment head, fragment
//!   boundary, or fragment checkpoint) is still known to the minimized
//!   tree. The Checkpoint-truncation precision is honoured.
//!
//! - **Determinism** ([`minimize_is_deterministic_within_process`],
//!   [`minimize_is_deterministic_across_constructions`],
//!   [`minimize_is_idempotent`]): same content → same output, regardless
//!   of construction order or call count.
//!
//! - **Downstream stability**
//!   ([`minimal_hash_is_stable_across_constructions`],
//!   [`fingerprint_summary_of_minimized_tree_is_stable`]): the
//!   user-visible derived values are stable.
//!
//! Requires the `test_utils` and `bolero` features.

#![cfg(all(feature = "test_utils", feature = "bolero"))]
#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::collections::{BTreeMap, BTreeSet};

use sedimentree_core::{
    commit::CountLeadingZeroBytes,
    crypto::fingerprint::FingerprintSeed,
    depth::DepthMetric,
    fragment::{Fragment, checkpoint::Checkpoint},
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
    test_utils::ArbitraryDag,
};

// ─── Naive loose-commit pruning ─────────────────────────────────────────────

/// Per-commit DFS reference for loose-commit pruning.
///
/// For each commit `c`, walk all paths toward children, recording for each
/// terminal path either:
/// - "rangeless" if the path reaches a tip without crossing any boundary,
/// - the first boundary commit `B` encountered otherwise (`B` is `c`'s
///   `range_head` along that path).
///
/// `c` is kept iff there is at least one rangeless path, OR at least one
/// path whose first boundary is uncovered.
fn prune_loose_commits_naive<M: DepthMetric>(
    commits: &[LooseCommit],
    kept_fragments: &[Fragment],
    m: &M,
) -> BTreeSet<CommitId> {
    let mut covered_range_heads: BTreeSet<CommitId> = BTreeSet::new();
    let mut covered_checkpoints: BTreeSet<Checkpoint> = BTreeSet::new();
    for f in kept_fragments {
        covered_range_heads.insert(f.head());
        covered_range_heads.extend(f.boundary().iter().copied());
        covered_checkpoints.extend(f.checkpoints().iter().copied());
    }
    let is_covered = |range_head: CommitId| -> bool {
        covered_range_heads.contains(&range_head)
            || covered_checkpoints.contains(&Checkpoint::new(range_head))
    };

    // Child-direction adjacency from parent links. External parents are
    // ignored, matching `CommitDag::from_commits`.
    let commit_id_set: BTreeSet<CommitId> = commits.iter().map(LooseCommit::head).collect();
    let mut child_map: BTreeMap<CommitId, BTreeSet<CommitId>> = BTreeMap::new();
    for c in commits {
        for parent in c.parents() {
            if commit_id_set.contains(parent) {
                child_map.entry(*parent).or_default().insert(c.head());
            }
        }
    }

    commits
        .iter()
        .filter(|c| should_keep_naive(c.head(), &child_map, m, &is_covered))
        .map(LooseCommit::head)
        .collect()
}

fn should_keep_naive<M: DepthMetric, F: Fn(CommitId) -> bool>(
    c: CommitId,
    child_map: &BTreeMap<CommitId, BTreeSet<CommitId>>,
    m: &M,
    is_covered: &F,
) -> bool {
    // A boundary commit is its own range head.
    if m.to_depth(c).is_boundary() {
        return !is_covered(c);
    }

    let mut visited: BTreeSet<CommitId> = BTreeSet::new();
    let mut stack: Vec<CommitId> = vec![c];

    while let Some(current) = stack.pop() {
        if !visited.insert(current) {
            continue;
        }

        // First boundary descending from c — it's one of c's range
        // heads. Don't descend past it.
        if current != c && m.to_depth(current).is_boundary() {
            if !is_covered(current) {
                return true;
            }
            continue;
        }

        match child_map.get(&current) {
            // Reached a tip without crossing a boundary: c is rangeless
            // via this path.
            None => return true,
            Some(kids) if kids.is_empty() => return true,
            Some(kids) => {
                for k in kids {
                    if !visited.contains(k) {
                        stack.push(*k);
                    }
                }
            }
        }
    }

    false
}

// ─── Coverage helpers ───────────────────────────────────────────────────────

/// The "coverage" of a fragment set: the union of every fragment's head,
/// boundary commits, and checkpoints (each as `Checkpoint`-truncated values).
///
/// Two fragment sets with equal coverage are functionally equivalent for
/// the purposes of [`Sedimentree::minimize`]'s loose-commit pruning: any
/// loose commit dropped under one is also dropped under the other.
fn coverage<'a, I: IntoIterator<Item = &'a Fragment>>(fragments: I) -> BTreeSet<Checkpoint> {
    let mut cov: BTreeSet<Checkpoint> = BTreeSet::new();
    for f in fragments {
        cov.insert(Checkpoint::new(f.head()));
        for b in f.boundary() {
            cov.insert(Checkpoint::new(*b));
        }
        cov.extend(f.checkpoints().iter().copied());
    }
    cov
}

// ─── Properties ─────────────────────────────────────────────────────────────

/// Production `simplify` and the per-commit DFS reference produce the
/// same surviving commit set, given the same kept fragments. Headline
/// correctness property for `simplify`.
#[test]
fn loose_commit_pruning_agrees_with_naive() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            // Feed production's kept fragments to the naive pruner so
            // we test loose-commit pruning in isolation from
            // fragment-minimization.
            let kept_fragments: Vec<Fragment> = prod.fragments().cloned().collect();
            let original_commits: Vec<LooseCommit> = tree.loose_commits().cloned().collect();

            let prod_commits: BTreeSet<CommitId> =
                prod.loose_commits().map(LooseCommit::head).collect();
            let naive_commits = prune_loose_commits_naive(
                &original_commits,
                &kept_fragments,
                &CountLeadingZeroBytes,
            );

            assert_eq!(
                prod_commits, naive_commits,
                "loose-commit pruning diverges between production simplify and naive DFS"
            );
        });
}

/// **No data loss.** Every `CommitId` known to the original tree is
/// still knowable from the minimized tree, where "knowable" means
/// `Checkpoint::new(id)` appears in the tree's combined coverage
/// (loose-commit heads, fragment heads/boundaries/checkpoints) — the
/// precision `simplify` uses.
///
/// Catches `minimize` dropping a loose commit when no kept fragment
/// retains evidence of it (genuine op-loss, not redundancy elimination).
#[test]
fn minimize_preserves_known_commit_ids() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let known_before = knowable_checkpoints(tree);
            let known_after = knowable_checkpoints(&prod);

            // `known_after` may be a strict superset (kept fragments can
            // re-state checkpoints from dropped peers); we only require
            // that nothing the input knew about is missing.
            let lost: BTreeSet<&Checkpoint> = known_before.difference(&known_after).collect();
            assert!(
                lost.is_empty(),
                "minimize lost knowledge of {} commit(s); first few: {:?}",
                lost.len(),
                lost.iter().take(5).collect::<Vec<_>>(),
            );
        });
}

/// Per-commit version of [`minimize_preserves_known_commit_ids`]: any
/// loose commit `C` that `minimize` drops must have
/// `Checkpoint::new(C.head())` in some kept fragment's coverage.
#[test]
fn dropped_loose_commits_are_covered_by_kept_fragments() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let kept_loose: BTreeSet<CommitId> =
                prod.loose_commits().map(LooseCommit::head).collect();

            let kept_fragment_coverage: BTreeSet<Checkpoint> = coverage(prod.fragments());

            for c in tree.loose_commits() {
                let id = c.head();
                if !kept_loose.contains(&id) {
                    let cp = Checkpoint::new(id);
                    assert!(
                        kept_fragment_coverage.contains(&cp),
                        "loose commit {id:?} was dropped but its checkpoint \
                         is not in any kept fragment"
                    );
                }
            }
        });
}

/// Every `CommitId` reachable from the original tree's heads (via
/// loose-commit parent pointers and fragment heads/boundaries) is still
/// knowable in the minimized tree. Stronger than
/// [`minimize_preserves_known_commit_ids`] — walks the full ancestor
/// graph, not just directly-stored `CommitId`s. The closest pure-
/// `sedimentree_core` analogue of "Automerge round-trip preserves all
/// change hashes".
#[test]
fn all_reachable_commit_ids_remain_knowable() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let reachable_before = reachable_commit_ids(tree);
            let known_after = knowable_checkpoints(&prod);

            for id in &reachable_before {
                let cp = Checkpoint::new(*id);
                assert!(
                    known_after.contains(&cp),
                    "commit {id:?} reachable from input tree's heads but \
                     not knowable from minimized tree"
                );
            }
        });
}

/// All `Checkpoint`s a tree carries, considering loose commits AND
/// fragments at the truncation precision actually used by `simplify`.
fn knowable_checkpoints(tree: &Sedimentree) -> BTreeSet<Checkpoint> {
    let mut s: BTreeSet<Checkpoint> = BTreeSet::new();
    for c in tree.loose_commits() {
        s.insert(Checkpoint::new(c.head()));
    }
    for f in tree.fragments() {
        s.insert(Checkpoint::new(f.head()));
        for b in f.boundary() {
            s.insert(Checkpoint::new(*b));
        }
        s.extend(f.checkpoints().iter().copied());
    }
    s
}

/// Every `CommitId` reachable by walking `LooseCommit::parents()` backward
/// from any commit in the tree, plus every `Fragment` head and boundary.
fn reachable_commit_ids(tree: &Sedimentree) -> BTreeSet<CommitId> {
    let mut all: BTreeSet<CommitId> = BTreeSet::new();
    let mut stack: Vec<CommitId> = Vec::new();

    for c in tree.loose_commits() {
        all.insert(c.head());
        stack.push(c.head());
    }
    for f in tree.fragments() {
        all.insert(f.head());
        stack.push(f.head());
        for b in f.boundary() {
            all.insert(*b);
            stack.push(*b);
        }
    }

    // Walk parent pointers transitively.
    let parents_of: BTreeMap<CommitId, BTreeSet<CommitId>> = tree
        .loose_commits()
        .map(|c| (c.head(), c.parents().iter().copied().collect()))
        .collect();

    while let Some(id) = stack.pop() {
        if let Some(parents) = parents_of.get(&id) {
            for p in parents {
                if all.insert(*p) {
                    stack.push(*p);
                }
            }
        }
    }

    all
}

/// Fragment minimization preserves total coverage.
///
/// Whichever fragments production picks among mutual-dominance
/// alternatives, the combined coverage (head ∪ boundary ∪ checkpoints,
/// `Checkpoint`-truncated) of the kept set must equal that of the input
/// set. Otherwise minimization would lose information.
///
/// This is order-independent and so passes regardless of `HashMap`
/// iteration order.
#[test]
fn fragment_minimization_preserves_coverage() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let cov_before = coverage(tree.fragments());
            let cov_after = coverage(prod.fragments());

            assert_eq!(
                cov_before,
                cov_after,
                "fragment minimization lost coverage: \
                 before={} entries, after={} entries",
                cov_before.len(),
                cov_after.len()
            );
        });
}

/// Multiple `minimize` calls on the same input within one process
/// produce equal `Sedimentree`s.
#[test]
fn minimize_is_deterministic_within_process() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let m1 = tree.minimize(&CountLeadingZeroBytes);
            let m2 = tree.minimize(&CountLeadingZeroBytes);
            assert_eq!(m1, m2, "minimize not deterministic within a process");
        });
}

/// For any tree, `minimize_in_place` produces the same result as the
/// rebuild-based `minimize`.
#[test]
fn minimize_in_place_equals_minimize() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let rebuilt = tree.minimize(&CountLeadingZeroBytes);

            let mut in_place = tree.clone();
            in_place.minimize_in_place(&CountLeadingZeroBytes);

            assert_eq!(
                in_place, rebuilt,
                "minimize_in_place diverged from rebuild minimize"
            );
        });
}

/// `minimize_in_place` is idempotent: applying it to an already-minimal tree
/// leaves it unchanged (and still equal to a fresh rebuild minimize).
#[test]
fn minimize_in_place_is_idempotent() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let mut t = tree.clone();
            t.minimize_in_place(&CountLeadingZeroBytes);
            let once = t.clone();
            t.minimize_in_place(&CountLeadingZeroBytes);
            assert_eq!(t, once, "minimize_in_place not idempotent");
            assert_eq!(
                t,
                tree.minimize(&CountLeadingZeroBytes),
                "idempotent in-place result differs from rebuild minimize"
            );
        });
}

/// Building the same logical `Sedimentree` from differently-ordered
/// inputs produces equal `minimize` outputs. Regression test for the
/// cross-process non-determinism bug: pre-fix, `minimize` iterated
/// `self.fragments.values()` to populate a per-depth `Vec`, leaking the
/// `HashMap`'s hasher-state-dependent order into its output.
#[test]
fn minimize_is_deterministic_across_constructions() {
    use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};

    bolero::check!()
        .with_arbitrary::<(ArbitraryDag, u64)>()
        .for_each(|(ArbitraryDag { tree }, shuffle_seed)| {
            let mut frags: Vec<Fragment> = tree.fragments().cloned().collect();
            let mut commits: Vec<LooseCommit> = tree.loose_commits().cloned().collect();

            let mut rng = SmallRng::seed_from_u64(*shuffle_seed);
            frags.shuffle(&mut rng);
            commits.shuffle(&mut rng);

            let alt = Sedimentree::new(frags, commits);
            let m_orig = tree.minimize(&CountLeadingZeroBytes);
            let m_alt = alt.minimize(&CountLeadingZeroBytes);

            assert_eq!(
                m_orig, m_alt,
                "minimize output depends on Sedimentree construction order"
            );
        });
}

/// `tree.minimize().minimize() == tree.minimize()`. Pre-fix this could
/// fail: `Sedimentree::new` produced a fresh `HashMap` whose iteration
/// order differed from the input's, so re-minimize picked a different
/// mutual-dominance representative.
#[test]
fn minimize_is_idempotent() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let once = tree.minimize(&CountLeadingZeroBytes);
            let twice = once.minimize(&CountLeadingZeroBytes);
            assert_eq!(once, twice, "minimize is not idempotent");
        });
}

/// With no fragments, `minimize` keeps every commit.
#[test]
fn minimize_with_no_fragments_keeps_all_commits() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let commits: Vec<LooseCommit> = tree.loose_commits().cloned().collect();
            let stripped = Sedimentree::new(vec![], commits.clone());

            let minimized = stripped.minimize(&CountLeadingZeroBytes);
            let prod_set: BTreeSet<CommitId> =
                minimized.loose_commits().map(LooseCommit::head).collect();
            let orig_set: BTreeSet<CommitId> = commits.iter().map(LooseCommit::head).collect();

            assert_eq!(prod_set, orig_set, "no fragments → all commits kept");
        });
}

// ─── Downstream stability ───────────────────────────────────────────────────

/// `MinimalTreeHash` is stable across `Sedimentree` constructions.
/// Headline user-facing consequence of the determinism fix:
/// `minimal_hash` calls `minimize` internally, so two peers with
/// identical content must compute identical hashes regardless of local
/// `HashMap` ordering.
#[test]
fn minimal_hash_is_stable_across_constructions() {
    use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};

    bolero::check!()
        .with_arbitrary::<(ArbitraryDag, u64)>()
        .for_each(|(ArbitraryDag { tree }, shuffle_seed)| {
            let mut frags: Vec<Fragment> = tree.fragments().cloned().collect();
            let mut commits: Vec<LooseCommit> = tree.loose_commits().cloned().collect();

            let mut rng = SmallRng::seed_from_u64(*shuffle_seed);
            frags.shuffle(&mut rng);
            commits.shuffle(&mut rng);

            let alt = Sedimentree::new(frags, commits);
            let h_orig = tree.minimal_hash(&CountLeadingZeroBytes);
            let h_alt = alt.minimal_hash(&CountLeadingZeroBytes);

            assert_eq!(
                h_orig.as_bytes(),
                h_alt.as_bytes(),
                "minimal_hash differs for Sedimentrees with identical content but different construction order"
            );
        });
}

/// `tree.minimize().fingerprint_summarize(seed)` is stable across
/// constructions. `fingerprint_summarize` is itself order-independent;
/// this pins the composed sync-flow behaviour against `minimize`'s
/// pre-fix non-determinism.
#[test]
fn fingerprint_summary_of_minimized_tree_is_stable() {
    use rand::{SeedableRng, rngs::SmallRng, seq::SliceRandom};

    bolero::check!()
        .with_arbitrary::<(ArbitraryDag, u64)>()
        .for_each(|(ArbitraryDag { tree }, shuffle_seed)| {
            let mut frags: Vec<Fragment> = tree.fragments().cloned().collect();
            let mut commits: Vec<LooseCommit> = tree.loose_commits().cloned().collect();

            let mut rng = SmallRng::seed_from_u64(*shuffle_seed);
            frags.shuffle(&mut rng);
            commits.shuffle(&mut rng);

            let alt = Sedimentree::new(frags, commits);

            // Use a fixed seed so the only variable is the tree.
            let seed = FingerprintSeed::new(0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210);

            let s_orig = tree
                .minimize(&CountLeadingZeroBytes)
                .fingerprint_summarize(&seed);
            let s_alt = alt
                .minimize(&CountLeadingZeroBytes)
                .fingerprint_summarize(&seed);

            assert_eq!(
                s_orig, s_alt,
                "fingerprint summary of minimized tree differs across constructions"
            );
        });
}
