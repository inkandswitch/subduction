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
/// - the first boundary commit `B` encountered otherwise (B is `c`'s
///   range_head along that path).
///
/// `c` is kept iff there is at least one rangeless path, OR at least one
/// path whose first boundary is uncovered.
///
/// Production uses reverse-topological-order accumulation from each tip;
/// the naive form does an independent DFS from each commit. Different
/// algorithm, same answer.
fn prune_loose_commits_naive<M: DepthMetric>(
    commits: &[LooseCommit],
    kept_fragments: &[Fragment],
    m: &M,
) -> BTreeSet<CommitId> {
    // Coverage set built from kept fragments.
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

    // Build child-direction adjacency from parent links. Only include
    // edges between commits in our set; external parent refs are ignored
    // (matches `CommitDag::from_commits`).
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
    // If c is itself a boundary, c's only range head is c itself.
    if m.to_depth(c).is_boundary() {
        return !is_covered(c);
    }

    // Stack-based DFS toward children, stopping at boundaries.
    let mut visited: BTreeSet<CommitId> = BTreeSet::new();
    let mut stack: Vec<CommitId> = vec![c];

    while let Some(current) = stack.pop() {
        if !visited.insert(current) {
            continue;
        }

        if current != c && m.to_depth(current).is_boundary() {
            // First boundary encountered going from c toward children:
            // `current` is one of c's range_heads.
            if !is_covered(current) {
                return true; // uncovered range → keep c
            }
            continue; // don't descend past the boundary
        }

        // Descend to children.
        match child_map.get(&current) {
            None => {
                // current is a tip (no children) and not a boundary →
                // path reached tip without crossing any boundary →
                // c is rangeless via this path → keep c.
                return true;
            }
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

    // No path was rangeless and no first-boundary was uncovered → drop c.
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

/// Loose-commit pruning agrees with the per-commit DFS naive reference.
///
/// Given the same set of kept fragments (production's), production's
/// `simplify`-based pruning and the naive DFS-based pruning must produce
/// the same surviving commit set.
///
/// This is the headline correctness property for the perf change:
/// `simplify` is what was modified, and `prune_loose_commits_naive` is
/// algorithmically distinct (different traversal direction, different
/// per-commit vs per-tip iteration shape).
#[test]
fn loose_commit_pruning_agrees_with_naive() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            // Use production's kept fragments to feed the naive pruner —
            // we want to test the loose-commit pruning step in isolation,
            // not the fragment-minimization step.
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
/// still knowable from the minimized tree.
///
/// "Knowable" means: the `Checkpoint::new(commit_id)` truncation appears
/// somewhere in the tree's combined `Checkpoint`-set (loose-commit
/// heads, fragment heads, fragment boundaries, or fragment checkpoints).
/// This matches the precision `simplify_inner` actually uses for
/// coverage decisions.
///
/// Catches the family of bugs where `minimize` drops a loose commit but
/// no kept fragment retains evidence of its existence — i.e., genuine
/// op-loss as opposed to redundancy elimination.
#[test]
fn minimize_preserves_known_commit_ids() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let known_before = knowable_checkpoints(&tree);
            let known_after = knowable_checkpoints(&prod);

            // We require: known_before is a subset of known_after.
            // (Equality would also be fine, but the kept fragments may
            // legitimately re-state checkpoints from dropped fragments.
            // What we cannot tolerate is the kept set _missing_ anything
            // the input set knew about.)
            let lost: BTreeSet<&Checkpoint> = known_before.difference(&known_after).collect();
            assert!(
                lost.is_empty(),
                "minimize lost knowledge of {} commit(s); first few: {:?}",
                lost.len(),
                lost.iter().take(5).collect::<Vec<_>>(),
            );
        });
}

/// Loose-commit pruning never drops an unknowable commit.
///
/// Specifically: if `minimize` drops a loose commit C from `t.loose_commits()`,
/// then `Checkpoint::new(C.head())` must appear in the kept fragments'
/// combined coverage (otherwise we've genuinely lost C from the tree).
///
/// This is the per-commit version of [`minimize_preserves_known_commit_ids`]
/// — same invariant viewed from the loose-commit side. Strictly stronger
/// against the failure mode "drop a commit even though no fragment
/// covers it".
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

/// All `CommitId`s reachable from the original tree's heads (via parent
/// pointers in loose commits, plus fragment heads/boundaries) are still
/// knowable in the minimized tree.
///
/// Stronger than `minimize_preserves_known_commit_ids` because it walks
/// the full ancestor graph from heads, not just the directly-stored
/// `CommitId`s. This is the closest property we can write to "an
/// Automerge document re-derived from the minimized tree contains all
/// the same change hashes" without actually pulling in Automerge.
#[test]
fn all_reachable_commit_ids_remain_knowable() {
    bolero::check!()
        .with_arbitrary::<ArbitraryDag>()
        .for_each(|ArbitraryDag { tree }| {
            let prod = tree.minimize(&CountLeadingZeroBytes);

            let reachable_before = reachable_commit_ids(&tree);
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
/// This is order-independent and so passes regardless of HashMap
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

/// Production `minimize` is deterministic *within* a process.
///
/// Multiple calls on the same input within one process must produce
/// equal `Sedimentree`s.
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

/// Production `minimize` is deterministic *across constructions*.
///
/// Building the same logical `Sedimentree` from two different orderings
/// of input fragments and commits must produce equal `minimize` outputs.
///
/// This is the regression test for the cross-process non-determinism
/// bug. Different `Sedimentree::new` calls produce internal `Map`s with
/// different (hasher-state-dependent) iteration orders. Before the fix,
/// `minimize` leaked that order into its output by iterating
/// `self.fragments.values()` to populate a per-depth `Vec`, so two
/// `Sedimentree`s constructed differently could yield different
/// `minimize` outputs even with identical input fragments and commits.
///
/// Two `Sedimentree::new` invocations within one process can in
/// principle still hash identically, so to be sure we shuffle the
/// inputs deterministically and assert agreement.
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

/// `minimize` is idempotent: minimizing twice yields the same tree.
///
/// Regression test for the determinism fix. Before the fix, this
/// failed because `tree.minimize()` produced N fragments, but
/// re-minimizing produced N-1 — the first pass did not reach a fixed
/// point because `Map<CommitId, Fragment>` iteration order varies
/// across `Sedimentree::new` invocations, so re-minimization picked a
/// different mutual-dominance representative.
///
/// The fix sorts fragments by head bytes before processing, making
/// the per-depth iteration order independent of `HashMap` hasher state.
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
///
/// This is the headline user-facing property. Two peers with identical
/// content must compute identical `minimal_hash` values, regardless of
/// how they constructed their local `Sedimentree` (which depends on
/// `HashMap` iteration order — a per-process random thing).
///
/// `minimal_hash` calls `self.minimize(m)` internally, so this property
/// is the direct downstream consequence of the determinism fix. Before
/// the fix, two peers with identical state could compute different
/// hashes and (depending on consumer logic) believe they were out of
/// sync, repeatedly trigger redundant resync, or fail equality checks.
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

/// `fingerprint_summarize` of a minimized tree is stable across
/// constructions.
///
/// `fingerprint_summarize` itself is order-independent (uses `BTreeSet`
/// internally), but the typical sync flow is `tree.minimize().fingerprint_summarize()`,
/// where `minimize` was the source of non-determinism. This property
/// pins the composed behaviour: two peers with identical content
/// produce identical wire summaries.
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
