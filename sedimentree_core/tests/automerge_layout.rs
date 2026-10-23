//! Sedimentree over Automerge's own fragment layout.
//!
//! The other suites build fragments with sedimentree's decomposition, where
//! a fragment carries every ancestor of its head down to its boundary.
//! Automerge forms fragments differently (`change_graph.rs`,
//! `cache_fragment`): a fragment carries the ancestors of its head that the
//! fragments it was formed on top of do not, so it also leaves out the
//! ancestors of its boundary that a concurrent branch reaches without
//! crossing a boundary commit. A walk down from a fragment's head can
//! therefore reach commits the fragment does not carry, and these tests
//! replay random DAGs with forks and merges through an emulation of that
//! layout to check that no such walk hides data.
//!
//! # Known bug UNCARRIED: `minimize` can drop commits no kept fragment carries
//!
//! `minimize` drops a loose commit when every range it belongs to is
//! covered by a kept fragment. A fragment formed on top of others (its
//! boundary) does not carry the ancestors of that boundary a concurrent
//! branch reaches, but those commits are in its range all the same. As long
//! as the fragments it was formed on are held too, one of them carries
//! them. A node holding the fragment without them, which arrival out of
//! formation order produces until they arrive, drops those commits from
//! what it offers, and its readers cannot get them from it. They stay in
//! storage and held.

#![allow(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stderr
)]

use std::collections::BTreeSet;

use rand::{Rng, SeedableRng, rngs::SmallRng};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::fingerprint::FingerprintSeed,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{Sedimentree, minimized::MinimizedSedimentree},
    test_utils::sync_budget::known_bug,
};

/// Random histories per test, alternating 300 and 800 commits.
const SEEDS: u64 = 1_000;

const SID: SedimentreeId = SedimentreeId::new([0x7C; 32]);

// ════════════════════════════════════════════════════════════════════════
// Random DAGs in Automerge's fragment layout
// ════════════════════════════════════════════════════════════════════════

/// A random DAG: commit `i`'s parents all precede it.
struct Dag {
    parents: Vec<Vec<usize>>,
    depth: Vec<u8>,
    /// Ancestors-or-self of each commit.
    ancestors: Vec<BTreeSet<usize>>,
}

impl Dag {
    /// `n` commits on up to four concurrent branches that fork from recent
    /// commits and merge pairwise; about one commit in 20 has depth 1 and
    /// one in 150 depth 2.
    fn random(rng: &mut SmallRng, n: usize) -> Self {
        let mut parents: Vec<Vec<usize>> = vec![Vec::new()];
        let mut heads = vec![0usize];
        for i in 1..n {
            let r: f64 = rng.gen_range(0.0..1.0);
            if heads.len() >= 2 && (r < 0.08 || heads.len() > 4) {
                let a = heads.swap_remove(rng.gen_range(0..heads.len()));
                let b = heads.swap_remove(rng.gen_range(0..heads.len()));
                parents.push(vec![a, b]);
                heads.push(i);
            } else if r < 0.18 {
                let p = rng.gen_range(i.saturating_sub(30)..i);
                heads.retain(|h| *h != p);
                parents.push(vec![p]);
                heads.push(i);
            } else {
                let k = rng.gen_range(0..heads.len());
                parents.push(vec![heads[k]]);
                heads[k] = i;
            }
        }
        let depth = (0..n)
            .map(|i| match rng.gen_range(0..300) {
                _ if i == 0 => 0,
                0..2 => 2,
                2..17 => 1,
                _ => 0,
            })
            .collect();
        let mut ancestors: Vec<BTreeSet<usize>> = Vec::with_capacity(n);
        for (i, ps) in parents.iter().enumerate() {
            let mut a = BTreeSet::from([i]);
            for p in ps {
                a.extend(ancestors[*p].iter().copied());
            }
            ancestors.push(a);
        }
        Self {
            parents,
            depth,
            ancestors,
        }
    }

    const fn len(&self) -> usize {
        self.parents.len()
    }

    fn id(&self, i: usize) -> CommitId {
        let mut b = [0u8; 32];
        let d = self.depth[i] as usize;
        b[d] = 0xC0;
        b[d + 1..d + 9].copy_from_slice(&(i as u64).to_le_bytes());
        CommitId::new(b)
    }

    fn ids<'a>(&'a self, commits: impl IntoIterator<Item = &'a usize>) -> BTreeSet<CommitId> {
        commits.into_iter().map(|i| self.id(*i)).collect()
    }

    fn blob(i: usize) -> Blob {
        Blob::new((i as u64).to_le_bytes().to_vec())
    }

    fn commit(&self, i: usize) -> LooseCommit {
        LooseCommit::new(
            SID,
            self.id(i),
            self.parents[i].iter().map(|p| self.id(*p)).collect(),
            BlobMeta::new(&Self::blob(i)),
        )
    }
}

/// A fragment as Automerge forms it.
#[derive(Clone)]
struct AmFragment {
    head: usize,
    /// The fragments it was formed on top of: every listed fragment of the
    /// same or a higher level that its head covers. Its boundary.
    deps: Vec<usize>,
    /// Ancestors of the head that no dep covers.
    members: BTreeSet<usize>,
}

impl AmFragment {
    fn to_fragment(&self, dag: &Dag) -> Fragment {
        let checkpoints: Vec<CommitId> = self
            .members
            .iter()
            .filter(|m| dag.depth[**m] > 0)
            .map(|m| dag.id(*m))
            .collect();
        let payload: Vec<u8> = self
            .members
            .iter()
            .flat_map(|m| Dag::blob(*m).into_contents())
            .collect();
        Fragment::new(
            SID,
            dag.id(self.head),
            dag.ids(&self.deps),
            &checkpoints,
            BlobMeta::new(&Blob::new(payload)),
        )
    }
}

/// Every fragment Automerge forms as the commits of `dag` are added in
/// order, superseded ones included. Forming a fragment supersedes the
/// shallower listed fragments it covers.
fn am_fragments(dag: &Dag) -> Vec<AmFragment> {
    let mut listed: Vec<AmFragment> = Vec::new();
    let mut all = Vec::new();
    for i in 0..dag.len() {
        let level = dag.depth[i];
        if level == 0 {
            continue;
        }
        let (covered, rest): (Vec<_>, Vec<_>) = listed
            .drain(..)
            .partition(|f| dag.ancestors[i].contains(&f.head));
        listed = rest;
        let deps: Vec<AmFragment> = covered
            .into_iter()
            .filter(|f| dag.depth[f.head] >= level)
            .collect();
        let below: BTreeSet<usize> = deps
            .iter()
            .flat_map(|d| dag.ancestors[d.head].iter().copied())
            .collect();
        let fragment = AmFragment {
            head: i,
            deps: deps.iter().map(|d| d.head).collect(),
            members: dag.ancestors[i].difference(&below).copied().collect(),
        };
        listed.extend(deps);
        listed.push(fragment.clone());
        all.push(fragment);
    }
    all
}

/// A random subset of `fragments` that holds each one only together with
/// the fragments it was formed on, as a node that received them in the
/// order they were formed would.
fn dependency_closed<'a>(rng: &mut SmallRng, fragments: &'a [AmFragment]) -> Vec<&'a AmFragment> {
    let p: f64 = rng.gen_range(0.0..1.0);
    let mut held = BTreeSet::new();
    let mut subset = Vec::new();
    for f in fragments {
        if f.deps.iter().all(|d| held.contains(d)) && rng.gen_bool(p) {
            held.insert(f.head);
            subset.push(f);
        }
    }
    subset
}

/// The fragments formed first, up to a random point.
fn formed_first<'a>(rng: &mut SmallRng, fragments: &'a [AmFragment]) -> Vec<&'a AmFragment> {
    let k = rng.gen_range(0..=fragments.len());
    fragments.iter().take(k).collect()
}

/// A random subset of `fragments`, regardless of what each was formed on.
fn any_subset<'a>(rng: &mut SmallRng, fragments: &'a [AmFragment]) -> Vec<&'a AmFragment> {
    let p: f64 = rng.gen_range(0.0..1.0);
    fragments.iter().filter(|_| rng.gen_bool(p)).collect()
}

fn tree(dag: &Dag, fragments: &[&AmFragment], loose: &BTreeSet<usize>) -> Sedimentree {
    Sedimentree::new(
        fragments.iter().map(|f| f.to_fragment(dag)).collect(),
        loose.iter().map(|c| dag.commit(*c)).collect(),
    )
}

// ════════════════════════════════════════════════════════════════════════
// Offering: a responder withholds only what the requestor has
// ════════════════════════════════════════════════════════════════════════

/// A responder doesn't offer the loose commits it takes to be carried by a
/// requestor's fragment it lacks but whose head it holds loose. Each
/// withheld commit must really be the requestor's: held loose, carried by
/// one of its fragments, or carried by a fragment the same diff offers.
///
/// The responder is a relay fed by writers that upload a new fragment's
/// head loose before the fragment: it holds a dependency-closed set of
/// fragments, every commit of a causal prefix loose unless a held fragment
/// carries it (then only some), and every fragment head in that prefix
/// loose. The requestor holds arbitrary fragments and loose commits.
#[test]
fn withheld_commits_are_ones_the_requestor_has() {
    let metric = CountLeadingZeroBytes;
    let mut withheld_total = 0usize;
    for seed in 0..SEEDS {
        let mut rng = SmallRng::seed_from_u64(seed);
        let dag = Dag::random(&mut rng, if seed % 2 == 0 { 300 } else { 800 });
        let fragments = am_fragments(&dag);
        let n = dag.len();

        let relay_fragments = dependency_closed(&mut rng, &fragments);
        let carried: BTreeSet<usize> = relay_fragments
            .iter()
            .flat_map(|f| f.members.iter().copied())
            .collect();
        let keep_carried: f64 = rng.gen_range(0.3..1.0);
        let cut = rng.gen_range(n / 2..=n);
        let mut relay_loose: BTreeSet<usize> = (0..cut)
            .filter(|i| dag.depth[*i] == 0 && (!carried.contains(i) || rng.gen_bool(keep_carried)))
            .collect();
        relay_loose.extend(fragments.iter().map(|f| f.head).filter(|h| *h < cut));
        let relay = tree(&dag, &relay_fragments, &relay_loose);

        let fragment_share: f64 = rng.gen_range(0.0..1.0);
        let loose_share: f64 = rng.gen_range(0.0..0.8);
        let requestor_fragments: Vec<&AmFragment> = fragments
            .iter()
            .filter(|_| rng.gen_bool(fragment_share))
            .collect();
        let requestor_loose: BTreeSet<usize> =
            (0..n).filter(|_| rng.gen_bool(loose_share)).collect();
        let summary = tree(&dag, &requestor_fragments, &requestor_loose)
            .fingerprint_summarize(&FingerprintSeed::new(seed, seed ^ 0x55));

        let plain: BTreeSet<CommitId> = relay
            .minimize(&metric)
            .diff_remote_fingerprints(&summary)
            .local_only_commits
            .iter()
            .map(|(id, _)| **id)
            .collect();
        let mut resident = MinimizedSedimentree::new(relay);
        let diff = resident.responder_diff(&metric, &summary);
        let offered: BTreeSet<CommitId> =
            diff.local_only_commits.iter().map(|(id, _)| **id).collect();
        let offered_fragments: BTreeSet<CommitId> = diff
            .local_only_fragments
            .iter()
            .map(|(id, _)| **id)
            .collect();

        let mut requestor_has = dag.ids(&requestor_loose);
        for f in &fragments {
            if requestor_fragments.iter().any(|r| r.head == f.head)
                || offered_fragments.contains(&dag.id(f.head))
            {
                requestor_has.extend(dag.ids(&f.members));
            }
        }
        let withheld: BTreeSet<CommitId> = plain.difference(&offered).copied().collect();
        let missing: Vec<&CommitId> = withheld
            .iter()
            .filter(|c| !requestor_has.contains(c))
            .collect();
        assert!(
            missing.is_empty(),
            "seed {seed}: withheld {} commit(s) the requestor lacks: {missing:?}",
            missing.len()
        );
        withheld_total += withheld.len();
    }
    eprintln!("withheld {withheld_total} commits over {SEEDS} seeds");
    assert!(withheld_total > 0, "the scenario never withheld anything");
}

// ════════════════════════════════════════════════════════════════════════
// Minimizing: a node drops only what a fragment it keeps carries
// ════════════════════════════════════════════════════════════════════════

/// Over random histories, the loose commits `minimize` drops that no
/// fragment it keeps carries, and the number of seeds with any. The node
/// holds the fragments `pick` chooses, every fragment head loose, and a
/// random share of the other commits loose.
fn uncarried_drops(
    pick: impl for<'a> Fn(&mut SmallRng, &'a [AmFragment]) -> Vec<&'a AmFragment>,
) -> (usize, u64) {
    let (mut dropped, mut seeds) = (0, 0);
    for seed in 0..SEEDS {
        let mut rng = SmallRng::seed_from_u64(seed);
        let dag = Dag::random(&mut rng, if seed % 2 == 0 { 300 } else { 800 });
        let fragments = am_fragments(&dag);
        let loose_share: f64 = rng.gen_range(0.3..1.0);
        let mut loose: BTreeSet<usize> = (0..dag.len())
            .filter(|i| dag.depth[*i] == 0 && rng.gen_bool(loose_share))
            .collect();
        loose.extend(fragments.iter().map(|f| f.head));
        let held = pick(&mut rng, &fragments);

        let minimal = tree(&dag, &held, &loose).minimize(&CountLeadingZeroBytes);
        let kept: BTreeSet<CommitId> = minimal.loose_commits().map(LooseCommit::head).collect();
        let kept_fragments: BTreeSet<CommitId> = minimal.fragments().map(Fragment::head).collect();
        let carried: BTreeSet<CommitId> = held
            .iter()
            .filter(|f| kept_fragments.contains(&dag.id(f.head)))
            .flat_map(|f| dag.ids(&f.members))
            .collect();
        let uncarried = dag
            .ids(&loose)
            .into_iter()
            .filter(|c| !kept.contains(c) && !carried.contains(c))
            .count();
        dropped += uncarried;
        seeds += u64::from(uncarried > 0);
    }
    (dropped, seeds)
}

/// A node that holds each fragment together with the fragments it was
/// formed on (every fragment, a dependency-closed subset, or those formed
/// first) drops nothing a kept fragment does not carry. Guard.
#[test]
fn minimize_with_dependency_closed_fragments_drops_only_carried_commits() {
    for (name, (dropped, seeds)) in [
        (
            "all",
            uncarried_drops(|_, fragments| fragments.iter().collect()),
        ),
        ("dependency-closed", uncarried_drops(dependency_closed)),
        ("formed first", uncarried_drops(formed_first)),
    ] {
        assert_eq!(
            dropped, 0,
            "{name}: minimize dropped {dropped} commit(s) no kept fragment carries, in {seeds} of {SEEDS} seeds"
        );
    }
}

/// Ideal: whatever fragments a node holds, `minimize` drops nothing a kept
/// fragment does not carry. Fails for nodes holding a fragment without the
/// fragments it was formed on (UNCARRIED).
#[test]
#[should_panic(expected = "KNOWN BUG UNCARRIED:")]
fn minimize_drops_only_carried_commits() {
    let (dropped, seeds) = uncarried_drops(any_subset);
    known_bug(
        "UNCARRIED",
        dropped == 0,
        &format!(
            "minimize dropped {dropped} commit(s) no kept fragment carries, in {seeds} of {SEEDS} seeds"
        ),
    );
}
