//! Shared fixtures for the sync-efficiency ("sync budget") suites of
//! `sedimentree_core` and `subduction_core`. Each suite keeps its own
//! baseline file (`<crate>/tests/sync_budget_baselines.txt`) and passes its
//! path to [`ratchet`].
//!
//! # Two kinds of assertion
//!
//! * **Ratchet**: [`ratchet`] compares a measured value against the
//!   committed baseline. All metrics are lower-is-better and fully
//!   deterministic, so the check is exact: a higher value fails as a
//!   `SYNC REGRESSION`, and a lower one fails as a `STALE BASELINE`, so an
//!   improvement is locked in by the PR that makes it rather than leaving
//!   room for a later regression to creep back. Rewrite the baselines with
//!   `UPDATE_SYNC_BASELINES=1 cargo test ...` and review the diff.
//!   Every ratchet scenario also checks that nothing was lost (the receiver
//!   converged), so a lower number can never mean "synced less".
//!
//! * **Target (known bug)**: the ideal-efficiency invariant, asserted with
//!   [`known_bug`] inside a `#[should_panic(expected = "KNOWN BUG <id>:")]`
//!   test. Today the assertion fails, the test panics with the expected
//!   message, and the test passes. When the bug is fixed the test reports
//!   `test did not panic as expected`; delete the `should_panic` line to
//!   promote it to a real assertion (and re-record the matching ratchets).
//!   Always include the trailing colon in `expected`, so `C1` does not also
//!   match `C13` or `C1/C2`.
//!
//! # History shapes
//!
//! A [`History`] is a deterministic commit DAG ([`Shape`]) whose fragments
//! come from sedimentree's own decomposition
//! ([`CommitStore::build_fragment_store`]), run incrementally as each
//! fragment head is appended, the way a client builds them while typing.
//! Depth comes from `CountLeadingZeroBytes` on the commit id: a depth-1
//! commit every [`FRAGMENT_PERIOD`] commits (the rate random hashes give).
//!
//! * [`Shape::Linear`]: a single chain, depth-1 fragments only.
//! * [`Shape::Branchy`]: periodic two-way fork + merge, and a depth-2 commit
//!   every [`DEEP_PERIOD`] commits, so there are merges inside fragments, a
//!   depth-2 fragment with depth-1 checkpoints, and depth-1 fragments that
//!   the depth-2 one supports.
//!
//! It can be laid out two ways ([`Layout`]):
//!
//! * [`Layout::AutomergeRepo`]: what automerge-repo stores today. A fragment
//!   head is *only* a fragment, never a loose commit; every other commit is
//!   a loose commit.
//! * [`Layout::Full`]: every commit (heads included) is a loose commit, plus
//!   the fragments. This is the layout `CommitDag::simplify` was written for.

#![allow(
    clippy::cast_possible_truncation,
    clippy::len_without_is_empty,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::print_stderr,
    clippy::missing_panics_doc
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    fmt::Write as _,
    sync::Mutex,
};

use alloc::{borrow::ToOwned, string::String, vec, vec::Vec};

use crate::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::{CommitStore, FragmentState},
    depth::{CountLeadingZeroBytes, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

// ════════════════════════════════════════════════════════════════════════
// History fixtures
// ════════════════════════════════════════════════════════════════════════

/// The sedimentree every fixture lives in.
pub const SID: SedimentreeId = SedimentreeId::new([0x5D; 32]);

/// One depth-1 commit (and therefore one fragment) every this many commits.
/// `CountLeadingZeroBytes` gives a random hash depth >= 1 with p = 1/256.
pub const FRAGMENT_PERIOD: usize = 256;

/// [`Shape::Branchy`] only: one depth-2 commit every this many commits.
pub const DEEP_PERIOD: usize = 4 * FRAGMENT_PERIOD;

/// [`Shape::Branchy`] only: a fork + merge every this many commits.
pub const MERGE_PERIOD: usize = 16;

/// Size of each change blob. Fixed so byte counts are deterministic.
pub const COMMIT_BLOB_LEN: usize = 64;

/// The commit DAG of a [`History`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// `0 <- 1 <- 2 <- ...`; commit `i % FRAGMENT_PERIOD == FRAGMENT_PERIOD - 1`
    /// has depth 1.
    Linear,
    /// Like `Linear`, but at every `i % MERGE_PERIOD == 4` the chain forks
    /// (commit `i` is a sibling of `i - 1`, both children of `i - 2`) and
    /// commit `i + 1` merges them. Commit `i % DEEP_PERIOD == DEEP_PERIOD - 1`
    /// has depth 2.
    Branchy,
}

impl Shape {
    /// Depth (leading zero bytes) of commit `i`.
    #[must_use]
    pub const fn depth(self, i: usize) -> u8 {
        if matches!(self, Self::Branchy) && i % DEEP_PERIOD == DEEP_PERIOD - 1 {
            2
        } else if i % FRAGMENT_PERIOD == FRAGMENT_PERIOD - 1 {
            1
        } else {
            0
        }
    }

    /// Parents of commit `i`, by index.
    #[must_use]
    pub fn parents(self, i: usize) -> Vec<usize> {
        match (self, i % MERGE_PERIOD) {
            (_, _) if i == 0 => Vec::new(),
            (Self::Branchy, 4) if i >= 2 => vec![i - 2],
            (Self::Branchy, 5) => vec![i - 2, i - 1],
            _ => vec![i - 1],
        }
    }

    /// Deterministic id for commit `i`, with exactly [`Shape::depth`]
    /// leading zero bytes.
    #[must_use]
    pub fn id(self, i: usize) -> CommitId {
        let mut b = [0u8; 32];
        let idx = (i as u64).to_le_bytes();
        match self.depth(i) {
            0 => {
                b[0] = 0xC0;
                b[1..9].copy_from_slice(&idx);
            }
            1 => {
                b[1] = 0xD1;
                b[2..10].copy_from_slice(&idx);
            }
            _ => {
                b[2] = 0xD2;
                b[3..11].copy_from_slice(&idx);
            }
        }
        CommitId::new(b)
    }

    /// Commit `i` with its blob.
    #[must_use]
    pub fn commit(self, i: usize) -> (LooseCommit, Blob) {
        let blob = commit_blob(i);
        let parents: BTreeSet<CommitId> = self.parents(i).into_iter().map(|p| self.id(p)).collect();
        (
            LooseCommit::new(SID, self.id(i), parents, BlobMeta::new(&blob)),
            blob,
        )
    }
}

/// Deterministic blob for commit `i`.
#[must_use]
pub fn commit_blob(i: usize) -> Blob {
    let mut v = vec![0xA5u8; COMMIT_BLOB_LEN];
    v[..8].copy_from_slice(&(i as u64).to_le_bytes());
    Blob::new(v)
}

/// How a [`History`] is laid out as loose commits + fragments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    /// automerge-repo today: fragment heads are never stored as loose commits.
    AutomergeRepo,
    /// Every commit is a loose commit, plus the fragments.
    Full,
}

/// The commit graph seen through sedimentree's [`CommitStore`], so fixtures
/// get their fragments from the real decomposition.
#[derive(Debug)]
struct Dag(Map<CommitId, Set<CommitId>>);

impl CommitStore<'_> for Dag {
    type Node = Set<CommitId>;
    type LookupError = Infallible;

    fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
        Ok(self.0.get(&id).cloned())
    }
}

/// A deterministic history of `n` commits with fragments.
#[derive(Debug, Clone)]
pub struct History {
    /// The commit DAG this history follows.
    pub shape: Shape,
    /// Every commit, oldest first, with its blob (fragment heads included).
    pub commits: Vec<(LooseCommit, Blob)>,
    /// Every fragment, oldest first, with its blob (including fragments a
    /// deeper fragment supports, which `minimize` drops).
    pub fragments: Vec<(Fragment, Blob)>,
    /// Fragment head -> the commits whose payload that fragment's blob carries.
    pub members: BTreeMap<CommitId, BTreeSet<CommitId>>,
}

impl History {
    /// A linear history of `n` commits ([`Shape::Linear`]).
    #[must_use]
    pub fn linear(n: usize) -> Self {
        Self::build(Shape::Linear, n)
    }

    /// A history of `n` commits with merges and depth-2 fragments
    /// ([`Shape::Branchy`]).
    #[must_use]
    pub fn branchy(n: usize) -> Self {
        Self::build(Shape::Branchy, n)
    }

    /// Append `n` commits of `shape`, building the fragment for each
    /// fragment head as it arrives. A fragment's blob is its members'
    /// blobs, oldest first.
    #[must_use]
    pub fn build(shape: Shape, n: usize) -> Self {
        let index: BTreeMap<CommitId, usize> = (0..n).map(|i| (shape.id(i), i)).collect();
        let mut dag = Dag(Map::new());
        let mut known: Map<CommitId, FragmentState<Set<CommitId>>> = Map::new();
        let mut commits = Vec::with_capacity(n);
        let mut fragments = Vec::new();
        let mut members = BTreeMap::new();
        for i in 0..n {
            let (commit, blob) = shape.commit(i);
            dag.0
                .insert(commit.head(), commit.parents().iter().copied().collect());
            commits.push((commit, blob));
            if shape.depth(i) == 0 {
                continue;
            }
            let head = shape.id(i);
            dag.build_fragment_store(&[head], &mut known, &CountLeadingZeroBytes)
                .expect("every commit is in the DAG");
            let state = known
                .get(&head)
                .expect("fragment built for its head")
                .clone();
            let mut member_idx: Vec<usize> = state.members().iter().map(|m| index[m]).collect();
            member_idx.sort_unstable();
            let mut payload = Vec::with_capacity(member_idx.len() * COMMIT_BLOB_LEN);
            for j in &member_idx {
                payload.extend_from_slice(commit_blob(*j).as_slice());
            }
            let blob = Blob::new(payload);
            members.insert(head, member_idx.iter().map(|j| shape.id(*j)).collect());
            fragments.push((state.to_fragment(SID, BlobMeta::new(&blob)), blob));
        }
        Self {
            shape,
            commits,
            fragments,
            members,
        }
    }

    /// Number of commits in the history.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.commits.len()
    }

    /// Id of commit `i`.
    #[must_use]
    pub fn id(&self, i: usize) -> CommitId {
        self.shape.id(i)
    }

    /// Commit `i` with its blob.
    #[must_use]
    pub fn commit(&self, i: usize) -> (LooseCommit, Blob) {
        self.shape.commit(i)
    }

    /// The next commit that would extend this history (not added).
    #[must_use]
    pub fn next_commit(&self) -> (LooseCommit, Blob) {
        self.shape.commit(self.len())
    }

    /// Every commit carried by some fragment's blob.
    #[must_use]
    pub fn covered(&self) -> BTreeSet<CommitId> {
        self.members.values().flatten().copied().collect()
    }

    /// Commits carried by no fragment: the live tail after the last head.
    #[must_use]
    pub fn tail(&self) -> BTreeSet<CommitId> {
        let covered = self.covered();
        self.commits
            .iter()
            .map(|(c, _)| c.head())
            .filter(|id| !covered.contains(id))
            .collect()
    }

    /// Heads of the fragments a minimal tree keeps: those whose head is not
    /// a member of a deeper fragment. (An oracle independent of
    /// `Fragment::supports`, which `minimize` uses.)
    #[must_use]
    pub fn top_fragments(&self) -> BTreeSet<CommitId> {
        let depth = |h: &CommitId| CountLeadingZeroBytes.to_depth(*h);
        self.members
            .keys()
            .filter(|h| {
                !self
                    .members
                    .iter()
                    .any(|(g, m)| depth(g) > depth(h) && m.contains(h))
            })
            .copied()
            .collect()
    }

    /// The loose commits a peer stores under `layout`.
    #[must_use]
    pub fn loose(&self, layout: Layout) -> Vec<(LooseCommit, Blob)> {
        self.commits
            .iter()
            .filter(|(c, _)| layout == Layout::Full || !self.members.contains_key(&c.head()))
            .cloned()
            .collect()
    }

    /// The (unminimized) sedimentree a peer holds under `layout`.
    #[must_use]
    pub fn tree(&self, layout: Layout) -> Sedimentree {
        Sedimentree::new(
            self.fragments.iter().map(|(f, _)| f.clone()).collect(),
            self.loose(layout).into_iter().map(|(c, _)| c).collect(),
        )
    }

    /// Of `commits`, those whose payload is also carried by one of the
    /// fragments with heads in `fragment_heads`.
    #[must_use]
    pub fn redundant<'a>(
        &self,
        commits: impl IntoIterator<Item = &'a CommitId>,
        fragment_heads: impl IntoIterator<Item = &'a CommitId>,
    ) -> BTreeSet<CommitId> {
        let carried: BTreeSet<CommitId> = fragment_heads
            .into_iter()
            .filter_map(|h| self.members.get(h))
            .flatten()
            .copied()
            .collect();
        commits
            .into_iter()
            .filter(|c| carried.contains(c))
            .copied()
            .collect()
    }
}

// ════════════════════════════════════════════════════════════════════════
// Known-bug targets
// ════════════════════════════════════════════════════════════════════════

/// Assert an ideal-efficiency invariant that a known bug currently breaks.
///
/// Panics with `KNOWN BUG <cause>: ...` when `ideal_holds` is false. Use
/// only inside a `#[should_panic(expected = "KNOWN BUG <cause>:")]` test, so
/// the suite stays green while the bug exists and goes red ("did not panic
/// as expected") the moment it is fixed.
#[track_caller]
pub fn known_bug(cause: &str, ideal_holds: bool, detail: &str) {
    assert!(ideal_holds, "KNOWN BUG {cause}: {detail}");
    eprintln!(
        "KNOWN BUG {cause} appears FIXED ({detail}). Remove `#[should_panic]` from this \
         test to promote it to a real assertion, and re-record the baselines."
    );
}

// ════════════════════════════════════════════════════════════════════════
// Ratchet baselines
// ════════════════════════════════════════════════════════════════════════

/// When set (to anything non-empty other than `0`), [`ratchet`] rewrites
/// the baseline file with the measured values instead of checking them.
pub const UPDATE_ENV: &str = "UPDATE_SYNC_BASELINES";

/// Serializes read-modify-write of the baseline file across the test
/// threads of one binary.
static BASELINES: Mutex<()> = Mutex::new(());

fn update_mode() -> bool {
    std::env::var(UPDATE_ENV).is_ok_and(|v| !v.is_empty() && v != "0")
}

fn load(file: &str) -> BTreeMap<String, u64> {
    let Ok(text) = std::fs::read_to_string(file) else {
        return BTreeMap::new();
    };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| {
            let (k, v) = l
                .split_once('=')
                .unwrap_or_else(|| panic!("malformed baseline line {l:?} in {file}"));
            let v: u64 = v
                .trim()
                .parse()
                .unwrap_or_else(|e| panic!("bad value in baseline line {l:?}: {e}"));
            (k.trim().to_owned(), v)
        })
        .collect()
}

fn store(file: &str, values: &BTreeMap<String, u64>) {
    let mut out = String::from(
        "# Sync-efficiency ratchet baselines. Lower is better for every metric.\n\
         # Values are deterministic and checked exactly: a higher value fails as a\n\
         # regression, a lower one fails as a stale baseline. Re-record with:\n\
         #   UPDATE_SYNC_BASELINES=1 cargo test --features arbitrary --test sync_efficiency\n\
         # and commit the diff only when the change is an improvement (or an\n\
         # intentional, reviewed trade-off).\n",
    );
    for (k, v) in values {
        writeln!(out, "{k} = {v}").expect("write to String");
    }
    std::fs::write(file, out).expect("write baseline file");
}

/// Ratchet `metric` (lower is better) against its committed baseline in
/// `baselines`, a file path (each suite passes its crate's
/// `tests/sync_budget_baselines.txt`).
///
/// The measured values are deterministic (fixed ids, blobs, keys and
/// seeds; repeated runs, including under heavy CPU load, are
/// byte-identical), so there is no tolerance: any increase fails as a
/// regression, and any decrease fails as a stale baseline that the same
/// change must tighten. In update mode the baseline is overwritten with
/// `current` instead.
#[track_caller]
pub fn ratchet(baselines: &str, metric: &str, current: u64) {
    let _guard = BASELINES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut values = load(baselines);

    if update_mode() {
        if values.get(metric) != Some(&current) {
            eprintln!("{UPDATE_ENV}: {metric} = {current}");
            values.insert(metric.to_owned(), current);
            store(baselines, &values);
        }
        return;
    }

    let Some(&baseline) = values.get(metric) else {
        panic!(
            "no baseline for sync metric `{metric}` (measured {current}); \
             run with {UPDATE_ENV}=1 to record it in {baselines}"
        );
    };

    assert!(
        current <= baseline,
        "SYNC REGRESSION: `{metric}` = {current}, baseline {baseline}. If this increase \
         is intended, rerun with {UPDATE_ENV}=1 and justify the new baseline in review."
    );
    assert!(
        current >= baseline,
        "STALE BASELINE: `{metric}` improved to {current} (baseline {baseline}). Lock the \
         improvement in by rerunning with {UPDATE_ENV}=1 and committing the baseline diff."
    );
}
