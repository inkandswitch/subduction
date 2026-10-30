//! [`MinimizedSedimentree`]: a [`Sedimentree`] owner that tracks whether the
//! tree still needs minimizing, and which items it has absorbed.
//!
//! # Why the dirty flag lives here, not on `Sedimentree`
//!
//! [`Sedimentree`] is a pure data type: equality, ordering, and hashing are
//! defined solely by its contents, and it has no notion of "minimal vs. not".
//! The dirty bit that drives *lazy* minimization is a caching concern, so it
//! belongs on this wrapper rather than polluting the data type's identity.
//!
//! # Held items
//!
//! Minimization drops loose commits a kept fragment covers, and fragments a
//! deeper one supports. The dropped items are still *held* (they are in
//! storage, or inside a fragment we keep), so the wrapper remembers the head
//! of every loose commit and fragment it has absorbed. Receiving a held item
//! again is not news: it is not re-added, so it cannot come back as a stray
//! the next minimize can't prune (the items that connected it to its fragment
//! were dropped with it). Without this, the minimal form depended on the order
//! items arrived in and on how often they were re-sent.
//!
//! # Behavior
//!
//! - Mutations ([`add_commit`](Self::add_commit) /
//!   [`add_fragment`](Self::add_fragment) / [`merge`](Self::merge)) of items
//!   not yet held mark the tree dirty but do **not** minimize.
//! - [`ensure_minimized`](Self::ensure_minimized) minimizes **in place** (via
//!   [`Sedimentree::minimize_in_place`]) only if dirty, then clears the flag.
//!   A burst of writes followed by one `ensure_minimized` costs a single
//!   minimize instead of one per write, and re-calling it on a clean tree is
//!   free.
//!
//! The wrapper [`Deref`]s to [`Sedimentree`] for read-only access. Callers that
//! depend on the minimal form (wire / sync paths) must call
//! [`minimized`](Self::minimized) (or [`ensure_minimized`](Self::ensure_minimized))
//! first.

use alloc::{collections::BTreeSet, vec::Vec};
use core::ops::Deref;

use crate::{
    collections::Set,
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    depth::DepthMetric,
    fragment::Fragment,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{CommitOrFragment, FingerprintDiff, FingerprintSummary, Sedimentree},
};

/// A [`Sedimentree`] plus an external "needs minimizing" flag and the heads
/// of every item it holds.
///
/// See the [module docs](self) for why these live here rather than on
/// `Sedimentree`.
#[derive(Debug, Clone, Default)]
pub struct MinimizedSedimentree {
    tree: Sedimentree,
    /// Heads of every loose commit added, including those minimization has
    /// since dropped from `tree`.
    held_commits: Set<CommitId>,
    /// Heads of every fragment added, including those minimization has
    /// since dropped from `tree`.
    held_fragments: Set<CommitId>,
    /// `true` when `tree` may not be in minimal form (a mutation happened
    /// since the last [`ensure_minimized`](Self::ensure_minimized)).
    dirty: bool,
}

impl MinimizedSedimentree {
    /// Wrap an existing tree, holding everything in it. The tree is assumed
    /// to be in an unknown state, so it starts dirty (the next
    /// [`ensure_minimized`](Self::ensure_minimized) will minimize it).
    ///
    /// Pass the full history (e.g. as loaded from storage), not a minimized
    /// copy: items missing here are not known to be held.
    #[must_use]
    pub fn new(tree: Sedimentree) -> Self {
        Self {
            held_commits: tree.commits.keys().copied().collect(),
            held_fragments: tree.fragments.keys().copied().collect(),
            tree,
            dirty: true,
        }
    }

    /// Wrap a tree that is already known to be minimal and that is all this
    /// node holds (e.g. an empty tree). Starts clean.
    ///
    /// Only the items in `tree` count as held. For a tree produced by
    /// [`Sedimentree::minimize`], use [`new`](Self::new) on the unminimized
    /// input instead, so the items minimization dropped stay held.
    #[must_use]
    pub fn already_minimal(tree: Sedimentree) -> Self {
        Self {
            dirty: false,
            ..Self::new(tree)
        }
    }

    /// Borrow the underlying tree without minimizing. May be non-minimal if
    /// dirty; prefer [`minimized`](Self::minimized) when the minimal form is
    /// required.
    #[must_use]
    pub const fn tree(&self) -> &Sedimentree {
        &self.tree
    }

    /// Whether the tree may currently be non-minimal.
    #[must_use]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Whether a loose commit with this head has been added, whether or not
    /// minimization has since dropped it.
    #[must_use]
    pub fn holds_commit(&self, head: CommitId) -> bool {
        self.held_commits.contains(&head)
    }

    /// Whether a fragment with this head has been added, whether or not
    /// minimization has since dropped it.
    #[must_use]
    pub fn holds_fragment(&self, head: CommitId) -> bool {
        self.held_fragments.contains(&head)
    }

    /// Add a loose commit. Returns `true` if it was not already held.
    ///
    /// A commit that is already held is not re-added: if minimization
    /// dropped it, a fragment we keep covers it. If it is still in the tree,
    /// the add only applies [`Sedimentree::add_commit`]'s tiebreak between
    /// equivocating payloads.
    pub fn add_commit(&mut self, commit: LooseCommit) -> bool {
        let head = commit.head();
        let newly_held = self.held_commits.insert(head);
        if newly_held || self.tree.has_loose_commit(head) {
            self.tree.add_commit(commit);
            self.dirty = true;
        }
        newly_held
    }

    /// Add a fragment. Returns `true` if it was not already held.
    ///
    /// As with [`add_commit`](Self::add_commit), a held fragment that
    /// minimization dropped is not re-added.
    pub fn add_fragment(&mut self, fragment: Fragment) -> bool {
        let head = fragment.head();
        let newly_held = self.held_fragments.insert(head);
        if newly_held || self.tree.has_fragment(head) {
            self.tree.add_fragment(fragment);
            self.dirty = true;
        }
        newly_held
    }

    /// Add every item of another tree, as
    /// [`add_commit`](Self::add_commit) / [`add_fragment`](Self::add_fragment).
    pub fn merge(&mut self, other: Sedimentree) {
        for item in other.into_items() {
            match item {
                CommitOrFragment::Commit(commit) => {
                    self.add_commit(commit);
                }
                CommitOrFragment::Fragment(fragment) => {
                    self.add_fragment(fragment);
                }
            }
        }
    }

    /// Minimize the tree in place if it is dirty, then clear the flag.
    ///
    /// No-op (and no allocation / no traversal) when already clean.
    pub fn ensure_minimized<M: DepthMetric>(&mut self, depth_metric: &M) {
        if self.dirty {
            self.tree.minimize_in_place(depth_metric);
            self.dirty = false;
        }
    }

    /// Ensure the tree is minimal, then borrow it. Use this on read paths that
    /// require the minimal form (heads / fingerprint summaries / wire diff).
    pub fn minimized<M: DepthMetric>(&mut self, depth_metric: &M) -> &Sedimentree {
        self.ensure_minimized(depth_metric);
        &self.tree
    }

    /// Compute the tree's heads, minimizing in place first only if dirty.
    ///
    /// Cheaper than `tree.minimized(m).clone().heads(m)`: it minimizes at most
    /// once (gated by the dirty flag), avoids cloning the tree out, and skips
    /// the redundant re-minimization inside [`Sedimentree::heads`] by calling
    /// [`Sedimentree::heads_assuming_minimal`] on the already-minimal tree.
    pub fn heads<M: DepthMetric>(&mut self, depth_metric: &M) -> Vec<CommitId> {
        self.ensure_minimized(depth_metric);
        self.tree.heads_assuming_minimal()
    }

    /// Diff the minimal tree against a requestor's summary, as
    /// [`Sedimentree::diff_remote_fingerprints`], but echo back only the
    /// fingerprints of items this tree does not hold, and don't offer loose
    /// commits that a fragment the requestor listed carries.
    ///
    /// The minimal tree is what we offer; what we hold also includes the
    /// items minimization dropped. Echoing one of those (e.g. a loose commit
    /// a kept fragment covers) would ask the requestor to upload data we
    /// already have. Items held only inside a fragment's blob, never as a
    /// loose commit, cannot be recognized and are still echoed.
    ///
    /// A summary carries no fragment boundaries, so what a requestor's
    /// fragment carries is otherwise only known for fragments we hold too.
    /// But a requestor fragment we lack, whose head we hold as a loose
    /// commit, carries that head's range, and we don't offer the commits
    /// that belong to no other range.
    pub fn responder_diff<M: DepthMetric>(
        &mut self,
        depth_metric: &M,
        remote: &FingerprintSummary,
    ) -> FingerprintDiff<'_> {
        self.ensure_minimized(depth_metric);
        let mut diff = self.tree.diff_remote_fingerprints(remote);
        let seed = remote.seed();
        if !diff.local_only_commits.is_empty() && !diff.remote_only_fragment_fingerprints.is_empty()
        {
            let carried = carried_by_requestor_fragments(
                &self.tree,
                depth_metric,
                seed,
                &diff.remote_only_fragment_fingerprints,
            );
            diff.local_only_commits
                .retain(|(id, _)| !carried.contains(*id));
        }

        let mut unknown_commits: BTreeSet<Fingerprint<CommitId>> = diff
            .remote_only_commit_fingerprints
            .iter()
            .copied()
            .collect();
        let mut unknown_fragments: BTreeSet<Fingerprint<CommitId>> = diff
            .remote_only_fragment_fingerprints
            .iter()
            .copied()
            .collect();
        if unknown_commits.is_empty() && unknown_fragments.is_empty() {
            return diff;
        }

        // Hash what we hold under the requestor's seed and strike it from
        // the (usually few) unknown fingerprints, rather than collecting a
        // set of everything held. Fragment heads count as held commits: a
        // fragment's blob carries its head.
        if !unknown_commits.is_empty() {
            for id in &self.held_commits {
                unknown_commits.remove(&Fingerprint::new(seed, id));
            }
        }
        for id in &self.held_fragments {
            let fp = Fingerprint::new(seed, id);
            unknown_commits.remove(&fp);
            unknown_fragments.remove(&fp);
        }
        diff.remote_only_commit_fingerprints
            .retain(|fp| unknown_commits.contains(fp));
        diff.remote_only_fragment_fingerprints
            .retain(|fp| unknown_fragments.contains(fp));
        diff
    }

    /// Consume the wrapper and return the inner tree (without minimizing).
    #[must_use]
    pub fn into_tree(self) -> Sedimentree {
        self.tree
    }
}

/// The loose commits of `tree` that the requestor's fragments we lack
/// (`fragment_fingerprints`) are known to carry.
///
/// This is the round in which a writer that first synced a new fragment's
/// commits loose, head included, sends the fragment itself: its own tree has
/// dropped the commits the fragment covers, but ours still holds them loose
/// until we ingest the fragment. Offering them would send back what the
/// fragment already carries.
///
/// A fragment covers the range its head starts, as in [`Sedimentree::minimize`]:
/// the head and its ancestors down to the next boundary commit. A commit can
/// belong to several ranges, though, and is only known to be carried if
/// every range it belongs to is headed by such a fragment. A commit also
/// reachable from another range's head (a boundary we did not match, or a
/// tip) may lie outside the fragment: Automerge's fragments exclude the
/// ancestors of their boundary, and a concurrent branch reaches those
/// without crossing a boundary commit.
///
/// This trusts the requestor's list. A requestor holding a fragment that
/// does not carry its head's range (a forged one) is not offered that range
/// while we hold it loose; see the Batch Sync design doc.
fn carried_by_requestor_fragments<M: DepthMetric>(
    tree: &Sedimentree,
    depth_metric: &M,
    seed: &FingerprintSeed,
    fragment_fingerprints: &[Fingerprint<CommitId>],
) -> Set<CommitId> {
    let requestor_heads: BTreeSet<&Fingerprint<CommitId>> = fragment_fingerprints.iter().collect();
    let is_boundary = |id: &CommitId| depth_metric.to_depth(*id).is_boundary();
    let (matched, unmatched): (Vec<CommitId>, Vec<CommitId>) = tree
        .commits
        .keys()
        .copied()
        .filter(is_boundary)
        .partition(|id| requestor_heads.contains(&Fingerprint::new(seed, id)));
    if matched.is_empty() {
        return Set::new();
    }

    // Commits reachable from `heads` without crossing a boundary commit.
    let ranges = |heads: Vec<CommitId>| {
        let mut members: Set<CommitId> = Set::new();
        let mut stack = heads;
        while let Some(id) = stack.pop() {
            if !members.insert(id) {
                continue;
            }
            if let Some(commit) = tree.commits.get(&id) {
                stack.extend(commit.parents().iter().filter(|p| !is_boundary(p)));
            }
        }
        members
    };

    let has_child: Set<CommitId> = tree
        .commits
        .values()
        .flat_map(|commit| commit.parents().iter().copied())
        .collect();
    let tips = tree
        .commits
        .keys()
        .filter(|id| !has_child.contains(*id) && !is_boundary(id))
        .copied();
    let elsewhere = ranges(unmatched.into_iter().chain(tips).collect());
    let mut carried = ranges(matched);
    carried.retain(|id| !elsewhere.contains(id));
    carried
}

impl Deref for MinimizedSedimentree {
    type Target = Sedimentree;

    fn deref(&self) -> &Sedimentree {
        &self.tree
    }
}

impl From<Sedimentree> for MinimizedSedimentree {
    fn from(tree: Sedimentree) -> Self {
        Self::new(tree)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, reason = "bounded test fixtures")]
mod tests {
    use alloc::{collections::BTreeSet, vec, vec::Vec};

    use crate::{
        blob::{Blob, BlobMeta},
        crypto::fingerprint::{Fingerprint, FingerprintSeed},
        depth::CountLeadingZeroBytes,
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
        sedimentree::{FingerprintSummary, Sedimentree},
        test_utils::{commit_id_with_depth, make_fragment_at_depth},
    };

    use super::MinimizedSedimentree;

    fn loose_commit(seed: u8) -> LooseCommit {
        let blob_meta = BlobMeta::new(&Blob::new(vec![seed]));
        LooseCommit::new(
            SedimentreeId::new([seed; 32]),
            CommitId::new([seed; 32]),
            BTreeSet::new(),
            blob_meta,
        )
        .expect("valid metadata")
    }

    /// A tree whose `minimize` drops a fragment: a deep fragment dominates a
    /// shallower one (mirrors `minimize_multi_depth_deep_dominates_shallow`).
    fn dominating_tree() -> Sedimentree {
        let deep_boundary = commit_id_with_depth(1, 100);
        let shallow_head = commit_id_with_depth(2, 1);
        let shallow_boundary = commit_id_with_depth(1, 101);

        let deep = make_fragment_at_depth(
            3,
            1,
            BTreeSet::from([deep_boundary]),
            &[shallow_head, shallow_boundary],
        );
        let shallow = make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);
        Sedimentree::new(vec![deep, shallow], vec![])
    }

    #[test]
    fn new_starts_dirty_already_minimal_starts_clean() {
        assert!(MinimizedSedimentree::new(dominating_tree()).is_dirty());
        assert!(!MinimizedSedimentree::already_minimal(dominating_tree()).is_dirty());
        assert!(!MinimizedSedimentree::default().is_dirty());
    }

    #[test]
    fn ensure_minimized_collapses_and_clears_dirty() {
        let full = dominating_tree();
        assert_eq!(full.fragments().count(), 2, "precondition: 2 fragments");

        let mut m = MinimizedSedimentree::new(full.clone());
        m.ensure_minimized(&CountLeadingZeroBytes);

        assert!(!m.is_dirty());
        // The dominated shallow fragment is gone.
        assert_eq!(m.tree().fragments().count(), 1);
        // Result matches the rebuild-based minimize.
        assert_eq!(*m.tree(), full.minimize(&CountLeadingZeroBytes));
    }

    #[test]
    fn ensure_minimized_is_noop_when_clean() {
        let minimal = dominating_tree().minimize(&CountLeadingZeroBytes);
        let mut m = MinimizedSedimentree::already_minimal(minimal.clone());
        m.ensure_minimized(&CountLeadingZeroBytes); // clean → no-op
        assert_eq!(*m.tree(), minimal);
        assert!(!m.is_dirty());
    }

    #[test]
    fn add_fragment_sets_dirty() {
        let mut m = MinimizedSedimentree::already_minimal(Sedimentree::default());
        assert!(!m.is_dirty());

        let frag = make_fragment_at_depth(2, 9, BTreeSet::from([commit_id_with_depth(1, 9)]), &[]);
        m.add_fragment(frag);
        assert!(m.is_dirty(), "add_fragment must mark dirty");
    }

    #[test]
    fn add_commit_sets_dirty() {
        let mut m = MinimizedSedimentree::already_minimal(Sedimentree::default());
        assert!(!m.is_dirty());

        m.add_commit(loose_commit(7));
        assert!(m.is_dirty(), "add_commit must mark dirty");
    }

    #[test]
    fn merge_sets_dirty() {
        let mut m = MinimizedSedimentree::already_minimal(Sedimentree::default());
        assert!(!m.is_dirty());

        let other = Sedimentree::new(vec![], vec![loose_commit(8)]);
        m.merge(other);
        assert!(m.is_dirty(), "merge must mark dirty");
    }

    /// A dirty tree read through `minimized(...)` produces the same
    /// `FingerprintSummary` as `tree.minimize(metric).fingerprint_summarize()`.
    /// The wire diff relies on the minimal form, so `minimized` must minimize
    /// before exposing the tree.
    #[test]
    fn dirty_tree_summary_matches_minimize() {
        let full = dominating_tree();
        let seed = FingerprintSeed::new(0x1234, 0x5678);

        let expected = full
            .clone()
            .minimize(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        let mut m = MinimizedSedimentree::new(full);
        assert!(m.is_dirty());
        let actual = m
            .minimized(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        assert_eq!(actual, expected, "minimized read must match minimize");
    }

    /// The same equivalence holds when the tree is mutated through the wrapper
    /// (the ingest pattern: add, then read).
    #[test]
    fn summary_after_wrapper_mutation_matches_minimize() {
        let seed = FingerprintSeed::new(7, 9);

        let deep_boundary = commit_id_with_depth(1, 100);
        let shallow_head = commit_id_with_depth(2, 1);
        let shallow_boundary = commit_id_with_depth(1, 101);
        let deep = make_fragment_at_depth(
            3,
            1,
            BTreeSet::from([deep_boundary]),
            &[shallow_head, shallow_boundary],
        );
        let shallow = make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);

        let expected = Sedimentree::new(vec![deep.clone(), shallow.clone()], vec![])
            .minimize(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        let mut m = MinimizedSedimentree::already_minimal(Sedimentree::default());
        m.add_fragment(shallow);
        m.add_fragment(deep);
        assert!(m.is_dirty());
        let actual = m
            .minimized(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        assert_eq!(actual, expected);
    }

    #[test]
    fn minimized_returns_minimal_form() {
        let full = dominating_tree();
        let mut m = MinimizedSedimentree::new(full.clone());

        let minimal_ref = m.minimized(&CountLeadingZeroBytes);
        assert_eq!(*minimal_ref, full.minimize(&CountLeadingZeroBytes));
        assert!(!m.is_dirty());
    }

    /// A loose commit `x` under a boundary commit `h`, and a fragment
    /// headed by `h`: minimizing drops both commits as covered.
    fn covered_run() -> (Sedimentree, LooseCommit) {
        let sid = SedimentreeId::new([3; 32]);
        let blob_meta = BlobMeta::new(&Blob::new(vec![3]));
        let x = LooseCommit::new(sid, CommitId::new([0x33; 32]), BTreeSet::new(), blob_meta);
        let fragment = make_fragment_at_depth(1, 4, BTreeSet::new(), &[]);
        let h = LooseCommit::new(sid, fragment.head(), BTreeSet::from([x.head()]), blob_meta);
        (Sedimentree::new(vec![fragment], vec![x.clone(), h]), x)
    }

    #[test]
    fn dropped_commit_stays_held_and_is_not_readded() {
        let (full, x) = covered_run();
        let mut m = MinimizedSedimentree::new(full);
        m.ensure_minimized(&CountLeadingZeroBytes);
        assert!(!m.has_loose_commit(x.head()), "precondition: x is covered");
        let minimal = m.tree().clone();

        assert!(m.holds_commit(x.head()));
        assert!(!m.add_commit(x), "a held commit is not new");
        assert!(!m.is_dirty());
        // Re-added, `x` would be a tip with no loose path to the fragment's
        // head, so minimize could never prune it again.
        assert_eq!(*m.minimized(&CountLeadingZeroBytes), minimal);
    }

    /// A requestor that holds a commit we pruned is not asked to upload it,
    /// though the minimal tree alone would echo it back.
    #[test]
    fn diff_does_not_request_held_commits() {
        let (full, x) = covered_run();
        let seed = FingerprintSeed::new(1, 2);
        let remote = Sedimentree::new(vec![], vec![x.clone()]).fingerprint_summarize(&seed);

        let mut m = MinimizedSedimentree::new(full.clone());
        let offered = full.minimize(&CountLeadingZeroBytes);
        assert_eq!(
            offered
                .diff_remote_fingerprints(&remote)
                .remote_only_commit_fingerprints,
            vec![Fingerprint::new(&seed, &x.head())],
            "precondition: the minimal tree alone echoes x"
        );

        let diff = m.responder_diff(&CountLeadingZeroBytes, &remote);
        assert!(diff.remote_only_commit_fingerprints.is_empty());
        assert!(diff.remote_only_fragment_fingerprints.is_empty());
    }

    /// The commits each requestor diff offers.
    fn offered(diff: &crate::sedimentree::FingerprintDiff<'_>) -> BTreeSet<CommitId> {
        diff.local_only_commits.iter().map(|(id, _)| **id).collect()
    }

    /// Loose commits in sedimentree `[5; 32]` with the given parents.
    fn commits(edges: &[(CommitId, &[CommitId])]) -> Vec<LooseCommit> {
        let meta = BlobMeta::new(&Blob::new(vec![5]));
        edges
            .iter()
            .map(|(id, parents)| {
                LooseCommit::new(
                    SedimentreeId::new([5; 32]),
                    *id,
                    parents.iter().copied().collect(),
                    meta,
                )
            })
            .collect()
    }

    /// A summary listing only a fragment headed by `head` with `boundary`.
    fn requestor_fragment(head: CommitId, boundary: CommitId) -> FingerprintSummary {
        let fragment = Fragment::new(
            SedimentreeId::new([5; 32]),
            head,
            BTreeSet::from([boundary]),
            &[],
            BlobMeta::new(&Blob::new(vec![5])),
        );
        Sedimentree::new(vec![fragment], vec![]).fingerprint_summarize(&FingerprintSeed::new(3, 4))
    }

    /// A requestor sending a fragment we lack, headed by a commit we hold
    /// loose, is not offered the loose commits of that commit's range, only
    /// what lies below the range's boundary.
    #[test]
    fn diff_does_not_offer_the_range_of_a_requestor_fragment() {
        let below = commit_id_with_depth(1, 50);
        let x1 = CommitId::new([0x51; 32]);
        let x2 = CommitId::new([0x52; 32]);
        let head = commit_id_with_depth(1, 53);
        let full = Sedimentree::new(
            vec![],
            commits(&[(below, &[]), (x1, &[below]), (x2, &[x1]), (head, &[x2])]),
        );
        let remote = requestor_fragment(head, below);
        assert_eq!(
            offered(&full.diff_remote_fingerprints(&remote)),
            BTreeSet::from([below, x1, x2]),
            "precondition: the plain diff offers the fragment's range"
        );

        let mut m = MinimizedSedimentree::new(full);
        let diff = m.responder_diff(&CountLeadingZeroBytes, &remote);
        assert_eq!(offered(&diff), BTreeSet::from([below]));
    }

    /// Automerge's fragment headed `head` with boundary `boundary` carries
    /// `head`, `merge` and `branch`, but not the boundary's ancestors `y`
    /// and `root`, which the concurrent `branch` reaches without crossing
    /// the boundary. A requestor holding only that fragment lacks them, so
    /// they are still offered: they also belong to the boundary's range,
    /// which no requestor fragment heads.
    #[test]
    fn diff_offers_what_a_concurrent_branch_reaches_below_the_boundary() {
        let root = CommitId::new([0x40; 32]);
        let y = CommitId::new([0x41; 32]);
        let boundary = commit_id_with_depth(1, 60);
        let branch = CommitId::new([0x42; 32]);
        let merge = CommitId::new([0x43; 32]);
        let head = commit_id_with_depth(1, 61);
        let full = Sedimentree::new(
            vec![],
            commits(&[
                (root, &[]),
                (y, &[root]),
                (boundary, &[y]),
                (branch, &[y]),
                (merge, &[boundary, branch]),
                (head, &[merge]),
            ]),
        );
        let mut tree = MinimizedSedimentree::new(full);
        let diff = tree.responder_diff(&CountLeadingZeroBytes, &requestor_fragment(head, boundary));
        assert_eq!(offered(&diff), BTreeSet::from([root, y, boundary]));
    }

    #[test]
    fn dropped_fragment_stays_held_and_is_not_readded() {
        let full = dominating_tree();
        let mut m = MinimizedSedimentree::new(full.clone());
        m.ensure_minimized(&CountLeadingZeroBytes);
        let minimal = m.tree().clone();
        let dropped: Vec<Fragment> = full
            .fragments()
            .filter(|f| !minimal.has_fragment(f.head()))
            .cloned()
            .collect();
        assert_eq!(dropped.len(), 1, "precondition: one fragment dominated");

        m.merge(Sedimentree::new(dropped.clone(), vec![]));
        assert!(!m.is_dirty(), "merging held items is a no-op");
        for fragment in dropped {
            assert!(m.holds_fragment(fragment.head()));
            assert!(!m.add_fragment(fragment));
        }
        assert_eq!(*m.minimized(&CountLeadingZeroBytes), minimal);
    }

    #[test]
    fn deref_exposes_tree_reads() {
        let m = MinimizedSedimentree::already_minimal(dominating_tree());
        // `fragments()` reached through Deref<Target = Sedimentree>.
        assert_eq!(m.fragments().count(), 2);
    }

    /// The wrapper's `heads` (which minimizes once if dirty, then reads heads
    /// off the minimal tree) must agree with `Sedimentree::heads` regardless of
    /// the starting dirty/clean state — for a tree that minimization actually
    /// reshapes.
    #[test]
    fn wrapper_heads_match_sedimentree_heads() {
        let full = dominating_tree();
        let expected: BTreeSet<_> = full.heads(&CountLeadingZeroBytes).into_iter().collect();

        // From a dirty wrapper.
        let mut dirty = MinimizedSedimentree::new(full.clone());
        let from_dirty: BTreeSet<_> = dirty.heads(&CountLeadingZeroBytes).into_iter().collect();
        assert_eq!(from_dirty, expected);
        assert!(!dirty.is_dirty(), "heads must leave the wrapper minimized");

        // From a clean wrapper (already minimal).
        let mut clean =
            MinimizedSedimentree::already_minimal(full.minimize(&CountLeadingZeroBytes));
        let from_clean: BTreeSet<_> = clean.heads(&CountLeadingZeroBytes).into_iter().collect();
        assert_eq!(from_clean, expected);
    }
}
