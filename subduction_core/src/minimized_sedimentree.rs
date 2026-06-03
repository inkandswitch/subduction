//! [`MinimizedSedimentree`]: a [`Sedimentree`] owner that tracks whether the
//! tree still needs minimizing.
//!
//! # Why the dirty flag lives here, not on `Sedimentree`
//!
//! `Sedimentree` (in `sedimentree_core`) is a pure data type: equality,
//! ordering, and hashing are defined solely by its contents, and it has no
//! notion of "minimal vs. not". Minimization is a *policy* of the owning sync
//! layer, so the dirty bit that drives lazy minimization belongs to the owner
//! — this wrapper — rather than polluting the data type.
//!
//! # Behavior
//!
//! - Mutations ([`add_commit`](Self::add_commit) /
//!   [`add_fragment`](Self::add_fragment) / [`merge`](Self::merge)) mark the
//!   tree dirty but do **not** minimize.
//! - [`ensure_minimized`](Self::ensure_minimized) minimizes **in place** (via
//!   [`Sedimentree::minimize_in_place`]) only if dirty, then clears the flag.
//!   A burst of writes followed by one `ensure_minimized` costs a single
//!   minimize instead of one per write, and re-calling it on a clean tree is
//!   free.
//!
//! The wrapper [`Deref`]s to [`Sedimentree`] for read-only access. Callers that
//! depend on the minimal form (the wire / sync paths) must call
//! [`ensure_minimized`](Self::ensure_minimized) first — which the owner already
//! does at every point it previously eagerly minimized.

use core::ops::Deref;

use sedimentree_core::{
    depth::DepthMetric, fragment::Fragment, loose_commit::LooseCommit, sedimentree::Sedimentree,
};

/// A [`Sedimentree`] plus an external "needs minimizing" flag.
///
/// See the [module docs](self) for why the flag is here rather than on
/// `Sedimentree`.
#[derive(Debug, Clone, Default)]
pub struct MinimizedSedimentree {
    tree: Sedimentree,
    /// `true` when `tree` may not be in minimal form (a mutation happened
    /// since the last [`ensure_minimized`](Self::ensure_minimized)).
    dirty: bool,
}

impl MinimizedSedimentree {
    /// Wrap an existing tree. The tree is assumed to be in an unknown state,
    /// so it starts dirty (the next [`ensure_minimized`](Self::ensure_minimized)
    /// will minimize it).
    #[must_use]
    pub const fn new(tree: Sedimentree) -> Self {
        Self { tree, dirty: true }
    }

    /// Wrap a tree that is already known to be minimal (e.g. freshly produced
    /// by [`Sedimentree::minimize`]). Starts clean.
    #[must_use]
    pub const fn already_minimal(tree: Sedimentree) -> Self {
        Self { tree, dirty: false }
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

    /// Add a loose commit, marking the tree dirty. Returns `true` if the
    /// commit was newly added (mirrors [`Sedimentree::add_commit`]).
    pub fn add_commit(&mut self, commit: LooseCommit) -> bool {
        let added = self.tree.add_commit(commit);
        self.dirty = true;
        added
    }

    /// Add a fragment, marking the tree dirty. Returns `true` if the fragment
    /// was newly added (mirrors [`Sedimentree::add_fragment`]).
    pub fn add_fragment(&mut self, fragment: Fragment) -> bool {
        let added = self.tree.add_fragment(fragment);
        self.dirty = true;
        added
    }

    /// Merge another tree in, marking the tree dirty.
    pub fn merge(&mut self, other: Sedimentree) {
        self.tree.merge(other);
        self.dirty = true;
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

    /// Consume the wrapper and return the inner tree (without minimizing).
    #[must_use]
    pub fn into_tree(self) -> Sedimentree {
        self.tree
    }
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
mod tests {
    use alloc::{collections::BTreeSet, vec};

    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        commit::CountLeadingZeroBytes,
        crypto::fingerprint::FingerprintSeed,
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
        sedimentree::Sedimentree,
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

    /// Wire-correctness invariant the in-place/dirty-flag design hinges on:
    /// reading a **dirty** (not-yet-minimized) tree through `minimized(...)`
    /// must produce the SAME `FingerprintSummary` as the old eager-collapse
    /// behavior (`tree.minimize(metric).fingerprint_summarize(seed)`).
    ///
    /// If `minimized` ever forgot to minimize, or minimized differently, the
    /// wire diff would silently send the wrong fingerprints. This pins it.
    #[test]
    fn dirty_tree_summary_matches_eager_collapse() {
        let full = dominating_tree();
        let seed = FingerprintSeed::new(0x1234, 0x5678);

        // Old behavior: eagerly collapse, then summarize.
        let eager_summary = full
            .clone()
            .minimize(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        // New behavior: wrap dirty (un-minimized), read via `minimized`.
        let mut m = MinimizedSedimentree::new(full);
        assert!(m.is_dirty(), "precondition: tree starts dirty");
        let lazy_summary = m
            .minimized(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        assert_eq!(
            lazy_summary, eager_summary,
            "lazy minimize-on-read must match eager collapse for wire summaries"
        );
    }

    /// Same invariant, but with the mutation applied *through the wrapper*
    /// after construction (the real ingest pattern: add then read).
    #[test]
    fn summary_after_wrapper_mutation_matches_eager_collapse() {
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

        // Build the equivalent tree two ways.
        let eager = Sedimentree::new(vec![deep.clone(), shallow.clone()], vec![])
            .minimize(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        let mut m = MinimizedSedimentree::already_minimal(Sedimentree::default());
        m.add_fragment(shallow);
        m.add_fragment(deep);
        assert!(m.is_dirty());
        let lazy = m
            .minimized(&CountLeadingZeroBytes)
            .fingerprint_summarize(&seed);

        assert_eq!(
            lazy, eager,
            "wrapper-mutated dirty read must match eager collapse"
        );
    }

    #[test]
    fn minimized_returns_minimal_form() {
        let full = dominating_tree();
        let mut m = MinimizedSedimentree::new(full.clone());

        let minimal_ref = m.minimized(&CountLeadingZeroBytes);
        assert_eq!(*minimal_ref, full.minimize(&CountLeadingZeroBytes));
        assert!(!m.is_dirty());
    }

    #[test]
    fn deref_exposes_tree_reads() {
        let m = MinimizedSedimentree::already_minimal(dominating_tree());
        // `fragments()` reached through Deref<Target = Sedimentree>.
        assert_eq!(m.fragments().count(), 2);
    }
}
