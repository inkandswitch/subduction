//! The main Sedimentree data structure and related types.

mod commit_dag;

use alloc::{
    collections::{BTreeSet, VecDeque},
    vec,
    vec::Vec,
};

use crate::{
    collections::{Entry, Map, Set},
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    depth::{Depth, DepthMetric, MAX_STRATA_DEPTH},
    fragment::{Fragment, checkpoint::Checkpoint, id::FragmentId},
    loose_commit::{LooseCommit, id::CommitId},
    topsorted::Topsorted,
};

/// A compact summary of a [`Sedimentree`] for wire transmission.
///
/// Uses SipHash-2-4 fingerprints instead of full structural data.
/// Each side computes fingerprints with the shared [`FingerprintSeed`]
/// and performs set difference on u64 values.
///
/// Bandwidth: ~16 bytes (seed) + 8 bytes per item, vs ~100+ bytes
/// per item with [`SedimentreeSummary`].
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FingerprintSummary {
    seed: FingerprintSeed,
    commit_fingerprints: BTreeSet<Fingerprint<CommitId>>,
    fragment_fingerprints: BTreeSet<Fingerprint<FragmentId>>,
}

impl FingerprintSummary {
    /// Constructor for a [`FingerprintSummary`].
    #[must_use]
    pub const fn new(
        seed: FingerprintSeed,
        commit_fingerprints: BTreeSet<Fingerprint<CommitId>>,
        fragment_fingerprints: BTreeSet<Fingerprint<FragmentId>>,
    ) -> Self {
        Self {
            seed,
            commit_fingerprints,
            fragment_fingerprints,
        }
    }

    /// The seed used to compute the fingerprints.
    #[must_use]
    pub const fn seed(&self) -> &FingerprintSeed {
        &self.seed
    }

    /// The fingerprints of commit causal identities.
    #[must_use]
    pub const fn commit_fingerprints(&self) -> &BTreeSet<Fingerprint<CommitId>> {
        &self.commit_fingerprints
    }

    /// The fingerprints of fragment causal identities.
    #[must_use]
    pub const fn fragment_fingerprints(&self) -> &BTreeSet<Fingerprint<FragmentId>> {
        &self.fragment_fingerprints
    }
}

/// The result of diffing a local [`Sedimentree`] against a remote
/// [`FingerprintSummary`].
///
/// The responder knows:
/// - Which of its own items the requestor is missing (full data available)
/// - Which of the requestor's fingerprints it doesn't recognize (echoed back)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintDiff<'a> {
    /// Fragments the responder has that the requestor is missing,
    /// paired with their precomputed digests (from the `Sedimentree` map key).
    pub local_only_fragments: Vec<(&'a Digest<Fragment>, &'a Fragment)>,

    /// Commits the responder has that the requestor is missing,
    /// paired with their content-addressed digests.
    pub local_only_commits: Vec<(&'a CommitId, &'a LooseCommit)>,

    /// Requestor's commit fingerprints that the responder doesn't have locally.
    /// Echoed back so the requestor can reverse-lookup and send the data.
    pub remote_only_commit_fingerprints: Vec<Fingerprint<CommitId>>,

    /// Requestor's fragment fingerprints that the responder doesn't have locally.
    /// Echoed back so the requestor can reverse-lookup and send the data.
    pub remote_only_fragment_fingerprints: Vec<Fingerprint<FragmentId>>,
}

/// A pre-captured reverse-lookup table for resolving fingerprints back to digests.
///
/// Created by [`Sedimentree::fingerprint_resolver`], this type captures
/// the fingerprint-to-digest mappings _at the time of construction_,
/// ensuring that subsequent tree mutations (e.g., [`Sedimentree::minimize`]) cannot
/// invalidate the reverse lookup.
///
/// This is the type-level enforcement of the invariant: the reverse-lookup
/// must use the same tree state that generated the [`FingerprintSummary`].
///
/// # Example
///
/// ```ignore
/// let resolver = tree.fingerprint_resolver(&seed);
/// let summary = resolver.summary();
/// // ... send summary to peer, receive RequestedData ...
/// // ... ingest response, minimize tree (safe — resolver is independent) ...
/// let digest = resolver.resolve_commit(&fingerprint);
/// ```
#[derive(Debug, Clone)]
pub struct FingerprintResolver {
    summary: FingerprintSummary,
    commit_fp_to_id: Map<Fingerprint<CommitId>, CommitId>,
    fragment_fp_to_digest: Map<Fingerprint<FragmentId>, Digest<Fragment>>,
}

impl FingerprintResolver {
    /// The [`FingerprintSummary`] for wire transmission.
    #[must_use]
    pub const fn summary(&self) -> &FingerprintSummary {
        &self.summary
    }

    /// The seed used to compute the fingerprints.
    #[must_use]
    pub const fn seed(&self) -> &FingerprintSeed {
        self.summary.seed()
    }

    /// Resolve a commit fingerprint to its content-addressed digest.
    ///
    /// Returns `None` if the fingerprint was not in the tree at construction time.
    #[must_use]
    pub fn resolve_commit(&self, fp: &Fingerprint<CommitId>) -> Option<CommitId> {
        self.commit_fp_to_id.get(fp).copied()
    }

    /// Resolve a fragment fingerprint to its content-addressed digest.
    ///
    /// Returns `None` if the fingerprint was not in the tree at construction time.
    #[must_use]
    pub fn resolve_fragment(&self, fp: &Fingerprint<FragmentId>) -> Option<Digest<Fragment>> {
        self.fragment_fp_to_digest.get(fp).copied()
    }
}

/// The difference between two [`Sedimentree`]s.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diff<'a> {
    /// Fragments present in the right tree but not the left.
    pub left_missing_fragments: Vec<&'a Fragment>,

    /// Commits present in the right tree but not the left.
    pub left_missing_commits: Vec<&'a LooseCommit>,

    /// Fragments present in the left tree but not the right.
    pub right_missing_fragments: Vec<&'a Fragment>,

    /// Commits present in the left tree but not the right.
    pub right_missing_commits: Vec<&'a LooseCommit>,
}

/// All of the Sedimentree metadata about all the fragments for a series of payload.
#[derive(Default, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(PartialOrd, Ord, Hash))]
pub struct Sedimentree {
    fragments: Map<Digest<Fragment>, Fragment>,
    commits: Map<CommitId, LooseCommit>,
}

impl Sedimentree {
    /// Constructor for a [`Sedimentree`].
    ///
    /// If `commits` contains duplicate [`CommitId`] values, only one is
    /// retained (last-in-wins per [`BTreeMap`] collection semantics).
    /// For well-behaved peers this should never happen since each commit
    /// has a unique user-supplied identifier.
    #[must_use]
    pub fn new(fragments: Vec<Fragment>, commits: Vec<LooseCommit>) -> Self {
        Self {
            fragments: fragments
                .into_iter()
                .map(|f| (Digest::hash(&f), f))
                .collect(),
            commits: commits.into_iter().map(|c| (c.head(), c)).collect(),
        }
    }

    /// Merge another [`Sedimentree`] into this one.
    pub fn merge(&mut self, other: Sedimentree) {
        self.fragments.extend(other.fragments);
        self.commits.extend(other.commits);
    }

    /// The minimal ordered hash of this [`Sedimentree`].
    #[must_use]
    pub fn minimal_hash<M: DepthMetric>(&self, depth_metric: &M) -> MinimalTreeHash {
        let minimal = self.minimize(depth_metric);

        // Hash causal identities (head, boundary, commit heads)
        let mut ids: Vec<CommitId> = minimal
            .fragments()
            .flat_map(|s| core::iter::once(s.head()).chain(s.boundary().iter().copied()))
            .chain(minimal.commits.values().map(LooseCommit::head))
            .collect();
        ids.sort();

        let mut h = blake3::Hasher::new();
        for id in &ids {
            h.update(id.as_bytes());
        }

        // Hash checkpoints separately
        let mut checkpoints: Vec<Checkpoint> = minimal
            .fragments()
            .flat_map(|s| s.checkpoints().iter().copied())
            .collect();
        checkpoints.sort();
        for cp in &checkpoints {
            h.update(cp.as_bytes());
        }
        MinimalTreeHash(*h.finalize().as_bytes())
    }

    /// Add a loose commit to the [`Sedimentree`].
    ///
    /// Returns `true` if the commit was not already present
    pub fn add_commit(&mut self, commit: LooseCommit) -> bool {
        let id = commit.head();
        match self.commits.entry(id) {
            Entry::Vacant(e) => {
                e.insert(commit);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Add a fragment to the [`Sedimentree`].
    ///
    /// Returns `true` if the stratum was not already present
    pub fn add_fragment(&mut self, fragment: Fragment) -> bool {
        let digest = Digest::hash(&fragment);
        match self.fragments.entry(digest) {
            Entry::Vacant(e) => {
                e.insert(fragment);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Compute the difference between two local [`Sedimentree`]s.
    #[must_use]
    pub fn diff<'a>(&'a self, other: &'a Sedimentree) -> Diff<'a> {
        let self_commit_keys: Set<_> = self.commits.keys().collect();
        let other_commit_keys: Set<_> = other.commits.keys().collect();
        let self_fragment_keys: Set<_> = self.fragments.keys().collect();
        let other_fragment_keys: Set<_> = other.fragments.keys().collect();

        Diff {
            // Items in right but not left = what left is missing
            left_missing_fragments: other_fragment_keys
                .difference(&self_fragment_keys)
                .filter_map(|k| other.fragments.get(k))
                .collect(),
            left_missing_commits: other_commit_keys
                .difference(&self_commit_keys)
                .filter_map(|k| other.commits.get(k))
                .collect(),
            // Items in left but not right = what right is missing
            right_missing_fragments: self_fragment_keys
                .difference(&other_fragment_keys)
                .filter_map(|k| self.fragments.get(k))
                .collect(),
            right_missing_commits: self_commit_keys
                .difference(&other_commit_keys)
                .filter_map(|k| self.commits.get(k))
                .collect(),
        }
    }

    /// Iterate over all fragments in this [`Sedimentree`].
    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.values()
    }

    /// Iterate over all fragments with their precomputed digests.
    ///
    /// The digest is the map key, computed once at insertion time.
    /// Use this instead of `fragments().map(|f| Digest::hash(f))` to
    /// avoid re-hashing.
    pub fn fragment_entries(&self) -> impl Iterator<Item = (&Digest<Fragment>, &Fragment)> {
        self.fragments.iter()
    }

    /// Iterate over all loose commits in this [`Sedimentree`].
    pub fn loose_commits(&self) -> impl Iterator<Item = &LooseCommit> {
        self.commits.values()
    }

    /// Iterate over all loose commits with their precomputed digests.
    pub fn commit_entries(&self) -> impl Iterator<Item = (&CommitId, &LooseCommit)> {
        self.commits.iter()
    }

    /// Returns true if this [`Sedimentree`] has a commit with the given causal identity.
    #[must_use]
    pub fn has_loose_commit(&self, id: CommitId) -> bool {
        self.commits.contains_key(&id)
    }

    /// Returns true if this [`Sedimentree`] has a fragment starting with the given digest.
    #[must_use]
    pub fn has_fragment_starting_with<M: DepthMetric>(
        &self,
        id: CommitId,
        depth_metric: &M,
    ) -> bool {
        self.heads(depth_metric).contains(&id)
    }

    /// Topologically sort the fragments and loose commits for reassembly.
    ///
    /// Returns [`SedimentreeItem`]s in dependency order: deepest fragments first,
    /// so that each item's causal dependencies are loaded before the item
    /// itself. This eliminates orphan-queue overhead in consumers that
    /// reconstruct a document by loading blobs sequentially (e.g. via
    /// Automerge's `load_incremental`).
    ///
    /// The ordering is determined by the sedimentree DAG structure:
    /// - A fragment depends on any fragment whose head appears in its
    ///   boundary set.
    /// - A loose commit depends on any fragment whose head appears in
    ///   its parent set.
    ///
    /// Uses Kahn's algorithm. Returns [`CycleError`] if the dependency
    /// graph contains a cycle, which indicates corrupt fragment metadata
    /// (e.g. from a malicious peer).
    ///
    /// # Errors
    ///
    /// Returns [`CycleError`] if the fragment/commit dependency graph
    /// contains a cycle.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let order = sedimentree.topsorted_blob_order()?;
    /// let fragments: Vec<_> = sedimentree.fragments().collect();
    /// let loose: Vec<_> = sedimentree.loose_commits().collect();
    ///
    /// let mut buf = Vec::new();
    /// for item in &order {
    ///     match item {
    ///         SedimentreeItem::Fragment(i) => buf.extend(load_blob(fragments[*i])),
    ///         SedimentreeItem::LooseCommit(i) => buf.extend(load_blob(loose[*i])),
    ///     }
    /// }
    /// doc.load_incremental(&buf).unwrap();
    /// ```
    #[allow(clippy::indexing_slicing)] // Indices bounded by construction from enumerate()
    pub fn topsorted_blob_order(&self) -> Result<Topsorted<SedimentreeItem>, CycleError> {
        let fragments: Vec<&Fragment> = self.fragments.values().collect();
        let loose: Vec<&LooseCommit> = self.commits.values().collect();
        let n_frags = fragments.len();
        let n_loose = loose.len();
        let n_total = n_frags + n_loose;

        if n_total == 0 {
            return Ok(Topsorted(Vec::new()));
        }

        // Map: fragment head identifier → fragment index
        let head_to_frag: Map<CommitId, usize> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| (f.head(), i))
            .collect();

        // Build in-degree counts and adjacency lists.
        let mut in_degree = vec![0u32; n_total];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n_total];

        // Fragment → fragment edges: F_i depends on F_j if F_i's boundary
        // contains F_j's head. Self-loops (i == j) are included so that
        // Kahn's algorithm detects them as cycles.
        for (i, frag) in fragments.iter().enumerate() {
            for boundary_digest in frag.boundary() {
                if let Some(&j) = head_to_frag.get(boundary_digest) {
                    in_degree[i] += 1;
                    dependents[j].push(i);
                }
            }
        }

        // Map: loose commit head → loose index (for loose→loose edges)
        let id_to_loose: Map<CommitId, usize> = loose
            .iter()
            .enumerate()
            .map(|(i, c)| (c.head(), i))
            .collect();

        // Loose commit dependency edges:
        // - L depends on fragment F if any of L's parents equals F's head.
        // - L depends on loose commit P if any of L's parents equals P's digest.
        for (li, commit) in loose.iter().enumerate() {
            let idx = n_frags + li;
            let mut seen_deps: Set<usize> = Set::new();
            for parent in commit.parents() {
                if let Some(&fi) = head_to_frag.get(parent)
                    && seen_deps.insert(fi)
                {
                    in_degree[idx] += 1;
                    dependents[fi].push(idx);
                }
                if let Some(&pli) = id_to_loose.get(parent) {
                    let parent_idx = n_frags + pli;
                    // Self-loops (parent_idx == idx) are included so that
                    // Kahn's algorithm detects them as cycles.
                    if seen_deps.insert(parent_idx) {
                        in_degree[idx] += 1;
                        dependents[parent_idx].push(idx);
                    }
                }
            }
        }

        // Kahn's algorithm
        let mut queue: VecDeque<usize> = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push_back(i);
            }
        }

        let mut order: Vec<SedimentreeItem> = Vec::with_capacity(n_total);
        while let Some(i) = queue.pop_front() {
            if i < n_frags {
                order.push(SedimentreeItem::Fragment(i));
            } else {
                order.push(SedimentreeItem::LooseCommit(i - n_frags));
            }
            for &dep in &dependents[i] {
                in_degree[dep] -= 1;
                if in_degree[dep] == 0 {
                    queue.push_back(dep);
                }
            }
        }

        if order.len() != n_total {
            return Err(CycleError {
                total: n_total,
                sorted: order.len(),
            });
        }

        Ok(Topsorted(order))
    }

    /// Prune a [`Sedimentree`].
    ///
    /// Minimize the [`Sedimentree`] by removing any fragments that are
    /// fully supported by other fragments, and removing any loose commits
    /// that are not needed to support the remaining fragments.
    ///
    /// Uses a group-by-depth algorithm (Option B) that processes fragments
    /// from deepest to shallowest. Deeper fragments are always kept, and
    /// shallower fragments are discarded if their entire range (head + boundary)
    /// is supported by already-accepted deeper fragments.
    ///
    /// Complexity: `O(|M| × avg_checkpoints)` where M = total fragments.
    #[must_use]
    pub fn minimize<M: DepthMetric>(&self, depth_metric: &M) -> Sedimentree {
        // 1. Group fragments by depth
        let mut by_depth: Map<Depth, Vec<&Fragment>> = Map::new();
        for fragment in self.fragments.values() {
            by_depth
                .entry(fragment.depth(depth_metric))
                .or_default()
                .push(fragment);
        }

        // 2. Collect depths and sort descending (deepest first)
        let mut depths: Vec<Depth> = by_depth.keys().copied().collect();
        depths.sort_by(|a, b| b.cmp(a)); // Descending

        // 3. Process deepest first, building supported set
        let mut minimized_fragments = Vec::<Fragment>::new();
        let mut supported: Set<Checkpoint> = Set::new();

        for depth in depths {
            if let Some(group) = by_depth.remove(&depth) {
                for fragment in group {
                    // Check if this fragment is fully supported by deeper fragments
                    let dominated = supported.contains(&Checkpoint::new(fragment.head()))
                        && fragment
                            .summary()
                            .boundary()
                            .iter()
                            .all(|b| supported.contains(&Checkpoint::new(*b)));

                    if !dominated {
                        // Accept this fragment and add its commits to supported set
                        supported.insert(Checkpoint::new(fragment.head()));
                        supported.extend(fragment.checkpoints().iter().copied());
                        for b in fragment.summary().boundary() {
                            supported.insert(Checkpoint::new(*b));
                        }

                        minimized_fragments.push(fragment.clone());
                    }
                }
            }
        }

        // 4. Simplify loose commits relative to minimized fragments
        let dag = commit_dag::CommitDag::from_commits(self.commits.values());
        let simplified_dag = dag.simplify(&minimized_fragments, depth_metric);

        let commits = self
            .commits
            .values()
            .filter(|c| simplified_dag.contains_commit(&c.head()))
            .cloned()
            .collect();

        Sedimentree::new(minimized_fragments, commits)
    }

    /// Create a [`FingerprintSummary`] from this [`Sedimentree`].
    ///
    /// Computes SipHash-2-4 fingerprints of each item's causal identity
    /// using the given seed. Much smaller than [`summarize`](Self::summarize)
    /// for wire transmission.
    #[must_use]
    pub fn fingerprint_summarize(&self, seed: &FingerprintSeed) -> FingerprintSummary {
        let commit_fingerprints = self
            .commits
            .values()
            .map(|c| Fingerprint::new(seed, &c.head()))
            .collect();

        let fragment_fingerprints = self
            .fragments
            .values()
            .map(|f| Fingerprint::new(seed, &f.fragment_id()))
            .collect();

        FingerprintSummary::new(*seed, commit_fingerprints, fragment_fingerprints)
    }

    /// Create a [`FingerprintResolver`] that captures both the wire summary
    /// and the reverse-lookup tables from the current tree state.
    ///
    /// The resolver is independent of subsequent mutations to this tree
    /// (e.g., [`minimize`](Self::minimize)), so fingerprint resolution
    /// remains valid even after the in-memory tree changes.
    ///
    /// Use [`FingerprintResolver::summary`] to get the [`FingerprintSummary`]
    /// for wire transmission, and [`FingerprintResolver::resolve_commit`] /
    /// [`FingerprintResolver::resolve_fragment`] to reverse-lookup echoed
    /// fingerprints.
    #[must_use]
    pub fn fingerprint_resolver(&self, seed: &FingerprintSeed) -> FingerprintResolver {
        let commit_fp_to_id: Map<Fingerprint<CommitId>, CommitId> = self
            .commits
            .iter()
            .map(|(id, c)| (Fingerprint::new(seed, &c.head()), *id))
            .collect();

        let fragment_fp_to_digest: Map<Fingerprint<FragmentId>, Digest<Fragment>> = self
            .fragments
            .iter()
            .map(|(digest, f)| (Fingerprint::new(seed, &f.fragment_id()), *digest))
            .collect();

        let commit_fingerprints = commit_fp_to_id.keys().copied().collect();
        let fragment_fingerprints = fragment_fp_to_digest.keys().copied().collect();
        let summary = FingerprintSummary::new(*seed, commit_fingerprints, fragment_fingerprints);

        FingerprintResolver {
            summary,
            commit_fp_to_id,
            fragment_fp_to_digest,
        }
    }

    /// Compute the difference between a local [`Sedimentree`] and a remote
    /// [`FingerprintSummary`].
    ///
    /// The responder uses the requestor's seed to fingerprint its own items,
    /// then performs set difference on u64 values. Returns full data for
    /// items the requestor is missing, and echoed fingerprints for items
    /// the responder is missing.
    #[must_use]
    pub fn diff_remote_fingerprints<'a>(
        &'a self,
        remote: &FingerprintSummary,
    ) -> FingerprintDiff<'a> {
        let seed = remote.seed();

        // Find local items the requestor doesn't have
        let local_only_commits: Vec<(&CommitId, &LooseCommit)> = self
            .commits
            .iter()
            .filter(|(_, c)| {
                !remote
                    .commit_fingerprints
                    .contains(&Fingerprint::new(seed, &c.head()))
            })
            .collect();

        let local_only_fragments: Vec<(&Digest<Fragment>, &Fragment)> = self
            .fragments
            .iter()
            .filter(|(_, f)| {
                !remote
                    .fragment_fingerprints
                    .contains(&Fingerprint::new(seed, &f.fragment_id()))
            })
            .collect();

        // Find requestor fingerprints we don't have locally (echo back)
        let local_commit_fps: BTreeSet<Fingerprint<CommitId>> = self
            .commits
            .values()
            .map(|c| Fingerprint::new(seed, &c.head()))
            .collect();

        let local_fragment_fps: BTreeSet<Fingerprint<FragmentId>> = self
            .fragments
            .values()
            .map(|f| Fingerprint::new(seed, &f.fragment_id()))
            .collect();

        let remote_only_commit_fingerprints: Vec<Fingerprint<CommitId>> = remote
            .commit_fingerprints
            .iter()
            .filter(|fp| !local_commit_fps.contains(fp))
            .copied()
            .collect();

        let remote_only_fragment_fingerprints: Vec<Fingerprint<FragmentId>> = remote
            .fragment_fingerprints
            .iter()
            .filter(|fp| !local_fragment_fps.contains(fp))
            .copied()
            .collect();

        FingerprintDiff {
            local_only_fragments,
            local_only_commits,
            remote_only_commit_fingerprints,
            remote_only_fragment_fingerprints,
        }
    }

    /// The heads of a Sedimentree are the end hashes of all strata which are
    /// not the start of any other strata or supported by any lower stratum
    /// and which do not appear in the [`LooseCommit`] graph, plus the heads of
    /// the loose commit graph.
    #[must_use]
    pub fn heads<M: DepthMetric>(&self, depth_metric: &M) -> Vec<CommitId> {
        let minimized = self.minimize(depth_metric);
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.values());
        let mut heads = Vec::<CommitId>::new();
        for fragment in minimized.fragments.values() {
            if !minimized
                .fragments
                .values()
                .any(|s| s.boundary().contains(&fragment.head()))
                && fragment
                    .boundary()
                    .iter()
                    .all(|end| !dag.contains_commit(end))
            {
                heads.extend(fragment.boundary().iter().copied());
            }
        }
        heads.extend(dag.heads());
        heads
    }

    /// Consume this [`Sedimentree`] and return an iterator over all its items.
    pub fn into_items(self) -> impl Iterator<Item = CommitOrFragment> {
        self.fragments
            .into_values()
            .map(CommitOrFragment::Fragment)
            .chain(self.commits.into_values().map(CommitOrFragment::Commit))
    }
}

/// An item in a [`Sedimentree`], used to identify fragments and loose
/// commits in the topologically sorted order returned by
/// [`Sedimentree::topsorted_blob_order`].
///
/// The index corresponds to the position of the fragment or loose commit
/// in the iteration order of [`Sedimentree::fragments`] or
/// [`Sedimentree::loose_commits`], respectively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SedimentreeItem {
    /// A fragment, by index into [`Sedimentree::fragments`].
    Fragment(usize),

    /// A loose commit, by index into [`Sedimentree::loose_commits`].
    LooseCommit(usize),
}

/// The sedimentree's fragment/commit dependency graph contains a cycle.
///
/// This indicates corrupt metadata — e.g. from a malicious peer sending
/// fragment metadata with circular boundary→head references. In a
/// correctly-constructed sedimentree backed by hash-linked commits,
/// cycles are impossible (they would require a hash collision).
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("cycle in sedimentree dependency graph: {total} items but only {sorted} could be sorted")]
pub struct CycleError {
    total: usize,
    sorted: usize,
}

/// An enum over either a [`LooseCommit`] or a [`Fragment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOrFragment {
    /// A loose commit.
    Commit(LooseCommit),

    /// A fragment.
    Fragment(Fragment),
}

impl core::fmt::Debug for Sedimentree {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Sedimentree")
            .field("fragments", &self.fragments.len())
            .field("commits", &self.commits.len())
            .finish()
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Sedimentree {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let fragments: Vec<Fragment> = u.arbitrary()?;
        let commits: Vec<LooseCommit> = u.arbitrary()?;
        Ok(Self::new(fragments, commits))
    }
}

/// The minimum ordered hash of a [`Sedimentree`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct MinimalTreeHash([u8; 32]);

impl MinimalTreeHash {
    /// The bytes of this [`MinimalTreeHash`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<[u8; 32]> for MinimalTreeHash {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

/// Checks if any of the given commits has a commit boundary.
pub fn has_commit_boundary<I: IntoIterator<Item = D>, D: Into<CommitId>, M: DepthMetric>(
    commits: I,
    depth_metric: &M,
) -> bool {
    commits
        .into_iter()
        .any(|id| depth_metric.to_depth(id.into()) <= MAX_STRATA_DEPTH)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::many_single_char_names
)]
mod tests {
    use testresult::TestResult;

    use crate::{
        blob::{Blob, BlobMeta},
        id::SedimentreeId,
    };

    use super::*;

    fn make_sedimentree_id(seed: u8) -> SedimentreeId {
        SedimentreeId::new([seed; 32])
    }

    fn make_blob_meta(seed: u8) -> BlobMeta {
        let blob = Blob::new(vec![seed]);
        BlobMeta::new(&blob)
    }

    fn make_commit(seed: u8) -> LooseCommit {
        let sedimentree_id = make_sedimentree_id(seed);
        let blob = Blob::from(&[seed][..]);
        let blob_meta = BlobMeta::new(&blob);
        let head = CommitId::new({
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            bytes
        });
        LooseCommit::new(sedimentree_id, head, BTreeSet::new(), blob_meta)
    }

    fn make_fragment(seed: u8) -> Fragment {
        let sedimentree_id = make_sedimentree_id(seed);
        let mut head_bytes = [0u8; 32];
        head_bytes[0] = seed;
        let mut boundary_bytes = [0u8; 32];
        boundary_bytes[0] = seed;
        boundary_bytes[1] = 1;
        let blob = Blob::from(&[seed][..]);
        let blob_meta = BlobMeta::new(&blob);
        Fragment::new(
            sedimentree_id,
            CommitId::new(head_bytes),
            BTreeSet::from([CommitId::new(boundary_bytes)]),
            &[],
            blob_meta,
        )
    }

    #[test]
    fn diff_superset_commits() {
        // Scenario: B is a superset of A
        let shared = vec![make_commit(1), make_commit(2)];
        let extra = vec![make_commit(3), make_commit(4)];

        let a = Sedimentree::new(vec![], shared.clone());
        let b = Sedimentree::new(vec![], [shared, extra.clone()].concat());

        let diff = a.diff(&b);

        // A is missing the extra commits (what B has that A doesn't)
        assert_eq!(diff.left_missing_commits.len(), 2);
        // B is missing nothing
        assert!(diff.right_missing_commits.is_empty());
        assert!(diff.left_missing_fragments.is_empty());
        assert!(diff.right_missing_fragments.is_empty());
    }

    #[test]
    fn diff_superset_fragments() {
        // Scenario: B is a superset of A
        let shared = vec![make_fragment(1), make_fragment(2)];
        let extra = vec![make_fragment(3)];

        let a = Sedimentree::new(shared.clone(), vec![]);
        let b = Sedimentree::new([shared, extra].concat(), vec![]);

        let diff = a.diff(&b);

        // A is missing the extra fragment
        assert_eq!(diff.left_missing_fragments.len(), 1);
        // B is missing nothing
        assert!(diff.right_missing_fragments.is_empty());
        assert!(diff.left_missing_commits.is_empty());
        assert!(diff.right_missing_commits.is_empty());
    }

    #[test]
    fn diff_diverged_with_overlap() {
        // Scenario: A and B share some history but have diverged
        let shared_commits = vec![make_commit(1), make_commit(2)];
        let a_only_commits = vec![make_commit(10), make_commit(11)];
        let b_only_commits = vec![make_commit(20), make_commit(21), make_commit(22)];

        let shared_fragments = vec![make_fragment(1)];
        let a_only_fragments = vec![make_fragment(10)];
        let b_only_fragments = vec![make_fragment(20), make_fragment(21)];

        let a = Sedimentree::new(
            [shared_fragments.clone(), a_only_fragments.clone()].concat(),
            [shared_commits.clone(), a_only_commits.clone()].concat(),
        );
        let b = Sedimentree::new(
            [shared_fragments, b_only_fragments].concat(),
            [shared_commits, b_only_commits].concat(),
        );

        let diff = a.diff(&b);

        // A is missing B's unique items
        assert_eq!(diff.left_missing_commits.len(), 3); // b_only_commits
        assert_eq!(diff.left_missing_fragments.len(), 2); // b_only_fragments

        // B is missing A's unique items
        assert_eq!(diff.right_missing_commits.len(), 2); // a_only_commits
        assert_eq!(diff.right_missing_fragments.len(), 1); // a_only_fragments
    }

    // ── topsorted_blob_order ──────────────────────────────────────

    /// Build a fragment with a specific head and boundary, suitable for
    /// testing the dependency graph in `topsorted_blob_order`.
    fn make_fragment_with_deps(head_bytes: [u8; 32], boundary: BTreeSet<CommitId>) -> Fragment {
        let sid = make_sedimentree_id(0);
        let blob_meta = make_blob_meta(0);
        Fragment::new(sid, CommitId::new(head_bytes), boundary, &[], blob_meta)
    }

    fn unique_head(id: u8) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[31] = id;
        bytes
    }

    fn unique_commit_id(id: u8) -> CommitId {
        CommitId::new(unique_head(id))
    }

    #[test]
    fn topsorted_empty_sedimentree() -> TestResult {
        let tree = Sedimentree::default();
        let order = tree.topsorted_blob_order()?;
        assert!(order.is_empty());
        Ok(())
    }

    #[test]
    fn topsorted_single_fragment() -> TestResult {
        let mut tree = Sedimentree::default();
        let f = make_fragment_with_deps(unique_head(1), BTreeSet::new());
        tree.add_fragment(f);

        let order = tree.topsorted_blob_order()?;
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], SedimentreeItem::Fragment(0));
        Ok(())
    }

    #[test]
    fn topsorted_single_loose_commit() -> TestResult {
        let mut tree = Sedimentree::default();
        tree.add_commit(make_commit(1));

        let order = tree.topsorted_blob_order()?;
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], SedimentreeItem::LooseCommit(0));
        Ok(())
    }

    /// Helper: extract fragment head positions from a topsort ordering.
    fn fragment_head_positions(order: &[SedimentreeItem], tree: &Sedimentree) -> Vec<[u8; 32]> {
        let fragments: Vec<_> = tree.fragments().collect();
        order
            .iter()
            .filter_map(|item| match item {
                SedimentreeItem::Fragment(i) => Some(*fragments[*i].head().as_bytes()),
                SedimentreeItem::LooseCommit(_) => None,
            })
            .collect()
    }

    #[test]
    fn topsorted_linear_chain_deepest_first() -> TestResult {
        // C (deep) ← B (mid) ← A (shallow)
        // A.boundary = {B.head}, B.boundary = {C.head}, C.boundary = {}
        let c_head = unique_head(3);
        let b_head = unique_head(2);
        let a_head = unique_head(1);

        let c = make_fragment_with_deps(c_head, BTreeSet::new());
        let b = make_fragment_with_deps(b_head, BTreeSet::from([CommitId::new(c_head)]));
        let a = make_fragment_with_deps(a_head, BTreeSet::from([CommitId::new(b_head)]));

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);
        tree.add_fragment(c);

        let order = tree.topsorted_blob_order()?;
        assert_eq!(order.len(), 3);

        let head_order = fragment_head_positions(&order, &tree);

        let c_pos = head_order
            .iter()
            .position(|h| *h == c_head)
            .ok_or("C not found")?;
        let b_pos = head_order
            .iter()
            .position(|h| *h == b_head)
            .ok_or("B not found")?;
        let a_pos = head_order
            .iter()
            .position(|h| *h == a_head)
            .ok_or("A not found")?;
        assert!(c_pos < b_pos, "C (deepest) must come before B");
        assert!(b_pos < a_pos, "B must come before A (shallowest)");
        Ok(())
    }

    #[test]
    fn topsorted_diamond_dag() -> TestResult {
        //     D (deep)
        //    / \
        //   B   C
        //    \ /
        //     A (shallow)
        let d_head = unique_head(4);
        let b_head = unique_head(2);
        let c_head = unique_head(3);
        let a_head = unique_head(1);

        let d = make_fragment_with_deps(d_head, BTreeSet::new());
        let b = make_fragment_with_deps(b_head, BTreeSet::from([CommitId::new(d_head)]));
        let c = make_fragment_with_deps(c_head, BTreeSet::from([CommitId::new(d_head)]));
        let a = make_fragment_with_deps(
            a_head,
            BTreeSet::from([CommitId::new(b_head), CommitId::new(c_head)]),
        );

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);
        tree.add_fragment(c);
        tree.add_fragment(d);

        let order = tree.topsorted_blob_order()?;
        assert_eq!(order.len(), 4);

        let head_order = fragment_head_positions(&order, &tree);

        let d_pos = head_order
            .iter()
            .position(|h| *h == d_head)
            .ok_or("D not found")?;
        let b_pos = head_order
            .iter()
            .position(|h| *h == b_head)
            .ok_or("B not found")?;
        let c_pos = head_order
            .iter()
            .position(|h| *h == c_head)
            .ok_or("C not found")?;
        let a_pos = head_order
            .iter()
            .position(|h| *h == a_head)
            .ok_or("A not found")?;

        assert!(d_pos < b_pos, "D must come before B");
        assert!(d_pos < c_pos, "D must come before C");
        assert!(b_pos < a_pos, "B must come before A");
        assert!(c_pos < a_pos, "C must come before A");
        Ok(())
    }

    #[test]
    fn topsorted_loose_commit_after_fragment() -> TestResult {
        let f_head = unique_head(1);
        let f = make_fragment_with_deps(f_head, BTreeSet::new());

        let sid = make_sedimentree_id(0);
        let blob_meta = make_blob_meta(99);
        let loose_id = unique_commit_id(99);
        let loose = LooseCommit::new(
            sid,
            loose_id,
            BTreeSet::from([CommitId::new(f_head)]),
            blob_meta,
        );

        let mut tree = Sedimentree::default();
        tree.add_fragment(f);
        tree.add_commit(loose);

        let order = tree.topsorted_blob_order()?;
        assert_eq!(order.len(), 2);

        let frag_pos = order
            .iter()
            .position(|i| matches!(i, SedimentreeItem::Fragment(_)))
            .ok_or("no fragment in order")?;
        let loose_pos = order
            .iter()
            .position(|i| matches!(i, SedimentreeItem::LooseCommit(_)))
            .ok_or("no loose commit in order")?;

        assert!(
            frag_pos < loose_pos,
            "fragment must come before dependent loose commit"
        );
        Ok(())
    }

    #[test]
    fn topsorted_cycle_returns_error() {
        let a_head = unique_head(1);
        let b_head = unique_head(2);

        let a = make_fragment_with_deps(a_head, BTreeSet::from([CommitId::new(b_head)]));
        let b = make_fragment_with_deps(b_head, BTreeSet::from([CommitId::new(a_head)]));

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);

        let err = tree
            .topsorted_blob_order()
            .expect_err("cycle should produce CycleError");
        assert_eq!(
            err.to_string(),
            "cycle in sedimentree dependency graph: 2 items but only 0 could be sorted"
        );
    }

    #[cfg(all(test, feature = "bolero"))]
    #[allow(clippy::expect_used, clippy::indexing_slicing, clippy::similar_names)]
    mod proptests {
        use core::sync::atomic::{AtomicU64, Ordering};

        use rand::{Rng, SeedableRng, rngs::SmallRng};

        use super::*;
        use crate::{commit::CountLeadingZeroBytes, fragment::FragmentSummary};

        static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

        fn id_with_leading_zeros(zeros_count: u32) -> CommitId {
            let seed = SEED_COUNTER.fetch_add(1, Ordering::Relaxed);
            let mut rng = SmallRng::seed_from_u64(seed);
            let mut byte_arr: [u8; 32] = rng.r#gen::<[u8; 32]>();
            for slot in byte_arr.iter_mut().take(zeros_count as usize) {
                *slot = 0;
            }
            // Ensure the byte after the zeros is non-zero to prevent accidentally
            // having more leading zeros than intended
            if (zeros_count as usize) < 32 {
                let idx = zeros_count as usize;
                #[allow(clippy::indexing_slicing)]
                if byte_arr[idx] == 0 {
                    byte_arr[idx] = 1; // Make it non-zero
                }
            }
            CommitId::new(byte_arr)
        }

        #[test]
        fn fragment_supports_higher_levels() {
            #[derive(Debug)]
            struct Scenario {
                deeper: Fragment,
                shallower: FragmentSummary,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    #[allow(clippy::enum_variant_names)]
                    #[derive(arbitrary::Arbitrary)]
                    enum ShallowerDepthType {
                        StartsAtStartBoundaryAtCheckpoint,
                        StartsAtCheckpointBoundaryAtCheckpoint,
                        StartsAtCheckpointBoundaryAtBoundary,
                    }

                    let start_hash = id_with_leading_zeros(10);
                    let deeper_boundary_hash = id_with_leading_zeros(10);

                    let shallower_start_hash: CommitId;
                    let shallower_boundary_hash: CommitId;
                    let mut checkpoints = Vec::<CommitId>::arbitrary(u)?;
                    let lower_level_type = ShallowerDepthType::arbitrary(u)?;
                    match lower_level_type {
                        ShallowerDepthType::StartsAtStartBoundaryAtCheckpoint => {
                            shallower_start_hash = start_hash;
                            shallower_boundary_hash = id_with_leading_zeros(9);
                            checkpoints.push(shallower_boundary_hash);
                        }
                        ShallowerDepthType::StartsAtCheckpointBoundaryAtCheckpoint => {
                            shallower_start_hash = id_with_leading_zeros(9);
                            shallower_boundary_hash = id_with_leading_zeros(9);
                            checkpoints.push(shallower_start_hash);
                            checkpoints.push(shallower_boundary_hash);
                        }
                        ShallowerDepthType::StartsAtCheckpointBoundaryAtBoundary => {
                            shallower_start_hash = id_with_leading_zeros(9);
                            checkpoints.push(shallower_start_hash);
                            shallower_boundary_hash = deeper_boundary_hash;
                        }
                    }

                    let deeper = Fragment::new(
                        SedimentreeId::arbitrary(u)?,
                        start_hash,
                        BTreeSet::from([deeper_boundary_hash]),
                        &checkpoints,
                        BlobMeta::arbitrary(u)?,
                    );
                    let shallower = FragmentSummary::new(
                        shallower_start_hash,
                        BTreeSet::from([shallower_boundary_hash]),
                        BlobMeta::arbitrary(u)?,
                    );

                    Ok(Self { deeper, shallower })
                }
            }
            bolero::check!().with_arbitrary::<Scenario>().for_each(
                |Scenario { deeper, shallower }| {
                    assert!(deeper.supports(shallower, &CountLeadingZeroBytes));
                },
            );
        }

        #[test]
        fn minimized_loose_commit_dag_doesnt_change() {
            #[derive(Debug)]
            struct Scenario {
                commits: Vec<LooseCommit>,
            }
            impl<'a> arbitrary::Arbitrary<'a> for Scenario {
                fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                    let sedimentree_id = SedimentreeId::arbitrary(u)?;
                    let mut frontier: Vec<CommitId> = Vec::new();
                    let num_commits: u32 = u.int_in_range(1..=20)?;
                    let mut result = Vec::with_capacity(num_commits as usize);
                    let mut rng = SmallRng::seed_from_u64(u.arbitrary()?);
                    for _ in 0..num_commits {
                        let contents = Vec::<u8>::arbitrary(u)?;
                        let blob = crate::blob::Blob::from(contents);
                        let blob_meta = BlobMeta::new(&blob);
                        let head = CommitId::new(rng.r#gen());
                        let mut parents = BTreeSet::new();
                        let mut num_parents = u.int_in_range(0..=frontier.len())?;
                        let mut parent_choices = frontier.iter().collect::<Vec<_>>();
                        while num_parents > 0 {
                            let parent = u.choose(&parent_choices)?;
                            parents.insert(**parent);
                            #[allow(clippy::unwrap_used)]
                            parent_choices
                                .remove(parent_choices.iter().position(|p| p == parent).unwrap());
                            num_parents -= 1;
                        }
                        let commit = LooseCommit::new(sedimentree_id, head, parents, blob_meta);
                        frontier.retain(|p| !commit.parents().contains(p));
                        frontier.push(commit.head());
                        result.push(commit);
                    }
                    Ok(Scenario { commits: result })
                }
            }
            bolero::check!()
                .with_arbitrary::<Scenario>()
                .for_each(|Scenario { commits }| {
                    let tree = Sedimentree::new(vec![], commits.clone());
                    let minimized = tree.minimize(&CountLeadingZeroBytes);
                    assert_eq!(tree, minimized);
                });
        }

        #[test]
        fn diff_self_is_empty() {
            bolero::check!()
                .with_arbitrary::<Sedimentree>()
                .for_each(|tree| {
                    let diff = tree.diff(tree);
                    assert!(
                        diff.left_missing_fragments.is_empty(),
                        "self-diff should have no left missing fragments"
                    );
                    assert!(
                        diff.left_missing_commits.is_empty(),
                        "self-diff should have no left missing commits"
                    );
                    assert!(
                        diff.right_missing_fragments.is_empty(),
                        "self-diff should have no right missing fragments"
                    );
                    assert!(
                        diff.right_missing_commits.is_empty(),
                        "self-diff should have no right missing commits"
                    );
                });
        }

        #[test]
        fn diff_is_symmetric() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, Sedimentree)>()
                .for_each(|(a, b)| {
                    let ab = a.diff(b);
                    let ba = b.diff(a);

                    // What a is missing from b == what b has that a doesn't
                    assert_eq!(
                        ab.left_missing_fragments.len(),
                        ba.right_missing_fragments.len(),
                        "left_missing in a.diff(b) should equal right_missing in b.diff(a)"
                    );
                    assert_eq!(
                        ab.right_missing_fragments.len(),
                        ba.left_missing_fragments.len(),
                        "right_missing in a.diff(b) should equal left_missing in b.diff(a)"
                    );
                    assert_eq!(
                        ab.left_missing_commits.len(),
                        ba.right_missing_commits.len(),
                        "left_missing commits in a.diff(b) should equal right_missing in b.diff(a)"
                    );
                    assert_eq!(
                        ab.right_missing_commits.len(),
                        ba.left_missing_commits.len(),
                        "right_missing commits in a.diff(b) should equal left_missing in b.diff(a)"
                    );
                });
        }

        #[test]
        fn diff_merge_produces_equal_trees() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, Sedimentree)>()
                .for_each(|(a, b)| {
                    let diff = a.diff(b);

                    // Apply diff to make copies equal
                    let mut a_updated = a.clone();
                    let mut b_updated = b.clone();

                    // Add what a is missing (from b)
                    for fragment in diff.left_missing_fragments {
                        a_updated.add_fragment(fragment.clone());
                    }
                    for commit in diff.left_missing_commits {
                        a_updated.add_commit(commit.clone());
                    }

                    // Add what b is missing (from a)
                    for fragment in diff.right_missing_fragments {
                        b_updated.add_fragment(fragment.clone());
                    }
                    for commit in diff.right_missing_commits {
                        b_updated.add_commit(commit.clone());
                    }

                    assert_eq!(
                        a_updated, b_updated,
                        "after applying diff, trees should be equal"
                    );
                });
        }

        #[test]
        fn minimize_output_subset_of_input() {
            bolero::check!()
                .with_arbitrary::<Sedimentree>()
                .for_each(|tree| {
                    let minimized = tree.minimize(&CountLeadingZeroBytes);
                    let input_set: Set<_> = tree.fragments().cloned().collect();
                    for fragment in minimized.fragments() {
                        assert!(
                            input_set.contains(fragment),
                            "minimized output contains fragment not in input"
                        );
                    }
                });
        }

        #[test]
        fn minimize_no_mutual_support() {
            bolero::check!()
                .with_arbitrary::<Sedimentree>()
                .for_each(|tree| {
                    let minimized = tree.minimize(&CountLeadingZeroBytes);
                    let fragments: Vec<_> = minimized.fragments().collect();
                    for (i, f1) in fragments.iter().enumerate() {
                        for (j, f2) in fragments.iter().enumerate() {
                            if i != j {
                                assert!(
                                    !f1.supports(f2.summary(), &CountLeadingZeroBytes),
                                    "fragment {i} supports fragment {j} in minimized output"
                                );
                            }
                        }
                    }
                });
        }

        #[test]
        fn minimize_idempotent() {
            bolero::check!()
                .with_arbitrary::<Sedimentree>()
                .for_each(|tree| {
                    let once = tree.minimize(&CountLeadingZeroBytes);
                    let twice = once.minimize(&CountLeadingZeroBytes);

                    let once_set: Set<_> = once.fragments().cloned().collect();
                    let twice_set: Set<_> = twice.fragments().cloned().collect();

                    assert_eq!(
                        once_set, twice_set,
                        "minimize should be idempotent: minimize(minimize(x)) == minimize(x)"
                    );
                });
        }

        mod algebraic {
            use super::*;

            #[test]
            fn merge_is_commutative() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree)>()
                    .for_each(|(a, b)| {
                        let mut ab = a.clone();
                        ab.merge(b.clone());

                        let mut ba = b.clone();
                        ba.merge(a.clone());

                        assert_eq!(ab, ba, "merge must be commutative: a ∪ b == b ∪ a");
                    });
            }

            #[test]
            fn merge_is_associative() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree, Sedimentree)>()
                    .for_each(|(a, b, c)| {
                        let mut ab_c = a.clone();
                        ab_c.merge(b.clone());
                        ab_c.merge(c.clone());

                        let mut a_bc = a.clone();
                        let mut bc = b.clone();
                        bc.merge(c.clone());
                        a_bc.merge(bc);

                        assert_eq!(
                            ab_c, a_bc,
                            "merge must be associative: (a ∪ b) ∪ c == a ∪ (b ∪ c)"
                        );
                    });
            }

            #[test]
            fn merge_is_idempotent() {
                bolero::check!()
                    .with_arbitrary::<Sedimentree>()
                    .for_each(|tree| {
                        let mut doubled = tree.clone();
                        doubled.merge(tree.clone());

                        assert_eq!(*tree, doubled, "merge must be idempotent: a ∪ a == a");
                    });
            }
        }

        mod minimize_plus_merge {
            use super::*;

            #[test]
            fn minimize_merge_commutes_after_minimize() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree)>()
                    .for_each(|(a, b)| {
                        let mut ab = a.clone();
                        ab.merge(b.clone());
                        let min_ab = ab.minimize(&CountLeadingZeroBytes);

                        let mut ba = b.clone();
                        ba.merge(a.clone());
                        let min_ba = ba.minimize(&CountLeadingZeroBytes);

                        assert_eq!(min_ab, min_ba, "minimize(a ∪ b) == minimize(b ∪ a)");
                    });
            }

            #[test]
            fn minimize_of_merge_subsumes_both_minimized() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree)>()
                    .for_each(|(a, b)| {
                        let min_a = a.minimize(&CountLeadingZeroBytes);
                        let min_b = b.minimize(&CountLeadingZeroBytes);

                        let mut merged = a.clone();
                        merged.merge(b.clone());
                        let min_merged = merged.minimize(&CountLeadingZeroBytes);

                        // Every commit in minimize(a) must appear in minimize(a ∪ b)
                        // OR be covered by a fragment in minimize(a ∪ b)
                        let merged_commit_keys: Set<_> =
                            min_merged.loose_commits().map(Digest::hash).collect();
                        let merged_frag_keys: Set<_> =
                            min_merged.fragments().map(Digest::hash).collect();

                        // All fragments from either minimized input must either
                        // appear in the merged result or be dominated by a deeper one
                        for frag in min_a.fragments().chain(min_b.fragments()) {
                            let frag_digest = Digest::hash(frag);
                            let present = merged_frag_keys.contains(&frag_digest);
                            let dominated = min_merged
                                .fragments()
                                .any(|mf| mf.supports(frag.summary(), &CountLeadingZeroBytes));
                            assert!(
                                present || dominated,
                                "fragment from minimized input must be present or dominated in minimize(merge)"
                            );
                        }

                        // All commits from either minimized input must either
                        // appear in the merged result or be covered by a fragment
                        for commit in min_a.loose_commits().chain(min_b.loose_commits()) {
                            let commit_cas = Digest::hash(commit);
                            let present = merged_commit_keys.contains(&commit_cas);
                            let covered = min_merged
                                .fragments()
                                .any(|f| f.supports_block(commit.head()));
                            assert!(
                                present || covered,
                                "commit from minimized input must be present or covered in minimize(merge)"
                            );
                        }
                    });
            }
        }

        mod fingerprint {
            use super::*;
            #[test]
            fn fingerprint_diff_self_requests_nothing() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, FingerprintSeed)>()
                    .for_each(|(tree, seed)| {
                        let summary = tree.fingerprint_summarize(seed);
                        let diff = tree.diff_remote_fingerprints(&summary);

                        assert!(
                            diff.local_only_commits.is_empty(),
                            "diffing against own summary should find no local-only commits"
                        );
                        assert!(
                            diff.local_only_fragments.is_empty(),
                            "diffing against own summary should find no local-only fragments"
                        );
                        assert!(
                            diff.remote_only_commit_fingerprints.is_empty(),
                            "diffing against own summary should find no remote-only commit fingerprints"
                        );
                        assert!(
                            diff.remote_only_fragment_fingerprints.is_empty(),
                            "diffing against own summary should find no remote-only fragment fingerprints"
                        );
                    });
            }

            #[test]
            fn fingerprint_summary_deterministic() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, FingerprintSeed)>()
                    .for_each(|(tree, seed)| {
                        let s1 = tree.fingerprint_summarize(seed);
                        let s2 = tree.fingerprint_summarize(seed);
                        assert_eq!(s1, s2, "fingerprint_summarize must be deterministic");
                    });
            }

            #[test]
            fn fingerprint_summary_item_count_matches() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, FingerprintSeed)>()
                    .for_each(|(tree, seed)| {
                        let summary = tree.fingerprint_summarize(seed);

                        // Fingerprint count should match unique item count
                        // (fingerprints are u64 so collisions are possible but
                        // vanishingly rare — we check <=)
                        assert!(
                            summary.commit_fingerprints().len() <= tree.loose_commits().count(),
                            "commit fingerprint count should not exceed commit count"
                        );
                        assert!(
                            summary.fragment_fingerprints().len() <= tree.fragments().count(),
                            "fragment fingerprint count should not exceed fragment count"
                        );
                    });
            }

            #[test]
            fn fingerprint_diff_superset_finds_extras() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree, FingerprintSeed)>()
                    .for_each(|(a, b, seed)| {
                        // Merge a into b so b is a superset of a
                        let mut superset = a.clone();
                        superset.merge(b.clone());

                        let a_summary = a.fingerprint_summarize(seed);
                        let diff = superset.diff_remote_fingerprints(&a_summary);

                        // The superset should have no remote-only fingerprints
                        // (everything in a is also in superset)
                        assert!(
                            diff.remote_only_commit_fingerprints.is_empty(),
                            "superset should recognize all of subset's commit fingerprints"
                        );
                        assert!(
                            diff.remote_only_fragment_fingerprints.is_empty(),
                            "superset should recognize all of subset's fragment fingerprints"
                        );
                    });
            }
        }

        mod heads {
            use super::*;

            #[test]
            fn heads_nonempty_when_commits_exist() {
                bolero::check!()
                    .with_arbitrary::<Sedimentree>()
                    .for_each(|tree| {
                        if tree.loose_commits().count() > 0 {
                            let heads = tree.heads(&CountLeadingZeroBytes);
                            assert!(
                                !heads.is_empty(),
                                "a sedimentree with loose commits must have at least one head"
                            );
                        }
                    });
            }

            #[test]
            fn heads_empty_iff_tree_empty() {
                bolero::check!()
                    .with_arbitrary::<Sedimentree>()
                    .for_each(|tree| {
                        let heads = tree.heads(&CountLeadingZeroBytes);
                        if tree.loose_commits().count() == 0 && tree.fragments().count() == 0 {
                            assert!(heads.is_empty(), "an empty tree must have no heads");
                        }
                    });
            }

            #[test]
            fn heads_no_duplicates() {
                bolero::check!()
                    .with_arbitrary::<Sedimentree>()
                    .for_each(|tree| {
                        let heads = tree.heads(&CountLeadingZeroBytes);
                        let unique: Set<_> = heads.iter().collect();
                        assert_eq!(
                            heads.len(),
                            unique.len(),
                            "heads must not contain duplicates"
                        );
                    });
            }

            #[test]
            fn heads_stable_after_minimize() {
                bolero::check!()
                    .with_arbitrary::<Sedimentree>()
                    .for_each(|tree| {
                        let heads_before = tree.heads(&CountLeadingZeroBytes);
                        let minimized = tree.minimize(&CountLeadingZeroBytes);
                        let heads_after = minimized.heads(&CountLeadingZeroBytes);

                        let before_set: Set<_> = heads_before.into_iter().collect();
                        let after_set: Set<_> = heads_after.into_iter().collect();

                        assert_eq!(
                            before_set, after_set,
                            "heads must be identical before and after minimize"
                        );
                    });
            }
        }

        mod add_commit_and_fragment_invariants {
            use super::*;
            #[test]
            fn add_commit_to_minimized_re_minimizes_cleanly() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, LooseCommit)>()
                    .for_each(|(tree, commit)| {
                        let mut minimized = tree.minimize(&CountLeadingZeroBytes);
                        minimized.add_commit(commit.clone());
                        let re_minimized = minimized.minimize(&CountLeadingZeroBytes);

                        // minimize invariants hold
                        let frags: Vec<_> = re_minimized.fragments().collect();
                        for (i, f1) in frags.iter().enumerate() {
                            for (j, f2) in frags.iter().enumerate() {
                                if i != j {
                                    assert!(
                                        !f1.supports(f2.summary(), &CountLeadingZeroBytes),
                                        "no mutual support after add_commit + re-minimize"
                                    );
                                }
                            }
                        }

                        // re-minimizing again is idempotent
                        let triple = re_minimized.minimize(&CountLeadingZeroBytes);
                        assert_eq!(
                            re_minimized, triple,
                            "minimize must be idempotent after add_commit"
                        );
                    });
            }

            #[test]
            fn add_fragment_to_minimized_re_minimizes_cleanly() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Fragment)>()
                    .for_each(|(tree, fragment)| {
                        let mut minimized = tree.minimize(&CountLeadingZeroBytes);
                        minimized.add_fragment(fragment.clone());
                        let re_minimized = minimized.minimize(&CountLeadingZeroBytes);

                        let frags: Vec<_> = re_minimized.fragments().collect();
                        for (i, f1) in frags.iter().enumerate() {
                            for (j, f2) in frags.iter().enumerate() {
                                if i != j {
                                    assert!(
                                        !f1.supports(f2.summary(), &CountLeadingZeroBytes),
                                        "no mutual support after add_fragment + re-minimize"
                                    );
                                }
                            }
                        }

                        let triple = re_minimized.minimize(&CountLeadingZeroBytes);
                        assert_eq!(
                            re_minimized, triple,
                            "minimize must be idempotent after add_fragment"
                        );
                    });
            }
        }

        mod diff_and_merge {
            use super::*;
            #[test]
            fn diff_apply_converges() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree)>()
                    .for_each(|(a, b)| {
                        let diff = a.diff(b);

                        let mut a_patched = a.clone();
                        for f in diff.left_missing_fragments {
                            a_patched.add_fragment(f.clone());
                        }
                        for c in diff.left_missing_commits {
                            a_patched.add_commit(c.clone());
                        }

                        let mut b_patched = b.clone();
                        for f in diff.right_missing_fragments {
                            b_patched.add_fragment(f.clone());
                        }
                        for c in diff.right_missing_commits {
                            b_patched.add_commit(c.clone());
                        }

                        assert_eq!(
                            a_patched, b_patched,
                            "applying diff to both sides must produce identical trees"
                        );
                    });
            }

            #[test]
            fn minimize_after_diff_apply_converges() {
                bolero::check!()
                    .with_arbitrary::<(Sedimentree, Sedimentree)>()
                    .for_each(|(a, b)| {
                        let diff = a.diff(b);

                        let mut a_patched = a.clone();
                        for f in diff.left_missing_fragments {
                            a_patched.add_fragment(f.clone());
                        }
                        for c in diff.left_missing_commits {
                            a_patched.add_commit(c.clone());
                        }

                        let mut b_patched = b.clone();
                        for f in diff.right_missing_fragments {
                            b_patched.add_fragment(f.clone());
                        }
                        for c in diff.right_missing_commits {
                            b_patched.add_commit(c.clone());
                        }

                        let min_a = a_patched.minimize(&CountLeadingZeroBytes);
                        let min_b = b_patched.minimize(&CountLeadingZeroBytes);

                        assert_eq!(
                            min_a, min_b,
                            "minimize(diff-patched a) == minimize(diff-patched b)"
                        );
                    });
            }

            #[test]
            fn topsort_respects_all_dependency_edges() {
                use crate::test_utils::ArbitraryDag;

                bolero::check!()
                    .with_arbitrary::<ArbitraryDag>()
                    .for_each(|dag| {
                        let order = dag
                            .tree
                            .topsorted_blob_order()
                            .expect("acyclic DAG should not produce CycleError");

                        // Build position map: fragment head → position in output
                        let fragments: Vec<_> = dag.tree.fragments().collect();
                        let loose: Vec<_> = dag.tree.loose_commits().collect();

                        let mut head_to_pos: Map<CommitId, usize> = Map::new();
                        for (pos, item) in order.iter().enumerate() {
                            match item {
                                SedimentreeItem::Fragment(i) => {
                                    head_to_pos.insert(fragments[*i].head(), pos);
                                }
                                SedimentreeItem::LooseCommit(_) => {}
                            }
                        }

                        // Verify: every fragment's boundary deps appear earlier
                        for (pos, item) in order.iter().enumerate() {
                            match item {
                                SedimentreeItem::Fragment(i) => {
                                    for boundary_digest in fragments[*i].boundary() {
                                        if let Some(&dep_pos) =
                                            head_to_pos.get(boundary_digest)
                                        {
                                            assert!(
                                                dep_pos < pos,
                                                "fragment dependency must appear before dependent"
                                            );
                                        }
                                    }
                                }
                                SedimentreeItem::LooseCommit(i) => {
                                    for parent in loose[*i].parents() {
                                        if let Some(&dep_pos) = head_to_pos.get(parent) {
                                            assert!(
                                                dep_pos < pos,
                                                "loose commit's fragment dependency must appear before commit"
                                            );
                                        }
                                    }
                                }
                            }
                        }

                        // Verify: output length matches input
                        assert_eq!(
                            order.len(),
                            fragments.len() + loose.len(),
                            "topsort must include all items"
                        );
                    });
            }

            #[test]
            fn topsort_detects_cycles() {
                use crate::test_utils::CyclicGraph;

                bolero::check!()
                    .with_arbitrary::<CyclicGraph>()
                    .for_each(|dag| {
                        let result = dag.tree.topsorted_blob_order();
                        assert!(
                            result.is_err(),
                            "cyclic graph must produce CycleError, got Ok with {} items",
                            result.map(|t| t.len()).unwrap_or(0)
                        );
                    });
            }
        }
    }

    #[allow(clippy::similar_names)]
    mod minimize_tests {
        use alloc::{collections::BTreeSet, vec, vec::Vec};

        use super::{make_blob_meta, make_sedimentree_id};
        use crate::{
            commit::CountLeadingZeroBytes,
            fragment::Fragment,
            sedimentree::Sedimentree,
            test_utils::{digest_with_depth, make_fragment_at_depth},
        };

        /// Helper to collect fragments from a Sedimentree for easier assertions.
        fn collect_fragments(tree: &Sedimentree) -> Vec<Fragment> {
            tree.fragments().cloned().collect()
        }

        // ============================================================
        // Correctness Tests
        // ============================================================

        #[test]
        fn minimize_empty_sedimentree() {
            let tree = Sedimentree::new(vec![], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 0);
            assert_eq!(minimized.loose_commits().count(), 0);
        }

        #[test]
        fn minimize_single_fragment() {
            let boundary_digest = digest_with_depth(1, 100);
            let fragment = make_fragment_at_depth(2, 1, BTreeSet::from([boundary_digest]), &[]);
            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);

            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            assert_eq!(fragments.len(), 1);
            assert!(fragments.contains(&fragment));
        }

        #[test]
        fn minimize_multi_depth_deep_dominates_shallow() {
            // Create a deep fragment (depth 3)
            let deep_boundary = digest_with_depth(1, 100);
            let shallow_head = digest_with_depth(2, 1);
            let shallow_boundary = digest_with_depth(1, 101);

            // Deep fragment has shallow's head and boundary in its checkpoints
            let deep_fragment = make_fragment_at_depth(
                3,
                1,
                BTreeSet::from([deep_boundary]),
                &[shallow_head, shallow_boundary],
            );

            // Shallow fragment
            let shallow_fragment =
                make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);

            let tree = Sedimentree::new(
                vec![deep_fragment.clone(), shallow_fragment.clone()],
                vec![],
            );
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // Only deep fragment should remain
            assert_eq!(fragments.len(), 1);
            assert!(fragments.contains(&deep_fragment));
            assert!(!fragments.contains(&shallow_fragment));
        }

        #[test]
        fn minimize_same_depth_partial_overlap_keeps_both() {
            // Two fragments at same depth with different heads
            let boundary1 = digest_with_depth(1, 100);
            let boundary2 = digest_with_depth(1, 101);

            let fragment1 = make_fragment_at_depth(2, 1, BTreeSet::from([boundary1]), &[]);
            let fragment2 = make_fragment_at_depth(2, 2, BTreeSet::from([boundary2]), &[]);

            let tree = Sedimentree::new(vec![fragment1.clone(), fragment2.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // Both should remain (neither supports the other)
            assert_eq!(fragments.len(), 2);
            assert!(fragments.contains(&fragment1));
            assert!(fragments.contains(&fragment2));
        }

        #[test]
        fn minimize_all_same_depth() {
            // Multiple fragments all at depth 2
            let input_fragments: Vec<Fragment> = (0..5)
                .map(|i| {
                    let boundary = digest_with_depth(1, 100 + i);
                    make_fragment_at_depth(2, i, BTreeSet::from([boundary]), &[])
                })
                .collect();

            let tree = Sedimentree::new(input_fragments.clone(), vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // All should remain (same depth, can't support each other)
            assert_eq!(fragments.len(), 5);
            for fragment in &input_fragments {
                assert!(fragments.contains(fragment));
            }
        }

        #[test]
        fn minimize_deep_partial_support_keeps_shallow() {
            // Deep fragment supports shallow's head but NOT its boundary
            let deep_boundary = digest_with_depth(1, 100);
            let shallow_head = digest_with_depth(2, 1);
            let shallow_boundary = digest_with_depth(1, 101); // NOT in deep's checkpoints

            // Deep fragment only has shallow's head in checkpoints
            let deep_fragment = make_fragment_at_depth(
                3,
                1,
                BTreeSet::from([deep_boundary]),
                &[shallow_head], // Only head, not boundary
            );

            let shallow_fragment =
                make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);

            let tree = Sedimentree::new(
                vec![deep_fragment.clone(), shallow_fragment.clone()],
                vec![],
            );
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // Both should remain (deep doesn't fully support shallow)
            assert_eq!(fragments.len(), 2);
        }

        #[test]
        fn minimize_collective_support() {
            // Two deep fragments together support a shallow one
            let shallow_head = digest_with_depth(2, 1);
            let shallow_boundary = digest_with_depth(1, 101);

            // Deep fragment 1 has shallow's head
            let deep1_boundary = digest_with_depth(1, 100);
            let deep1 =
                make_fragment_at_depth(3, 1, BTreeSet::from([deep1_boundary]), &[shallow_head]);

            // Deep fragment 2 has shallow's boundary
            let deep2_boundary = digest_with_depth(1, 102);
            let deep2 =
                make_fragment_at_depth(3, 2, BTreeSet::from([deep2_boundary]), &[shallow_boundary]);

            let shallow = make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);

            let tree =
                Sedimentree::new(vec![deep1.clone(), deep2.clone(), shallow.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // Shallow should be discarded (collectively supported by deep1 + deep2)
            assert_eq!(fragments.len(), 2);
            assert!(fragments.contains(&deep1));
            assert!(fragments.contains(&deep2));
            assert!(!fragments.contains(&shallow));
        }

        #[test]
        fn minimize_unsupported_shallow_kept() {
            // Deep fragment and shallow fragment with no overlap
            let deep_boundary = digest_with_depth(1, 100);
            let deep_checkpoint = digest_with_depth(2, 50);
            let deep =
                make_fragment_at_depth(3, 1, BTreeSet::from([deep_boundary]), &[deep_checkpoint]);

            // Shallow with completely different commits
            let shallow_boundary = digest_with_depth(1, 200);
            let shallow = make_fragment_at_depth(2, 10, BTreeSet::from([shallow_boundary]), &[]);

            let tree = Sedimentree::new(vec![deep.clone(), shallow.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);
            let fragments = collect_fragments(&minimized);

            // Both should remain (no overlap)
            assert_eq!(fragments.len(), 2);
            assert!(fragments.contains(&deep));
            assert!(fragments.contains(&shallow));
        }

        // ============================================================
        // Robustness Tests (malformed metadata)
        // ============================================================

        #[test]
        fn minimize_fragment_empty_checkpoints() {
            let boundary = digest_with_depth(1, 100);
            let fragment = make_fragment_at_depth(2, 1, BTreeSet::from([boundary]), &[]);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 1);
        }

        #[test]
        fn minimize_fragment_empty_boundary() {
            let sedimentree_id = make_sedimentree_id(1);
            let head = digest_with_depth(2, 1);
            let blob_meta = make_blob_meta(1);
            let fragment = Fragment::new(sedimentree_id, head, BTreeSet::new(), &[], blob_meta);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 1);
        }

        #[test]
        fn minimize_head_equals_boundary() {
            // Degenerate case: head is also in boundary
            let sedimentree_id = make_sedimentree_id(1);
            let head = digest_with_depth(2, 1);
            let blob_meta = make_blob_meta(1);
            let fragment =
                Fragment::new(sedimentree_id, head, BTreeSet::from([head]), &[], blob_meta);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // Should still work
            assert_eq!(minimized.fragments().count(), 1);
        }

        #[test]
        fn minimize_head_in_checkpoints() {
            // Head appears in own checkpoints (redundant but valid)
            let sedimentree_id = make_sedimentree_id(1);
            let head = digest_with_depth(2, 1);
            let boundary = digest_with_depth(1, 100);
            let blob_meta = make_blob_meta(1);
            let fragment = Fragment::new(
                sedimentree_id,
                head,
                BTreeSet::from([boundary]),
                &[head],
                blob_meta,
            );

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 1);
        }
    }

    /// Tests verifying that `minimize` correctly prunes loose commits
    /// when they are covered by fragments.
    ///
    /// These tests use human-readable `CommitId` values with explicit
    /// leading-zero structure to control the depth metric:
    ///
    /// ```text
    /// Depth 0:  [0x01, 0x00, ..., N]   — no leading zero bytes
    /// Depth 1:  [0x00, 0x01, ..., N]   — one leading zero byte
    /// Depth 2+: [0x00, 0x00, 0x01, N]  — two leading zero bytes (≥ MAX_STRATA_DEPTH)
    /// ```
    ///
    /// The trailing byte `N` distinguishes commits at the same depth.
    mod minimize_commit_coverage_tests {
        use alloc::{collections::BTreeSet, vec};

        use crate::{
            blob::{Blob, BlobMeta},
            commit::CountLeadingZeroBytes,
            fragment::Fragment,
            id::SedimentreeId,
            loose_commit::{LooseCommit, id::CommitId},
            sedimentree::Sedimentree,
        };

        const SID: SedimentreeId = SedimentreeId::new([0x42; 32]);

        /// Commit identifier at depth 0 (no leading zero bytes).
        ///
        /// Layout: `[n, 0, 0, ..., 0]` — first byte is non-zero.
        const fn d0(n: u8) -> CommitId {
            assert!(n != 0, "n must be non-zero for depth 0");
            let mut b = [0u8; 32];
            b[0] = n;
            CommitId::new(b)
        }

        /// Commit identifier at depth 1 (one leading zero byte).
        ///
        /// Layout: `[0, n, 0, ..., 0]` — one leading zero, then non-zero.
        const fn d1(n: u8) -> CommitId {
            assert!(n != 0, "n must be non-zero for depth 1");
            let mut b = [0u8; 32];
            b[1] = n;
            CommitId::new(b)
        }

        /// Commit identifier at depth 2 (two leading zero bytes, ≥ `MAX_STRATA_DEPTH`).
        ///
        /// Layout: `[0, 0, n, 0, ..., 0]` — two leading zeros, then non-zero.
        /// The distinguishing byte `n` is at index 2, well within the
        /// 12-byte `Checkpoint` truncation window.
        const fn d2(n: u8) -> CommitId {
            assert!(n != 0, "n must be non-zero for depth 2");
            let mut b = [0u8; 32];
            b[2] = n;
            CommitId::new(b)
        }

        fn blob_meta() -> BlobMeta {
            BlobMeta::new(&Blob::new(vec![0xAB]))
        }

        fn commit(head: CommitId, parents: &[CommitId]) -> LooseCommit {
            LooseCommit::new(SID, head, parents.iter().copied().collect(), blob_meta())
        }

        fn fragment(head: CommitId, boundary: &[CommitId], checkpoints: &[CommitId]) -> Fragment {
            Fragment::new(
                SID,
                head,
                boundary.iter().copied().collect(),
                checkpoints,
                blob_meta(),
            )
        }

        fn remaining_commit_ids(tree: &Sedimentree) -> BTreeSet<CommitId> {
            tree.loose_commits().map(LooseCommit::head).collect()
        }

        // ============================================================
        // Linear chain: fragment covers all intermediate commits
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d0) ← D(d0) ← E(d2)
        ///     oldest                            newest
        ///   ╰── fragment(head=E, boundary={A}) ──╯
        /// ```
        ///
        /// All commits A–E are in a block bounded by two depth-2 commits.
        /// The fragment's `supports_block` covers E (head), A (boundary),
        /// and B/C/D should be pruned because they're in a block fully
        /// covered by the fragment.
        #[test]
        fn linear_chain_all_intermediates_pruned() {
            let a = d2(1); // oldest, root
            let b = d0(2);
            let c = d0(3);
            let d = d0(4);
            let e = d2(5); // newest, tip

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
            ];

            // head = newest depth-2 commit, boundary = oldest depth-2 commit
            let frag = fragment(e, &[a], &[]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "all commits in a fully covered block should be pruned"
            );
        }

        /// Same chain, but no fragment — all commits survive minimize.
        #[test]
        fn linear_chain_no_fragment_all_survive() {
            let a = d2(1);
            let b = d0(2);
            let c = d0(3);
            let d = d0(4);
            let e = d2(5);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
            ];

            let tree = Sedimentree::new(vec![], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(
                remaining_commit_ids(&minimized).len(),
                5,
                "without fragments, no commits should be pruned"
            );
        }

        // ============================================================
        // Fragment with checkpoints covers intermediate depth-1 commits
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d1) ← D(d0) ← E(d2)
        ///     oldest                            newest
        ///   ╰── fragment(head=E, boundary={A}, checkpoints={C}) ──╯
        /// ```
        ///
        /// C is a depth-1 commit recorded as a checkpoint. The fragment
        /// covers the entire block A→E. All commits should be pruned.
        #[test]
        fn checkpoint_covers_intermediate_depth1_commit() {
            let a = d2(1);
            let b = d0(2);
            let c = d1(3); // intermediate, recorded as checkpoint
            let d = d0(4);
            let e = d2(5);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
            ];

            let frag = fragment(e, &[a], &[c]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "all commits in a block with checkpoints should be pruned"
            );
        }

        // ============================================================
        // Commits between checkpoints are also covered
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d1) ← D(d0) ← E(d1) ← F(d0) ← G(d2)
        ///     oldest                                              newest
        ///   ╰── fragment(head=G, boundary={A}, checkpoints={C, E}) ──╯
        /// ```
        ///
        /// B is between A and C, D is between C and E, F is between E and G.
        /// All three intermediate depth-0 commits should be pruned.
        #[test]
        fn commits_between_checkpoints_are_covered() {
            let a = d2(1);
            let b = d0(2);
            let c = d1(3);
            let d = d0(4);
            let e = d1(5);
            let f = d0(6);
            let g = d2(7);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
                commit(f, &[e]),
                commit(g, &[f]),
            ];

            let frag = fragment(g, &[a], &[c, e]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "commits between checkpoints should be pruned when block is covered"
            );
        }

        // ============================================================
        // Partial coverage: commits outside the fragment survive
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d2)    D(d0) ← E(d0)
        ///   ╰── fragment(head=C, boundary={A}) ──╯
        /// ```
        ///
        /// D and E are not in any block covered by the fragment
        /// (they're disconnected and have no depth-2 boundaries).
        /// They should survive minimize.
        #[test]
        fn commits_outside_fragment_survive() {
            let a = d2(1);
            let b = d0(2);
            let c = d2(3);
            let d = d0(4);
            let e = d0(5);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[]),
                commit(e, &[d]),
            ];

            let frag = fragment(c, &[a], &[]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            let remaining = remaining_commit_ids(&minimized);
            assert!(
                remaining.contains(&d) && remaining.contains(&e),
                "commits outside the fragment's block should survive: got {remaining:?}"
            );
        }

        // ============================================================
        // Diamond DAG: fragment covers both branches
        // ============================================================

        /// ```text
        ///        A(d2)      newest
        ///       ╱    ╲
        ///     B(d0)  C(d0)
        ///       ╲    ╱
        ///        D(d2)      oldest
        /// ╰── fragment(head=A, boundary={D}) ──╯
        /// ```
        ///
        /// B and C are intermediate depth-0 commits on separate branches.
        /// The fragment covers the entire block.
        #[test]
        fn diamond_dag_intermediates_pruned() {
            let d = d2(4); // oldest, root
            let b = d0(2);
            let c = d0(3);
            let a = d2(1); // newest, tip

            let commits = vec![
                commit(d, &[]),
                commit(b, &[d]),
                commit(c, &[d]),
                commit(a, &[b, c]),
            ];

            // head = newest (A), boundary = oldest (D)
            let frag = fragment(a, &[d], &[]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "diamond DAG intermediates should be pruned when block is covered"
            );
        }

        // ============================================================
        // Diamond with checkpoint on one branch
        // ============================================================

        /// ```text
        ///        A(d2)      newest
        ///       ╱    ╲
        ///     B(d1)  C(d0)
        ///       ╲    ╱
        ///        D(d2)      oldest
        /// ╰── fragment(head=A, boundary={D}, checkpoints={B}) ──╯
        /// ```
        #[test]
        fn diamond_with_checkpoint_on_one_branch() {
            let d = d2(4); // oldest, root
            let b = d1(2); // checkpoint
            let c = d0(3);
            let a = d2(1); // newest, tip

            let commits = vec![
                commit(d, &[]),
                commit(b, &[d]),
                commit(c, &[d]),
                commit(a, &[b, c]),
            ];

            let frag = fragment(a, &[d], &[b]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "diamond with checkpoint should prune all commits in covered block"
            );
        }

        // ============================================================
        // Two fragments covering adjacent blocks
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d2) ← D(d0) ← E(d2)
        ///     oldest                            newest
        ///   ╰── frag1(C, {A}) ──╯╰── frag2(E, {C}) ──╯
        /// ```
        ///
        /// Two fragments each cover one block. B and D are
        /// intermediate depth-0 commits in different blocks.
        /// frag1 covers the block from C (newer) back to A (older).
        /// frag2 covers the block from E (newest) back to C.
        #[test]
        fn adjacent_fragments_cover_all_intermediates() {
            let a = d2(1); // oldest
            let b = d0(2);
            let c = d2(3);
            let d = d0(4);
            let e = d2(5); // newest

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
            ];

            let frag1 = fragment(c, &[a], &[]);
            let frag2 = fragment(e, &[c], &[]);
            let tree = Sedimentree::new(vec![frag1, frag2], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "two adjacent fragments should prune all intermediates"
            );
        }

        // ============================================================
        // Gap between fragments: uncovered intermediates survive
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d2) ← D(d0) ← E(d0) ← F(d2) ← G(d0) ← H(d2) ← I(d0) ← J(d2)
        ///     oldest                                                                        newest
        ///   ╰── frag1(C, {A}) ──╯                                        ╰── frag2(J, {H}) ──╯
        /// ```
        ///
        /// The block identified by F contains [F, E, D]. F is NOT in any
        /// fragment's head, boundary, or checkpoints — so this block is
        /// genuinely uncovered and D, E should survive.
        ///
        /// Meanwhile B (in block C, covered by frag1) and I (in block J,
        /// covered by frag2) should be pruned.
        #[test]
        fn gap_between_fragments_intermediates_survive() {
            let a = d2(1);
            let b = d0(2);
            let c = d2(3);
            let d = d0(4);
            let e = d0(5);
            let f = d2(6); // block boundary NOT in any fragment
            let g = d0(7);
            let h = d2(8);
            let i = d0(9);
            let j = d2(10);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
                commit(f, &[e]),
                commit(g, &[f]),
                commit(h, &[g]),
                commit(i, &[h]),
                commit(j, &[i]),
            ];

            // frag1 covers C→A, frag2 covers J→H
            // Block F (containing F, E, D) and block H (containing H, G)
            // are NOT covered by any fragment.
            // Block C (B) and block J (I) ARE covered.
            let frag1 = fragment(c, &[a], &[]);
            let frag2 = fragment(j, &[h], &[]);
            let tree = Sedimentree::new(vec![frag1, frag2], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            let remaining = remaining_commit_ids(&minimized);
            assert!(
                remaining.contains(&d) && remaining.contains(&e),
                "commits in uncovered block F should survive: got {remaining:?}"
            );
            assert!(
                !remaining.contains(&b) && !remaining.contains(&i),
                "commits in covered blocks should be pruned: got {remaining:?}"
            );
        }

        // ============================================================
        // Tail commits (before any depth-2 boundary) survive
        // ============================================================

        /// ```text
        ///   X(d0) ← Y(d0) ← A(d2) ← B(d0) ← C(d2)
        ///     oldest                            newest
        ///                    ╰── frag(C, {A}) ──╯
        /// ```
        ///
        /// X and Y are older than A (the first depth-2 boundary). In
        /// reverse-topo from C, the traversal hits A and starts a block,
        /// but X and Y come _after_ A is flushed. They end up in a block
        /// whose end is A — and since the fragment supports A (it's the
        /// boundary), that block is also covered.
        ///
        /// To get truly "blockless" tail commits we need them reachable
        /// from a tip but with no depth-2 commit anywhere in their
        /// ancestry path.
        #[test]
        fn tail_commits_on_separate_branch_survive() {
            let x = d0(10); // oldest, root, separate branch
            let y = d0(11);
            let a = d2(1);
            let b = d0(2);
            let c = d2(3); // newest tip

            let commits = vec![
                commit(x, &[]),
                commit(y, &[x]),
                // a and the main chain are disconnected from x/y
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
            ];

            // Fragment covers the main chain only
            let frag = fragment(c, &[a], &[]);
            let tree = Sedimentree::new(vec![frag], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            let remaining = remaining_commit_ids(&minimized);
            assert!(
                remaining.contains(&x) && remaining.contains(&y),
                "blockless commits on a separate branch should survive: got {remaining:?}"
            );
        }

        // ============================================================
        // Multi-level: deeper fragment dominates shallower
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d1) ← D(d0) ← E(d2)
        ///     oldest                            newest
        ///
        ///   deep_frag(head=E, boundary={A}, checkpoints={C})   depth 2
        ///   shallow_frag(head=C, boundary={A})                  depth 1
        /// ```
        ///
        /// The deep fragment (depth 2, head=E) covers the entire range
        /// A→E. The shallow fragment (depth 1, head=C) covers A→C, a
        /// subset. After minimize:
        /// - `deep_frag` supports `shallow_frag` (E covers C via checkpoint,
        ///   A is a boundary subset)
        /// - Only `deep_frag` survives
        /// - All loose commits (B, D) are in the single block E→A, covered
        ///   by `deep_frag` → all pruned
        #[test]
        fn deeper_fragment_dominates_shallower_all_intermediates_pruned() {
            let a = d2(1); // oldest
            let b = d0(2);
            let c = d1(3); // checkpoint in deep fragment, head of shallow
            let d = d0(4);
            let e = d2(5); // newest

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
            ];

            let deep_frag = fragment(e, &[a], &[c]);
            let shallow_frag = fragment(c, &[a], &[]);

            let tree = Sedimentree::new(vec![deep_frag.clone(), shallow_frag.clone()], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // Deep should dominate shallow
            let frags: BTreeSet<_> = minimized.fragments().map(|f| f.head()).collect();
            assert!(
                frags.contains(&e) && !frags.contains(&c),
                "deep fragment should dominate shallow: surviving heads = {frags:?}"
            );

            // All loose commits should be pruned
            assert!(
                remaining_commit_ids(&minimized).is_empty(),
                "all intermediates should be pruned when deep fragment covers the range"
            );
        }

        // ============================================================
        // Multi-level: non-overlapping ranges both survive
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d1) ← D(d0) ← E(d2) ← F(d0) ← G(d2)
        ///     oldest                                              newest
        ///
        ///   shallow_frag(head=C, boundary={A})         covers A→C
        ///   deep_frag(head=G, boundary={E}, ckpts={})  covers E→G
        /// ```
        ///
        /// The deep fragment covers G→E (depth 2). The shallow fragment
        /// covers C→A (depth 1). They don't overlap — C is not in
        /// `deep_frag`'s range. Both survive minimize.
        ///
        /// For loose commits: B is in block C→A (covered by shallow_frag),
        /// so B is pruned. D is in block E→C; block E is covered by
        /// deep_frag (E is its boundary), so D is also pruned. F is in
        /// block G→E (covered by deep_frag), so F is pruned.
        #[test]
        fn different_depth_non_overlapping_both_survive() {
            let a = d2(1);
            let b = d0(2);
            let c = d1(3);
            let d = d0(4);
            let e = d2(5);
            let f = d0(6);
            let g = d2(7);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
                commit(f, &[e]),
                commit(g, &[f]),
            ];

            let shallow_frag = fragment(c, &[a], &[]);
            let deep_frag = fragment(g, &[e], &[]);

            let tree = Sedimentree::new(vec![shallow_frag.clone(), deep_frag.clone()], commits);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // Both fragments should survive (non-overlapping)
            let frags: BTreeSet<_> = minimized.fragments().map(|f| f.head()).collect();
            assert_eq!(
                frags.len(),
                2,
                "both fragments should survive: heads = {frags:?}"
            );

            // All depth-0 intermediates should be pruned (each is in a
            // block whose boundary commit is covered by some fragment)
            let remaining = remaining_commit_ids(&minimized);
            assert!(
                !remaining.contains(&b) && !remaining.contains(&d) && !remaining.contains(&f),
                "all depth-0 intermediates should be pruned: got {remaining:?}"
            );
        }

        // ============================================================
        // Three levels: depth 2 dominates depth 1, separate depth 1 survives
        // ============================================================

        /// ```text
        ///   A(d2) ← B(d0) ← C(d1) ← D(d0) ← E(d2) ← F(d0) ← G(d1) ← H(d0) ← I(d2)
        ///     oldest                                                                newest
        ///
        ///   deep_frag(head=I, boundary={E}, checkpoints={G})    covers I→E
        ///   shallow1(head=G, boundary={E})                      covers G→E (dominated by deep)
        ///   shallow2(head=C, boundary={A})                      covers C→A (NOT dominated)
        /// ```
        ///
        /// `shallow1` is dominated by `deep_frag` (G is a checkpoint, E is
        /// boundary subset). `shallow2` is independent — C and A are not
        /// in `deep_frag`'s range. After minimize:
        /// - `deep_frag` and `shallow2` survive; `shallow1` is pruned
        /// - F and H are in block I→E, covered by deep_frag → pruned
        /// - B is in block C→A or E→C, covered by shallow2 or deep_frag → pruned
        /// - D is in block E→C, block E covered by deep_frag → pruned
        #[test]
        fn three_levels_partial_domination() {
            let a = d2(1);
            let b = d0(2);
            let c = d1(3);
            let d = d0(4);
            let e = d2(5);
            let f = d0(6);
            let g = d1(7);
            let h = d0(8);
            let i = d2(9);

            let commits = vec![
                commit(a, &[]),
                commit(b, &[a]),
                commit(c, &[b]),
                commit(d, &[c]),
                commit(e, &[d]),
                commit(f, &[e]),
                commit(g, &[f]),
                commit(h, &[g]),
                commit(i, &[h]),
            ];

            let deep_frag = fragment(i, &[e], &[g]);
            let shallow1 = fragment(g, &[e], &[]);
            let shallow2 = fragment(c, &[a], &[]);

            let tree = Sedimentree::new(
                vec![deep_frag.clone(), shallow1.clone(), shallow2.clone()],
                commits,
            );
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            let frag_heads: BTreeSet<_> = minimized.fragments().map(|f| f.head()).collect();

            // deep_frag and shallow2 survive; shallow1 is dominated
            assert!(
                frag_heads.contains(&i) && frag_heads.contains(&c),
                "deep and independent shallow should survive: heads = {frag_heads:?}"
            );
            assert!(
                !frag_heads.contains(&g),
                "dominated shallow1 should be pruned: heads = {frag_heads:?}"
            );

            // All intermediates pruned
            let remaining = remaining_commit_ids(&minimized);
            assert!(
                remaining.is_empty(),
                "all intermediates should be pruned by the two surviving fragments: got {remaining:?}"
            );
        }
    }

    /// Tests verifying that `fingerprint_summarize` and `diff_remote_fingerprints`
    /// operate correctly on pre-minimized trees.
    ///
    /// The key invariant: since the in-memory `Sedimentree` is always minimized
    /// after each batch of inserts, fingerprint summaries naturally contain only
    /// the minimal covering — no covered commits or dominated fragments leak into
    /// the sync protocol.
    #[allow(clippy::similar_names)]
    mod fingerprint_minimize_tests {
        use alloc::{collections::BTreeSet, vec};

        use crate::{
            commit::CountLeadingZeroBytes,
            crypto::fingerprint::{Fingerprint, FingerprintSeed},
            sedimentree::Sedimentree,
            test_utils::{TestGraph, seeded_rng},
        };

        /// Commits fully covered by a fragment should be pruned by minimize,
        /// and therefore absent from the fingerprint summary.
        ///
        /// ```text
        ///        A (depth 2, head) ┐
        ///       / \                │
        ///      B   C (depth 0)    │ FRAG(A→D)
        ///       \ /                │
        ///        D (depth 2)      ┘
        ///        |
        ///        E (depth 0, below fragment)
        /// ```
        #[test]
        fn fingerprint_summarize_on_minimized_excludes_covered_commits() {
            let mut rng = seeded_rng(200);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("c", 0), ("d", 2), ("e", 0)],
                &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d"), ("d", "e")],
            );

            let fragment = graph.make_fragment("a", &["d"], &["b", "c"]);
            let tree = graph.to_sedimentree_with_fragments(vec![fragment]);
            let minimized = tree.minimize(graph.depth_metric());

            let seed = FingerprintSeed::new(42, 99);
            let summary = minimized.fingerprint_summarize(&seed);

            // Covered commits B and C should not appear in the summary
            assert!(
                !minimized.has_loose_commit(graph.node_hash("b")),
                "commit B should have been pruned by minimize"
            );
            assert!(
                !minimized.has_loose_commit(graph.node_hash("c")),
                "commit C should have been pruned by minimize"
            );

            // E (below fragment boundary) should remain
            assert!(
                minimized.has_loose_commit(graph.node_hash("e")),
                "commit E should survive minimize (below fragment boundary)"
            );

            // The fingerprint summary should have exactly 1 commit (E) and 1 fragment
            assert_eq!(
                summary.commit_fingerprints().len(),
                1,
                "only uncovered commit E should be fingerprinted"
            );
            assert_eq!(
                summary.fragment_fingerprints().len(),
                1,
                "exactly one fragment should be fingerprinted"
            );
        }

        /// A shallow fragment dominated by a deeper one should be pruned,
        /// so only the deep fragment appears in the fingerprint summary.
        #[test]
        fn fingerprint_summarize_on_minimized_excludes_dominated_fragments() {
            let shallow_head = crate::test_utils::digest_with_depth(2, 1);
            let shallow_boundary = crate::test_utils::digest_with_depth(1, 101);
            let deep_boundary = crate::test_utils::digest_with_depth(1, 100);

            // Deep fragment has shallow's head and boundary in checkpoints
            let deep_fragment = crate::test_utils::make_fragment_at_depth(
                3,
                1,
                BTreeSet::from([deep_boundary]),
                &[shallow_head, shallow_boundary],
            );
            let shallow_fragment = crate::test_utils::make_fragment_at_depth(
                2,
                1,
                BTreeSet::from([shallow_boundary]),
                &[],
            );

            let tree = Sedimentree::new(
                vec![deep_fragment.clone(), shallow_fragment.clone()],
                vec![],
            );
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            let seed = FingerprintSeed::new(42, 99);
            let summary = minimized.fingerprint_summarize(&seed);

            // Only the deep fragment should be fingerprinted
            assert_eq!(
                summary.fragment_fingerprints().len(),
                1,
                "only deep fragment should survive minimization"
            );

            // Verify it's the deep fragment, not the shallow one
            let deep_fp = Fingerprint::new(&seed, &deep_fragment.fragment_id());
            assert!(
                summary.fragment_fingerprints().contains(&deep_fp),
                "deep fragment fingerprint should be in summary"
            );
        }

        /// Commits that are NOT covered by any fragment should survive
        /// minimization and appear in the fingerprint summary.
        #[test]
        fn fingerprint_summarize_on_minimized_keeps_uncovered_commits() {
            let mut rng = seeded_rng(201);

            // Simple chain: A → B → C, no fragments
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 0), ("b", 0), ("c", 0)],
                &[("a", "b"), ("b", "c")],
            );

            let tree = graph.to_sedimentree();
            let minimized = tree.minimize(graph.depth_metric());

            let seed = FingerprintSeed::new(42, 99);
            let summary = minimized.fingerprint_summarize(&seed);

            // All 3 commits should survive (no fragments to cover them)
            assert_eq!(
                summary.commit_fingerprints().len(),
                3,
                "all uncovered commits should be fingerprinted"
            );
            assert_eq!(
                summary.fragment_fingerprints().len(),
                0,
                "no fragments should be fingerprinted"
            );
        }

        /// When local has a fragment covering commits, and remote has nothing,
        /// the diff on the minimized local should send only the fragment
        /// (not the covered commits, since they were pruned).
        ///
        /// This is the core bandwidth win: sync sends fragments, not the
        /// individual commits they subsume.
        #[test]
        fn diff_minimized_local_sends_only_fragment() {
            let mut rng = seeded_rng(202);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
                &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
            );

            let fragment = graph.make_fragment("a", &["d"], &["b", "c"]);
            let local = graph
                .to_sedimentree_with_fragments(vec![fragment])
                .minimize(graph.depth_metric());

            // Remote has nothing
            let remote = Sedimentree::new(vec![], vec![]);

            let seed = FingerprintSeed::new(42, 99);
            let remote_summary = remote.fingerprint_summarize(&seed);
            let diff = local.diff_remote_fingerprints(&remote_summary);

            // Local should send exactly 1 fragment
            assert_eq!(
                diff.local_only_fragments.len(),
                1,
                "local should send exactly the fragment"
            );

            // Local should NOT send covered commits (they were pruned by minimize)
            assert_eq!(
                diff.local_only_commits.len(),
                0,
                "covered commits should not be sent (pruned by minimize)"
            );
        }

        /// When remote has a fragment and local has the underlying loose commits,
        /// both sides see the other as missing items. This is the accepted
        /// limitation: the diff protocol can't know a fragment covers certain
        /// commits without understanding fragment semantics.
        ///
        /// After one sync round, both sides will have the fragment, and the
        /// next minimize prunes the redundant commits. Self-healing.
        #[test]
        fn diff_minimized_remote_has_fragment_local_has_commits() {
            let mut rng = seeded_rng(203);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("d", 2)],
                &[("a", "b"), ("b", "d")],
            );

            let fragment = graph.make_fragment("a", &["d"], &["b"]);

            // Remote has only the fragment (minimized away the commits)
            let remote =
                Sedimentree::new(vec![fragment.clone()], vec![]).minimize(graph.depth_metric());

            // Local has only the loose commits (no fragment yet)
            let local = graph.to_sedimentree().minimize(graph.depth_metric());

            let seed = FingerprintSeed::new(42, 99);

            // Remote tells local what it has
            let remote_summary = remote.fingerprint_summarize(&seed);
            let diff_at_local = local.diff_remote_fingerprints(&remote_summary);

            // Local doesn't have the fragment → remote's fragment fingerprint
            // shows up as remote-only
            assert_eq!(
                diff_at_local.remote_only_fragment_fingerprints.len(),
                1,
                "remote has a fragment that local doesn't"
            );

            // Local has commits that remote doesn't recognize
            // (remote pruned them because the fragment covers them)
            assert!(
                !diff_at_local.local_only_commits.is_empty(),
                "local should offer commits that remote's minimized tree doesn't have"
            );
        }

        /// After both sides sync and minimize, their fingerprint summaries
        /// should converge — the same set of fragments and commits, so
        /// the diff between them is empty.
        #[test]
        fn post_sync_minimize_converges() {
            let mut rng = seeded_rng(204);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("d", 2)],
                &[("a", "b"), ("b", "d")],
            );

            let fragment = graph.make_fragment("a", &["d"], &["b"]);

            // Peer A: has fragment + loose commits
            let peer_a = graph.to_sedimentree_with_fragments(vec![fragment.clone()]);
            // Peer B: has only loose commits
            let mut peer_b = graph.to_sedimentree();

            // Simulate sync: A sends its fragment to B, B sends its commits to A
            // (A already has the commits, B gets the fragment)
            peer_b.add_fragment(fragment.clone());

            // After sync, both minimize
            let peer_a_min = peer_a.minimize(graph.depth_metric());
            let peer_b_min = peer_b.minimize(graph.depth_metric());

            let seed = FingerprintSeed::new(42, 99);
            let summary_a = peer_a_min.fingerprint_summarize(&seed);
            let summary_b = peer_b_min.fingerprint_summarize(&seed);

            // Fingerprint summaries should be identical
            assert_eq!(
                summary_a.commit_fingerprints(),
                summary_b.commit_fingerprints(),
                "commit fingerprints should converge after sync + minimize"
            );
            assert_eq!(
                summary_a.fragment_fingerprints(),
                summary_b.fragment_fingerprints(),
                "fragment fingerprints should converge after sync + minimize"
            );

            // The diff between them should show nothing to send
            let diff = peer_a_min.diff_remote_fingerprints(&summary_b);
            assert!(
                diff.local_only_commits.is_empty(),
                "no commits to send after convergence"
            );
            assert!(
                diff.local_only_fragments.is_empty(),
                "no fragments to send after convergence"
            );
            assert!(
                diff.remote_only_commit_fingerprints.is_empty(),
                "no remote-only commit fingerprints after convergence"
            );
            assert!(
                diff.remote_only_fragment_fingerprints.is_empty(),
                "no remote-only fragment fingerprints after convergence"
            );
        }
    }

    /// Tests for [`FingerprintResolver`] — the type-level guarantee that
    /// fingerprint reverse-lookups are independent of subsequent tree mutations.
    #[allow(clippy::similar_names)]
    mod fingerprint_resolver_tests {
        use alloc::vec;

        use crate::{
            crypto::fingerprint::FingerprintSeed,
            loose_commit::id::CommitId,
            sedimentree::Sedimentree,
            test_utils::{TestGraph, seeded_rng},
        };

        /// The critical invariant: a resolver created before `minimize` can
        /// still resolve fingerprints for commits that minimize prunes away.
        #[test]
        fn resolver_survives_minimize() {
            let mut rng = seeded_rng(303);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("d", 2)],
                &[("a", "b"), ("b", "d")],
            );

            // Tree with only loose commits (no fragments yet)
            let tree = graph.to_sedimentree();
            let seed = FingerprintSeed::new(42, 99);

            // Capture resolver BEFORE adding a fragment and minimizing
            let resolver = tree.fingerprint_resolver(&seed);

            // Verify we can resolve all commits right now
            let commit_count = resolver.summary().commit_fingerprints().len();
            assert!(commit_count > 0, "tree should have commits");

            let resolved_before: Vec<CommitId> = resolver
                .summary()
                .commit_fingerprints()
                .iter()
                .filter_map(|fp| resolver.resolve_commit(fp))
                .collect();
            assert_eq!(
                resolved_before.len(),
                commit_count,
                "all commit fingerprints should resolve before minimize"
            );

            // Now simulate what happens during sync: add a covering fragment
            // and minimize. The in-memory tree loses commits.
            let fragment = graph.make_fragment("a", &["d"], &["b"]);
            let mut tree_after = tree.clone();
            tree_after.add_fragment(fragment);
            let minimized = tree_after.minimize(graph.depth_metric());

            // The minimized tree has fewer commits (some pruned by fragment)
            let minimized_commit_count = minimized.loose_commits().count();
            assert!(
                minimized_commit_count < commit_count,
                "minimize should prune some commits (got {minimized_commit_count}, \
                 had {commit_count})"
            );

            // The resolver STILL resolves all original fingerprints
            let resolved_after: Vec<CommitId> = resolver
                .summary()
                .commit_fingerprints()
                .iter()
                .filter_map(|fp| resolver.resolve_commit(fp))
                .collect();
            assert_eq!(
                resolved_after.len(),
                commit_count,
                "resolver must still resolve all {commit_count} commit fingerprints \
                 after minimize (the resolver is independent of tree mutations)"
            );
        }

        /// Deep fragment dominates a shallow fragment on the same chain.
        ///
        /// ```text
        /// a(depth=3) → b(depth=1) → c(depth=0)
        /// ```
        ///
        /// Alice has shallow fragment `b→c` plus all loose commits. After
        /// adding deep fragment `a→c` and minimizing, the shallow fragment
        /// is pruned. The resolver must still resolve the pruned fragment's
        /// fingerprint.
        #[test]
        fn resolver_survives_fragment_domination() {
            let mut rng = seeded_rng(304);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 3), ("b", 1), ("c", 0)],
                &[("a", "b"), ("b", "c")],
            );

            // Shallow fragment covering b→c
            let shallow = graph.make_fragment("b", &["c"], &[]);
            let tree = graph.to_sedimentree_with_fragments(vec![shallow]);
            let seed = FingerprintSeed::new(55, 66);

            let resolver = tree.fingerprint_resolver(&seed);
            let frag_count = resolver.summary().fragment_fingerprints().len();
            assert_eq!(frag_count, 1, "tree should have 1 fragment (shallow)");
            let commit_count = resolver.summary().commit_fingerprints().len();
            assert!(commit_count > 0, "tree should have loose commits");

            // Add a deep fragment that dominates the shallow one
            let deep = graph.make_fragment("a", &["c"], &["b"]);
            let mut tree_after = tree.clone();
            tree_after.add_fragment(deep);
            let minimized = tree_after.minimize(graph.depth_metric());

            // The shallow fragment should be pruned (dominated by deep)
            let min_frag_count = minimized.fragments().count();
            assert_eq!(
                min_frag_count, 1,
                "only the deep fragment should survive minimize"
            );

            // Resolver still resolves the pruned shallow fragment fingerprint
            for fp in resolver.summary().fragment_fingerprints() {
                assert!(
                    resolver.resolve_fragment(fp).is_some(),
                    "resolver must resolve pruned fragment fingerprint {fp}"
                );
            }

            // And all original commit fingerprints
            for fp in resolver.summary().commit_fingerprints() {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve commit fingerprint {fp} after fragment domination"
                );
            }
        }

        /// Simulates the full sync scenario that triggered the original bug:
        ///
        /// 1. Alice has loose commits, creates a resolver
        /// 2. Bob responds with a fragment covering those commits + requesting them
        /// 3. Alice ingests Bob's fragment, minimizes (pruning her commits)
        /// 4. Alice uses the _original_ resolver to resolve Bob's request
        ///
        /// Without `FingerprintResolver`, step 4 would fail because the
        /// commits were pruned from Alice's in-memory tree.
        #[test]
        fn resolver_handles_full_sync_scenario() {
            let mut rng = seeded_rng(305);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("d", 2)],
                &[("a", "b"), ("b", "d")],
            );

            // Alice: only loose commits
            let alice_tree = graph.to_sedimentree();
            let seed = FingerprintSeed::new(42, 99);

            // Step 1: Alice creates resolver (captures tree state)
            let resolver = alice_tree.fingerprint_resolver(&seed);

            // Bob: has a fragment covering the same commits
            let fragment = graph.make_fragment("a", &["d"], &["b"]);
            let bob_tree =
                Sedimentree::new(vec![fragment.clone()], vec![]).minimize(graph.depth_metric());

            // Step 2: Bob diffs against Alice's summary, sees commit fingerprints
            // he doesn't have → echoes them back as `requesting`
            let diff = bob_tree.diff_remote_fingerprints(resolver.summary());
            assert!(
                !diff.remote_only_commit_fingerprints.is_empty(),
                "Bob should request commits he doesn't have"
            );

            // Step 3: Alice ingests Bob's fragment and minimizes
            let mut alice_after = alice_tree.clone();
            alice_after.add_fragment(fragment);
            let _alice_minimized = alice_after.minimize(graph.depth_metric());

            // Step 4: Alice resolves Bob's requested fingerprints using the
            // ORIGINAL resolver (not the now-minimized tree)
            for fp in &diff.remote_only_commit_fingerprints {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve requested commit fingerprint {fp} \
                     even after Alice's tree was minimized"
                );
            }
        }

        /// Partial coverage: Bob's fragment covers only some of Alice's
        /// commits. After minimize, some commits are pruned and some remain
        /// as loose commits. The resolver must resolve _all_ original
        /// fingerprints (both pruned and surviving).
        ///
        /// ```text
        /// a(2) → b(0) → c(0) → d(2)
        /// Bob's fragment: a → b (covers a and b only)
        /// Surviving loose: c, d
        /// ```
        #[test]
        fn sync_scenario_partial_coverage() {
            let mut rng = seeded_rng(306);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
                &[("a", "b"), ("b", "c"), ("c", "d")],
            );

            let alice_tree = graph.to_sedimentree();
            let seed = FingerprintSeed::new(42, 99);
            let resolver = alice_tree.fingerprint_resolver(&seed);

            let commit_count = resolver.summary().commit_fingerprints().len();
            assert_eq!(commit_count, 4, "Alice should have 4 commits");

            // Bob has a fragment covering only a→b
            let fragment = graph.make_fragment("a", &["b"], &[]);
            let bob_tree =
                Sedimentree::new(vec![fragment.clone()], vec![]).minimize(graph.depth_metric());

            let diff = bob_tree.diff_remote_fingerprints(resolver.summary());
            assert!(
                !diff.remote_only_commit_fingerprints.is_empty(),
                "Bob should request commits he doesn't have"
            );

            // Alice ingests and minimizes — only a is pruned (covered by fragment)
            let mut alice_after = alice_tree.clone();
            alice_after.add_fragment(fragment);
            let minimized = alice_after.minimize(graph.depth_metric());
            let surviving = minimized.loose_commits().count();
            assert!(
                surviving > 0 && surviving < commit_count,
                "some but not all commits should survive (got {surviving}/{commit_count})"
            );

            // Resolver resolves ALL requested fingerprints (pruned and surviving)
            for fp in &diff.remote_only_commit_fingerprints {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve {fp} (partial coverage)"
                );
            }
        }

        /// Diamond DAG: fragment covers commits reachable via multiple paths.
        ///
        /// ```text
        ///     a(2)
        ///    / \
        ///   b   c   (both depth 0)
        ///    \ /
        ///     d(2)
        /// ```
        ///
        /// Bob's fragment: a → d (covers all 4 via both paths)
        #[test]
        fn sync_scenario_diamond_dag() {
            let mut rng = seeded_rng(307);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
                &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
            );

            let alice_tree = graph.to_sedimentree();
            let seed = FingerprintSeed::new(42, 99);
            let resolver = alice_tree.fingerprint_resolver(&seed);
            assert_eq!(
                resolver.summary().commit_fingerprints().len(),
                4,
                "Alice should have 4 commits"
            );

            let fragment = graph.make_fragment("a", &["d"], &["b", "c"]);
            let bob_tree =
                Sedimentree::new(vec![fragment.clone()], vec![]).minimize(graph.depth_metric());

            let diff = bob_tree.diff_remote_fingerprints(resolver.summary());
            assert!(
                !diff.remote_only_commit_fingerprints.is_empty(),
                "Bob should request commits"
            );

            let mut alice_after = alice_tree.clone();
            alice_after.add_fragment(fragment);
            let minimized = alice_after.minimize(graph.depth_metric());
            let surviving = minimized.loose_commits().count();
            assert!(
                surviving < 4,
                "diamond fragment should prune commits (got {surviving}/4)"
            );

            for fp in &diff.remote_only_commit_fingerprints {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve {fp} (diamond DAG)"
                );
            }
        }

        /// Bob requests _fragment_ fingerprints, not just commit fingerprints.
        ///
        /// Alice has a shallow fragment + loose commits. Bob has a deeper
        /// fragment. Bob diffs Alice's summary → echoes her fragment
        /// fingerprints. After Alice ingests Bob's deep fragment and
        /// minimizes, her shallow fragment is pruned. The resolver must
        /// still resolve the fragment fingerprint.
        ///
        /// ```text
        /// a(3) → b(1) → c(0)
        /// Alice: fragment b→c + loose commits {a, b, c}
        /// Bob:   fragment a→c (deeper, dominates b→c)
        /// ```
        #[test]
        fn sync_scenario_fragment_fingerprint_resolution() {
            let mut rng = seeded_rng(308);
            let graph = TestGraph::new(
                &mut rng,
                &[("a", 3), ("b", 1), ("c", 0)],
                &[("a", "b"), ("b", "c")],
            );

            // Alice: shallow fragment + loose commits
            let shallow = graph.make_fragment("b", &["c"], &[]);
            let alice_tree = graph.to_sedimentree_with_fragments(vec![shallow]);
            let seed = FingerprintSeed::new(42, 99);
            let resolver = alice_tree.fingerprint_resolver(&seed);

            assert_eq!(
                resolver.summary().fragment_fingerprints().len(),
                1,
                "Alice should have 1 fragment"
            );

            // Bob: deep fragment that dominates Alice's shallow one
            let deep = graph.make_fragment("a", &["c"], &["b"]);
            let bob_tree =
                Sedimentree::new(vec![deep.clone()], vec![]).minimize(graph.depth_metric());

            let diff = bob_tree.diff_remote_fingerprints(resolver.summary());

            // Bob doesn't have Alice's shallow fragment → requests it
            assert!(
                !diff.remote_only_fragment_fingerprints.is_empty(),
                "Bob should request Alice's fragment fingerprint"
            );

            // Alice ingests deep fragment and minimizes (shallow gets pruned)
            let mut alice_after = alice_tree.clone();
            alice_after.add_fragment(deep);
            let minimized = alice_after.minimize(graph.depth_metric());
            assert_eq!(
                minimized.fragments().count(),
                1,
                "only deep fragment should survive"
            );

            // Resolver must still resolve the pruned shallow fragment's fingerprint
            for fp in &diff.remote_only_fragment_fingerprints {
                assert!(
                    resolver.resolve_fragment(fp).is_some(),
                    "resolver must resolve pruned fragment fingerprint {fp}"
                );
            }

            // And commit fingerprints too
            for fp in &diff.remote_only_commit_fingerprints {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve commit fingerprint {fp}"
                );
            }
        }

        /// Multiple fragments from Bob cover different subsets of Alice's
        /// commits. Cascading pruning removes commits covered by either
        /// fragment.
        ///
        /// ```text
        /// Chain 1: a(2) → b(0) → c(2)
        /// Chain 2: d(2) → e(0) → f(2)
        /// Shared:  c → g(0) → f
        ///
        /// Bob's fragment 1: a → c (covers a, b)
        /// Bob's fragment 2: d → f (covers d, e)
        /// ```
        #[test]
        fn sync_scenario_multiple_cascading_fragments() {
            let mut rng = seeded_rng(309);
            let graph = TestGraph::new(
                &mut rng,
                &[
                    ("a", 2),
                    ("b", 0),
                    ("c", 2),
                    ("d", 2),
                    ("e", 0),
                    ("f", 2),
                    ("g", 0),
                ],
                &[
                    ("a", "b"),
                    ("b", "c"),
                    ("d", "e"),
                    ("e", "f"),
                    ("c", "g"),
                    ("g", "f"),
                ],
            );

            let alice_tree = graph.to_sedimentree();
            let seed = FingerprintSeed::new(42, 99);
            let resolver = alice_tree.fingerprint_resolver(&seed);

            let commit_count = resolver.summary().commit_fingerprints().len();
            assert_eq!(commit_count, 7, "Alice should have 7 commits");

            // Bob has two fragments covering different subsets
            let frag1 = graph.make_fragment("a", &["c"], &["b"]);
            let frag2 = graph.make_fragment("d", &["f"], &["e"]);
            let bob_tree = Sedimentree::new(vec![frag1.clone(), frag2.clone()], vec![])
                .minimize(graph.depth_metric());

            let diff = bob_tree.diff_remote_fingerprints(resolver.summary());
            assert!(
                !diff.remote_only_commit_fingerprints.is_empty(),
                "Bob should request commits"
            );

            // Alice ingests both fragments and minimizes
            let mut alice_after = alice_tree.clone();
            alice_after.add_fragment(frag1);
            alice_after.add_fragment(frag2);
            let minimized = alice_after.minimize(graph.depth_metric());
            let surviving = minimized.loose_commits().count();
            assert!(
                surviving < commit_count,
                "cascading fragments should prune commits (got {surviving}/{commit_count})"
            );

            for fp in &diff.remote_only_commit_fingerprints {
                assert!(
                    resolver.resolve_commit(fp).is_some(),
                    "resolver must resolve {fp} (cascading fragments)"
                );
            }
        }
    }

    /// Property tests for [`FingerprintResolver`].
    #[cfg(feature = "bolero")]
    mod fingerprint_resolver_proptests {
        use super::*;
        use crate::commit::CountLeadingZeroBytes;

        /// For any tree and seed, the resolver resolves every fingerprint
        /// from its own summary.
        #[test]
        fn resolver_resolves_all_own_summary_fingerprints() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, FingerprintSeed)>()
                .for_each(|(tree, seed)| {
                    let resolver = tree.fingerprint_resolver(seed);

                    for fp in resolver.summary().commit_fingerprints() {
                        assert!(
                            resolver.resolve_commit(fp).is_some(),
                            "resolver must resolve every commit fingerprint from its summary"
                        );
                    }

                    for fp in resolver.summary().fragment_fingerprints() {
                        assert!(
                            resolver.resolve_fragment(fp).is_some(),
                            "resolver must resolve every fragment fingerprint from its summary"
                        );
                    }
                });
        }

        /// The resolver's summary is identical to `fingerprint_summarize`
        /// with the same seed.
        #[test]
        fn resolver_summary_equals_fingerprint_summarize() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, FingerprintSeed)>()
                .for_each(|(tree, seed)| {
                    let summary = tree.fingerprint_summarize(seed);
                    let resolver = tree.fingerprint_resolver(seed);
                    assert_eq!(
                        summary,
                        *resolver.summary(),
                        "resolver summary must equal fingerprint_summarize"
                    );
                });
        }

        /// Fingerprints from a different seed should not resolve. With
        /// independent u64 `SipHash` outputs, the collision probability
        /// for `n` items is ~n²/2⁶⁴, so zero cross-resolutions are
        /// expected for any reasonably sized tree.
        #[test]
        fn resolver_cross_seed_produces_no_hits() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, FingerprintSeed, FingerprintSeed)>()
                .for_each(|(tree, seed_a, seed_b)| {
                    if seed_a == seed_b {
                        return;
                    }

                    let resolver_a = tree.fingerprint_resolver(seed_a);
                    let summary_b = tree.fingerprint_summarize(seed_b);

                    let commit_collisions = summary_b
                        .commit_fingerprints()
                        .iter()
                        .filter(|fp| resolver_a.resolve_commit(fp).is_some())
                        .count();

                    let fragment_collisions = summary_b
                        .fragment_fingerprints()
                        .iter()
                        .filter(|fp| resolver_a.resolve_fragment(fp).is_some())
                        .count();

                    assert_eq!(
                        commit_collisions,
                        0,
                        "cross-seed commit collisions should be zero \
                         ({commit_collisions} hits for {} items)",
                        summary_b.commit_fingerprints().len()
                    );
                    assert_eq!(
                        fragment_collisions,
                        0,
                        "cross-seed fragment collisions should be zero \
                         ({fragment_collisions} hits for {} items)",
                        summary_b.fragment_fingerprints().len()
                    );
                });
        }

        /// After merging new data and minimizing, the original resolver
        /// still resolves all its original fingerprints.
        #[test]
        fn resolver_stable_across_merge_and_minimize() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, Sedimentree, FingerprintSeed)>()
                .for_each(|(tree_a, tree_b, seed)| {
                    let resolver = tree_a.fingerprint_resolver(seed);
                    let original_commit_fps: Vec<_> = resolver
                        .summary()
                        .commit_fingerprints()
                        .iter()
                        .copied()
                        .collect();
                    let original_frag_fps: Vec<_> = resolver
                        .summary()
                        .fragment_fingerprints()
                        .iter()
                        .copied()
                        .collect();

                    // Merge new data and minimize — simulates receiving a sync response
                    let mut merged = tree_a.clone();
                    merged.merge(tree_b.clone());
                    let _minimized = merged.minimize(&CountLeadingZeroBytes);

                    // The resolver is independent — still resolves everything
                    for fp in &original_commit_fps {
                        assert!(
                            resolver.resolve_commit(fp).is_some(),
                            "resolver must still resolve commit fingerprint after merge+minimize"
                        );
                    }

                    for fp in &original_frag_fps {
                        assert!(
                            resolver.resolve_fragment(fp).is_some(),
                            "resolver must still resolve fragment fingerprint after merge+minimize"
                        );
                    }
                });
        }
    }
}
