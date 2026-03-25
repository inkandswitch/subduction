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
    /// paired with their precomputed digests.
    pub local_only_commits: Vec<(&'a Digest<LooseCommit>, &'a LooseCommit)>,

    /// Requestor's commit fingerprints that the responder doesn't have locally.
    /// Echoed back so the requestor can reverse-lookup and send the data.
    pub remote_only_commit_fingerprints: Vec<Fingerprint<CommitId>>,

    /// Requestor's fragment fingerprints that the responder doesn't have locally.
    /// Echoed back so the requestor can reverse-lookup and send the data.
    pub remote_only_fragment_fingerprints: Vec<Fingerprint<FragmentId>>,
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
    commits: Map<Digest<LooseCommit>, LooseCommit>,
}

impl Sedimentree {
    /// Constructor for a [`Sedimentree`].
    #[must_use]
    pub fn new(fragments: Vec<Fragment>, commits: Vec<LooseCommit>) -> Self {
        Self {
            fragments: fragments
                .into_iter()
                .map(|f| (Digest::hash(&f), f))
                .collect(),
            commits: commits.into_iter().map(|c| (Digest::hash(&c), c)).collect(),
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

        // Hash full digests (head, boundary, commits)
        let mut digests: Vec<Digest<LooseCommit>> = minimal
            .fragments()
            .flat_map(|s| core::iter::once(s.head()).chain(s.boundary().iter().copied()))
            .chain(minimal.commits.keys().copied())
            .collect();
        digests.sort();

        let mut h = blake3::Hasher::new();
        for d in &digests {
            h.update(d.as_bytes());
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
        let digest = Digest::hash(&commit);
        match self.commits.entry(digest) {
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
    pub fn commit_entries(&self) -> impl Iterator<Item = (&Digest<LooseCommit>, &LooseCommit)> {
        self.commits.iter()
    }

    /// Returns true if this [`Sedimentree`] has a commit with the given digest.
    #[must_use]
    pub fn has_loose_commit(&self, digest: Digest<LooseCommit>) -> bool {
        self.commits.contains_key(&digest)
    }

    /// Returns true if this [`Sedimentree`] has a fragment starting with the given digest.
    #[must_use]
    pub fn has_fragment_starting_with<M: DepthMetric>(
        &self,
        digest: Digest<LooseCommit>,
        depth_metric: &M,
    ) -> bool {
        self.heads(depth_metric).contains(&digest)
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

        // Map: fragment head digest → fragment index
        let head_to_frag: Map<Digest<LooseCommit>, usize> = fragments
            .iter()
            .enumerate()
            .map(|(i, f)| (f.head(), i))
            .collect();

        // Build in-degree counts and adjacency lists.
        let mut in_degree = vec![0u32; n_total];
        let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n_total];

        // Fragment → fragment edges: F_i depends on F_j if F_i's boundary
        // contains F_j's head.
        for (i, frag) in fragments.iter().enumerate() {
            for boundary_digest in frag.boundary() {
                if let Some(&j) = head_to_frag.get(boundary_digest) {
                    if i != j {
                        in_degree[i] += 1;
                        dependents[j].push(i);
                    }
                }
            }
        }

        // Loose commit → fragment edges: loose commit L depends on
        // fragment F if any of L's parents equals F's head.
        for (li, commit) in loose.iter().enumerate() {
            let idx = n_frags + li;
            let mut seen_deps: Set<usize> = Set::new();
            for parent in commit.parents() {
                if let Some(&fi) = head_to_frag.get(parent) {
                    if seen_deps.insert(fi) {
                        in_degree[idx] += 1;
                        dependents[fi].push(idx);
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
            .iter()
            .filter(|(digest, _)| simplified_dag.contains_commit(digest))
            .map(|(_, c)| c.clone())
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
            .map(|c| Fingerprint::new(seed, &c.commit_id()))
            .collect();

        let fragment_fingerprints = self
            .fragments
            .values()
            .map(|f| Fingerprint::new(seed, &f.fragment_id()))
            .collect();

        FingerprintSummary::new(*seed, commit_fingerprints, fragment_fingerprints)
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
        let local_only_commits: Vec<(&Digest<LooseCommit>, &LooseCommit)> = self
            .commits
            .iter()
            .filter(|(_, c)| {
                !remote
                    .commit_fingerprints
                    .contains(&Fingerprint::new(seed, &c.commit_id()))
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
            .map(|c| Fingerprint::new(seed, &c.commit_id()))
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
    pub fn heads<M: DepthMetric>(&self, depth_metric: &M) -> Vec<Digest<LooseCommit>> {
        let minimized = self.minimize(depth_metric);
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.values());
        let mut heads = Vec::<Digest<LooseCommit>>::new();
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
pub fn has_commit_boundary<
    I: IntoIterator<Item = D>,
    D: Into<Digest<LooseCommit>>,
    M: DepthMetric,
>(
    commits: I,
    depth_metric: &M,
) -> bool {
    commits
        .into_iter()
        .any(|digest| depth_metric.to_depth(digest.into()) <= MAX_STRATA_DEPTH)
}

#[cfg(test)]
mod tests {
    use alloc::vec;

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
        LooseCommit::new(sedimentree_id, BTreeSet::new(), blob_meta)
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
            Digest::force_from_bytes(head_bytes),
            BTreeSet::from([Digest::force_from_bytes(boundary_bytes)]),
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
    fn make_fragment_with_deps(
        head_bytes: [u8; 32],
        boundary: BTreeSet<Digest<LooseCommit>>,
    ) -> Fragment {
        let sid = make_sedimentree_id(0);
        let blob_meta = make_blob_meta(0);
        Fragment::new(
            sid,
            Digest::force_from_bytes(head_bytes),
            boundary,
            &[],
            blob_meta,
        )
    }

    fn unique_head(id: u8) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[31] = id;
        bytes
    }

    #[test]
    fn topsorted_empty_sedimentree() {
        let tree = Sedimentree::default();
        let order = tree.topsorted_blob_order().expect("no cycle");
        assert!(order.is_empty());
    }

    #[test]
    fn topsorted_single_fragment() {
        let mut tree = Sedimentree::default();
        let f = make_fragment_with_deps(unique_head(1), BTreeSet::new());
        tree.add_fragment(f);

        let order = tree.topsorted_blob_order().expect("no cycle");
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], SedimentreeItem::Fragment(0));
    }

    #[test]
    fn topsorted_single_loose_commit() {
        let mut tree = Sedimentree::default();
        tree.add_commit(make_commit(1));

        let order = tree.topsorted_blob_order().expect("no cycle");
        assert_eq!(order.len(), 1);
        assert_eq!(order[0], SedimentreeItem::LooseCommit(0));
    }

    #[test]
    fn topsorted_linear_chain_deepest_first() {
        // C (deep) ← B (mid) ← A (shallow)
        // A.boundary = {B.head}, B.boundary = {C.head}, C.boundary = {}
        let c_head = unique_head(3);
        let b_head = unique_head(2);
        let a_head = unique_head(1);

        let c = make_fragment_with_deps(c_head, BTreeSet::new());
        let b = make_fragment_with_deps(b_head, BTreeSet::from([Digest::force_from_bytes(c_head)]));
        let a = make_fragment_with_deps(a_head, BTreeSet::from([Digest::force_from_bytes(b_head)]));

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);
        tree.add_fragment(c);

        let order = tree.topsorted_blob_order().expect("no cycle");
        assert_eq!(order.len(), 3);

        // Map SedimentreeItem indices back to heads for verification.
        let fragments: Vec<_> = tree.fragments().collect();
        let head_order: Vec<[u8; 32]> = order
            .iter()
            .map(|item| match item {
                SedimentreeItem::Fragment(i) => *fragments[*i].head().as_bytes(),
                SedimentreeItem::LooseCommit(_) => panic!("unexpected loose commit"),
            })
            .collect();

        // C must come before B, B must come before A
        let c_pos = head_order.iter().position(|h| *h == c_head).unwrap();
        let b_pos = head_order.iter().position(|h| *h == b_head).unwrap();
        let a_pos = head_order.iter().position(|h| *h == a_head).unwrap();
        assert!(c_pos < b_pos, "C (deepest) must come before B");
        assert!(b_pos < a_pos, "B must come before A (shallowest)");
    }

    #[test]
    fn topsorted_diamond_dag() {
        //     D (deep)
        //    / \
        //   B   C
        //    \ /
        //     A (shallow)
        //
        // A.boundary = {B.head, C.head}
        // B.boundary = {D.head}
        // C.boundary = {D.head}
        // D.boundary = {}
        let d_head = unique_head(4);
        let b_head = unique_head(2);
        let c_head = unique_head(3);
        let a_head = unique_head(1);

        let d = make_fragment_with_deps(d_head, BTreeSet::new());
        let b = make_fragment_with_deps(b_head, BTreeSet::from([Digest::force_from_bytes(d_head)]));
        let c = make_fragment_with_deps(c_head, BTreeSet::from([Digest::force_from_bytes(d_head)]));
        let a = make_fragment_with_deps(
            a_head,
            BTreeSet::from([
                Digest::force_from_bytes(b_head),
                Digest::force_from_bytes(c_head),
            ]),
        );

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);
        tree.add_fragment(c);
        tree.add_fragment(d);

        let order = tree.topsorted_blob_order().expect("no cycle");
        assert_eq!(order.len(), 4);

        let fragments: Vec<_> = tree.fragments().collect();
        let head_order: Vec<[u8; 32]> = order
            .iter()
            .map(|item| match item {
                SedimentreeItem::Fragment(i) => *fragments[*i].head().as_bytes(),
                SedimentreeItem::LooseCommit(_) => panic!("unexpected loose commit"),
            })
            .collect();

        let d_pos = head_order.iter().position(|h| *h == d_head).unwrap();
        let b_pos = head_order.iter().position(|h| *h == b_head).unwrap();
        let c_pos = head_order.iter().position(|h| *h == c_head).unwrap();
        let a_pos = head_order.iter().position(|h| *h == a_head).unwrap();

        assert!(d_pos < b_pos, "D must come before B");
        assert!(d_pos < c_pos, "D must come before C");
        assert!(b_pos < a_pos, "B must come before A");
        assert!(c_pos < a_pos, "C must come before A");
    }

    #[test]
    fn topsorted_loose_commit_after_fragment() {
        // Fragment F with head H, loose commit L whose parent is H.
        // F must come before L.
        let f_head = unique_head(1);
        let f = make_fragment_with_deps(f_head, BTreeSet::new());

        let sid = make_sedimentree_id(0);
        let blob_meta = make_blob_meta(99);
        let loose = LooseCommit::new(
            sid,
            BTreeSet::from([Digest::force_from_bytes(f_head)]),
            blob_meta,
        );

        let mut tree = Sedimentree::default();
        tree.add_fragment(f);
        tree.add_commit(loose);

        let order = tree.topsorted_blob_order().expect("no cycle");
        assert_eq!(order.len(), 2);

        let frag_pos = order
            .iter()
            .position(|i| matches!(i, SedimentreeItem::Fragment(_)))
            .unwrap();
        let loose_pos = order
            .iter()
            .position(|i| matches!(i, SedimentreeItem::LooseCommit(_)))
            .unwrap();

        assert!(
            frag_pos < loose_pos,
            "fragment must come before dependent loose commit"
        );
    }

    #[test]
    fn topsorted_cycle_returns_error() {
        // Create two fragments that reference each other's heads.
        // A.boundary = {B.head}, B.boundary = {A.head}
        let a_head = unique_head(1);
        let b_head = unique_head(2);

        let a = make_fragment_with_deps(a_head, BTreeSet::from([Digest::force_from_bytes(b_head)]));
        let b = make_fragment_with_deps(b_head, BTreeSet::from([Digest::force_from_bytes(a_head)]));

        let mut tree = Sedimentree::default();
        tree.add_fragment(a);
        tree.add_fragment(b);

        let result = tree.topsorted_blob_order();
        assert!(result.is_err(), "cycle should produce CycleError");

        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "cycle in sedimentree dependency graph: 2 items but only 0 could be sorted"
        );
    }

    #[cfg(all(test, feature = "bolero"))]
    #[allow(clippy::similar_names)]
    mod proptests {
        use core::sync::atomic::{AtomicU64, Ordering};

        use rand::{Rng, SeedableRng, rngs::SmallRng};

        use super::*;
        use crate::{commit::CountLeadingZeroBytes, fragment::FragmentSummary};

        static SEED_COUNTER: AtomicU64 = AtomicU64::new(0);

        fn hash_with_leading_zeros(zeros_count: u32) -> Digest<LooseCommit> {
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
            Digest::force_from_bytes(byte_arr)
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

                    let start_hash = hash_with_leading_zeros(10);
                    let deeper_boundary_hash = hash_with_leading_zeros(10);

                    let shallower_start_hash: Digest<LooseCommit>;
                    let shallower_boundary_hash: Digest<LooseCommit>;
                    let mut checkpoints = Vec::<Digest<LooseCommit>>::arbitrary(u)?;
                    let lower_level_type = ShallowerDepthType::arbitrary(u)?;
                    match lower_level_type {
                        ShallowerDepthType::StartsAtStartBoundaryAtCheckpoint => {
                            shallower_start_hash = start_hash;
                            shallower_boundary_hash = hash_with_leading_zeros(9);
                            checkpoints.push(shallower_boundary_hash);
                        }
                        ShallowerDepthType::StartsAtCheckpointBoundaryAtCheckpoint => {
                            shallower_start_hash = hash_with_leading_zeros(9);
                            shallower_boundary_hash = hash_with_leading_zeros(9);
                            checkpoints.push(shallower_start_hash);
                            checkpoints.push(shallower_boundary_hash);
                        }
                        ShallowerDepthType::StartsAtCheckpointBoundaryAtBoundary => {
                            shallower_start_hash = hash_with_leading_zeros(9);
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
                    let mut frontier: Vec<Digest<LooseCommit>> = Vec::new();
                    let num_commits: u32 = u.int_in_range(1..=20)?;
                    let mut result = Vec::with_capacity(num_commits as usize);
                    for _ in 0..num_commits {
                        let contents = Vec::<u8>::arbitrary(u)?;
                        let blob = crate::blob::Blob::from(contents);
                        let blob_meta = BlobMeta::new(&blob);
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
                        let commit = LooseCommit::new(sedimentree_id, parents, blob_meta);
                        frontier.retain(|p| !commit.parents().contains(p));
                        frontier.push(Digest::hash(&commit));
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
                            let commit_digest = Digest::hash(commit);
                            let present = merged_commit_keys.contains(&commit_digest);
                            let covered = min_merged
                                .fragments()
                                .any(|f| f.supports_block(commit_digest));
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
}
