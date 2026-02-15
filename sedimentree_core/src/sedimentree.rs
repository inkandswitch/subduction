//! The main Sedimentree data structure and related types.

mod commit_dag;

use alloc::{collections::BTreeSet, vec::Vec};

use crate::{
    collections::{Map, Set},
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    depth::{Depth, DepthMetric, MAX_STRATA_DEPTH},
    fragment::{Fragment, FragmentSpec, FragmentSummary, checkpoint::Checkpoint, id::FragmentId},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};

/// A less detailed representation of a Sedimentree that omits strata checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Default, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(not(feature = "std"), derive(Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SedimentreeSummary {
    #[n(0)]
    #[cbor(with = "crate::cbor::set")]
    fragment_summaries: Set<FragmentSummary>,
    #[n(1)]
    #[cbor(with = "crate::cbor::set")]
    commits: Set<LooseCommit>,
}

impl SedimentreeSummary {
    /// Constructor for a [`SedimentreeSummary`].
    #[must_use]
    pub const fn new(
        fragment_summaries: Set<FragmentSummary>,
        commits: Set<LooseCommit>,
    ) -> SedimentreeSummary {
        SedimentreeSummary {
            fragment_summaries,
            commits,
        }
    }

    /// The set of fragment summaries in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn fragment_summaries(&self) -> &Set<FragmentSummary> {
        &self.fragment_summaries
    }

    /// The set of loose commits in this [`SedimentreeSummary`].
    #[must_use]
    pub const fn loose_commits(&self) -> &Set<LooseCommit> {
        &self.commits
    }

    /// Create a [`RemoteDiff`] with empty local fragments and commits.
    #[must_use]
    pub fn as_remote_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_fragment_summaries: self.fragment_summaries.iter().collect(),
            remote_commits: self.commits.iter().collect(),
            local_fragments: Vec::new(),
            local_commits: Vec::new(),
        }
    }
}

/// A compact summary of a [`Sedimentree`] for wire transmission.
///
/// Uses SipHash-2-4 fingerprints instead of full structural data.
/// Each side computes fingerprints with the shared [`FingerprintSeed`]
/// and performs set difference on u64 values.
///
/// Bandwidth: ~16 bytes (seed) + 8 bytes per item, vs ~100+ bytes
/// per item with [`SedimentreeSummary`].
#[derive(Clone, Debug, Hash, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct FingerprintSummary {
    #[n(0)]
    seed: FingerprintSeed,

    #[n(1)]
    commit_fingerprints: BTreeSet<Fingerprint<CommitId>>,

    #[n(2)]
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
    /// Fragments the responder has that the requestor is missing.
    pub local_only_fragments: Vec<&'a Fragment>,

    /// Commits the responder has that the requestor is missing.
    pub local_only_commits: Vec<&'a LooseCommit>,

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

/// The difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteDiff<'a> {
    /// Fragments present in the remote tree but not the local.
    pub remote_fragment_summaries: Vec<&'a FragmentSummary>,

    /// Commits present in the remote tree but not the local.
    pub remote_commits: Vec<&'a LooseCommit>,

    /// Fragments present in the local tree but not the remote.
    pub local_fragments: Vec<&'a Fragment>,

    /// Commits present in the local tree but not the remote.
    pub local_commits: Vec<&'a LooseCommit>,
}

/// All of the Sedimentree metadata about all the fragments for a series of payload.
#[derive(Default, Clone, PartialEq, Eq)]
#[cfg_attr(not(feature = "std"), derive(PartialOrd, Ord, Hash))]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Sedimentree {
    fragments: Set<Fragment>,
    commits: Set<LooseCommit>,
}

impl Sedimentree {
    /// Constructor for a [`Sedimentree`].
    #[must_use]
    pub fn new(fragments: Vec<Fragment>, commits: Vec<LooseCommit>) -> Self {
        Self {
            fragments: fragments.into_iter().collect(),
            commits: commits.into_iter().collect(),
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
            .chain(minimal.commits.iter().map(LooseCommit::digest))
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
        self.commits.insert(commit)
    }

    /// Add a fragment to the [`Sedimentree`].
    ///
    /// Returns `true` if the stratum was not already present
    pub fn add_fragment(&mut self, fragment: Fragment) -> bool {
        self.fragments.insert(fragment)
    }

    /// Compute the difference between two local [`Sedimentree`]s.
    #[must_use]
    pub fn diff<'a>(&'a self, other: &'a Sedimentree) -> Diff<'a> {
        Diff {
            // Items in right but not left = what left is missing
            left_missing_fragments: other.fragments.difference(&self.fragments).collect(),
            left_missing_commits: other.commits.difference(&self.commits).collect(),
            // Items in left but not right = what right is missing
            right_missing_fragments: self.fragments.difference(&other.fragments).collect(),
            right_missing_commits: self.commits.difference(&other.commits).collect(),
        }
    }

    /// Compute the difference between a local [`Sedimentree`] and a remote [`SedimentreeSummary`].
    #[must_use]
    pub fn diff_remote<'a>(&'a self, remote: &'a SedimentreeSummary) -> RemoteDiff<'a> {
        let fragment_by_summary: Map<&FragmentSummary, &Fragment> =
            self.fragments.iter().map(|f| (f.summary(), f)).collect();

        let our_fragments_meta: Set<&FragmentSummary> =
            fragment_by_summary.keys().copied().collect();
        let their_fragments: Set<&FragmentSummary> = remote.fragment_summaries.iter().collect();

        let local_fragments: Vec<&Fragment> = our_fragments_meta
            .difference(&their_fragments)
            .filter_map(|summary| fragment_by_summary.get(summary).copied())
            .collect();

        let remote_fragments = their_fragments.difference(&our_fragments_meta);

        let our_commits = self.commits.iter().collect::<Set<&LooseCommit>>();
        let their_commits = remote.commits.iter().collect();
        let local_commits = our_commits.difference(&their_commits);
        let remote_commits = their_commits.difference(&our_commits);

        RemoteDiff {
            remote_fragment_summaries: remote_fragments.into_iter().copied().collect(),
            remote_commits: remote_commits.into_iter().copied().collect(),
            local_fragments,
            local_commits: local_commits.into_iter().copied().collect(),
        }
    }

    /// Iterate over all fragments in this [`Sedimentree`].
    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.iter()
    }

    /// Iterate over all loose commits in this [`Sedimentree`].
    pub fn loose_commits(&self) -> impl Iterator<Item = &LooseCommit> {
        self.commits.iter()
    }

    /// Returns true if this [`Sedimentree`] has a fragment with the given digest.
    #[must_use]
    pub fn has_loose_commit(&self, digest: Digest<LooseCommit>) -> bool {
        self.loose_commits().any(|c| c.digest() == digest)
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
    /// Complexity: O(M × avg_checkpoints) where M = total fragments.
    #[must_use]
    pub fn minimize<M: DepthMetric>(&self, depth_metric: &M) -> Sedimentree {
        // 1. Group fragments by depth
        let mut by_depth: Map<Depth, Vec<&Fragment>> = Map::new();
        for fragment in &self.fragments {
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
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let simplified_dag = dag.simplify(&minimized_fragments, depth_metric);

        let commits = self
            .commits
            .iter()
            .filter(|&c| simplified_dag.contains_commit(&c.digest()))
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
            .iter()
            .map(|c| Fingerprint::new(seed, &c.commit_id()))
            .collect();

        let fragment_fingerprints = self
            .fragments
            .iter()
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
        let local_only_commits: Vec<&LooseCommit> = self
            .commits
            .iter()
            .filter(|c| {
                !remote
                    .commit_fingerprints
                    .contains(&Fingerprint::new(seed, &c.commit_id()))
            })
            .collect();

        let local_only_fragments: Vec<&Fragment> = self
            .fragments
            .iter()
            .filter(|f| {
                !remote
                    .fragment_fingerprints
                    .contains(&Fingerprint::new(seed, &f.fragment_id()))
            })
            .collect();

        // Find requestor fingerprints we don't have locally (echo back)
        let local_commit_fps: BTreeSet<Fingerprint<CommitId>> = self
            .commits
            .iter()
            .map(|c| Fingerprint::new(seed, &c.commit_id()))
            .collect();

        let local_fragment_fps: BTreeSet<Fingerprint<FragmentId>> = self
            .fragments
            .iter()
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

    /// Create a [`SedimentreeSummary`] from this [`Sedimentree`].
    ///
    /// This omits the checkpoints from each fragment.
    /// It is useful for sending over the wire.
    #[must_use]
    pub fn summarize(&self) -> SedimentreeSummary {
        SedimentreeSummary {
            fragment_summaries: self
                .fragments
                .iter()
                .map(|fragment| fragment.summary().clone())
                .collect(),
            commits: self.commits.clone(),
        }
    }

    /// The heads of a Sedimentree are the end hashes of all strata which are
    /// not the start of any other strata or supported by any lower stratum
    /// and which do not appear in the [`LooseCommit`] graph, plus the heads of
    /// the loose commit graph.
    #[must_use]
    pub fn heads<M: DepthMetric>(&self, depth_metric: &M) -> Vec<Digest<LooseCommit>> {
        let minimized = self.minimize(depth_metric);
        let dag = commit_dag::CommitDag::from_commits(minimized.commits.iter());
        let mut heads = Vec::<Digest<LooseCommit>>::new();
        for fragment in &minimized.fragments {
            if !minimized
                .fragments
                .iter()
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
            .into_iter()
            .map(CommitOrFragment::Fragment)
            .chain(self.commits.into_iter().map(CommitOrFragment::Commit))
    }

    /// Given a [`SedimentreeId`], return the [`Fragment`]s that are missing to fill in the gaps.
    #[must_use]
    pub fn missing_fragments<M: DepthMetric>(
        &self,
        id: SedimentreeId,
        seed: &FingerprintSeed,
        depth_metric: &M,
    ) -> Vec<FragmentSpec> {
        let dag = commit_dag::CommitDag::from_commits(self.commits.iter());
        let mut runs_by_level =
            Map::<crate::depth::Depth, (Digest<LooseCommit>, Vec<Digest<LooseCommit>>)>::new();
        let mut all_bundles = Vec::new();
        let fragment_refs: Vec<&Fragment> = self.fragments.iter().collect();
        for commit_hash in dag.canonical_sequence(&fragment_refs, depth_metric) {
            let level = depth_metric.to_depth(commit_hash);
            for (run_level, (_start, checkpoints)) in &mut runs_by_level {
                if run_level < &level {
                    checkpoints.push(commit_hash);
                }
            }
            if level >= MAX_STRATA_DEPTH
                && let Some((head, checkpoints)) = runs_by_level.remove(&level)
            {
                if self.fragments.iter().any(|s| s.supports_block(commit_hash)) {
                    runs_by_level.insert(level, (commit_hash, Vec::new()));
                } else {
                    let checkpoint_fps = checkpoints
                        .iter()
                        .map(|d| {
                            Fingerprint::new(
                                seed,
                                &CommitId::new(Digest::from_bytes(d.into_bytes())),
                            )
                        })
                        .collect();
                    all_bundles.push(FragmentSpec::new(
                        id,
                        head,
                        *seed,
                        checkpoint_fps,
                        BTreeSet::from([commit_hash]),
                    ));
                }
            }
        }
        all_bundles
    }

    /// Create a [`RemoteDiff`] with empty remote fragments and commits.
    #[must_use]
    pub fn as_local_diff(&self) -> RemoteDiff<'_> {
        RemoteDiff {
            remote_fragment_summaries: Vec::new(),
            remote_commits: Vec::new(),
            local_fragments: self.fragments.iter().collect(),
            local_commits: self.commits.iter().collect(),
        }
    }
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

    use crate::blob::BlobMeta;

    use super::*;

    fn make_commit(seed: u8) -> LooseCommit {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        let digest = Digest::from_bytes(bytes);
        let blob_meta = BlobMeta::new(&[seed]);
        LooseCommit::new(digest, BTreeSet::new(), blob_meta)
    }

    fn make_fragment(seed: u8) -> Fragment {
        let mut head_bytes = [0u8; 32];
        head_bytes[0] = seed;
        let mut boundary_bytes = [0u8; 32];
        boundary_bytes[0] = seed;
        boundary_bytes[1] = 1;
        let blob_meta = BlobMeta::new(&[seed]);
        Fragment::new(
            Digest::from_bytes(head_bytes),
            BTreeSet::from([Digest::from_bytes(boundary_bytes)]),
            &[],
            blob_meta,
        )
    }

    #[test]
    fn diff_identical_non_empty_trees() {
        // Two separate trees with identical content
        let commits = vec![make_commit(1), make_commit(2), make_commit(3)];
        let fragments = vec![make_fragment(1), make_fragment(2)];

        let a = Sedimentree::new(fragments.clone(), commits.clone());
        let b = Sedimentree::new(fragments, commits);

        let diff = a.diff(&b);

        assert!(
            diff.left_missing_commits.is_empty(),
            "identical trees have no left missing commits"
        );
        assert!(
            diff.right_missing_commits.is_empty(),
            "identical trees have no right missing commits"
        );
        assert!(
            diff.left_missing_fragments.is_empty(),
            "identical trees have no left missing fragments"
        );
        assert!(
            diff.right_missing_fragments.is_empty(),
            "identical trees have no right missing fragments"
        );

        // Also test diff_remote
        let b_summary = b.summarize();
        let remote_diff = a.diff_remote(&b_summary);

        assert!(
            remote_diff.local_commits.is_empty(),
            "identical trees have no local-only commits"
        );
        assert!(
            remote_diff.remote_commits.is_empty(),
            "identical trees have no remote-only commits"
        );
        assert!(
            remote_diff.local_fragments.is_empty(),
            "identical trees have no local-only fragments"
        );
        assert!(
            remote_diff.remote_fragment_summaries.is_empty(),
            "identical trees have no remote-only fragments"
        );
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

    #[test]
    fn diff_remote_superset() {
        // Scenario: remote is a superset of local
        let shared = vec![make_commit(1), make_commit(2)];
        let remote_extra = vec![make_commit(3)];

        let local = Sedimentree::new(vec![], shared.clone());
        let remote = Sedimentree::new(vec![], [shared, remote_extra].concat());
        let remote_summary = remote.summarize();

        let diff = local.diff_remote(&remote_summary);

        // Local has nothing unique
        assert!(diff.local_commits.is_empty());
        assert!(diff.local_fragments.is_empty());

        // Remote has 1 commit local doesn't have
        assert_eq!(diff.remote_commits.len(), 1);
        assert!(diff.remote_fragment_summaries.is_empty());
    }

    #[test]
    fn diff_remote_diverged_with_overlap() {
        // Scenario: local and remote share history but diverged
        let shared = vec![make_commit(1)];
        let local_only = vec![make_commit(10)];
        let remote_only = vec![make_commit(20), make_commit(21)];

        let local = Sedimentree::new(vec![], [shared.clone(), local_only].concat());
        let remote = Sedimentree::new(vec![], [shared, remote_only].concat());
        let remote_summary = remote.summarize();

        let diff = local.diff_remote(&remote_summary);

        // Local has 1 unique commit
        assert_eq!(diff.local_commits.len(), 1);
        // Remote has 2 unique commits
        assert_eq!(diff.remote_commits.len(), 2);
    }

    mod proptests {
        use alloc::vec;
        use core::sync::atomic::{AtomicU64, Ordering};

        use rand::{Rng, SeedableRng, rngs::SmallRng};

        use crate::{blob::BlobMeta, commit::CountLeadingZeroBytes};

        use super::super::*;

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
            Digest::from_bytes(byte_arr)
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
                    let mut frontier: Vec<Digest<LooseCommit>> = Vec::new();
                    let num_commits: u32 = u.int_in_range(1..=20)?;
                    let mut result = Vec::with_capacity(num_commits as usize);
                    for _ in 0..num_commits {
                        let contents = Vec::<u8>::arbitrary(u)?;
                        let blob_meta = BlobMeta::new(&contents);
                        let hash = Digest::<LooseCommit>::arbitrary(u)?;
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
                        frontier.retain(|p| !parents.contains(p));
                        frontier.push(hash);
                        result.push(LooseCommit::new(hash, parents, blob_meta));
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
        fn diff_remote_matches_diff_for_local_items() {
            bolero::check!()
                .with_arbitrary::<(Sedimentree, Sedimentree)>()
                .for_each(|(local, remote)| {
                    let remote_summary = remote.summarize();
                    let remote_diff = local.diff_remote(&remote_summary);
                    let local_diff = local.diff(remote);

                    // local_fragments in diff_remote should match right_missing_fragments in diff
                    // (what we have that they don't)
                    assert_eq!(
                        remote_diff.local_fragments.len(),
                        local_diff.right_missing_fragments.len(),
                        "diff_remote local_fragments should match diff right_missing_fragments"
                    );

                    // local_commits should match right_missing_commits
                    assert_eq!(
                        remote_diff.local_commits.len(),
                        local_diff.right_missing_commits.len(),
                        "diff_remote local_commits should match diff right_missing_commits"
                    );

                    // remote items should match left_missing (what they have that we don't)
                    // Note: remote_fragment_summaries vs left_missing_fragments - summaries don't have checkpoints
                    assert_eq!(
                        remote_diff.remote_fragment_summaries.len(),
                        local_diff.left_missing_fragments.len(),
                        "diff_remote remote_fragment_summaries count should match diff left_missing_fragments"
                    );
                    assert_eq!(
                        remote_diff.remote_commits.len(),
                        local_diff.left_missing_commits.len(),
                        "diff_remote remote_commits should match diff left_missing_commits"
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
                                    "fragment {} supports fragment {} in minimized output",
                                    i,
                                    j
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
    }

    mod minimize_tests {
        use alloc::{collections::BTreeSet, vec, vec::Vec};

        use crate::{
            blob::BlobMeta,
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
            let head = digest_with_depth(2, 1);
            let blob_meta = BlobMeta::new(&[1]);
            let fragment = Fragment::new(head, BTreeSet::new(), &[], blob_meta);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 1);
        }

        #[test]
        fn minimize_head_equals_boundary() {
            // Degenerate case: head is also in boundary
            let head = digest_with_depth(2, 1);
            let blob_meta = BlobMeta::new(&[1]);
            let fragment = Fragment::new(head, BTreeSet::from([head]), &[], blob_meta);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // Should still work
            assert_eq!(minimized.fragments().count(), 1);
        }

        #[test]
        fn minimize_head_in_checkpoints() {
            // Head appears in own checkpoints (redundant but valid)
            let head = digest_with_depth(2, 1);
            let boundary = digest_with_depth(1, 100);
            let blob_meta = BlobMeta::new(&[1]);
            let fragment = Fragment::new(head, BTreeSet::from([boundary]), &[head], blob_meta);

            let tree = Sedimentree::new(vec![fragment.clone()], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            assert_eq!(minimized.fragments().count(), 1);
        }

        // ============================================================
        // Invariant Tests
        // ============================================================

        #[test]
        fn minimize_output_subset_of_input() {
            let input_fragments: Vec<Fragment> = (0..10)
                .map(|i| {
                    let boundary = digest_with_depth(1, 100 + i);
                    make_fragment_at_depth(2, i, BTreeSet::from([boundary]), &[])
                })
                .collect();

            let tree = Sedimentree::new(input_fragments.clone(), vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // Every fragment in output must be in input
            for fragment in minimized.fragments() {
                assert!(
                    input_fragments.contains(fragment),
                    "minimized output contains fragment not in input"
                );
            }
        }

        #[test]
        fn minimize_no_mutual_support_in_output() {
            // Create a mix of depths
            let deep_boundary = digest_with_depth(1, 100);
            let shallow_head = digest_with_depth(2, 1);
            let shallow_boundary = digest_with_depth(1, 101);

            let deep = make_fragment_at_depth(
                3,
                1,
                BTreeSet::from([deep_boundary]),
                &[shallow_head, shallow_boundary],
            );
            let shallow = make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);
            let unrelated =
                make_fragment_at_depth(2, 5, BTreeSet::from([digest_with_depth(1, 200)]), &[]);

            let tree = Sedimentree::new(vec![deep, shallow, unrelated], vec![]);
            let minimized = tree.minimize(&CountLeadingZeroBytes);

            // No fragment in output should support another in output
            let fragments = collect_fragments(&minimized);
            for (i, f1) in fragments.iter().enumerate() {
                for (j, f2) in fragments.iter().enumerate() {
                    if i != j {
                        assert!(
                            !f1.supports(f2.summary(), &CountLeadingZeroBytes),
                            "fragment {} supports fragment {} in minimized output",
                            i,
                            j
                        );
                    }
                }
            }
        }

        #[test]
        fn minimize_idempotent() {
            let deep_boundary = digest_with_depth(1, 100);
            let shallow_head = digest_with_depth(2, 1);
            let shallow_boundary = digest_with_depth(1, 101);

            let deep = make_fragment_at_depth(
                3,
                1,
                BTreeSet::from([deep_boundary]),
                &[shallow_head, shallow_boundary],
            );
            let shallow = make_fragment_at_depth(2, 1, BTreeSet::from([shallow_boundary]), &[]);

            let tree = Sedimentree::new(vec![deep, shallow], vec![]);
            let minimized1 = tree.minimize(&CountLeadingZeroBytes);
            let minimized2 = minimized1.minimize(&CountLeadingZeroBytes);

            let fragments1 = collect_fragments(&minimized1);
            let fragments2 = collect_fragments(&minimized2);

            assert_eq!(
                fragments1.len(),
                fragments2.len(),
                "minimize should be idempotent"
            );
            for f in &fragments1 {
                assert!(fragments2.contains(f));
            }
        }
    }
}
