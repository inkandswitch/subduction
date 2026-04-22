//! Test utilities for sedimentree testing.
//!
//! This module provides shared helpers for constructing test fixtures,
//! including identifiers with specific depths, fragments, and commit DAGs.
//!
//! Enable with the `test_utils` feature flag.

// Test utilities are allowed to panic for clearer test failures
#![allow(clippy::expect_used, clippy::panic)]

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use rand::{Rng, RngCore, SeedableRng, rngs::SmallRng};

use core::convert::Infallible;

use crate::{
    blob::{Blob, BlobMeta},
    collections::{Map, Set},
    commit::CommitStore,
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};

/// Create a [`CommitId`] with a specific number of leading zero bytes.
///
/// The `seed` parameter ensures different identifiers with the same depth
/// produce different values.
///
/// # Example
///
/// ```
/// use sedimentree_core::test_utils::commit_id_with_depth;
///
/// // Create a depth-2 identifier (2 leading zero bytes)
/// let d1 = commit_id_with_depth(2, 1);
/// let d2 = commit_id_with_depth(2, 2);
/// assert_ne!(d1, d2); // Different seeds produce different identifiers
/// ```
#[must_use]
pub fn commit_id_with_depth(leading_zeros: u8, seed: u8) -> CommitId {
    let mut bytes = [0u8; 32];
    // Set leading zeros (bytes already initialized to 0, but explicit for clarity)
    for slot in bytes.iter_mut().take(leading_zeros as usize) {
        *slot = 0;
    }
    // First non-zero byte (ensures exact depth)
    if let Some(slot) = bytes.get_mut(leading_zeros as usize) {
        *slot = 1;
    }
    // Seed for uniqueness
    if let Some(slot) = bytes.get_mut(leading_zeros as usize + 1) {
        *slot = seed;
    }
    CommitId::new(bytes)
}

/// Create a [`CommitId`] with leading zeros using an RNG for randomness.
///
/// Unlike [`commit_id_with_depth`], this uses an RNG for the non-zero bytes,
/// providing more randomness for property-based testing.
#[must_use]
pub fn random_commit_id_with_depth<R: Rng>(rng: &mut R, leading_zeros: u32) -> CommitId {
    let mut bytes: [u8; 32] = rng.r#gen();
    for slot in bytes.iter_mut().take(leading_zeros as usize) {
        *slot = 0;
    }
    // Ensure the byte after zeros is non-zero to get exact depth
    if let Some(slot) = bytes.get_mut(leading_zeros as usize)
        && *slot == 0
    {
        *slot = 1;
    }
    CommitId::new(bytes)
}

/// Create a random [`CommitId`].
#[must_use]
pub fn random_commit_id<R: Rng>(rng: &mut R) -> CommitId {
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    CommitId::new(bytes)
}

/// Create a random [`BlobMeta`].
#[must_use]
pub fn random_blob_meta<R: Rng>(rng: &mut R) -> BlobMeta {
    let mut contents = [0u8; 20];
    rng.fill_bytes(&mut contents);
    let blob = Blob::new(contents.into());
    BlobMeta::new(&blob)
}

/// Create a fragment at a given depth with specified boundary and checkpoints.
#[must_use]
pub fn make_fragment_at_depth(
    depth: u8,
    seed: u8,
    boundary: BTreeSet<CommitId>,
    checkpoints: &[CommitId],
) -> Fragment {
    let sedimentree_id = SedimentreeId::new([seed; 32]);
    let head = commit_id_with_depth(depth, seed);
    let blob = Blob::new(alloc::vec![seed]);
    let blob_meta = BlobMeta::new(&blob);
    Fragment::new(sedimentree_id, head, boundary, checkpoints, blob_meta)
}

/// Create a simple fragment with a single boundary commit.
#[must_use]
pub fn make_simple_fragment(head_depth: u8, head_seed: u8, boundary_seed: u8) -> Fragment {
    let boundary = commit_id_with_depth(1, boundary_seed);
    make_fragment_at_depth(head_depth, head_seed, BTreeSet::from([boundary]), &[])
}

/// A mock depth metric that returns predetermined depths for specific identifiers.
///
/// This allows tests to control which commits are "deep" (fragment-worthy)
/// without relying on hash patterns.
#[derive(Debug, Clone, Default)]
pub struct MockDepthMetric {
    depths: Map<CommitId, Depth>,
}

impl MockDepthMetric {
    /// Create a new empty mock depth metric.
    #[must_use]
    pub fn new() -> Self {
        Self { depths: Map::new() }
    }

    /// Set the depth for a specific identifier.
    pub fn set_depth(&mut self, id: CommitId, depth: Depth) {
        self.depths.insert(id, depth);
    }
}

impl DepthMetric for MockDepthMetric {
    fn to_depth(&self, id: CommitId) -> Depth {
        self.depths.get(&id).copied().unwrap_or(Depth(0))
    }
}

/// A test graph builder for constructing commit DAGs.
///
/// This provides a declarative way to build commit graphs for testing,
/// with named nodes and explicit parent relationships. Nodes are assigned
/// depths via a [`MockDepthMetric`] rather than by manipulating hash patterns.
#[derive(Debug, Clone)]
pub struct TestGraph {
    sedimentree_id: SedimentreeId,
    /// Map from node name to the actual commit
    commits: Map<String, LooseCommit>,
    /// Cached identifiers for fast lookup
    ids: Map<String, CommitId>,
    /// Mock depth metric with predetermined depths
    depth_metric: MockDepthMetric,
}

impl TestGraph {
    /// Create a new test graph from node definitions and edges.
    ///
    /// # Arguments
    ///
    /// * `rng` - Random number generator for blob content generation
    /// * `node_info` - List of (name, depth) pairs defining nodes. Depth is used
    ///   to configure the mock depth metric, not to control actual hash patterns.
    /// * `edges` - List of (parent, child) pairs defining edges (parent → child means
    ///   child has parent as a parent commit)
    ///
    /// # Panics
    ///
    /// Panics if an edge references a node name that doesn't exist in `node_info`.
    ///
    /// # Example
    ///
    /// ```
    /// use rand::{SeedableRng, rngs::SmallRng};
    /// use sedimentree_core::test_utils::TestGraph;
    ///
    /// let mut rng = SmallRng::seed_from_u64(42);
    /// let graph = TestGraph::new(
    ///     &mut rng,
    ///     &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
    ///     &[("a", "b"), ("b", "d"), ("a", "c"), ("c", "d")],
    /// );
    /// ```
    #[allow(clippy::panic)]
    pub fn new<R: Rng>(rng: &mut R, node_info: &[(&str, usize)], edges: &[(&str, &str)]) -> Self {
        // Generate sedimentree ID
        let mut bytes = [0u8; 32];
        rng.fill_bytes(&mut bytes);
        let sedimentree_id = SedimentreeId::new(bytes);

        // Build parent relationships by name first
        let mut parents_by_name: Map<String, Vec<String>> = Map::new();
        for (parent_name, child_name) in edges {
            parents_by_name
                .entry((*child_name).to_string())
                .or_default()
                .push((*parent_name).to_string());
        }

        // Create commits in topological order (nodes with no parents first)
        // For simplicity, we'll create all commits with empty parents first,
        // then update with actual parent identifiers
        let mut commits: Map<String, LooseCommit> = Map::new();
        let mut ids: Map<String, CommitId> = Map::new();
        let mut depth_metric = MockDepthMetric::new();

        // First pass: generate unique CommitIds for each node
        for (name, _depth) in node_info {
            let id_bytes: [u8; 32] = rng.r#gen();
            let commit_id = CommitId::new(id_bytes);
            ids.insert((*name).to_string(), commit_id);
        }

        // Second pass: create commits with correct parents and heads
        for (name, depth) in node_info {
            let parent_names = parents_by_name.get(*name);
            let parents: BTreeSet<CommitId> = parent_names
                .map(|names| {
                    names
                        .iter()
                        .map(|n| {
                            *ids.get(n)
                                .unwrap_or_else(|| panic!("Parent node not found: {n}"))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let blob = Blob::new(rng.r#gen::<[u8; 16]>().into());
            let blob_meta = BlobMeta::new(&blob);
            let commit_id = *ids.get(*name).expect("id should exist");
            let commit = LooseCommit::new(sedimentree_id, commit_id, parents, blob_meta);

            commits.insert((*name).to_string(), commit);

            #[allow(clippy::cast_possible_truncation)]
            depth_metric.set_depth(commit_id, Depth(*depth as u32));
        }

        Self {
            sedimentree_id,
            commits,
            ids,
            depth_metric,
        }
    }

    /// Get all commits as [`LooseCommit`] instances.
    #[must_use]
    pub fn commits(&self) -> Vec<LooseCommit> {
        self.commits.values().cloned().collect()
    }

    /// Get a commit by its node name.
    ///
    /// # Panics
    ///
    /// Panics if the node name doesn't exist.
    #[must_use]
    pub fn get_commit(&self, node: &str) -> &LooseCommit {
        self.commits.get(node).expect("node not found in graph")
    }

    /// Get a subset of commits by their node names.
    ///
    /// # Panics
    ///
    /// Panics if any node name doesn't exist.
    #[must_use]
    pub fn get_commits(&self, nodes: &[&str]) -> Vec<LooseCommit> {
        nodes.iter().map(|n| self.get_commit(n).clone()).collect()
    }

    /// Get the [`CommitId`] for a named node.
    ///
    /// # Panics
    ///
    /// Panics if the node name doesn't exist.
    #[must_use]
    #[allow(clippy::panic)]
    pub fn node_hash(&self, node: &str) -> CommitId {
        *self
            .ids
            .get(node)
            .unwrap_or_else(|| panic!("Node not found: {node}"))
    }

    /// Check if a node exists.
    #[must_use]
    pub fn has_node(&self, node: &str) -> bool {
        self.commits.contains_key(node)
    }

    /// Get all node names.
    #[must_use]
    pub fn node_names(&self) -> Vec<&str> {
        self.commits.keys().map(String::as_str).collect()
    }

    /// Get a reference to the mock depth metric.
    ///
    /// Use this when calling methods that require a depth metric.
    #[must_use]
    pub const fn depth_metric(&self) -> &MockDepthMetric {
        &self.depth_metric
    }

    /// Build a [`Sedimentree`] from this graph with no fragments.
    #[must_use]
    pub fn to_sedimentree(&self) -> Sedimentree {
        Sedimentree::new(vec![], self.commits())
    }

    /// Build a [`Sedimentree`] from this graph with the given fragments.
    #[must_use]
    pub fn to_sedimentree_with_fragments(&self, fragments: Vec<Fragment>) -> Sedimentree {
        Sedimentree::new(fragments, self.commits())
    }

    /// Look up a commit's parent set by identifier.
    ///
    /// Returns `None` if the identifier is not in the graph.
    #[must_use]
    pub fn lookup_parents(&self, id: CommitId) -> Option<Set<CommitId>> {
        self.commits.values().find_map(|c| {
            if c.head() == id {
                Some(c.parents().iter().copied().collect())
            } else {
                None
            }
        })
    }

    /// Create a fragment covering from `head_node` to `boundary_nodes`.
    ///
    /// Optionally include checkpoint nodes.
    ///
    /// # Panics
    ///
    /// Panics if `head_node` or any of `boundary_nodes`/`checkpoint_nodes` are not found in the graph.
    #[must_use]
    pub fn make_fragment(
        &self,
        head_node: &str,
        boundary_nodes: &[&str],
        checkpoint_nodes: &[&str],
    ) -> Fragment {
        let head = self.node_hash(head_node);
        let boundary: BTreeSet<_> = boundary_nodes.iter().map(|n| self.node_hash(n)).collect();
        let checkpoints: Vec<_> = checkpoint_nodes.iter().map(|n| self.node_hash(n)).collect();
        let commit = self
            .commits
            .get(head_node)
            .unwrap_or_else(|| panic!("Node not found: {head_node}"));
        let blob_meta = *commit.blob_meta();
        Fragment::new(self.sedimentree_id, head, boundary, &checkpoints, blob_meta)
    }
}

impl CommitStore<'_> for TestGraph {
    type Node = Set<CommitId>;
    type LookupError = Infallible;

    fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError> {
        Ok(self.lookup_parents(id))
    }
}

/// Create a seeded RNG for deterministic tests.
#[must_use]
pub fn seeded_rng(seed: u64) -> SmallRng {
    SmallRng::seed_from_u64(seed)
}

// ============================================================================
// Seeded helpers for benchmarks
// ============================================================================
//
// The helpers above take `&mut Rng` references — the property-testing convention. For
// benchmarks we often want a pure-seed API (reproducible without threading an RNG through
// every call site). These wrappers construct a `SmallRng` internally.

/// Deterministic [`CommitId`] derived from a u64 seed.
///
/// Equivalent to `random_commit_id(&mut seeded_rng(seed))` but with a shorter call site.
/// Useful for bench generators where threading an RNG is noise.
#[must_use]
pub fn commit_id_from_seed(seed: u64) -> CommitId {
    random_commit_id(&mut seeded_rng(seed))
}

/// Deterministic [`CommitId`] with exactly `leading_zeros` zero bytes.
///
/// Extends [`commit_id_with_depth`] to accept a u64 seed (instead of u8) and `usize` leading
/// zeros (instead of u8). Saturates at 32 leading zeros.
#[must_use]
pub fn commit_id_from_seed_with_leading_zeros(leading_zeros: usize, seed: u64) -> CommitId {
    let zeros = leading_zeros.min(32);
    let mut rng = seeded_rng(seed);
    // Clippy false positive: `zeros <= 32` but u32 is what the helper takes.
    #[allow(clippy::cast_possible_truncation)]
    let zeros_u32 = zeros as u32;
    random_commit_id_with_depth(&mut rng, zeros_u32)
}

/// Deterministic [`SedimentreeId`] derived from a u64 seed.
#[must_use]
pub fn sedimentree_id_from_seed(seed: u64) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    let mut rng = seeded_rng(seed);
    rng.fill_bytes(&mut bytes);
    SedimentreeId::new(bytes)
}

/// Deterministic [`Blob`] of exactly `size` bytes with pseudo-random contents.
#[must_use]
pub fn blob_from_seed(seed: u64, size: usize) -> Blob {
    let mut data = alloc::vec![0u8; size];
    let mut rng = seeded_rng(seed);
    rng.fill_bytes(data.as_mut_slice());
    Blob::new(data)
}

/// Synthetic [`BlobMeta`] with a forged digest — cheaper than hashing a real blob.
///
/// Use this when benches need `BlobMeta` values but don't care about digest integrity
/// (e.g. sedimentree structural benches). For benches that round-trip blobs through the
/// protocol, use [`random_blob_meta`] (which hashes an actual blob) or build a real blob
/// via [`blob_from_seed`] and call [`BlobMeta::new`](crate::blob::BlobMeta::new) on it.
#[must_use]
pub fn blob_meta_from_seed_forged(seed: u64, size_bytes: u64) -> BlobMeta {
    use crate::crypto::digest::Digest;
    let mut bytes = [0u8; 32];
    let mut rng = seeded_rng(seed);
    rng.fill_bytes(&mut bytes);
    BlobMeta::from_digest_size(Digest::force_from_bytes(bytes), size_bytes)
}

/// Synthetic [`LooseCommit`] with the given parents.
///
/// Head, sedimentree id, and blob-meta seeds are derived deterministically from `seed`.
#[must_use]
pub fn synthetic_commit(seed: u64, parents: BTreeSet<CommitId>) -> LooseCommit {
    let sedimentree_id = sedimentree_id_from_seed(seed);
    let head = commit_id_from_seed(seed.wrapping_add(500_000));
    let blob_meta = blob_meta_from_seed_forged(seed.wrapping_add(1_000_000), 1024);
    LooseCommit::new(sedimentree_id, head, parents, blob_meta)
}

/// Synthetic [`Fragment`] with configurable structural complexity.
///
/// | Parameter         | Effect                                                          |
/// |-------------------|-----------------------------------------------------------------|
/// | `head_seed`       | RNG seed; determines the head commit id and all derived seeds  |
/// | `boundary_count`  | Number of boundary commits (predecessors of this fragment)     |
/// | `checkpoint_count`| Number of checkpoints embedded in the fragment                 |
/// | `leading_zeros`   | Depth of the head and boundary commits                          |
///
/// This is the bench-oriented counterpart to [`make_fragment_at_depth`] — same idea, but
/// u64-seeded and with explicit boundary/checkpoint counts.
#[must_use]
pub fn synthetic_fragment(
    head_seed: u64,
    boundary_count: usize,
    checkpoint_count: usize,
    leading_zeros: usize,
) -> Fragment {
    let sedimentree_id = sedimentree_id_from_seed(head_seed);
    let head = commit_id_from_seed_with_leading_zeros(leading_zeros, head_seed);

    let boundary: BTreeSet<CommitId> = (0..boundary_count)
        .map(|i| {
            commit_id_from_seed_with_leading_zeros(
                leading_zeros,
                head_seed.wrapping_add(100).wrapping_add(i as u64),
            )
        })
        .collect();

    let checkpoints: Vec<CommitId> = (0..checkpoint_count)
        .map(|i| commit_id_from_seed(head_seed.wrapping_add(200).wrapping_add(i as u64)))
        .collect();

    let blob_meta = blob_meta_from_seed_forged(head_seed.wrapping_add(300), 4096);
    Fragment::new(sedimentree_id, head, boundary, &checkpoints, blob_meta)
}

// ============================================================================
// Canonical DAG shapes
// ============================================================================
//
// These produce realistically-shaped `LooseCommit` sequences for bench workloads. They share a
// seeded-RNG-free API so bench call sites stay readable.

/// Linear chain: each commit has exactly one parent, the previous commit.
#[must_use]
pub fn linear_chain(count: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(count);
    let mut prev = None;

    for i in 0..count {
        let parents = prev.map(|p| BTreeSet::from([p])).unwrap_or_default();
        let commit = synthetic_commit(base_seed.wrapping_add(i as u64), parents);
        prev = Some(commit.head());
        commits.push(commit);
    }

    commits
}

/// DAG with periodic 2-parent merges.
///
/// Every `merge_frequency`-th commit has two parents (the two most recent heads); other
/// commits have one parent. A sliding window of the 10 most recent heads bounds memory.
#[must_use]
pub fn merge_heavy_dag(
    count: usize,
    merge_frequency: usize,
    base_seed: u64,
) -> Vec<LooseCommit> {
    const RECENT_WINDOW: usize = 10;

    let merge_frequency = merge_frequency.max(1);
    let mut commits = Vec::with_capacity(count);
    let mut recent: Vec<CommitId> = Vec::with_capacity(RECENT_WINDOW);

    for i in 0..count {
        let parents = if i == 0 {
            BTreeSet::new()
        } else if i.is_multiple_of(merge_frequency) && recent.len() >= 2 {
            let mut last_two = recent.iter().rev().take(2).copied();
            let mut set = BTreeSet::new();

            if let Some(p) = last_two.next() {
                set.insert(p);
            }
            if let Some(p) = last_two.next() {
                set.insert(p);
            }

            set
        } else {
            recent
                .last()
                .copied()
                .map(|p| BTreeSet::from([p]))
                .unwrap_or_default()
        };

        let commit = synthetic_commit(base_seed.wrapping_add(i as u64), parents);
        recent.push(commit.head());

        if recent.len() > RECENT_WINDOW {
            recent.remove(0);
        }

        commits.push(commit);
    }

    commits
}

/// Wide DAG: `width` independent branches, each `depth_per_branch` commits long.
///
/// Useful for simulating fan-out from many concurrent writers.
#[must_use]
pub fn wide_dag(width: usize, depth_per_branch: usize, base_seed: u64) -> Vec<LooseCommit> {
    let mut commits = Vec::with_capacity(width.saturating_mul(depth_per_branch));

    for branch in 0..width {
        let branch_seed = base_seed.wrapping_add((branch as u64).wrapping_mul(10_000));
        let mut prev = None;

        for i in 0..depth_per_branch {
            let parents = prev.map(|p| BTreeSet::from([p])).unwrap_or_default();
            let commit = synthetic_commit(branch_seed.wrapping_add(i as u64), parents);
            prev = Some(commit.head());
            commits.push(commit);
        }
    }

    commits
}

// ============================================================================
// Composed trees
// ============================================================================

/// Build a [`Sedimentree`] with the given counts of fragments and loose commits.
///
/// Fragments cycle through depths 0, 1, 2 (wrapping) so the tree exercises the full
/// depth-metric range. Commits are arranged as a single linear chain.
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

/// Two [`Sedimentree`]s with a controlled overlap, useful for diff / merge benches.
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

/// An arbitrary DAG of fragments and loose commits with real dependency
/// edges.
///
/// Each fragment's boundary references the head of a previously generated
/// fragment (or nothing, for root fragments). Loose commits' parents may
/// reference fragment heads.
///
/// Use with `bolero::check!().with_arbitrary::<ArbitraryDag>()`.
#[cfg(feature = "arbitrary")]
#[derive(Debug)]
pub struct ArbitraryDag {
    /// The generated sedimentree.
    pub tree: Sedimentree,
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for ArbitraryDag {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let sid = SedimentreeId::arbitrary(u)?;
        let n_frags: usize = u.int_in_range(0..=8)?;
        let n_loose: usize = u.int_in_range(0..=5)?;

        let mut fragments = Vec::with_capacity(n_frags);
        let mut heads: Vec<CommitId> = Vec::with_capacity(n_frags);

        // Use a single RNG seeded once, so each head is unique by construction.
        let mut rng = SmallRng::seed_from_u64(u.arbitrary()?);

        for _ in 0..n_frags {
            let head = random_commit_id(&mut rng);
            let mut boundary = BTreeSet::new();

            // Each fragment references 0..=2 previous fragment heads
            let n_deps = u.int_in_range(0..=heads.len().min(2))?;
            let mut available: Vec<usize> = (0..heads.len()).collect();
            for _ in 0..n_deps {
                if available.is_empty() {
                    break;
                }
                let pick = u.choose_index(available.len())?;
                #[allow(clippy::indexing_slicing)]
                let idx = available.remove(pick);
                #[allow(clippy::indexing_slicing)]
                boundary.insert(heads[idx]);
            }

            let blob_meta = BlobMeta::arbitrary(u)?;
            fragments.push(Fragment::new(sid, head, boundary, &[], blob_meta));
            heads.push(head);
        }

        let mut commits = Vec::with_capacity(n_loose);
        for _ in 0..n_loose {
            let mut parents = BTreeSet::new();
            // Loose commit may reference 0..=1 fragment heads
            if !heads.is_empty() && u.arbitrary::<bool>()? {
                let idx = u.choose_index(heads.len())?;
                #[allow(clippy::indexing_slicing)]
                parents.insert(heads[idx]);
            }
            let blob_meta = BlobMeta::arbitrary(u)?;
            let commit_id = random_commit_id(&mut rng);
            commits.push(LooseCommit::new(sid, commit_id, parents, blob_meta));
        }

        Ok(ArbitraryDag {
            tree: Sedimentree::new(fragments, commits),
        })
    }
}

/// An arbitrary sedimentree whose fragment dependency graph contains at
/// least one cycle.
///
/// Generates a linear chain of fragments, then adds a back-edge from the
/// first fragment to the last, creating a cycle. This models the kind of
/// corrupt metadata a malicious peer might send.
///
/// Use with `bolero::check!().with_arbitrary::<CyclicGraph>()`.
#[cfg(feature = "arbitrary")]
#[derive(Debug)]
pub struct CyclicGraph {
    /// The generated sedimentree (contains a cycle).
    pub tree: Sedimentree,
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for CyclicGraph {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let sid = SedimentreeId::arbitrary(u)?;

        // Generate 2..=6 fragments in a valid chain first.
        let n_frags: usize = u.int_in_range(2..=6)?;
        let mut heads = Vec::with_capacity(n_frags);
        let mut fragments = Vec::with_capacity(n_frags);

        // Use a single RNG seeded once, so each head is unique by construction.
        let mut rng = SmallRng::seed_from_u64(u.arbitrary()?);

        for _ in 0..n_frags {
            let head = random_commit_id(&mut rng);
            let mut boundary = BTreeSet::new();
            // Reference the previous fragment's head (linear chain)
            if let Some(&prev) = heads.last() {
                boundary.insert(prev);
            }
            let blob_meta = BlobMeta::arbitrary(u)?;
            fragments.push(Fragment::new(sid, head, boundary, &[], blob_meta));
            heads.push(head);
        }

        // Introduce a cycle: make the first fragment also depend on the
        // last fragment's head (back-edge).
        #[allow(clippy::indexing_slicing)]
        {
            let last_head = *heads.last().expect("at least 2 fragments");
            let mut new_boundary = fragments[0].boundary().clone();
            new_boundary.insert(last_head);
            let blob_meta = BlobMeta::arbitrary(u)?;
            fragments[0] = Fragment::new(sid, heads[0], new_boundary, &[], blob_meta);
        }

        Ok(CyclicGraph {
            tree: Sedimentree::new(fragments, vec![]),
        })
    }
}

// ============================================================================
// Realistic depth distribution (post-#122)
// ============================================================================
//
// Under `CountLeadingZeroBytes`, a commit's depth is the number of leading zero bytes in its
// id. For uniformly-distributed ids (the natural output of BLAKE3), the probability of depth
// ≥ d is `256^-d`. Since PR #122, any commit with nonzero depth is a fragment boundary — so
// the realistic distribution over _fragments_ is geometric:
//
// | Depth | P(depth = d)              | Expected share of fragments |
// |-------|---------------------------|----------------------------|
// | 1     | 1/256 · (1 - 1/256) ≈ 1/256 | ~99.6%                     |
// | 2     | 1/256²                    | ~0.39% (1 in 256)          |
// | 3+    | < 1/256³                  | negligible                 |
//
// Pre-#122 fragment generators (e.g. `automerge_sedimentree/benches/egwalker.rs`) modelled
// _P(depth = 0)_ ≈ 255/256, which is wrong for fragments: a depth-0 commit is a loose commit,
// not a boundary. Benches using those generators measure an off-distribution workload.
//
// The helpers below produce the correct post-#122 distribution.

/// Sample a fragment depth from the realistic post-#122 geometric distribution.
///
/// Returns a depth in `1..=max_depth` (inclusive on both ends). Each step deeper is 1/256×
/// less likely than the previous one; `max_depth` of 4 covers roughly 1 - 256⁻⁴ ≈ 99.9999998%
/// of the distribution. Values above 4 are very rare; benches are free to clamp.
#[must_use]
pub fn realistic_fragment_depth<R: Rng>(rng: &mut R, max_depth: u32) -> u32 {
    let max_depth = max_depth.max(1);
    let mut depth: u32 = 1;

    // Each iteration extends to the next stratum with probability 1/256.
    while depth < max_depth {
        let extend: u8 = rng.r#gen();

        if extend == 0 {
            depth += 1;
        } else {
            break;
        }
    }

    depth
}

/// Generate a fragment whose head depth is sampled from the realistic distribution.
///
/// Unlike [`synthetic_fragment`] (which takes `leading_zeros` as an explicit parameter), this
/// helper samples the depth from [`realistic_fragment_depth`] before building the fragment.
/// Boundary commits are sampled independently at the same depth so the `supports_range`
/// relationships make sense.
#[must_use]
pub fn realistic_fragment<R: Rng>(
    rng: &mut R,
    boundary_count: usize,
    checkpoint_count: usize,
    max_depth: u32,
) -> Fragment {
    // Derive a stable "head seed" from a u64 drawn from the RNG so the downstream
    // `synthetic_fragment` construction is reproducible per-draw.
    let head_seed: u64 = rng.r#gen();

    // Usage keeps the u32 depth inline with the rest of the `random_commit_id_with_depth`
    // API; cast to `usize` only at the `synthetic_fragment` boundary.
    let depth = realistic_fragment_depth(rng, max_depth);

    synthetic_fragment(head_seed, boundary_count, checkpoint_count, depth as usize)
}

/// Generate `count` fragments whose depths follow the realistic distribution.
///
/// Ordering within the returned `Vec` is arbitrary; benches that need ordered input should
/// sort by head id or depth explicitly.
#[must_use]
pub fn realistic_fragments(count: usize, base_seed: u64, max_depth: u32) -> Vec<Fragment> {
    let mut rng = seeded_rng(base_seed);
    (0..count)
        .map(|_| realistic_fragment(&mut rng, 2, 5, max_depth))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

    #[test]
    fn commit_id_with_depth_produces_correct_leading_zeros() {
        let d = commit_id_with_depth(3, 1);
        let bytes = d.as_bytes();
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_ne!(bytes[3], 0);
    }

    #[test]
    fn commit_id_with_depth_different_seeds_differ() {
        let d1 = commit_id_with_depth(2, 1);
        let d2 = commit_id_with_depth(2, 2);
        assert_ne!(d1, d2);
    }

    #[test]
    fn test_graph_basic_construction() {
        let mut rng = seeded_rng(42);
        let graph = TestGraph::new(&mut rng, &[("a", 2), ("b", 0)], &[("a", "b")]);

        assert!(graph.has_node("a"));
        assert!(graph.has_node("b"));
        assert!(!graph.has_node("c"));

        let commits = graph.commits();
        assert_eq!(commits.len(), 2);
    }

    #[test]
    fn test_graph_diamond() -> TestResult {
        let mut rng = seeded_rng(42);
        let graph = TestGraph::new(
            &mut rng,
            &[("a", 2), ("b", 0), ("c", 0), ("d", 2)],
            &[("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")],
        );

        let commits = graph.commits();
        assert_eq!(commits.len(), 4);

        // Find commit d and verify it has two parents
        let d_id = graph.node_hash("d");
        let d_commit = commits
            .iter()
            .find(|c| c.head() == d_id)
            .ok_or("commit d not found")?;
        assert_eq!(d_commit.parents().len(), 2);

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Seeded helpers for benchmarks
    // ------------------------------------------------------------------------

    #[test]
    fn commit_id_from_seed_is_deterministic() {
        assert_eq!(commit_id_from_seed(42), commit_id_from_seed(42));
        assert_ne!(commit_id_from_seed(42), commit_id_from_seed(43));
    }

    #[test]
    fn commit_id_from_seed_with_leading_zeros_respects_zero_prefix() {
        for zeros in 0..=6 {
            for seed in 0..4 {
                let id = commit_id_from_seed_with_leading_zeros(zeros, seed);
                let bytes = id.as_bytes();
                for (idx, byte) in bytes.iter().enumerate().take(zeros) {
                    assert_eq!(*byte, 0, "byte {idx} should be zero for zeros={zeros}");
                }
            }
        }
    }

    #[test]
    fn blob_from_seed_length_matches() {
        for size in [0, 1, 64, 4096] {
            assert_eq!(blob_from_seed(0, size).as_slice().len(), size);
        }
    }

    #[test]
    fn linear_chain_is_actually_linear() {
        let commits = linear_chain(10, 0);
        for pair in commits.windows(2) {
            if let [prev, curr] = pair {
                assert_eq!(curr.parents().len(), 1);
                assert!(curr.parents().contains(&prev.head()));
            }
        }
    }

    #[test]
    fn merge_heavy_dag_produces_some_merges() {
        let commits = merge_heavy_dag(50, 5, 0);
        let merges = commits.iter().filter(|c| c.parents().len() == 2).count();
        assert!(merges > 0);
    }

    #[test]
    fn wide_dag_produces_width_times_depth() {
        let commits = wide_dag(5, 10, 0);
        assert_eq!(commits.len(), 50);
    }

    #[test]
    fn synthetic_sedimentree_builds() {
        let _tree = synthetic_sedimentree(8, 32, 0);
    }

    #[test]
    fn overlapping_pair_builds() {
        let (_a, _b) = overlapping_sedimentrees(4, 3, 8, 5, 0);
    }

    // ------------------------------------------------------------------------
    // Realistic depth distribution
    // ------------------------------------------------------------------------

    #[test]
    fn realistic_depth_never_returns_zero() {
        let mut rng = seeded_rng(42);
        for _ in 0..10_000 {
            let d = realistic_fragment_depth(&mut rng, 4);
            assert!(d >= 1, "depth-0 is not a fragment boundary post-#122");
            assert!(d <= 4, "exceeded max_depth");
        }
    }

    #[test]
    fn realistic_depth_distribution_skews_to_one() {
        // With 100_000 samples and max_depth = 3 the expected depth-1 share is ~99.6%. Use a
        // wide tolerance (> 95%) to keep the test robust against RNG chance.
        let mut rng = seeded_rng(7);
        let samples = 100_000;
        let mut depth_one = 0_usize;

        for _ in 0..samples {
            if realistic_fragment_depth(&mut rng, 3) == 1 {
                depth_one += 1;
            }
        }

        // `samples` is 100_000 and `depth_one ≤ samples`, both comfortably below f64's
        // mantissa limit of 2^52. Use `From` to satisfy `clippy::cast-lossless`; the
        // `u32`-from-`usize` step is bounded by `samples` (100k < u32::MAX).
        let samples_u32 = u32::try_from(samples).expect("samples fits in u32");
        let depth_one_u32 = u32::try_from(depth_one).expect("depth_one fits in u32");
        let ratio = f64::from(depth_one_u32) / f64::from(samples_u32);
        assert!(
            ratio > 0.95 && ratio < 1.0,
            "depth-1 share should be ~99.6%, got {ratio}"
        );
    }

    #[test]
    fn realistic_depth_respects_max() {
        // max_depth = 1 should always return 1.
        let mut rng = seeded_rng(0);
        for _ in 0..100 {
            assert_eq!(realistic_fragment_depth(&mut rng, 1), 1);
        }
    }

    #[test]
    fn realistic_fragments_produces_requested_count() {
        let frags = realistic_fragments(64, 123, 3);
        assert_eq!(frags.len(), 64);
    }
}
