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

use rand::{Rng, SeedableRng, rngs::SmallRng};

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
}
