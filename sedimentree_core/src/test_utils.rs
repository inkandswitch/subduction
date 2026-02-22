//! Test utilities for sedimentree testing.
//!
//! This module provides shared helpers for constructing test fixtures,
//! including digests with specific depths, fragments, and commit DAGs.
//!
//! Enable with the `test_utils` feature flag.

// Test utilities are allowed to panic for clearer test failures
#![allow(clippy::panic)]

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::{
    blob::{Blob, BlobMeta},
    collections::Map,
    crypto::digest::Digest,
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
};

/// Create a digest with a specific number of leading zero bytes.
///
/// The `seed` parameter ensures different digests with the same depth
/// produce different values.
///
/// # Example
///
/// ```
/// use sedimentree_core::test_utils::digest_with_depth;
///
/// // Create a depth-2 digest (2 leading zero bytes)
/// let d1 = digest_with_depth(2, 1);
/// let d2 = digest_with_depth(2, 2);
/// assert_ne!(d1, d2); // Different seeds produce different digests
/// ```
#[must_use]
pub fn digest_with_depth(leading_zeros: u8, seed: u8) -> Digest<LooseCommit> {
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
    Digest::force_from_bytes(bytes)
}

/// Create a digest with leading zeros using an RNG for randomness.
///
/// Unlike [`digest_with_depth`], this uses an RNG for the non-zero bytes,
/// providing more randomness for property-based testing.
#[must_use]
pub fn random_digest_with_depth<R: Rng>(rng: &mut R, leading_zeros: u32) -> Digest<LooseCommit> {
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
    Digest::force_from_bytes(bytes)
}

/// Create a random commit digest.
#[must_use]
pub fn random_commit_digest<R: Rng>(rng: &mut R) -> Digest<LooseCommit> {
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    Digest::force_from_bytes(bytes)
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
    boundary: BTreeSet<Digest<LooseCommit>>,
    checkpoints: &[Digest<LooseCommit>],
) -> Fragment {
    let sedimentree_id = SedimentreeId::new([seed; 32]);
    let head = digest_with_depth(depth, seed);
    let blob = Blob::new(alloc::vec![seed]);
    let blob_meta = BlobMeta::new(&blob);
    Fragment::new(sedimentree_id, head, boundary, checkpoints, blob_meta)
}

/// Create a simple fragment with a single boundary commit.
#[must_use]
pub fn make_simple_fragment(head_depth: u8, head_seed: u8, boundary_seed: u8) -> Fragment {
    let boundary = digest_with_depth(1, boundary_seed);
    make_fragment_at_depth(head_depth, head_seed, BTreeSet::from([boundary]), &[])
}

/// A mock depth metric that returns predetermined depths for specific digests.
///
/// This allows tests to control which commits are "deep" (fragment-worthy)
/// without relying on hash patterns.
#[derive(Debug, Clone, Default)]
pub struct MockDepthMetric {
    depths: Map<Digest<LooseCommit>, Depth>,
}

impl MockDepthMetric {
    /// Create a new empty mock depth metric.
    #[must_use]
    pub fn new() -> Self {
        Self { depths: Map::new() }
    }

    /// Set the depth for a specific digest.
    pub fn set_depth(&mut self, digest: Digest<LooseCommit>, depth: Depth) {
        self.depths.insert(digest, depth);
    }
}

impl DepthMetric for MockDepthMetric {
    fn to_depth(&self, digest: Digest<LooseCommit>) -> Depth {
        self.depths.get(&digest).copied().unwrap_or(Depth(0))
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
    /// Cached digests for fast lookup
    digests: Map<String, Digest<LooseCommit>>,
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
        // then update with actual parent digests
        let mut commits: Map<String, LooseCommit> = Map::new();
        let mut digests: Map<String, Digest<LooseCommit>> = Map::new();
        let mut depth_metric = MockDepthMetric::new();

        // First pass: create placeholder digests by creating commits without parents
        // We need to do this because parent digests depend on the commits themselves
        for (name, _depth) in node_info {
            let blob = Blob::new(rng.r#gen::<[u8; 16]>().into());
            let blob_meta = BlobMeta::new(&blob);
            let commit = LooseCommit::new(sedimentree_id, BTreeSet::new(), blob_meta);
            let digest = Digest::hash(&commit);
            commits.insert((*name).to_string(), commit);
            digests.insert((*name).to_string(), digest);
        }

        // Second pass: recreate commits with correct parents
        for (name, depth) in node_info {
            let parent_names = parents_by_name.get(*name);
            let parents: BTreeSet<Digest<LooseCommit>> = parent_names
                .map(|names| {
                    names
                        .iter()
                        .map(|n| {
                            *digests
                                .get(n)
                                .unwrap_or_else(|| panic!("Parent node not found: {n}"))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let blob = Blob::new(rng.r#gen::<[u8; 16]>().into());
            let blob_meta = BlobMeta::new(&blob);
            let commit = LooseCommit::new(sedimentree_id, parents, blob_meta);
            let digest = Digest::hash(&commit);

            commits.insert((*name).to_string(), commit);
            digests.insert((*name).to_string(), digest);

            #[allow(clippy::cast_possible_truncation)]
            depth_metric.set_depth(digest, Depth(*depth as u32));
        }

        Self {
            sedimentree_id,
            commits,
            digests,
            depth_metric,
        }
    }

    /// Get all commits as [`LooseCommit`] instances.
    #[must_use]
    pub fn commits(&self) -> Vec<LooseCommit> {
        self.commits.values().cloned().collect()
    }

    /// Get the digest for a named node.
    ///
    /// # Panics
    ///
    /// Panics if the node name doesn't exist.
    #[must_use]
    #[allow(clippy::panic)]
    pub fn node_hash(&self, node: &str) -> Digest<LooseCommit> {
        *self
            .digests
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

/// Create a seeded RNG for deterministic tests.
#[must_use]
pub fn seeded_rng(seed: u64) -> SmallRng {
    SmallRng::seed_from_u64(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

    #[test]
    fn digest_with_depth_produces_correct_leading_zeros() {
        let d = digest_with_depth(3, 1);
        let bytes = d.as_bytes();
        assert_eq!(bytes[0], 0);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 0);
        assert_ne!(bytes[3], 0);
    }

    #[test]
    fn digest_with_depth_different_seeds_differ() {
        let d1 = digest_with_depth(2, 1);
        let d2 = digest_with_depth(2, 2);
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
        let d_hash = graph.node_hash("d");
        let d_commit = commits
            .iter()
            .find(|c| Digest::hash(*c) == d_hash)
            .ok_or("commit d not found")?;
        assert_eq!(d_commit.parents().len(), 2);

        Ok(())
    }
}
