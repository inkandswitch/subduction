//! Test utilities for sedimentree testing.
//!
//! This module provides shared helpers for constructing test fixtures,
//! including digests with specific depths, fragments, and commit DAGs.
//!
//! Enable with the `test_utils` feature flag.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::{
    blob::BlobMeta, collections::Map, crypto::digest::Digest, fragment::Fragment,
    loose_commit::LooseCommit, sedimentree::Sedimentree,
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
    Digest::from_bytes(bytes)
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
    Digest::from_bytes(bytes)
}

/// Create a random commit digest.
#[must_use]
pub fn random_commit_digest<R: Rng>(rng: &mut R) -> Digest<LooseCommit> {
    let mut bytes = [0u8; 32];
    rng.fill_bytes(&mut bytes);
    Digest::from_bytes(bytes)
}

/// Create a random [`BlobMeta`].
#[must_use]
pub fn random_blob_meta<R: Rng>(rng: &mut R) -> BlobMeta {
    let mut contents = [0u8; 20];
    rng.fill_bytes(&mut contents);
    BlobMeta::new(&contents)
}

/// Create a fragment at a given depth with specified boundary and checkpoints.
#[must_use]
pub fn make_fragment_at_depth(
    depth: u8,
    seed: u8,
    boundary: BTreeSet<Digest<LooseCommit>>,
    checkpoints: &[Digest<LooseCommit>],
) -> Fragment {
    let head = digest_with_depth(depth, seed);
    let blob_meta = BlobMeta::new(&[seed]);
    Fragment::new(head, boundary, checkpoints, blob_meta)
}

/// Create a simple fragment with a single boundary commit.
#[must_use]
pub fn make_simple_fragment(head_depth: u8, head_seed: u8, boundary_seed: u8) -> Fragment {
    let boundary = digest_with_depth(1, boundary_seed);
    make_fragment_at_depth(head_depth, head_seed, BTreeSet::from([boundary]), &[])
}

/// A test graph builder for constructing commit DAGs.
///
/// This provides a declarative way to build commit graphs for testing,
/// with named nodes and explicit parent relationships.
#[derive(Debug, Clone)]
pub struct TestGraph {
    nodes: Map<String, Digest<LooseCommit>>,
    parents: Map<Digest<LooseCommit>, Vec<Digest<LooseCommit>>>,
    blob_metas: Map<Digest<LooseCommit>, BlobMeta>,
}

impl TestGraph {
    /// Create a new test graph from node definitions and edges.
    ///
    /// # Arguments
    ///
    /// * `rng` - Random number generator for hash generation
    /// * `node_info` - List of (name, depth) pairs defining nodes
    /// * `edges` - List of (parent, child) pairs defining edges
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
        let commit_hashes = Self::make_commit_hashes(rng, node_info);
        let mut nodes = Map::new();
        let mut blob_metas = Map::new();

        for (name, hash) in &commit_hashes {
            blob_metas.insert(*hash, random_blob_meta(rng));
            nodes.insert(name.clone(), *hash);
        }

        let mut parents: Map<Digest<LooseCommit>, Vec<Digest<LooseCommit>>> = Map::new();

        for (parent_name, child_name) in edges {
            let Some(child_hash) = nodes.get(*child_name) else {
                panic!("Child node not found: {child_name}");
            };
            let Some(parent_hash) = nodes.get(*parent_name) else {
                panic!("Parent node not found: {parent_name}");
            };
            parents.entry(*child_hash).or_default().push(*parent_hash);
        }

        Self {
            nodes,
            parents,
            blob_metas,
        }
    }

    /// Generate commit hashes for nodes.
    ///
    /// Each node gets a unique hash with the specified number of leading zeros.
    fn make_commit_hashes<R: Rng>(
        rng: &mut R,
        names: &[(&str, usize)],
    ) -> Map<String, Digest<LooseCommit>> {
        let mut commits = Map::new();

        for (name, level) in names {
            #[allow(clippy::cast_possible_truncation)]
            let hash = random_digest_with_depth(rng, *level as u32);
            commits.insert((*name).to_string(), hash);
        }
        commits
    }

    /// Get all commits as [`LooseCommit`] instances.
    #[must_use]
    pub fn commits(&self) -> Vec<LooseCommit> {
        self.nodes
            .values()
            .map(|hash| {
                let parents: BTreeSet<_> = self
                    .parents
                    .get(hash)
                    .map(|p| p.iter().copied().collect())
                    .unwrap_or_default();
                let blob_meta = self
                    .blob_metas
                    .get(hash)
                    .copied()
                    .unwrap_or_else(|| BlobMeta::new(&[]));
                LooseCommit::new(*hash, parents, blob_meta)
            })
            .collect()
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
            .nodes
            .get(node)
            .unwrap_or_else(|| panic!("Node not found: {node}"))
    }

    /// Check if a node exists.
    #[must_use]
    pub fn has_node(&self, node: &str) -> bool {
        self.nodes.contains_key(node)
    }

    /// Get all node names.
    #[must_use]
    pub fn node_names(&self) -> Vec<&str> {
        self.nodes.keys().map(String::as_str).collect()
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

    /// Create a fragment covering from `head_node` to `boundary_node`.
    ///
    /// Optionally include checkpoint nodes.
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
        let blob_meta = self
            .blob_metas
            .get(&head)
            .copied()
            .unwrap_or_else(|| BlobMeta::new(&[]));
        Fragment::new(head, boundary, &checkpoints, blob_meta)
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
            .find(|c| c.digest() == d_hash)
            .ok_or("commit d not found")?;
        assert_eq!(d_commit.parents().len(), 2);

        Ok(())
    }
}
