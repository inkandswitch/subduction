//! Internal commit DAG representation for Sedimentree operations.
//!
//! This module provides an adjacency representation of a commit DAG,
//! used internally for operations like [`Sedimentree::minimize`] and [`Sedimentree::heads`].
//! It is not part of the public API.
//!
//! [`Sedimentree::minimize`]: super::Sedimentree::minimize
//! [`Sedimentree::heads`]: super::Sedimentree::heads

use alloc::{vec, vec::Vec};

use crate::{
    collections::{Map, Set},
    crypto::digest::Digest,
    depth::{DepthMetric, MAX_STRATA_DEPTH},
    fragment::{Fragment, checkpoint::Checkpoint},
    loose_commit::LooseCommit,
};

// An adjacency list based representation of a commit DAG except that we use indexes into the
// `nodes` and `edges` vectors instead of pointers in order to please the borrow checker.
#[derive(Debug, Clone)]
pub(crate) struct CommitDag {
    nodes: Vec<Node>,
    node_map: Map<Digest<LooseCommit>, NodeIdx>,
    edges: Vec<Edge>,
}

#[derive(Debug, Clone)]
struct Node {
    hash: Digest<LooseCommit>,
    parents: Option<EdgeIdx>,
    children: Option<EdgeIdx>,
}

#[derive(Debug, Clone)]
struct Edge {
    source: NodeIdx,
    next: Option<EdgeIdx>,
}

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct NodeIdx(usize);

#[derive(Debug, Clone, Copy)]
struct EdgeIdx(usize);

impl CommitDag {
    pub(crate) fn from_commits<'a, I: Iterator<Item = &'a LooseCommit> + Clone>(
        commits: I,
    ) -> Self {
        let nodes = commits
            .clone()
            .map(|c| Node {
                hash: c.digest(),
                parents: None,
                children: None,
            })
            .collect::<Vec<_>>();
        let node_map = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| (node.hash, NodeIdx(idx)))
            .collect::<Map<_, _>>();

        let mut dag = CommitDag {
            nodes,
            node_map,
            edges: Vec::new(),
        };

        for commit in commits {
            #[allow(clippy::expect_used)]
            let child_idx = *dag
                .node_map
                .get(&commit.digest())
                .expect("commit digest not in node_map");

            for parent in commit.parents() {
                if let Some(parent) = dag.node_map.get(parent) {
                    dag.add_edge(*parent, child_idx);
                }
            }
        }
        dag
    }

    fn add_edge(&mut self, source: NodeIdx, target: NodeIdx) {
        // Add an edge in the child node
        let new_edge_idx = EdgeIdx(self.edges.len());
        let new_edge = Edge { source, next: None };
        self.edges.push(new_edge);

        #[allow(clippy::expect_used)]
        let target_node = self
            .nodes
            .get_mut(target.0)
            .expect("NodeId not in self.nodes");

        if let Some(edge_idx) = target_node.parents {
            #[allow(clippy::expect_used)]
            let mut edge = self
                .edges
                .get_mut(edge_idx.0)
                .expect("NodeId not in self.nodes");

            #[allow(clippy::expect_used)]
            while let Some(next) = edge.next {
                edge = self
                    .edges
                    .get_mut(next.0)
                    .expect("NodeId not in self.nodes");
            }
            edge.next = Some(new_edge_idx);
        } else {
            target_node.parents = Some(new_edge_idx);
        }

        // Now add an edge in the parent node
        let new_edge_idx = EdgeIdx(self.edges.len());
        let new_edge = Edge { source, next: None };
        self.edges.push(new_edge);

        #[allow(clippy::expect_used)]
        let source_node = self
            .nodes
            .get_mut(source.0)
            .expect("NodeId not in self.nodes");
        if let Some(edge_idx) = source_node.children {
            #[allow(clippy::expect_used)]
            let mut edge = self
                .edges
                .get_mut(edge_idx.0)
                .expect("NodeId not in self.nodes");

            #[allow(clippy::expect_used)]
            while let Some(next) = edge.next {
                edge = self
                    .edges
                    .get_mut(next.0)
                    .expect("next NodeId not in self.nodes");
            }
            edge.next = Some(new_edge_idx);
        } else {
            source_node.children = Some(new_edge_idx);
        }
    }

    pub(crate) fn simplify<S: DepthMetric>(&self, fragments: &[Fragment], strategy: &S) -> Self {
        // The work here is to identify which parts of a commit DAG can be
        // discarded based on the strata we have. This is a little bit fiddly.
        // Imagine this graph:
        //
        //        ┌─┐
        //        │A│
        //   ┌────┴─┴────┐
        //   │           │
        //   │           │
        //  ┌▼┐         ┌▼┐          ┌─┐
        //  │B│         │C│          │D│
        //  └┬┘         └┬┘          └┬┘
        //   │           │            │
        //   │           └─────┬──────┘
        //  ┌▼┐                │
        //  │I│               ┌▼┐
        //  └─┘               │E│
        //                    └┬┘
        //                     │
        //                     │
        //                    ┌▼┐
        //               ┌────┤F├────┐
        //               │    └─┘    │
        //               │           │
        //               │           │
        //               │           │
        //              ┌▼┐         ┌▼┐
        //              │G│         │H│
        //              └─┘         └─┘
        //
        // Let's say we have a checkpoint commit at F, D, I, and A and we have
        // strata containing A and F, which commits can we discard? Well, we
        // can't discard G or H because they are not in any blocks. We can't
        // discard I or B because they are not in any blocks that we have strata
        // for.
        //
        // The invariant we must maintain is that we always have a path from
        // some commit back to the last checkpoint commit (a commit with two
        // leading zeros). How do we identify the commits that can be discarded?
        //
        // If we start from the heads of the graph then we can walk up the graph
        // in a (reverse) depth first topological sort. First, this allows us to
        // easily identify which commits are not in a block (a range of commits
        // bounded by checkpoint commits) - it's all the commits which we
        // encounter in this traversal before we have ever encountered a
        // checkpoint commit. This same technique also allows us to identify all
        // the blocks in the graph and which commits are in each block (note
        // that a commit can be in multiple blocks).
        //
        // Now, which commits can be discarded? It is any commit which is only
        // in a block which is covered by at least one stratum in the tree.

        // Identify blocks by their end hash and store a mapping from commit hash to block end hash
        let mut commits_to_blocks = Map::new();
        let mut blockless_commits = Set::new();

        let mut tips = self.tips().collect::<Vec<_>>();
        tips.sort_by_key(|idx| {
            #[allow(clippy::expect_used)]
            self.nodes
                .get(idx.0)
                .expect("NodeId not in self.nodes")
                .hash
        });

        for tip in tips {
            let mut block: Option<(Digest<LooseCommit>, Vec<Digest<LooseCommit>>)> = None;
            for hash in self.reverse_topo(tip) {
                let depth = strategy.to_depth(hash);
                if depth >= MAX_STRATA_DEPTH {
                    // We're in a block and we just found a checkpoint, this must be the start hash
                    // for the block we're in. Flush the current block and start a new one.
                    if let Some((block, commits)) = block.take() {
                        for commit in commits {
                            blockless_commits.remove(&commit);
                            commits_to_blocks
                                .entry(commit)
                                .or_insert_with(Vec::new)
                                .push(block);
                        }
                    }
                    block = Some((hash, vec![hash]));
                }
                if let Some((_, commits)) = &mut block {
                    if depth < MAX_STRATA_DEPTH {
                        commits.push(hash);
                    }
                } else if !commits_to_blocks.contains_key(&hash) && depth < MAX_STRATA_DEPTH {
                    blockless_commits.insert(hash);
                }
            }
            // We never found a start hash for this block, so the start must be the root hash
            if let Some((end, commits)) = block.take() {
                commits_to_blocks
                    .entry(end)
                    .or_insert_with(Vec::new)
                    .push(end);
                for commit in commits {
                    commits_to_blocks
                        .entry(commit)
                        .or_insert_with(Vec::new)
                        .push(end);
                }
            }
        }

        // The commits we can discard are the ones where the blocks they are in are all supported
        let remaining_commits = commits_to_blocks
            .into_iter()
            .filter_map(|(commit, blocks)| {
                if blocks
                    .iter()
                    .all(|&b| fragments.iter().any(|s| s.supports_block(b)))
                {
                    None
                } else {
                    Some(commit)
                }
            })
            .chain(blockless_commits)
            .collect::<Vec<_>>();

        let nodes = remaining_commits
            .iter()
            .map(|&c| Node {
                hash: c,
                parents: None,
                children: None,
            })
            .collect::<Vec<_>>();
        let node_map = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| (node.hash, NodeIdx(idx)))
            .collect::<Map<_, _>>();

        let mut dag = CommitDag {
            nodes,
            node_map,
            edges: Vec::new(),
        };

        for (child_idx, commit) in remaining_commits.into_iter().enumerate() {
            for parent in self.parents_of_hash(commit) {
                if let Some(parent) = dag.node_map.get(&parent) {
                    dag.add_edge(*parent, NodeIdx(child_idx));
                }
            }
        }
        dag
    }

    fn tips(&self) -> impl Iterator<Item = NodeIdx> + '_ {
        self.nodes.iter().enumerate().filter_map(|(idx, node)| {
            if node.children.is_none() {
                Some(NodeIdx(idx))
            } else {
                None
            }
        })
    }

    fn parents(&self, node: NodeIdx) -> impl Iterator<Item = NodeIdx> + '_ {
        Parents::new(self, node)
    }

    fn parents_of_hash(
        &self,
        hash: Digest<LooseCommit>,
    ) -> impl Iterator<Item = Digest<LooseCommit>> + '_ {
        self.node_map
            .get(&hash)
            .map(|idx| {
                self.parents(*idx).map(|i| {
                    #[allow(clippy::expect_used)]
                    self.nodes
                        .get(i.0)
                        .expect("nodeId wasn't in self.nodes")
                        .hash
                })
            })
            .into_iter()
            .flatten()
    }

    fn reverse_topo(&self, start: NodeIdx) -> impl Iterator<Item = Digest<LooseCommit>> + '_ {
        ReverseTopo::new(self, start)
    }

    pub(crate) fn contains_commit(&self, commit: &Digest<LooseCommit>) -> bool {
        self.node_map.contains_key(commit)
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = Digest<LooseCommit>> + '_ {
        self.nodes.iter().filter_map(|node| {
            if node.children.is_none() {
                Some(node.hash)
            } else {
                None
            }
        })
    }

    /// All the commit hashes in this dag plus the stratum in the order in which they should
    /// be bundled into strata
    pub(crate) fn canonical_sequence<M: DepthMetric>(
        &self,
        fragments: &[&Fragment],
        hash_metric: &M,
    ) -> Vec<Digest<LooseCommit>> {
        // Pre-index: map boundary commits → fragments (deepest first)
        let mut fragments_by_boundary: Map<Digest<LooseCommit>, Vec<&&Fragment>> = Map::new();
        for fragment in fragments {
            for end in fragment.boundary() {
                fragments_by_boundary
                    .entry(*end)
                    .or_default()
                    .push(fragment);
            }
        }

        // Pre-index: reverse map for checkpoint resolution
        let checkpoint_to_digest: Map<Checkpoint, Digest<LooseCommit>> = self
            .nodes
            .iter()
            .map(|node| (Checkpoint::new(node.hash), node.hash))
            .collect();

        // Find the tips: heads of the commit DAG + fragment boundary commits
        // not contained in the commit DAG
        let mut boundary = Vec::new();
        for fragment in fragments {
            for end in fragment.boundary() {
                if !self.contains_commit(end) {
                    boundary.push(*end);
                }
            }
        }
        let mut heads = self.heads().chain(boundary).collect::<Vec<_>>();
        heads.sort();

        // Collect all commits via DFS from each head. When we reach a commit
        // that's in a fragment, resolve its checkpoints via the reverse map.
        let mut result = Vec::new();
        let mut visited = Set::new();

        for head in heads {
            let mut stack = vec![head];
            while let Some(commit) = stack.pop() {
                if visited.contains(&commit) {
                    continue;
                }
                visited.insert(commit);
                result.push(commit);

                if let Some(idx) = self.node_map.get(&commit) {
                    let mut parents = self
                        .parents(*idx)
                        .map(|i| {
                            #[allow(clippy::expect_used)]
                            self.nodes.get(i.0).expect("node is not in self.nodes").hash
                        })
                        .collect::<Vec<_>>();
                    parents.sort();
                    stack.extend(parents);
                } else if let Some(supporting) = fragments_by_boundary.get(&commit)
                    && let Some(fragment) = supporting.iter().max_by_key(|s| s.depth(hash_metric))
                {
                    for checkpoint in fragment.checkpoints() {
                        if let Some(full_digest) = checkpoint_to_digest.get(checkpoint) {
                            stack.push(*full_digest);
                        }
                    }
                    stack.push(fragment.head());
                }
            }
        }

        result
    }

    #[cfg(test)]
    fn commit_hashes(&self) -> impl Iterator<Item = Digest<LooseCommit>> + '_ {
        self.nodes.iter().map(|node| node.hash)
    }
}

struct ReverseTopo<'a> {
    dag: &'a CommitDag,
    stack: Vec<NodeIdx>,
    visited: Set<NodeIdx>,
}

impl<'a> ReverseTopo<'a> {
    fn new(dag: &'a CommitDag, start: NodeIdx) -> Self {
        let stack = vec![start];
        ReverseTopo {
            dag,
            stack,
            visited: Set::new(),
        }
    }
}

impl Iterator for ReverseTopo<'_> {
    type Item = Digest<LooseCommit>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            if self.visited.contains(&node) {
                continue;
            }
            self.visited.insert(node);
            let mut parents = self.dag.parents(node).collect::<Vec<_>>();
            parents.sort_by_key(|p| {
                #[allow(clippy::expect_used)]
                self.dag.nodes.get(p.0).expect("node is not in DAG").hash
            });
            self.stack.extend(parents);
            return Some(self.dag.nodes.get(node.0)?.hash);
        }
        None
    }
}

struct Parents<'a> {
    dag: &'a CommitDag,
    edge: Option<EdgeIdx>,
}

impl<'a> Parents<'a> {
    fn new(dag: &'a CommitDag, node: NodeIdx) -> Self {
        Parents {
            dag,
            edge: dag.nodes.get(node.0).and_then(|x| x.parents),
        }
    }
}

impl Iterator for Parents<'_> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(edge) = self.edge {
            let edge: &Edge = self.dag.edges.get(edge.0)?;
            self.edge = edge.next;
            Some(edge.source)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::collections::BTreeSet;

    use rand::{SeedableRng, rngs::SmallRng};

    use super::CommitDag;
    use crate::{
        collections::Set,
        commit::CountLeadingZeroBytes,
        loose_commit::LooseCommit,
        test_utils::{digest_with_depth, random_blob_meta},
    };

    /// No fragments: all commits with depth >= threshold remain as block boundaries.
    #[test]
    fn simplify_block_boundaries_without_fragments() {
        let mut rng = SmallRng::seed_from_u64(42);

        // Depths: a=2, b=0
        let a_hash = digest_with_depth(2, 0x01);
        let b_hash = digest_with_depth(0, 0x02);

        let b = LooseCommit::new(b_hash, BTreeSet::new(), random_blob_meta(&mut rng));
        let a = LooseCommit::new(a_hash, BTreeSet::from([b_hash]), random_blob_meta(&mut rng));

        let dag = CommitDag::from_commits([&a, &b].into_iter());

        let simplified = dag
            .simplify(&[], &CountLeadingZeroBytes)
            .commit_hashes()
            .collect::<Set<_>>();

        // With no fragments, simplify keeps block boundaries + heads
        // Both a and b should remain (a is head, b would be pruned but there's no fragment)
        assert_eq!(simplified, Set::from([a_hash, b_hash]));
    }

    /// Two consecutive block boundary commits (both depth >= threshold).
    #[test]
    fn simplify_consecutive_block_boundary_commits_without_fragments() {
        let mut rng = SmallRng::seed_from_u64(43);

        // Depths: a=2, b=2
        let a_hash = digest_with_depth(2, 0x01);
        let b_hash = digest_with_depth(2, 0x02);

        let b = LooseCommit::new(b_hash, BTreeSet::new(), random_blob_meta(&mut rng));
        let a = LooseCommit::new(a_hash, BTreeSet::from([b_hash]), random_blob_meta(&mut rng));

        let dag = CommitDag::from_commits([&a, &b].into_iter());

        let simplified = dag
            .simplify(&[], &CountLeadingZeroBytes)
            .commit_hashes()
            .collect::<Set<_>>();

        // Both are block boundaries, both remain
        assert_eq!(simplified, Set::from([a_hash, b_hash]));
    }

    #[test]
    fn test_parents() {
        let mut rng = SmallRng::seed_from_u64(44);

        let a_hash = digest_with_depth(0, 0x01);
        let b_hash = digest_with_depth(0, 0x02);
        let c_hash = digest_with_depth(0, 0x03);
        let d_hash = digest_with_depth(0, 0x04);

        let a = LooseCommit::new(a_hash, BTreeSet::new(), random_blob_meta(&mut rng));
        let b = LooseCommit::new(b_hash, BTreeSet::new(), random_blob_meta(&mut rng));
        let c = LooseCommit::new(
            c_hash,
            BTreeSet::from([a_hash, b_hash]),
            random_blob_meta(&mut rng),
        );
        let d = LooseCommit::new(d_hash, BTreeSet::from([c_hash]), random_blob_meta(&mut rng));

        let dag = CommitDag::from_commits([&a, &b, &c, &d].into_iter());

        assert_eq!(
            dag.parents_of_hash(c_hash).collect::<Set<_>>(),
            Set::from([a_hash, b_hash])
        );
    }
}
