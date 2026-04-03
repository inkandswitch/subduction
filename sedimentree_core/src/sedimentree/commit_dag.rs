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
    depth::{DepthMetric, MAX_STRATA_DEPTH},
    fragment::Fragment,
    loose_commit::{LooseCommit, id::CommitId},
};

// An adjacency list based representation of a commit DAG except that we use indexes into the
// `nodes` and `edges` vectors instead of pointers in order to please the borrow checker.
#[derive(Debug, Clone)]
pub(crate) struct CommitDag {
    nodes: Vec<Node>,
    node_map: Map<CommitId, NodeIdx>,
    edges: Vec<Edge>,
}

#[derive(Debug, Clone)]
struct Node {
    id: CommitId,
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
                id: c.head(),
                parents: None,
                children: None,
            })
            .collect::<Vec<_>>();
        let node_map = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| (node.id, NodeIdx(idx)))
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
                .get(&commit.head())
                .expect("commit id not in node_map");

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
            self.nodes.get(idx.0).expect("NodeId not in self.nodes").id
        });

        for tip in tips {
            let mut block: Option<(CommitId, Vec<CommitId>)> = None;
            for id in self.reverse_topo(tip) {
                let depth = strategy.to_depth(id);
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
                    block = Some((id, vec![id]));
                }
                if let Some((_, commits)) = &mut block {
                    if depth < MAX_STRATA_DEPTH {
                        commits.push(id);
                    }
                } else if !commits_to_blocks.contains_key(&id) && depth < MAX_STRATA_DEPTH {
                    blockless_commits.insert(id);
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
                id: c,
                parents: None,
                children: None,
            })
            .collect::<Vec<_>>();
        let node_map = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| (node.id, NodeIdx(idx)))
            .collect::<Map<_, _>>();

        let mut dag = CommitDag {
            nodes,
            node_map,
            edges: Vec::new(),
        };

        for (child_idx, commit) in remaining_commits.into_iter().enumerate() {
            for parent in self.parents_of_id(commit) {
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
        ParentsIter::new(self, node)
    }

    fn parents_of_id(&self, id: CommitId) -> impl Iterator<Item = CommitId> + '_ {
        self.node_map
            .get(&id)
            .map(|idx| {
                self.parents(*idx).map(|i| {
                    #[allow(clippy::expect_used)]
                    self.nodes.get(i.0).expect("nodeId wasn't in self.nodes").id
                })
            })
            .into_iter()
            .flatten()
    }

    fn reverse_topo(&self, start: NodeIdx) -> impl Iterator<Item = CommitId> + '_ {
        ReverseTopo::new(self, start)
    }

    pub(crate) fn contains_commit(&self, id: &CommitId) -> bool {
        self.node_map.contains_key(id)
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = CommitId> + '_ {
        self.nodes.iter().filter_map(|node| {
            if node.children.is_none() {
                Some(node.id)
            } else {
                None
            }
        })
    }

    #[cfg(test)]
    fn commit_ids(&self) -> impl Iterator<Item = CommitId> + '_ {
        self.nodes.iter().map(|node| node.id)
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
    type Item = CommitId;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            if self.visited.contains(&node) {
                continue;
            }
            self.visited.insert(node);
            let mut parents = self.dag.parents(node).collect::<Vec<_>>();
            parents.sort_by_key(|p| {
                #[allow(clippy::expect_used)]
                self.dag.nodes.get(p.0).expect("node is not in DAG").id
            });
            self.stack.extend(parents);
            return Some(self.dag.nodes.get(node.0)?.id);
        }
        None
    }
}

struct ParentsIter<'a> {
    dag: &'a CommitDag,
    edge: Option<EdgeIdx>,
}

impl<'a> ParentsIter<'a> {
    fn new(dag: &'a CommitDag, node: NodeIdx) -> Self {
        ParentsIter {
            dag,
            edge: dag.nodes.get(node.0).and_then(|x| x.parents),
        }
    }
}

impl Iterator for ParentsIter<'_> {
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

    use super::CommitDag;
    use crate::{
        blob::{Blob, BlobMeta},
        collections::{Map, Set},
        depth::{Depth, DepthMetric},
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
    };

    fn make_sedimentree_id(seed: u8) -> SedimentreeId {
        SedimentreeId::new([seed; 32])
    }

    fn make_commit(
        sedimentree_id: SedimentreeId,
        seed: u8,
        parents: BTreeSet<CommitId>,
    ) -> LooseCommit {
        let blob = Blob::from(&[seed][..]);
        let blob_meta = BlobMeta::new(&blob);
        let head = CommitId::new({
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            bytes
        });
        LooseCommit::new(sedimentree_id, head, parents, blob_meta)
    }

    /// A mock depth metric that returns predetermined depths for specific identifiers.
    struct MockDepthMetric {
        depths: Map<CommitId, Depth>,
    }

    impl MockDepthMetric {
        fn new() -> Self {
            Self { depths: Map::new() }
        }

        fn set_depth(&mut self, id: CommitId, depth: Depth) {
            self.depths.insert(id, depth);
        }
    }

    impl DepthMetric for MockDepthMetric {
        fn to_depth(&self, id: CommitId) -> Depth {
            self.depths.get(&id).copied().unwrap_or(Depth(0))
        }
    }

    /// No fragments: all commits with depth >= threshold remain as block boundaries.
    #[test]
    fn simplify_block_boundaries_without_fragments() {
        let sedimentree_id = make_sedimentree_id(1);

        // Create commits: b is root, a has b as parent
        let b = make_commit(sedimentree_id, 1, BTreeSet::new());
        let b_id = b.head();
        let a = make_commit(sedimentree_id, 2, BTreeSet::from([b_id]));
        let a_id = a.head();

        // Set up mock depths: a=2, b=0
        let mut depth_metric = MockDepthMetric::new();
        depth_metric.set_depth(a_id, Depth(2));
        depth_metric.set_depth(b_id, Depth(0));

        let dag = CommitDag::from_commits([&a, &b].into_iter());

        let simplified = dag
            .simplify(&[], &depth_metric)
            .commit_ids()
            .collect::<Set<_>>();

        // With no fragments, simplify keeps block boundaries + heads
        // Both a and b should remain (a is head, b would be pruned but there's no fragment)
        assert_eq!(simplified, Set::from([a_id, b_id]));
    }

    /// Two consecutive block boundary commits (both depth >= threshold).
    #[test]
    fn simplify_consecutive_block_boundary_commits_without_fragments() {
        let sedimentree_id = make_sedimentree_id(1);

        // Create commits: b is root, a has b as parent
        let b = make_commit(sedimentree_id, 1, BTreeSet::new());
        let b_id = b.head();
        let a = make_commit(sedimentree_id, 2, BTreeSet::from([b_id]));
        let a_id = a.head();

        // Set up mock depths: a=2, b=2
        let mut depth_metric = MockDepthMetric::new();
        depth_metric.set_depth(a_id, Depth(2));
        depth_metric.set_depth(b_id, Depth(2));

        let dag = CommitDag::from_commits([&a, &b].into_iter());

        let simplified = dag
            .simplify(&[], &depth_metric)
            .commit_ids()
            .collect::<Set<_>>();

        // Both are block boundaries, both remain
        assert_eq!(simplified, Set::from([a_id, b_id]));
    }

    #[test]
    fn test_parents() {
        let sedimentree_id = make_sedimentree_id(1);

        // Create a DAG: a and b are roots, c has both as parents, d has c as parent
        let a = make_commit(sedimentree_id, 1, BTreeSet::new());
        let a_id = a.head();
        let b = make_commit(sedimentree_id, 2, BTreeSet::new());
        let b_id = b.head();
        let c = make_commit(sedimentree_id, 3, BTreeSet::from([a_id, b_id]));
        let c_id = c.head();
        let d = make_commit(sedimentree_id, 4, BTreeSet::from([c_id]));

        let dag = CommitDag::from_commits([&a, &b, &c, &d].into_iter());

        assert_eq!(
            dag.parents_of_id(c_id).collect::<Set<_>>(),
            Set::from([a_id, b_id])
        );
    }
}
