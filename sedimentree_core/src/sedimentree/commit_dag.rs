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
    digest::Digest,
    fragment::Fragment,
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
    pub(crate) fn canonical_sequence<
        'a,
        I: Iterator<Item = &'a Fragment> + Clone + 'a,
        M: DepthMetric,
    >(
        &'a self,
        fragments: I,
        hash_metric: &'a M,
    ) -> impl Iterator<Item = Digest<LooseCommit>> + 'a {
        // First find the tips of the DAG, which is the heads of the commit DAG,
        // plus the end hashes of any fragments which are not contained in the
        // commit DAG
        let mut boundary = Vec::new();
        for fragment in fragments.clone() {
            for end in fragment.boundary() {
                if !self.contains_commit(end) {
                    boundary.push(*end);
                }
            }
        }
        let mut heads = self.heads().chain(boundary).collect::<Vec<_>>();
        heads.sort();

        // Then for each tip, do a reverse depth first traversal. When we reach
        // a commit which has a parent which is in a stratum, we just extend the
        // traversal with the commits and checkpoints from the given stratum
        heads.into_iter().flat_map(move |head| {
            let mut stack = vec![head];
            let mut visited = Set::new();
            let fragments = fragments.clone();
            core::iter::from_fn(move || {
                while let Some(commit) = stack.pop() {
                    if visited.contains(&commit) {
                        continue;
                    }
                    visited.insert(commit);
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
                    } else {
                        let mut supporting_fragments = fragments
                            .clone()
                            .filter(|s| s.boundary().contains(&commit))
                            .collect::<Vec<_>>();
                        supporting_fragments.sort_by_key(|s| s.depth(hash_metric));
                        if let Some(fragment) = supporting_fragments.pop() {
                            for commit in fragment.checkpoints() {
                                stack.push(*commit);
                            }
                            stack.push(fragment.head());
                        }
                    }
                    return Some(commit);
                }
                None
            })
        })
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
    use alloc::{
        string::{String, ToString},
        vec::Vec,
    };

    use rand::{SeedableRng, rngs::SmallRng};

    use super::CommitDag;
    use crate::{
        blob::BlobMeta,
        collections::{Map, Set},
        commit::CountLeadingZeroBytes,
        digest::Digest,
        loose_commit::LooseCommit,
    };

    /// Simple deterministic byte generator for tests (no rand dependency needed).
    fn deterministic_bytes(seed: u64) -> [u8; 32] {
        *blake3::hash(&seed.to_le_bytes()).as_bytes()
    }

    fn hash_with_leading_zeros<R: rand::Rng>(rng: &mut R, zeros_count: u32) -> Digest<LooseCommit> {
        let mut byte_arr: [u8; 32] = rng.r#gen::<[u8; 32]>();
        for slot in byte_arr.iter_mut().take(zeros_count as usize) {
            *slot = 0;
        }
        Digest::from_bytes(byte_arr)
    }

    #[derive(Debug)]
    struct TestGraph {
        nodes: Map<String, Digest<LooseCommit>>,
        parents: Map<Digest<LooseCommit>, Vec<Digest<LooseCommit>>>,
        commits: Map<Digest<LooseCommit>, BlobMeta>,
    }

    impl TestGraph {
        fn new<R: rand::Rng>(
            rng: &mut R,
            node_info: Vec<(&'static str, usize)>,
            edges: Vec<(&'static str, &'static str)>,
        ) -> Self {
            let commit_hashes = make_commit_hashes(rng, node_info);
            let mut nodes = Map::new();
            let mut commits = Map::new();
            for (commit_name, commit_hash) in commit_hashes {
                commits.insert(commit_hash, random_blob(rng));
                nodes.insert(commit_name, commit_hash);
            }
            let mut parents = Map::new();
            #[allow(clippy::panic)]
            for (parent, child) in edges {
                let Some(child_hash) = nodes.get(child) else {
                    panic!("Child node not found: {child}");
                };
                let Some(parent_hash) = nodes.get(parent) else {
                    panic!("Parent node not found: {parent}");
                };
                parents
                    .entry(*child_hash)
                    .or_insert_with(Vec::new)
                    .push(*parent_hash);
            }
            Self {
                nodes,
                parents,
                commits,
            }
        }

        fn commits(&self) -> Vec<LooseCommit> {
            let mut commits = Vec::new();
            for hash in self.nodes.values() {
                #[allow(clippy::unwrap_used)]
                let parents = self.parents.get(hash).unwrap_or(&Vec::new()).clone();
                #[allow(clippy::unwrap_used)]
                let blob_meta = *self.commits.get(hash).unwrap();
                commits.push(LooseCommit::new(*hash, parents, blob_meta));
            }
            commits
        }

        fn node_hash(&self, node: &str) -> Digest<LooseCommit> {
            #[allow(clippy::unwrap_used)]
            *self.nodes.get(node).unwrap()
        }

        fn as_dag(&self) -> CommitDag {
            CommitDag::from_commits(self.commits().iter())
        }
    }

    /// Given a set of (name, level) pairs, create a map from name to commit hash
    ///
    /// This function will ensure that the generated hashes are ascending
    fn make_commit_hashes<R: rand::Rng>(
        rng: &mut R,
        names: Vec<(&'static str, usize)>,
    ) -> Map<String, Digest<LooseCommit>> {
        let mut commits = Map::new();
        let mut last_commit = None;
        for (name, level) in names {
            loop {
                #[allow(clippy::cast_possible_truncation)]
                let hash = hash_with_leading_zeros(rng, level as u32);
                if let Some(last_commit_hash) = last_commit {
                    if hash > last_commit_hash {
                        last_commit = Some(hash);
                        commits.insert(name.to_string(), hash);
                        break;
                    }
                } else {
                    last_commit = Some(hash);
                    commits.insert(name.to_string(), hash);
                    break;
                }
            }
        }
        commits
    }

    fn random_commit_hash<R: rand::Rng>(rng: &mut R) -> Digest<LooseCommit> {
        let mut hash = [0; 32];
        rng.fill_bytes(&mut hash);
        Digest::from_bytes(hash)
    }

    fn random_blob<R: rand::Rng>(rng: &mut R) -> BlobMeta {
        let mut contents: [u8; 20] = [0; 20];
        rng.fill_bytes(&mut contents);
        BlobMeta::new(&contents)
    }

    macro_rules! simplify_test {
        (
            rng => $rng: expr,
            nodes => |node|level| $(|$node:ident | $level:literal| )*,
            graph => {$($from:ident  --> $to:ident)*},
            fragments => [$({start: $fragment_start: ident, end: $fragment_end: ident, checkpoints: [$($checkpoint: ident),*]})*],
            simplified => [$($remaining: ident),*]
        ) => {
            let node_info = vec![$((stringify!($node), $level)),*];
            let graph = TestGraph::new($rng, node_info, vec![$((stringify!($from), stringify!($to)),)*]);
            let fragments = vec![$(Fragment::new(
                graph.node_hash(stringify!($fragment_start)),
                vec![graph.node_hash(stringify!($fragment_end))],
                vec![$(graph.node_hash(stringify!($checkpoint)),)*],
                random_blob($rng),
            ),)*];
            let dag = graph.as_dag();
            let mut commit_name_map = Map::<Digest<LooseCommit>, _>::from_iter(vec![$((graph.node_hash(stringify!($from)), stringify!($from))),*]);
            $(
                commit_name_map.insert(graph.node_hash(stringify!($to)), stringify!($to));
            )*
            let expected_commits = Set::from_iter(vec![$(graph.node_hash(stringify!($remaining)),)*]);
            let actual_commits = dag.simplify(&fragments, &CountLeadingZeroBytes).commit_hashes().collect::<Set<_>>();
            let expected_message = pretty_hashes(&commit_name_map, &expected_commits);
            let actual_message = pretty_hashes(&commit_name_map, &actual_commits);
            assert_eq!(expected_commits, actual_commits, "\nexpected: {:?}, \nactual: {:?}", expected_message, actual_message);
        };
    }

    fn pretty_hashes(
        name_map: &Map<Digest<LooseCommit>, &'_ str>,
        hashes: &Set<Digest<LooseCommit>>,
    ) -> Set<String> {
        hashes
            .iter()
            .map(|h| {
                name_map
                    .get(h)
                    .map_or_else(|| h.to_string(), ToString::to_string)
            })
            .collect()
    }

    // #[test]
    // fn simplify_basic() {
    //     simplify_test!(
    //         rng => &mut rand::thread_rng(),
    //         nodes => | node | level |
    //                  |   a  |   2   |
    //                  |   b  |   0   |
    //                  |   c  |   0   |
    //                  |   d  |   2   |
    //                  |   e  |   0   |,
    //         graph => {
    //             a --> b
    //             a --> c
    //             b --> d
    //             d --> e
    //         },
    //         fragments => [
    //             {start: a, end: d, checkpoints: []}
    //         ],
    //         simplified => [a, c, e]
    //     );
    // }

    // #[test]
    // fn simplify_multiple_heads() {
    //     simplify_test!(
    //         rng => &mut rand::thread_rng(),
    //         nodes => | node | level |
    //                  |   a  |   0   |
    //                  |   b  |   0   |
    //                  |   c  |   2   |
    //                  |   d  |   2   |,
    //         graph => {
    //             a --> b
    //             c --> b
    //             b --> d
    //         },
    //         fragments => [
    //             {start: c, end: d, checkpoints: []}
    //         ],
    //         simplified => [a, c]
    //     );
    // }

    #[test]
    fn simplify_block_boundaries_without_fragments() {
        simplify_test!(
            rng => &mut SmallRng::seed_from_u64(42),
            nodes => | node | level |
                     |   a  |   2   |
                     |   b  |   0   |,
            graph => {
                a --> b
            },
            fragments => [],
            simplified => [a, b]
        );
    }

    #[test]
    fn simplify_consecutive_block_boundary_commits_without_fragments() {
        simplify_test!(
            rng => &mut SmallRng::seed_from_u64(43),
            nodes => | node | level |
                     |   a  |   2   |
                     |   b  |   2   |,
            graph => {
                a --> b
            },
            fragments => [],
            simplified => [a, b]
        );
    }

    #[test]
    fn test_parents() {
        let mut rng = SmallRng::seed_from_u64(44);
        let a = LooseCommit::new(random_commit_hash(&mut rng), vec![], random_blob(&mut rng));
        let b = LooseCommit::new(random_commit_hash(&mut rng), vec![], random_blob(&mut rng));
        let c = LooseCommit::new(
            random_commit_hash(&mut rng),
            vec![a.digest(), b.digest()],
            random_blob(&mut rng),
        );
        let d = LooseCommit::new(
            random_commit_hash(&mut rng),
            vec![c.digest()],
            random_blob(&mut rng),
        );
        let graph = CommitDag::from_commits(vec![&a, &b, &c, &d].into_iter());
        assert_eq!(
            graph.parents_of_hash(c.digest()).collect::<Set<_>>(),
            vec![a.digest(), b.digest()].into_iter().collect::<Set<_>>()
        );
    }
}
