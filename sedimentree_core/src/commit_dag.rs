use std::collections::{HashMap, HashSet};

use crate::{Digest, LooseCommit};

use super::Chunk;

// An adjacency list based representation of a commit DAG except that we use indexes into the
// `nodes` and `edges` vectors instead of pointers in order to please the borrow checker.
#[derive(Debug, Clone)]
pub(crate) struct CommitDag {
    nodes: Vec<Node>,
    node_map: HashMap<crate::Digest, NodeIdx>,
    edges: Vec<Edge>,
}

#[derive(Debug, Clone)]
struct Node {
    hash: crate::Digest,
    parents: Option<EdgeIdx>,
    children: Option<EdgeIdx>,
}

#[derive(Debug, Clone)]
struct Edge {
    source: NodeIdx,
    #[allow(dead_code)]
    target: NodeIdx,
    next: Option<EdgeIdx>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
            .collect::<HashMap<_, _>>();

        let mut dag = CommitDag {
            nodes,
            node_map,
            edges: Vec::new(),
        };

        for commit in commits {
            let child_idx = dag.node_map[&commit.digest()];
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
        let new_edge = Edge {
            source,
            target,
            next: None,
        };
        self.edges.push(new_edge);

        let target_node = &mut self.nodes[target.0];
        if let Some(edge_idx) = target_node.parents {
            let mut edge = &mut self.edges[edge_idx.0];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.0];
            }
            edge.next = Some(new_edge_idx);
        } else {
            target_node.parents = Some(new_edge_idx);
        }

        // Now add an edge in the parent node
        let new_edge_idx = EdgeIdx(self.edges.len());
        let new_edge = Edge {
            source,
            target,
            next: None,
        };
        self.edges.push(new_edge);

        let source_node = &mut self.nodes[source.0];
        if let Some(edge_idx) = source_node.children {
            let mut edge = &mut self.edges[edge_idx.0];
            while let Some(next) = edge.next {
                edge = &mut self.edges[next.0];
            }
            edge.next = Some(new_edge_idx);
        } else {
            source_node.children = Some(new_edge_idx);
        }
    }

    pub(crate) fn simplify(&self, chunks: &[Chunk]) -> Self {
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
        let mut commits_to_blocks = HashMap::new();
        let mut blockless_commits = HashSet::new();

        let mut tips = self.tips().collect::<Vec<_>>();
        tips.sort_by_key(|idx| self.nodes[idx.0].hash);

        for tip in tips {
            let mut block: Option<(Digest, Vec<Digest>)> = None;
            for hash in self.reverse_topo(tip) {
                let level = super::Depth::from(hash);
                if level >= crate::MAX_STRATA_DEPTH {
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
                    if level < crate::MAX_STRATA_DEPTH {
                        commits.push(hash);
                    }
                } else if !commits_to_blocks.contains_key(&hash) && level < crate::MAX_STRATA_DEPTH
                {
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
                    .all(|&b| chunks.iter().any(|s| s.supports_block(b)))
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
            .collect::<HashMap<_, _>>();

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

    fn parents_of_hash(&self, hash: Digest) -> impl Iterator<Item = Digest> + '_ {
        self.node_map
            .get(&hash)
            .map(|idx| self.parents(*idx).map(|i| self.nodes[i.0].hash))
            .into_iter()
            .flatten()
    }

    fn reverse_topo(&self, start: NodeIdx) -> impl Iterator<Item = Digest> + '_ {
        ReverseTopo::new(self, start)
    }

    pub(crate) fn contains_commit(&self, commit: &Digest) -> bool {
        self.node_map.contains_key(commit)
    }

    pub(crate) fn heads(&self) -> impl Iterator<Item = Digest> + '_ {
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
    pub(crate) fn canonical_sequence<'a, I: Iterator<Item = &'a Chunk> + Clone + 'a>(
        &'a self,
        chunks: I,
    ) -> impl Iterator<Item = Digest> + 'a {
        // First find the tips of the DAG, which is the heads of the commit DAG,
        // plus the end hashes of any chunks which are not contained in the
        // commit DAG
        let mut boundary = Vec::new();
        for chunk in chunks.clone() {
            for end in chunk.boundary() {
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
            let mut visited = HashSet::new();
            let chunks = chunks.clone();
            std::iter::from_fn(move || {
                while let Some(commit) = stack.pop() {
                    if visited.contains(&commit) {
                        continue;
                    }
                    visited.insert(commit);
                    if let Some(idx) = self.node_map.get(&commit) {
                        let mut parents = self
                            .parents(*idx)
                            .map(|i| self.nodes[i.0].hash)
                            .collect::<Vec<_>>();
                        parents.sort();
                        stack.extend(parents);
                    } else {
                        let mut supporting_chunks = chunks
                            .clone()
                            .filter(|s| s.boundary().contains(&commit))
                            .collect::<Vec<_>>();
                        supporting_chunks.sort_by_key(|s| s.depth());
                        if let Some(chunk) = supporting_chunks.pop() {
                            for commit in chunk.checkpoints() {
                                stack.push(*commit);
                            }
                            stack.push(chunk.head());
                        }
                    }
                    return Some(commit);
                }
                None
            })
        })
    }

    #[cfg(test)]
    fn commit_hashes(&self) -> impl Iterator<Item = Digest> + '_ {
        self.nodes.iter().map(|node| node.hash)
    }
}

struct ReverseTopo<'a> {
    dag: &'a CommitDag,
    stack: Vec<NodeIdx>,
    visited: HashSet<NodeIdx>,
}

impl<'a> ReverseTopo<'a> {
    fn new(dag: &'a CommitDag, start: NodeIdx) -> Self {
        let stack = vec![start];
        ReverseTopo {
            dag,
            stack,
            visited: HashSet::new(),
        }
    }
}

impl Iterator for ReverseTopo<'_> {
    type Item = Digest;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            if self.visited.contains(&node) {
                continue;
            }
            self.visited.insert(node);
            let mut parents = self.dag.parents(node).collect::<Vec<_>>();
            parents.sort_by_key(|p| self.dag.nodes[p.0].hash);
            self.stack.extend(parents);
            return Some(self.dag.nodes[node.0].hash);
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
            edge: dag.nodes[node.0].parents,
        }
    }
}

impl Iterator for Parents<'_> {
    type Item = NodeIdx;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(edge) = self.edge {
            let edge = &self.dag.edges[edge.0];
            self.edge = edge.next;
            Some(edge.source)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use nonempty::nonempty;
    use num::Num;

    use super::{
        super::{Chunk, LooseCommit},
        CommitDag,
    };
    use std::collections::{HashMap, HashSet};

    use crate::{blob::BlobMeta, Digest};

    fn hash_with_trailing_zeros<R: rand::Rng>(
        rng: &mut R,
        base: u32,
        trailing_zeros: u32,
    ) -> Digest {
        assert!(base > 1, "Base must be greater than 1");
        assert!(base <= 10, "Base must be less than 10");

        let zero_str = "0".repeat(trailing_zeros as usize);
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_lossless
        )]
        let num_digits = (256.0 / (base as f64).log2()).floor() as u64;

        let mut num_str = zero_str;
        num_str.push('1');
        #[allow(clippy::cast_possible_truncation)]
        while num_str.len() < num_digits as usize {
            let digit = rng.random_range(0..base);
            num_str.push_str(&digit.to_string());
        }
        // reverse the string to get the correct representation
        num_str = num_str.chars().rev().collect();
        #[allow(clippy::unwrap_used)]
        let num = num::BigInt::from_str_radix(&num_str, base).unwrap();

        let (_, mut bytes) = num.to_bytes_be();
        if bytes.len() < 32 {
            let mut padded_bytes = vec![0; 32 - bytes.len()];
            padded_bytes.extend(bytes);
            bytes = padded_bytes;
        }
        #[allow(clippy::unwrap_used)]
        let byte_arr: [u8; 32] = bytes.try_into().unwrap();
        Digest::from(byte_arr)
    }

    #[derive(Debug)]
    struct TestGraph {
        nodes: HashMap<String, Digest>,
        parents: HashMap<Digest, Vec<Digest>>,
        commits: HashMap<Digest, BlobMeta>,
    }

    impl TestGraph {
        fn new<R: rand::Rng>(
            rng: &mut R,
            node_info: Vec<(&'static str, usize)>,
            edges: Vec<(&'static str, &'static str)>,
        ) -> Self {
            let commit_hashes = make_commit_hashes(rng, node_info);
            let mut nodes = HashMap::new();
            let mut commits = HashMap::new();
            for (commit_name, commit_hash) in commit_hashes {
                commits.insert(commit_hash, random_blob(rng));
                nodes.insert(commit_name.to_string(), commit_hash);
            }
            let mut parents = HashMap::new();
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
                commits.push(LooseCommit {
                    #[allow(clippy::unwrap_used)]
                    blob: *self.commits.get(hash).unwrap(),
                    digest: *hash,
                    parents,
                });
            }
            commits
        }

        fn node_hash(&self, node: &str) -> Digest {
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
    ) -> HashMap<String, Digest> {
        let mut commits = HashMap::new();
        let mut last_commit = None;
        for (name, level) in names {
            loop {
                #[allow(clippy::cast_possible_truncation)]
                let hash = hash_with_trailing_zeros(rng, 10, level as u32);
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

    fn random_commit_hash<R: rand::Rng>(rng: &mut R) -> Digest {
        let mut hash = [0; 32];
        rng.fill_bytes(&mut hash);
        Digest::from(hash)
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
            chunks => [$({start: $chunk_start: ident, end: $chunk_end: ident, checkpoints: [$($checkpoint: ident),*]})*],
            simplified => [$($remaining: ident),*]
        ) => {
            let node_info = vec![$((stringify!($node), $level)),*];
            let graph = TestGraph::new($rng, node_info, vec![$((stringify!($from), stringify!($to)),)*]);
            let chunks = vec![$(Chunk::new(
                graph.node_hash(stringify!($chunk_start)),
                nonempty![graph.node_hash(stringify!($chunk_end))],
                vec![$(graph.node_hash(stringify!($checkpoint)),)*],
                random_blob($rng),
            ),)*];
            let dag = graph.as_dag();
            let mut commit_name_map = HashMap::<Digest, _>::from_iter(vec![$((graph.node_hash(stringify!($from)), stringify!($from))),*]);
            $(
                commit_name_map.insert(graph.node_hash(stringify!($to)), stringify!($to));
            )*
            let expected_commits = HashSet::from_iter(vec![$(graph.node_hash(stringify!($remaining)),)*]);
            let actual_commits = dag.simplify(&chunks).commit_hashes().collect::<HashSet<_>>();
            let expected_message = pretty_hashes(&commit_name_map, &expected_commits);
            let actual_message = pretty_hashes(&commit_name_map, &actual_commits);
            assert_eq!(expected_commits, actual_commits, "\nexpected: {:?}, \nactual: {:?}", expected_message, actual_message);
        };
    }

    fn pretty_hashes(
        name_map: &HashMap<Digest, &'_ str>,
        hashes: &HashSet<Digest>,
    ) -> HashSet<String> {
        hashes
            .iter()
            .map(|h| {
                name_map
                    .get(h)
                    .map_or_else(|| h.to_string(), ToString::to_string)
            })
            .collect()
    }

    #[test]
    fn simplify_basic() {
        simplify_test!(
            rng => &mut rand::rng(),
            nodes => | node | level |
                     |   a  |   2   |
                     |   b  |   0   |
                     |   c  |   0   |
                     |   d  |   2   |
                     |   e  |   0   |,
            graph => {
                a --> b
                a --> c
                b --> d
                d --> e
            },
            chunks => [
                {start: a, end: d, checkpoints: []}
            ],
            simplified => [a, c, e]
        );
    }

    #[test]
    fn simplify_multiple_heads() {
        simplify_test!(
            rng => &mut rand::rng(),
            nodes => | node | level |
                     |   a  |   0   |
                     |   b  |   0   |
                     |   c  |   2   |
                     |   d  |   2   |,
            graph => {
                a --> b
                c --> b
                b --> d
            },
            chunks => [
                {start: c, end: d, checkpoints: []}
            ],
            simplified => [a, c]
        );
    }

    #[test]
    fn simplify_block_boundaries_without_chunks() {
        simplify_test!(
            rng => &mut rand::rng(),
            nodes => | node | level |
                     |   a  |   2   |
                     |   b  |   0   |,
            graph => {
                a --> b
            },
            chunks => [],
            simplified => [a, b]
        );
    }

    #[test]
    fn simplify_consecutive_block_boundary_commits_without_chunks() {
        simplify_test!(
            rng => &mut rand::rng(),
            nodes => | node | level |
                     |   a  |   2   |
                     |   b  |   2   |,
            graph => {
                a --> b
            },
            chunks => [],
            simplified => [a, b]
        );
    }

    #[test]
    fn test_parents() {
        let mut rng = rand::rng();
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
            graph.parents_of_hash(c.digest()).collect::<HashSet<_>>(),
            vec![a.digest(), b.digest()]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }
}
