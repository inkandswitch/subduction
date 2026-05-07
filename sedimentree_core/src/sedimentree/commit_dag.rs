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
    depth::DepthMetric,
    fragment::{Fragment, checkpoint::Checkpoint},
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
        // Prepend to each adjacency list (O(1) instead of O(degree)).
        // Consumers iterate without assuming order. The two `Edge`
        // writes below are NOT duplicates: each carries the *other*
        // endpoint as `source` (parents-chain yields parents,
        // children-chain yields children).

        let parent_edge_idx = EdgeIdx(self.edges.len());
        #[allow(clippy::expect_used)]
        let target_node = self
            .nodes
            .get_mut(target.0)
            .expect("NodeId not in self.nodes");
        let prev_parent_head = target_node.parents;
        target_node.parents = Some(parent_edge_idx);
        self.edges.push(Edge {
            source,
            next: prev_parent_head,
        });

        let child_edge_idx = EdgeIdx(self.edges.len());
        #[allow(clippy::expect_used)]
        let source_node = self
            .nodes
            .get_mut(source.0)
            .expect("NodeId not in self.nodes");
        let prev_child_head = source_node.children;
        source_node.children = Some(child_edge_idx);
        self.edges.push(Edge {
            source: target,
            next: prev_child_head,
        });
    }

    /// Returns the set of [`CommitId`]s that survive simplification given
    /// `fragments`. A commit is dropped when every fragment range it
    /// belongs to is already covered by some fragment in `fragments`.
    ///
    /// Returns just the membership set rather than rebuilding a fresh
    /// [`CommitDag`]: the only caller ([`Sedimentree::minimize`]) needs
    /// `contains` lookups, not graph structure.
    pub(crate) fn simplify<S: DepthMetric>(
        &self,
        fragments: &[Fragment],
        strategy: &S,
    ) -> Set<CommitId> {
        // Algorithm: walk reverse-topo from each tip, partitioning
        // commits into fragment "ranges" (runs between boundaries — a
        // commit is a boundary when `Depth::is_boundary()`). A commit
        // can appear in multiple ranges (visited from multiple tips),
        // and is dropped iff every range it belongs to is covered by
        // some kept fragment.
        //
        // Example DAG (boundary commits in *bold*; fragments covering
        // *A* and *F*):
        //
        //        ┌─┐
        //        │A│
        //   ┌────┴─┴────┐
        //  ┌▼┐         ┌▼┐          ┌─┐
        //  │B│         │C│          │D│
        //  └┬┘         └┬┘          └┬┘
        //   │           │            │
        //   │           └─────┬──────┘
        //  ┌▼┐                │
        //  │I│               ┌▼┐
        //  └─┘               │E│
        //                    └┬┘
        //                    ┌▼┐
        //               ┌────┤F├────┐
        //              ┌▼┐         ┌▼┐
        //              │G│         │H│
        //              └─┘         └─┘
        //
        // We keep G and H (not in any range), and B and I (in a range
        // not covered by any fragment we have). We drop everything in
        // the A-covered and F-covered ranges.
        let mut commits_to_ranges = Map::new();
        let mut rangeless_commits = Set::new();

        let mut tips = self.tips().collect::<Vec<_>>();
        tips.sort_by_key(|idx| {
            #[allow(clippy::expect_used)]
            self.nodes.get(idx.0).expect("NodeId not in self.nodes").id
        });

        for tip in tips {
            let mut range: Option<(CommitId, Vec<CommitId>)> = None;
            for id in self.reverse_topo(tip) {
                let depth = strategy.to_depth(id);
                if depth.is_boundary() {
                    if let Some((range_head, commits)) = range.take() {
                        for commit in commits {
                            rangeless_commits.remove(&commit);
                            commits_to_ranges
                                .entry(commit)
                                .or_insert_with(Vec::new)
                                .push(range_head);
                        }
                    }
                    range = Some((id, vec![id]));
                }
                if let Some((_, commits)) = &mut range {
                    if !depth.is_boundary() {
                        commits.push(id);
                    }
                } else if !commits_to_ranges.contains_key(&id) && !depth.is_boundary() {
                    rangeless_commits.insert(id);
                }
            }
            // The walk reached a root without crossing another
            // boundary; treat the trailing range as unbounded at its
            // oldest end.
            if let Some((end, commits)) = range.take() {
                commits_to_ranges
                    .entry(end)
                    .or_insert_with(Vec::new)
                    .push(end);
                for commit in commits {
                    commits_to_ranges
                        .entry(commit)
                        .or_insert_with(Vec::new)
                        .push(end);
                }
            }
        }

        // Build the union of `CommitId`s that any fragment supports
        // (head, boundary, and checkpoints). Range heads recorded above
        // are full `CommitId`s; checkpoints are truncated, so we
        // collect both forms. See `Fragment::supports_block`.
        let mut covered_range_heads: Set<CommitId> = Set::new();
        let mut covered_checkpoints: Set<Checkpoint> = Set::new();
        for f in fragments {
            covered_range_heads.insert(f.head());
            covered_range_heads.extend(f.boundary().iter().copied());
            covered_checkpoints.extend(f.checkpoints().iter().copied());
        }

        commits_to_ranges
            .into_iter()
            .filter_map(|(commit, ranges)| {
                let all_covered = ranges.iter().all(|r| {
                    covered_range_heads.contains(r)
                        || covered_checkpoints.contains(&Checkpoint::new(*r))
                });
                if all_covered { None } else { Some(commit) }
            })
            .chain(rangeless_commits)
            .collect()
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

    #[cfg(test)]
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

    /// No fragments: all boundary commits remain as potential fragment heads.
    #[test]
    fn simplify_fragment_boundaries_without_fragments() {
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

        let simplified = dag.simplify(&[], &depth_metric);

        // With no fragments, simplify keeps boundary commits + heads
        // Both a and b should remain (a is boundary, b would be pruned but there's no fragment)
        assert_eq!(simplified, Set::from([a_id, b_id]));
    }

    /// Two consecutive boundary commits (both with nonzero depth).
    #[test]
    fn simplify_consecutive_boundary_commits_without_fragments() {
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

        let simplified = dag.simplify(&[], &depth_metric);

        // Both are boundary commits, both remain
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

    /// Property tests for `CommitDag` invariants under input ordering.
    ///
    /// `add_edge` prepends to per-node adjacency lists in O(1), so the
    /// internal edge storage order depends on the order `from_commits`
    /// receives commits. Consumers (`simplify`, `heads`, `contains_commit`)
    /// must be insensitive to that order.
    ///
    /// Gated on both `bolero` (for `bolero::check!` and the
    /// `arbitrary::Arbitrary` impl) and `test_utils` (which is what
    /// enables the `rand` optional dependency this module uses).
    /// `cargo test -p sedimentree_core --features bolero` alone is not
    /// sufficient — `rand` would be unresolved.
    #[cfg(all(feature = "bolero", feature = "test_utils"))]
    mod proptests {
        use alloc::vec::Vec;
        use rand::{Rng, SeedableRng, rngs::SmallRng};

        use super::*;
        use crate::{commit::CountLeadingZeroBytes, fragment::Fragment};

        /// A randomly-generated valid commit DAG.
        ///
        /// Each generated commit's `parents` reference only commits
        /// emitted earlier in the sequence, so the result is acyclic.
        #[derive(Debug)]
        struct ArbitraryCommits {
            commits: Vec<LooseCommit>,
        }

        impl<'a> arbitrary::Arbitrary<'a> for ArbitraryCommits {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                let sedimentree_id = SedimentreeId::arbitrary(u)?;
                let num_commits: u32 = u.int_in_range(0..=20)?;
                let mut frontier: Vec<CommitId> = Vec::new();
                let mut commits = Vec::with_capacity(num_commits as usize);
                let mut rng = SmallRng::seed_from_u64(u.arbitrary()?);

                for _ in 0..num_commits {
                    let head = CommitId::new(rng.r#gen());
                    let mut parents = BTreeSet::new();
                    let n_parents = u.int_in_range(0..=frontier.len().min(3))?;
                    let mut available: Vec<usize> = (0..frontier.len()).collect();
                    for _ in 0..n_parents {
                        if available.is_empty() {
                            break;
                        }
                        let pick = u.choose_index(available.len())?;
                        let idx = available.remove(pick);
                        #[allow(clippy::expect_used)]
                        let parent = frontier
                            .get(idx)
                            .copied()
                            .expect("idx came from 0..frontier.len() and frontier doesn't shrink");
                        parents.insert(parent);
                    }
                    let blob_meta = BlobMeta::arbitrary(u)?;
                    commits.push(LooseCommit::new(sedimentree_id, head, parents, blob_meta));
                    frontier.push(head);
                }

                Ok(ArbitraryCommits { commits })
            }
        }

        fn shuffle<T: Clone>(items: &[T], seed: u64) -> Vec<T> {
            use rand::seq::SliceRandom;
            let mut v: Vec<T> = items.to_vec();
            let mut rng = SmallRng::seed_from_u64(seed);
            v.shuffle(&mut rng);
            v
        }

        /// `simplify` output is independent of commit insertion order.
        /// Guards against `add_edge`'s per-node adjacency-list order
        /// leaking into observable behaviour.
        #[test]
        fn simplify_invariant_under_input_order() {
            bolero::check!()
                .with_arbitrary::<(ArbitraryCommits, u64)>()
                .for_each(|(ArbitraryCommits { commits }, shuffle_seed)| {
                    let dag1 = CommitDag::from_commits(commits.iter());
                    let shuffled = shuffle(commits, *shuffle_seed);
                    let dag2 = CommitDag::from_commits(shuffled.iter());

                    let s1 = dag1.simplify(&[], &CountLeadingZeroBytes);
                    let s2 = dag2.simplify(&[], &CountLeadingZeroBytes);

                    assert_eq!(
                        s1,
                        s2,
                        "simplify output depends on input order: {} commits",
                        commits.len()
                    );
                });
        }

        /// `heads` is independent of commit insertion order. The
        /// current implementation walks `node.children.is_none()` so
        /// this is trivially true today; the test guards against future
        /// changes.
        #[test]
        fn heads_invariant_under_input_order() {
            bolero::check!()
                .with_arbitrary::<(ArbitraryCommits, u64)>()
                .for_each(|(ArbitraryCommits { commits }, shuffle_seed)| {
                    let dag1 = CommitDag::from_commits(commits.iter());
                    let shuffled = shuffle(commits, *shuffle_seed);
                    let dag2 = CommitDag::from_commits(shuffled.iter());

                    let h1: Set<CommitId> = dag1.heads().collect();
                    let h2: Set<CommitId> = dag2.heads().collect();

                    assert_eq!(h1, h2, "heads depends on input order");
                });
        }

        /// `contains_commit` is independent of insertion order.
        /// Trivially true (a `node_map` lookup), included as a baseline.
        #[test]
        fn contains_commit_invariant_under_input_order() {
            bolero::check!()
                .with_arbitrary::<(ArbitraryCommits, u64)>()
                .for_each(|(ArbitraryCommits { commits }, shuffle_seed)| {
                    let dag1 = CommitDag::from_commits(commits.iter());
                    let shuffled = shuffle(commits, *shuffle_seed);
                    let dag2 = CommitDag::from_commits(shuffled.iter());

                    for c in commits {
                        let id = c.head();
                        assert_eq!(
                            dag1.contains_commit(&id),
                            dag2.contains_commit(&id),
                            "contains_commit disagrees on {id:?}"
                        );
                    }
                });
        }

        /// `simplify` with arbitrary fragments is independent of
        /// insertion order. Exercises the precomputed-coverage path
        /// across non-empty `fragments` slices.
        #[test]
        fn simplify_with_fragments_invariant_under_input_order() {
            bolero::check!()
                .with_arbitrary::<(ArbitraryCommits, Vec<Fragment>, u64)>()
                .for_each(|(ArbitraryCommits { commits }, fragments, shuffle_seed)| {
                    let dag1 = CommitDag::from_commits(commits.iter());
                    let shuffled = shuffle(commits, *shuffle_seed);
                    let dag2 = CommitDag::from_commits(shuffled.iter());

                    let s1 = dag1.simplify(fragments, &CountLeadingZeroBytes);
                    let s2 = dag2.simplify(fragments, &CountLeadingZeroBytes);

                    assert_eq!(s1, s2);
                });
        }
    }
}
