//! Abstractions for working with commits.

use crate::collections::{Map, Set};
use alloc::vec::Vec;
use core::error::Error;

use thiserror::Error;

use crate::{
    blob::BlobMeta,
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::id::CommitId,
};

/// An error indicating that a commit is missing from the store.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[error("missing commit: {0}")]
pub struct MissingCommitError(CommitId);

/// A trait for types that have parent hashes.
pub trait Parents {
    /// The parent identifiers of this node.
    fn parents(&self) -> Set<CommitId>;
}

#[allow(clippy::implicit_hasher)]
impl Parents for Set<CommitId> {
    fn parents(&self) -> Set<CommitId> {
        self.clone()
    }
}

#[derive(Debug, Error)]
/// An error for the [`fragment`] function.
pub enum FragmentError<'a, S: CommitStore<'a> + ?Sized> {
    /// An error occurred during lookup.
    #[error(transparent)]
    LookupError(S::LookupError),

    /// A commit was missing from the store.
    #[error(transparent)]
    MissingCommit(#[from] MissingCommitError),

    /// The head identifier has depth 0 and cannot be a fragment head.
    ///
    /// Depth-0 commits are loose commits, not fragment heads. The caller
    /// should filter them out before calling [`CommitStore::fragment`].
    #[error("depth-0 commit cannot be a fragment head: {0}")]
    DepthZeroHead(CommitId),
}

/// An abstraction over stores of commits that can be looked up by their identifier.
pub trait CommitStore<'a> {
    /// The type of node stored in the commit store.
    type Node: Parents;

    /// The error type returned when a lookup fails.
    type LookupError: Error;

    /// Looks up a commit by its identifier.
    ///
    /// # Errors
    ///
    /// Returns a [`Self::LookupError`] if the lookup fails.
    fn lookup(&self, id: CommitId) -> Result<Option<Self::Node>, Self::LookupError>;

    /// Constructs a single fragment starting from `head_id`.
    ///
    /// Walks from the head through parents, collecting all commits
    /// whose depth is _less than or equal to_ the head's depth. Commits
    /// with depth _strictly greater_ than the head become the boundary —
    /// they delimit the fragment and will head deeper (larger) strata.
    ///
    /// Same-depth commits encountered during the walk are absorbed as
    /// members but are _not_ recorded as checkpoints. Only commits whose
    /// depth is strictly between 0 and the head's depth (`0 < depth <
    /// head_depth`) are recorded as checkpoints. This means a depth-N
    /// fragment spans all the way back to the next depth-(N+1)+ commit,
    /// rather than stopping at each depth-N peer.
    ///
    /// `head_id` must have depth > 0; depth-0 commits are loose
    /// commits, not fragment heads.
    ///
    /// # Errors
    ///
    /// Returns [`FragmentError::MissingCommit`] if any commit in the walk
    /// is absent from the store.
    fn fragment<D: DepthMetric>(
        &self,
        head_id: CommitId,
        known_fragment_states: &Map<CommitId, FragmentState<Self::Node>>,
        strategy: &D,
    ) -> Result<FragmentState<Self::Node>, FragmentError<'a, Self>> {
        let head_depth = strategy.to_depth(head_id);
        if head_depth == Depth(0) {
            return Err(FragmentError::DepthZeroHead(head_id));
        }

        let mut visited: Set<CommitId> = Set::from([head_id]);
        let mut members: Set<CommitId> = Set::from([head_id]);
        let mut boundary: Map<CommitId, Self::Node> = Map::new();
        let mut checkpoints: Set<CommitId> = Set::new();

        let head_node = self
            .lookup(head_id)
            .map_err(FragmentError::LookupError)?
            .ok_or(FragmentError::MissingCommit(MissingCommitError(head_id)))?;

        let mut queue: Vec<CommitId> = head_node.parents().into_iter().collect();

        while let Some(id) = queue.pop() {
            if !visited.insert(id) {
                continue;
            }

            let node = self
                .lookup(id)
                .map_err(FragmentError::LookupError)?
                .ok_or(FragmentError::MissingCommit(MissingCommitError(id)))?;

            let depth = strategy.to_depth(id);
            if depth > head_depth {
                // Strictly deeper: this commit heads a larger stratum.
                // It becomes our boundary — don't expand its parents.
                boundary.insert(id, node);
            } else {
                // Same or shallower depth: absorbed into this fragment.
                members.insert(id);
                if depth > Depth(0) && depth < head_depth {
                    // Shallower stratum boundary within this fragment.
                    // Retained so we can determine the "supports"
                    // relationship between strata.
                    checkpoints.insert(id);
                }
                for p in node.parents() {
                    if !visited.contains(&p) {
                        queue.push(p);
                    }
                }
            }
        }

        // Strip overlap with already-known deeper fragments whose heads
        // coincide with our boundary.
        for boundary_hash in boundary.keys() {
            if let Some(known) = known_fragment_states.get(boundary_hash) {
                for m in known.members() {
                    members.remove(m);
                    checkpoints.remove(m);
                }
            }
        }

        Ok(FragmentState::new(head_id, members, checkpoints, boundary))
    }

    /// Builds a complete fragment store by walking from heads to roots.
    ///
    /// Fragments are built level-by-level: the document heads yield the
    /// shallowest fragments, whose boundaries become the heads for the
    /// next (deeper) level. Depth-0 commits are skipped — they are loose
    /// commits, not fragment heads — but their parents are still walked
    /// so that deeper fragment boundaries are discovered.
    ///
    /// Previously computed fragments in `known_fragment_states` are reused;
    /// newly built ones are inserted into the map and also returned.
    ///
    /// # Errors
    ///
    /// Returns a [`FragmentError`] if any lookup fails.
    fn build_fragment_store<'b, D: DepthMetric>(
        &self,
        head_ids: &[CommitId],
        known_fragment_states: &'b mut Map<CommitId, FragmentState<Self::Node>>,
        strategy: &D,
    ) -> Result<Vec<&'b FragmentState<Self::Node>>, FragmentError<'a, Self>> {
        let mut fresh_heads: Vec<CommitId> = Vec::new();
        let mut queue = head_ids.to_vec();
        let mut visited: Set<CommitId> = Set::new();

        while let Some(head) = queue.pop() {
            if !visited.insert(head) {
                continue;
            }

            // Already computed — just walk past to discover deeper levels.
            if let Some(state) = known_fragment_states.get(&head) {
                queue.extend(state.boundary().keys().copied());
                continue;
            }

            // Depth-0 commits are loose commits, not fragment heads.
            // Walk their parents so we still discover deeper boundaries.
            if strategy.to_depth(head) == Depth(0) {
                if let Some(node) = self.lookup(head).map_err(FragmentError::LookupError)? {
                    queue.extend(node.parents());
                }
                continue;
            }

            match self.fragment(head, known_fragment_states, strategy) {
                Ok(state) => {
                    queue.extend(state.boundary().keys().copied());
                    known_fragment_states.insert(head, state);
                    fresh_heads.push(head);
                }
                Err(FragmentError::MissingCommit(missing)) => {
                    tracing::debug!(%head, %missing, "skipping head with incomplete history");
                }
                Err(e) => return Err(e),
            }
        }

        let mut fresh = Vec::with_capacity(fresh_heads.len());
        for h in fresh_heads {
            #[allow(clippy::expect_used)]
            fresh.push(known_fragment_states.get(&h).expect("just inserted"));
        }
        Ok(fresh)
    }

    /// Parallel variant of [`build_fragment_store`](Self::build_fragment_store).
    ///
    /// Processes each depth level of the fragment tree in parallel using
    /// [rayon](https://docs.rs/rayon). Starting from the given heads, all
    /// fragments at the same level are built concurrently, then their
    /// boundaries become the next level's heads.
    ///
    /// Each parallel task receives a read-only snapshot of
    /// `known_fragment_states` for deduplication; newly computed states
    /// are merged sequentially after each level completes.
    ///
    /// Depth-0 commits are skipped (they are loose commits, not fragment
    /// heads), but their parents are walked so deeper boundaries are found.
    ///
    /// # Performance
    ///
    /// For a document with _n_ changes and _k_ fragment boundaries at the
    /// first level, this gives ~k-way parallelism. With
    /// [`CountLeadingZeroBytes`], k ≈ n/256, so a 200k-change document
    /// gets ~800-way parallelism.
    ///
    /// # Errors
    ///
    /// Returns a [`FragmentError`] if any lookup fails.
    #[cfg(feature = "rayon")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rayon")))]
    fn build_fragment_store_par<'b, D: DepthMetric + Sync>(
        &self,
        head_ids: &[CommitId],
        known_fragment_states: &'b mut Map<CommitId, FragmentState<Self::Node>>,
        strategy: &D,
    ) -> Result<Vec<&'b FragmentState<Self::Node>>, FragmentError<'a, Self>>
    where
        Self: Sync,
        Self::Node: Send + Sync + Clone,
        Self::LookupError: Send,
    {
        use rayon::prelude::*;

        let mut all_heads: Vec<CommitId> = Vec::new();
        let mut horizon = head_ids.to_vec();

        while !horizon.is_empty() {
            // Dedup against already-known fragments.
            horizon.retain(|h| !known_fragment_states.contains_key(h));
            if horizon.is_empty() {
                break;
            }

            // Walk past depth-0 commits (loose commits, not fragment heads)
            // to discover their deeper-depth parents.
            {
                let mut visited_d0: Set<CommitId> = Set::new();
                let mut pending: Vec<CommitId> = Vec::new();
                horizon.retain(|&h| {
                    if strategy.to_depth(h) == Depth(0) {
                        if visited_d0.insert(h) {
                            pending.push(h);
                        }
                        false
                    } else {
                        true
                    }
                });

                while let Some(d) = pending.pop() {
                    let Some(node) = self.lookup(d).map_err(FragmentError::LookupError)? else {
                        continue;
                    };
                    for p in node.parents() {
                        if known_fragment_states.contains_key(&p) {
                            continue;
                        }
                        if strategy.to_depth(p) == Depth(0) {
                            if visited_d0.insert(p) {
                                pending.push(p);
                            }
                        } else {
                            horizon.push(p);
                        }
                    }
                }
            }

            horizon.retain(|h| !known_fragment_states.contains_key(h));
            if horizon.is_empty() {
                break;
            }

            // Parallel phase: snapshot the known states so each task can
            // deduplicate members against previously computed fragments.
            let snapshot: Map<CommitId, FragmentState<Self::Node>> = known_fragment_states.clone();
            let level_results: Vec<Result<_, FragmentError<'a, Self>>> = horizon
                .par_iter()
                .filter_map(|&head| match self.fragment(head, &snapshot, strategy) {
                    Ok(state) => Some(Ok((head, state))),
                    Err(FragmentError::MissingCommit(missing)) => {
                        tracing::debug!(%head, %missing, "skipping head with incomplete history");
                        None
                    }
                    Err(e) => Some(Err(e)),
                })
                .collect();

            // Sequential phase: merge results into known_fragment_states.
            let mut successes = Vec::with_capacity(level_results.len());
            for result in level_results {
                successes.push(result?);
            }

            let mut next_horizon = Vec::new();
            for (head, state) in successes {
                next_horizon.extend(state.boundary().keys().copied());
                all_heads.push(head);
                known_fragment_states.insert(head, state);
            }
            horizon = next_horizon;
        }

        let mut fresh = Vec::with_capacity(all_heads.len());
        for head in all_heads {
            #[allow(clippy::expect_used)]
            let r = known_fragment_states.get(&head).expect("just inserted");
            fresh.push(r);
        }
        Ok(fresh)
    }
}

/// Re-export [`CountLeadingZeroBytes`] from the depth module.
pub use crate::depth::CountLeadingZeroBytes;

/// `Fragment`s are a consistent unit of document history,
/// which may end before the complete history is covered.
/// In this way, a document can be broken up into a series
/// of `Fragment`s that are consistent across replicas.
///
/// This is an experimental API, the fragment API is subject to change
/// and so should not be used in production just yet.
#[derive(Debug, Clone)]
pub struct FragmentState<T> {
    head_id: CommitId,
    members: Set<CommitId>,
    checkpoints: Set<CommitId>,
    boundary: Map<CommitId, T>,
}

impl<T> FragmentState<T> {
    /// Create a new `FragmentState`.
    #[must_use]
    pub const fn new(
        head_id: CommitId,
        members: Set<CommitId>,
        checkpoints: Set<CommitId>,
        boundary: Map<CommitId, T>,
    ) -> Self {
        Self {
            head_id,
            members,
            checkpoints,
            boundary,
        }
    }

    /// The "newest" element of the fragment.
    ///
    /// This identifier provides a stable point from which
    /// the rest of the fragment is built.
    #[must_use]
    pub const fn head_id(&self) -> CommitId {
        self.head_id
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_id`
    /// and the `boundary` (not including the boundary elements).
    #[must_use]
    pub const fn members(&self) -> &Set<CommitId> {
        &self.members
    }

    /// The checkpoints of the fragment.
    ///
    /// These are all of the [`CommitId`]s that match a valid level
    /// below the target, so that it is possible to know which other fragments
    /// this one covers.
    #[must_use]
    pub const fn checkpoints(&self) -> &Set<CommitId> {
        &self.checkpoints
    }

    /// The boundary from which the next set of fragments would be built.
    #[must_use]
    pub const fn boundary(&self) -> &Map<CommitId, T> {
        &self.boundary
    }

    /// Converts into a [`Fragment`] with the given [`BlobMeta`].
    #[must_use]
    pub fn to_fragment(self, sedimentree_id: SedimentreeId, blob_meta: BlobMeta) -> Fragment {
        let checkpoints: Vec<_> = self.checkpoints.iter().copied().collect();
        Fragment::new(
            sedimentree_id,
            self.head_id,
            self.boundary.keys().copied().collect(),
            &checkpoints,
            blob_meta,
        )
    }
}
