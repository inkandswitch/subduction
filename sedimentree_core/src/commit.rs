use std::{
    collections::{HashMap, HashSet},
    error::Error,
    mem::take,
};

use thiserror::Error;

use crate::{Depth, Digest};

#[derive(Debug, Clone, Error)]
#[error("missing commit: {0}")]
pub struct MissingCommitError(Digest);

pub trait Parents {
    fn parents(&self) -> HashSet<Digest>;
}

pub trait DepthStrategy {
    fn to_depth(&self, digest: Digest) -> Depth;
}

impl<Digestish: From<Digest>, Depthish: Into<Depth>> DepthStrategy for fn(Digestish) -> Depthish {
    fn to_depth(&self, digest: Digest) -> Depth {
        self(Digestish::from(digest)).into()
    }
}

impl<T: DepthStrategy> DepthStrategy for Box<T> {
    fn to_depth(&self, digest: Digest) -> Depth {
        T::to_depth(self, digest)
    }
}

pub trait CommitStore<'a> {
    type Node: Parents;
    type LookupError: Error;

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError>;

    fn fragment<D: DepthStrategy>(
        &self,
        head_digest: Digest,
        strategy: &D,
    ) -> Result<FragmentState<Self::Node>, Self::LookupError> {
        let min_depth = strategy.to_depth(head_digest);

        let mut visited: HashSet<Digest> = HashSet::from([head_digest]);
        let mut members: HashSet<Digest> = HashSet::from([head_digest]);

        let mut boundary: HashMap<Digest, Self::Node> = HashMap::new();
        let mut checkpoints: HashSet<Digest> = HashSet::new();

        let head_change = self.lookup(head_digest)?.expect("FIXME");
        let mut horizon: HashSet<Digest> = head_change.parents();

        while !horizon.is_empty() {
            let local_horizon = take(&mut horizon);
            for &digest in &local_horizon {
                let commit = self.lookup(digest)?.expect("FIXME");
                let is_newly_visited = visited.insert(digest);
                if !is_newly_visited {
                    continue;
                }

                members.insert(digest);

                let depth = strategy.to_depth(digest);
                if strategy.to_depth(digest) >= min_depth {
                    boundary.insert(digest, commit);
                } else {
                    if depth > Depth(0) {
                        checkpoints.insert(digest);
                    }
                    horizon.extend(commit.parents().iter().filter(|&d| !visited.contains(d)))
                }
            }
        }

        // Cleanup

        let mut cleanup_horizon: Vec<Digest> = Vec::new(); // boundary.keys().copied().collect::<Vec<_>>();
        for (boundary_hash, boundary_change) in boundary.iter() {
            members.remove(boundary_hash);
            let deps = boundary_change.parents();
            cleanup_horizon.extend(deps);
        }
        while !cleanup_horizon.is_empty() {
            let local_cleanup_horizon = take(&mut cleanup_horizon);
            for digest in local_cleanup_horizon {
                members.remove(&digest);
                checkpoints.remove(&digest);
                boundary.remove(&digest); // NOTE if one boundary covers another

                let is_newly_visited = visited.insert(digest);
                if !is_newly_visited {
                    continue;
                }

                let commit = self.lookup(digest)?.expect("FIXME");
                cleanup_horizon.extend(commit.parents().iter().filter(|&d| !visited.contains(d)))
            }
        }

        Ok(FragmentState::new(
            head_digest,
            members,
            checkpoints,
            boundary,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CountLeadingZeroBytes;

impl DepthStrategy for CountLeadingZeroBytes {
    fn to_depth(&self, digest: Digest) -> Depth {
        let mut acc = 0;
        for &byte in digest.as_bytes().iter() {
            if byte == 0 {
                acc += 1;
            } else {
                break;
            }
        }
        Depth(acc)
    }
}

/// EXPERIMENTAL: A "fragment" of Automerge history.
///
/// `Fragment`s are a consistent unit of document history,
/// which may end before the complete history is covered.
/// In this way, a document can be broken up into a series
/// of `Fragment`s that are consistent across replicas.
///
/// This is an experimental API, the fragmet API is subject to change
/// and so should not be used in production just yet.
#[derive(Debug, Clone)]
pub struct FragmentState<T> {
    head_digest: Digest,
    members: HashSet<Digest>,
    checkpoints: HashSet<Digest>,
    boundary: HashMap<Digest, T>,
}

impl<T> FragmentState<T> {
    pub fn new(
        head_digest: Digest,
        members: HashSet<Digest>,
        checkpoints: HashSet<Digest>,
        boundary: HashMap<Digest, T>,
    ) -> Self {
        Self {
            head_digest,
            members,
            checkpoints,
            boundary,
        }
    }

    /// The "newest" element of the fragment.
    ///
    /// This digest provides a stable point from which
    /// the restof the fragment is built.
    pub fn head_digest(&self) -> Digest {
        self.head_digest
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_digest`
    /// and the `boundary` (not including the boundary elements).
    pub fn members(&self) -> &HashSet<Digest> {
        &self.members
    }

    /// The checkpoints of the fragment.
    ///
    /// These are all of the [`Digest`]s that match a valid level
    /// below the target, so that it is possible to know which other fragments
    /// this one covers.
    pub fn checkpoints(&self) -> &HashSet<Digest> {
        &self.checkpoints
    }

    /// The boundary from which the next set of fragments would be built.
    pub fn boundary(&self) -> &HashMap<Digest, T> {
        &self.boundary
    }
}
