//! Abstractions for working with commits.

use crate::collections::{Map, Set};
use alloc::vec::Vec;
use core::{error::Error, mem::take, num::NonZero};

use thiserror::Error;

use crate::{
    blob::{BlobMeta, Digest},
    depth::{Depth, DepthMetric},
    fragment::Fragment,
};

/// An error indicating that a commit is missing from the store.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[error("missing commit: {0}")]
pub struct MissingCommitError(Digest);

/// A trait for types that have parent hashes.
pub trait Parents {
    /// The parent digests of this node.
    fn parents(&self) -> Set<Digest>;
}

#[allow(clippy::implicit_hasher)]
impl Parents for Set<Digest> {
    fn parents(&self) -> Set<Digest> {
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
}

/// An abstraction over stores of commits that can be looked up by their digest.
pub trait CommitStore<'a> {
    /// The type of node stored in the commit store.
    type Node: Parents;

    /// The error type returned when a lookup fails.
    type LookupError: Error;

    /// Looks up a commit by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`Self::LookupError`] if the lookup fails.
    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError>;

    /// Constructs a fragment of the commit history starting from the given head digest,
    ///
    /// # Errors
    ///
    /// Returns a [`Self::LookupError`] if any lookup fails.
    fn fragment<D: DepthMetric>(
        &self,
        head_digest: Digest,
        known_fragment_states: &Map<Digest, FragmentState<Self::Node>>,
        strategy: &D,
    ) -> Result<FragmentState<Self::Node>, FragmentError<'a, Self>> {
        let min_depth = strategy.to_depth(head_digest);

        let mut visited: Set<Digest> = Set::from([head_digest]);
        let mut members: Set<Digest> = Set::from([head_digest]);

        let mut boundary: Map<Digest, Self::Node> = Map::new();
        let mut checkpoints: Set<Digest> = Set::new();

        let head_change = self
            .lookup(head_digest)
            .map_err(|e| FragmentError::LookupError(e))?
            .ok_or_else(|| FragmentError::MissingCommit(MissingCommitError(head_digest)))?;
        let mut horizon: Set<Digest> = head_change.parents();

        while !horizon.is_empty() {
            let local_horizon = take(&mut horizon);
            for &digest in &local_horizon {
                let commit = self
                    .lookup(digest)
                    .map_err(|e| FragmentError::LookupError(e))?
                    .ok_or_else(|| FragmentError::MissingCommit(MissingCommitError(head_digest)))?;

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
                    horizon.extend(commit.parents().iter().filter(|&d| !visited.contains(d)));
                }
            }
        }

        // Cleanup

        let mut cleanup_horizon: Vec<Digest> = Vec::new();
        for (boundary_hash, boundary_change) in &boundary {
            members.remove(boundary_hash);

            if let Some(fragment_state) = known_fragment_states.get(boundary_hash) {
                for member in fragment_state.members() {
                    members.remove(member);
                }
                cleanup_horizon.extend(fragment_state.boundary().keys().copied());
            } else {
                let deps = boundary_change.parents();
                cleanup_horizon.extend(deps);
            }
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

                let commit = self
                    .lookup(digest)
                    .map_err(|e| FragmentError::LookupError(e))?
                    .ok_or_else(|| FragmentError::MissingCommit(MissingCommitError(head_digest)))?;

                cleanup_horizon.extend(commit.parents().iter().filter(|&d| !visited.contains(d)));
            }
        }

        Ok(FragmentState::new(
            head_digest,
            members,
            checkpoints,
            boundary,
        ))
    }

    /// Builds a fragment store starting from the given head digests.
    ///
    /// This reuses known fragment states to avoid redundant work.
    ///
    /// # Errors
    ///
    /// Returns a [`FragmentError`] if any lookup fails.
    fn build_fragment_store<'b, D: DepthMetric>(
        &self,
        head_digests: &[Digest],
        known_fragment_states: &'b mut Map<Digest, FragmentState<Self::Node>>,
        strategy: &D,
    ) -> Result<Vec<&'b FragmentState<Self::Node>>, FragmentError<'a, Self>> {
        let mut fresh_heads = Vec::new();
        let mut horizon = head_digests.to_vec();
        while let Some(head) = horizon.pop() {
            if let Some(state) = known_fragment_states.get(&head) {
                horizon.extend(state.boundary().keys().copied());
                continue;
            }

            let fragment_state = self.fragment(head, known_fragment_states, strategy)?;
            fresh_heads.push(head);
            horizon.extend(fragment_state.boundary().keys().copied());
            known_fragment_states.insert(head, fragment_state);
        }

        let mut fresh = Vec::with_capacity(fresh_heads.len());
        for head in fresh_heads {
            #[allow(clippy::expect_used)]
            let r = known_fragment_states.get(&head).expect("just inserted");
            fresh.push(r);
        }
        Ok(fresh)
    }
}

/// A depth strategy that counts leading zero bytes in the digest.
///
/// For example, the digest `0x00012345...` has a depth of 3,
/// the digest `0x00abcdef...` has a depth of 2,
/// and the digest `0x12345678...` has a depth of 0.
#[derive(Debug, Clone, Copy)]
pub struct CountLeadingZeroBytes;

impl DepthMetric for CountLeadingZeroBytes {
    fn to_depth(&self, digest: Digest) -> Depth {
        let mut acc = 0;
        for &byte in digest.as_bytes() {
            if byte == 0 {
                acc += 1;
            } else {
                break;
            }
        }
        Depth(acc)
    }
}

/// A depth strategy that counts trailing zeros in the digest in a given base.
#[derive(Debug, Clone, Copy)]
pub struct CountTrailingZerosInBase(NonZero<u8>);

impl CountTrailingZerosInBase {
    /// Creates a new `CountTrailingZerosInBase` strategy for the given base.
    ///
    /// # Panics
    ///
    /// Panics if `base` is less than 2.
    #[must_use]
    pub const fn new(base: NonZero<u8>) -> Self {
        Self(base)
    }
}

impl From<NonZero<u8>> for CountTrailingZerosInBase {
    fn from(base: NonZero<u8>) -> Self {
        Self::new(base)
    }
}

impl From<CountTrailingZerosInBase> for NonZero<u8> {
    fn from(strategy: CountTrailingZerosInBase) -> Self {
        strategy.0
    }
}

impl From<CountTrailingZerosInBase> for u8 {
    fn from(strategy: CountTrailingZerosInBase) -> Self {
        strategy.0.into()
    }
}

impl DepthMetric for CountTrailingZerosInBase {
    fn to_depth(&self, digest: Digest) -> Depth {
        let arr = digest.as_bytes();
        let inner_depth: u8 = self.0.into();
        let (_, bytes) = num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, arr)
            .to_radix_be(inner_depth.into());

        #[allow(clippy::expect_used)]
        let int = u32::try_from(bytes.into_iter().rev().take_while(|&i| i == 0).count())
            .expect("u32 should be big enough, but isn't");

        Depth(int)
    }
}

/// `Fragment`s are a consistent unit of document history,
/// which may end before the complete history is covered.
/// In this way, a document can be broken up into a series
/// of `Fragment`s that are consistent across replicas.
///
/// This is an experimental API, the fragment API is subject to change
/// and so should not be used in production just yet.
#[derive(Debug, Clone)]
pub struct FragmentState<T> {
    head_digest: Digest,
    members: Set<Digest>,
    checkpoints: Set<Digest>,
    boundary: Map<Digest, T>,
}

impl<T> FragmentState<T> {
    /// Create a new `FragmentState`.
    #[must_use]
    pub const fn new(
        head_digest: Digest,
        members: Set<Digest>,
        checkpoints: Set<Digest>,
        boundary: Map<Digest, T>,
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
    /// the rest of the fragment is built.
    #[must_use]
    pub const fn head_digest(&self) -> Digest {
        self.head_digest
    }

    /// All members of the fragment.
    ///
    /// This includes all history between the `head_digest`
    /// and the `boundary` (not including the boundary elements).
    #[must_use]
    pub const fn members(&self) -> &Set<Digest> {
        &self.members
    }

    /// The checkpoints of the fragment.
    ///
    /// These are all of the [`Digest`]s that match a valid level
    /// below the target, so that it is possible to know which other fragments
    /// this one covers.
    #[must_use]
    pub const fn checkpoints(&self) -> &Set<Digest> {
        &self.checkpoints
    }

    /// The boundary from which the next set of fragments would be built.
    #[must_use]
    pub const fn boundary(&self) -> &Map<Digest, T> {
        &self.boundary
    }

    /// Converts into a [`Fragment`] with the given [`BlobMeta`].
    #[must_use]
    pub fn to_fragment(self, blob_meta: BlobMeta) -> Fragment {
        Fragment::new(
            self.head_digest,
            self.boundary.keys().copied().collect(),
            self.checkpoints.iter().copied().collect(),
            blob_meta,
        )
    }
}
