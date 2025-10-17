//! # Automerge integration for Sedimentree

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    error::Error,
    mem::take,
};

use automerge::{Automerge, Bundle, Change, ChangeHash};
use sedimentree_core::{Depth, Digest};
use thiserror::Error;

pub(crate) const HASH_SIZE: usize = 32;

pub trait BoundaryMetric {
    fn is_boundary(&self, digest: Digest) -> bool;
}

#[derive(Debug, Clone, Copy, Error)]
#[error("unable to resolve all dependencies")]
pub struct UnableToResolveAllDependencies;

pub trait ChangeStore {
    type Item: Parents + Clone; // FIXME maybe this is Self?
    type FooError: Error;
    fn lookup(&self, digest: Digest) -> Result<&Self::Item, Self::FooError>;
}

pub trait Parents {
    type ParentError: Error;
    fn parents(&self) -> Result<Vec<Digest>, Self::ParentError>;
}

#[derive(Debug, Clone)]
pub struct TrailingBytesChunker(Depth);

impl BoundaryMetric for TrailingBytesChunker {
    fn is_boundary(&self, digest: Digest) -> bool {
        let bytes = digest.as_bytes();
        let idx = HASH_SIZE - (self.0 .0 as usize);
        bytes[idx..].iter().all(|&b| b == 0)
    }
}

#[derive(Debug, Clone)]
pub struct Boundary<T>(pub HashMap<Digest, T>);

pub trait Baz: ChangeStore {
    type Strategy: BoundaryMetric;

    fn fragment(
        &self,
        head_digest: Digest,
        strat: &Self::Strategy,
    ) -> Result<MyFragment<Self::Item>, UnableToResolveAllDependencies> {
        let mut boundary: HashMap<Digest, Self::Item> = HashMap::new();
        let mut visited: HashSet<Digest> = HashSet::from([head_digest]);
        let mut members: HashSet<Digest> = HashSet::from([head_digest]);

        let head_change = self
            .lookup(head_digest)
            .map_err(|_| UnableToResolveAllDependencies)?;
        let mut horizon: Vec<Digest> = head_change.parents().expect("FIXME").to_vec(); // FIXME uglu

        while !horizon.is_empty() {
            let local_horizon = take(&mut horizon);
            for &digest in &local_horizon {
                let item = self
                    .lookup(digest)
                    .map_err(|_| UnableToResolveAllDependencies)?;

                let is_newly_visited = visited.insert(digest);
                if !is_newly_visited {
                    continue;
                }

                members.insert(digest);

                if strat.is_boundary(digest) {
                    boundary.insert(digest, item.clone());
                } else {
                    horizon.extend(
                        item.parents()
                            .expect("FIXME")
                            .iter()
                            .filter(|&d| !visited.contains(d)),
                    )
                }
            }
        }

        // Cleanup

        let mut cleanup_horizon: Vec<Digest> = Vec::new(); // boundary.keys().copied().collect::<Vec<_>>();
        for (boundary_hash, boundary_change) in boundary.iter() {
            members.remove(boundary_hash);
            let deps = boundary_change.parents().expect("FIXME");
            cleanup_horizon.extend(deps);
        }
        while !cleanup_horizon.is_empty() {
            let local_cleanup_horizon = take(&mut cleanup_horizon);
            for digest in local_cleanup_horizon {
                members.remove(&digest);
                boundary.remove(&digest); // NOTE if one boundary covers another

                let is_newly_visited = visited.insert(digest);
                if !is_newly_visited {
                    continue;
                }

                let item = self
                    .lookup(digest)
                    .map_err(|_| UnableToResolveAllDependencies)?;
                cleanup_horizon.extend(
                    item.parents()
                        .expect("FIXME")
                        .iter()
                        .filter(|&d| !visited.contains(d)),
                )
            }
        }

        Ok(MyFragment::new(head_digest, members, boundary))
    }
}

impl Parents for Change {
    type ParentError = Infallible;

    fn parents(&self) -> Result<Vec<Digest>, Self::ParentError> {
        let parent_hashes: Vec<Digest> = self.deps().iter().map(hash_to_digest).collect();
        Ok(parent_hashes.clone())
    }
}

fn hash_to_digest(change_hash: &ChangeHash) -> Digest {
    Digest::from(change_hash.0)
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
pub struct MyFragment<T> {
    head_digest: Digest,
    members: HashSet<Digest>,
    boundary: HashMap<Digest, T>,
}

impl<T> MyFragment<T> {
    pub fn new(
        head_digest: Digest,
        members: HashSet<Digest>,
        boundary: HashMap<Digest, T>,
    ) -> Self {
        Self {
            head_digest,
            members,
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

    /// The boundary from which the next set of fragments would be built.
    pub fn boundary(&self) -> &HashMap<Digest, T> {
        &self.boundary
    }
}
