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

use std::{collections::HashSet, convert::Infallible};

use automerge::{Automerge, ChangeMetadata};
use sedimentree_core::{
    blob::Digest,
    commit::{CommitStore, Parents},
};

/// A newtype wrapper around Automerge's [`ChangeMetadata`].
#[derive(Debug, Clone)]
pub struct SedimentreeChangeMetadata<'a>(ChangeMetadata<'a>);

impl<'a> From<ChangeMetadata<'a>> for SedimentreeChangeMetadata<'a> {
    fn from(value: ChangeMetadata<'a>) -> Self {
        Self(value)
    }
}

impl<'a> From<SedimentreeChangeMetadata<'a>> for ChangeMetadata<'a> {
    fn from(value: SedimentreeChangeMetadata<'a>) -> Self {
        value.0
    }
}

impl Parents for SedimentreeChangeMetadata<'_> {
    fn parents(&self) -> HashSet<Digest> {
        self.0
            .deps
            .iter()
            .map(|change_hash| Digest::from(change_hash.0))
            .collect()
    }
}

/// A newtype wrapper around [`Automerge`] for use as a Sedimentree commit store.
#[derive(Debug, Clone)]
pub struct SedimentreeAutomerge<'a>(&'a Automerge);

impl<'a> From<&'a Automerge> for SedimentreeAutomerge<'a> {
    fn from(value: &'a Automerge) -> Self {
        Self(value)
    }
}

impl<'a> From<SedimentreeAutomerge<'a>> for &'a Automerge {
    fn from(value: SedimentreeAutomerge<'a>) -> Self {
        value.0
    }
}

impl<'a> CommitStore<'a> for SedimentreeAutomerge<'a> {
    type Node = SedimentreeChangeMetadata<'a>;
    type LookupError = Infallible;

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Node>, Self::LookupError> {
        let change_hash = automerge::ChangeHash(*digest.as_bytes());
        let change_meta = self.0.get_change_meta_by_hash(&change_hash);
        Ok(change_meta.map(SedimentreeChangeMetadata::from))
    }
}
