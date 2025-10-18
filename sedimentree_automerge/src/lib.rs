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

use automerge::ChangeMetadata;
use sedimentree_core::{Depth, Digest};
use thiserror::Error;

impl Parents for ChangeMetadata<'_> {
    fn parents(&self) -> HashSet<Digest> {
        self.deps
            .iter()
            .map(|change_hash| Digest::from(change_hash.0))
            .collect()
    }
}

impl CommitStore for Automerge {
    type Commit = ChangeMetadata<'_>;
    type LookupError = Fixme;

    fn lookup(&self, digest: Digest) -> Result<Option<Self::Commit>, Self::LookupError> {
        let change = self.get_change(&automerge::ChangeHash(
            digest.as_bytes().try_into().unwrap(),
        ))?;
        Ok(change.metadata().clone())
    }
}
