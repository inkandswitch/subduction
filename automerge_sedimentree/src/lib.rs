//! # Automerge integration for Sedimentree
//!
//! The ingestion API lives in [`ingest`]. It builds a [`Sedimentree`] from
//! an [`Automerge`] document via [`Automerge::fragments`] and
//! [`Automerge::bundle_fragments`] (upstream automerge's native
//! fragmentizer), replacing the older `CommitStore`-based path that this
//! crate previously exposed.
//!
//! [`Sedimentree`]: sedimentree_core::sedimentree::Sedimentree
//! [`Automerge`]: automerge::Automerge
//! [`Automerge::fragments`]: automerge::Automerge::fragments
//! [`Automerge::bundle_fragments`]: automerge::Automerge::bundle_fragments

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod ingest;
