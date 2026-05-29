//! # Automerge integration for Sedimentree
//!
//! The ingestion API lives in [`ingest`]. It builds a [`Sedimentree`] from
//! an [`Automerge`] document via [`Automerge::fragments`] and
//! [`Automerge::bundle_fragments`] (upstream automerge's native
//! fragmentizer), replacing the older `CommitStore`-based path that this
//! crate previously exposed.
//!
//! The [`indexed`] module retains a pre-indexed [`CommitStore`] impl that
//! drives `build_fragment_store` directly. It is no longer on the
//! production ingest path; tests and benches use it as a reference
//! implementation when validating the upstream fragmentizer output.
//!
//! [`Sedimentree`]: sedimentree_core::sedimentree::Sedimentree
//! [`CommitStore`]: sedimentree_core::commit::CommitStore
//! [`Automerge`]: automerge::Automerge
//! [`Automerge::fragments`]: automerge::Automerge::fragments
//! [`Automerge::bundle_fragments`]: automerge::Automerge::bundle_fragments

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod indexed;
pub mod ingest;
