//! # Subduction
//!
//! Subduction is a sync protocol for local-first applications built on [`Sedimentree`].
//!
//! ## Getting Started
//!
//! See the [`subduction`] module for the main [`Subduction`] type and **API guide**
//! covering naming conventions, API levels, and common patterns.
//!
//! ## Modules
//!
//! - [`subduction`] — Main sync manager and API guide
//! - [`connection`] — Connection traits and handshake protocol
//! - [`policy`] — Authorization policies ([`ConnectionPolicy`], [`StoragePolicy`])
//! - [`storage`] — Storage capabilities ([`Fetcher`], [`Putter`], [`Destroyer`])
//! - [`crypto`] — Cryptographic primitives ([`Signer`], [`Signed`], [`Verified`])
//! - [`peer`] — Peer identity ([`PeerId`])
//!
//! [`Sedimentree`]: sedimentree_core::sedimentree::Sedimentree
//! [`ConnectionPolicy`]: policy::ConnectionPolicy
//! [`StoragePolicy`]: policy::StoragePolicy
//! [`Fetcher`]: storage::fetcher::Fetcher
//! [`Putter`]: storage::putter::Putter
//! [`Destroyer`]: storage::destroyer::Destroyer
//! [`Signer`]: crypto::signer::Signer
//! [`Signed`]: crypto::signed::Signed
//! [`Verified`]: crypto::verified::Verified
//! [`PeerId`]: peer::id::PeerId

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod cbor;
pub mod connection;
pub mod crypto;
pub mod peer;
pub mod policy;
pub mod sharded_map;
pub mod storage;
pub mod subduction;
pub mod timestamp;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

pub use subduction::Subduction;

#[cfg(feature = "metrics")]
pub use storage::MetricsStorage;
