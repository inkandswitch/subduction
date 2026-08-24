//! # Subduction Runtime
//!
//! The generic async driver for [`subduction_protocol`]'s sans-io state
//! machine, written tagless-final style: platform capabilities are traits
//! over [`future_form::FutureForm`], so one driver serves both
//! multi-threaded (tokio) and single-threaded (browser Wasm) targets.
//!
//! ## Design
//!
//! An actor funnel owns the node — no locks anywhere:
//!
//! ```text
//!  read-loop futures ─┐  inputs   ┌────────────────┐  effects
//!  API handles       ─┼─────────▶ │ driver task     │ ─────────▶ transports
//!  timer (Clock)     ─┘ (channel) │ (&mut Node)     │            storage
//!                                 └────────────────┘            signer
//!                                         │ oneshot answers · app events
//!                                         ▼
//!                                    API callers
//! ```
//!
//! The driver task has exclusive access to the node, the frame table
//! (blob custody), and the connection registry. Capabilities are
//! injected: [`clock::Clock`], [`transport::Transport`],
//! [`storage::Storage`], [`policy::Policy`], and
//! [`subduction_crypto::signer::Signer`]. Scheduling stays with the
//! caller — [`driver::handle::Handle::connect`] hands back each
//! connection's read-loop future for the application to spawn on its own
//! runtime.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod clock;
#[cfg(feature = "conformance")]
#[cfg_attr(docsrs, doc(cfg(feature = "conformance")))]
pub mod conformance;
pub mod driver;
pub mod frames;
pub mod memory;
pub mod policy;
pub mod storage;
pub mod transport;
