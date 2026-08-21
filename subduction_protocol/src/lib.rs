//! # Subduction Protocol
//!
//! The sans-io core of Subduction: pure state machines that make every
//! protocol decision and perform no IO.
//!
//! ## Design
//!
//! A [`Node`](node::Node) composes one [`ConnMachine`] per transport
//! connection (handshake + inline forgery rejection), one [`CoreMachine`]
//! (trees, sync sessions, subscriptions), and the router between them.
//! Machines exchange [`Sealed`](edge::Sealed) messages over typed edges
//! that cannot be forged or reordered by driver code; the driver sees
//! only leaf work.
//!
//! The node consumes [`NodeEvent`]s (bytes from the wire, timer expiry,
//! completions of driver-performed work, local commands) and emits
//! [`NodeEffect`]s (frames to send, storage ops, signing requests). A
//! driver — any driver: tokio, browser Wasm, or a native platform
//! binding — executes the effects and feeds the results back as events.
//!
//! ```text
//!         NodeEvent            ┌─ Node ─────────────────────┐
//!   driver ──────────────────▶ │ ConnMachine ⇄ CoreMachine  │
//!                              │  (sealed edges, routed     │
//!   (IO, clocks, storage,      │   in-turn, no locks)       │
//!    signer custody)     ◀──── └────────────────────────────┘
//!         NodeEffect
//! ```
//!
//! Nothing in this crate blocks, sleeps, spawns, locks, or tells the time.
//! `no_std + alloc` by construction.
//!
//! ## Status
//!
//! Phase 2.5 — the split-machine architecture is complete and the old
//! single-machine implementation has been deleted. See
//! `design/sans-io.md` in the repository root for the architecture plan
//! and `.ignore/PLAN.md` for the working task list.
//!
//! [`ConnMachine`]: conn_machine::ConnMachine
//! [`CoreMachine`]: core_machine::CoreMachine
//! [`NodeEvent`]: node::NodeEvent
//! [`NodeEffect`]: node::NodeEffect

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

pub mod blob_ref;
pub mod command;
pub mod conn_machine;
pub mod core_machine;
pub mod edge;
pub mod effect;
pub mod event;
pub mod handshake;
pub mod id;
pub mod node;
pub mod nonce_cache;
pub mod outcome;
pub mod peer_id;
pub mod remote_heads;
pub mod stats;
pub mod storage;
pub mod ticket;
pub mod timestamp;
pub mod wall_clock;
pub mod wire;
