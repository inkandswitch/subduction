//! # Subduction Protocol
//!
//! The sans-io core of Subduction: a pure state machine that makes every
//! protocol decision and performs no IO.
//!
//! ## Design
//!
//! The machine consumes [`Event`]s (bytes from the wire, timer expiry,
//! completions of driver-performed work, local commands) and emits
//! [`Effect`]s (frames to send, timers to set, storage and crypto
//! operations to perform). A driver — any driver: tokio, browser Wasm, or
//! a native platform binding — executes the effects and feeds the results
//! back in as events.
//!
//! ```text
//!            events                    effects
//!   driver ──────────▶ ┌───────────┐ ──────────▶ driver
//!                      │  Machine  │
//!   (IO, clocks,       │ (&mut, no │             (IO, clocks,
//!    crypto workers)   │  locks)   │              crypto workers)
//!                      └───────────┘
//! ```
//!
//! Nothing in this crate blocks, sleeps, spawns, locks, or tells the time.
//! `no_std + alloc` by construction.
//!
//! ## Status
//!
//! Phase 1 scaffolding — the event/effect vocabulary is being defined.
//! See `design/sans-io.md` in the repository root for the architecture
//! plan and `.ignore/PLAN.md` for the working task list.
//!
//! [`Event`]: crate::Event
//! [`Effect`]: crate::Effect

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

pub mod command;
pub mod effect;
pub mod event;
pub mod handshake;
pub mod id;
pub mod machine;
pub mod nonce_cache;
pub mod outcome;
pub mod peer_id;
pub mod remote_heads;
pub mod stats;
pub mod storage;
pub mod timestamp;
pub mod ticket;
pub mod wall_clock;
pub mod wire;
