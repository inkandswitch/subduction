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

// `extern crate alloc;` returns with the first Phase 1 module
// (`unused_extern_crates` is denied workspace-wide).

// Phase 1 modules land here:
// pub mod event;      — Event / Command vocabulary
// pub mod effect;     — Effect / storage & crypto op vocabulary
// pub mod machine;    — the Machine itself
// pub mod outcome;    — structured transition outcomes (tier-3 telemetry)
// pub mod stats;      — pull-based counters (tier-2 telemetry)
// pub mod timestamp;  — opaque driver-supplied monotonic time
// pub mod token;      — generation/sequence completion tokens
// pub mod wire;       — wire messages + codec (copied verbatim from legacy)
