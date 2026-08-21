//! # Subduction Runtime
//!
//! The generic async driver for [`subduction_protocol`]'s sans-io state
//! machine, written tagless-final style: platform capabilities are traits
//! over [`future_form::FutureForm`], so one driver serves both
//! multi-threaded (tokio) and single-threaded (browser Wasm) targets.
//!
//! ## Design
//!
//! An actor funnel owns the machine — no locks anywhere:
//!
//! ```text
//!  conn read tasks ─┐  events   ┌────────────────┐  effects
//!  timer wheel     ─┼─────────▶ │ driver task     │ ─────────▶ transports
//!  API handles     ─┘ (channel) │ (&mut Machine)  │            storage
//!                               └────────────────┘            crypto workers
//!                                       │ oneshot answers
//!                                       ▼
//!                                  API callers
//! ```
//!
//! Expensive work (hashing, signing, verification, blob IO) never runs in
//! the driver's turn: the machine emits it as effects, workers execute it,
//! and completions flow back through the funnel as events. Telemetry:
//! boundary metrics are derived here from effect execution (tier 1), and
//! machine outcomes are pattern-matched into `metrics`/`tracing` (tier 3).
//!
//! ## Status
//!
//! Scaffolding — traits and the actor loop land after the
//! protocol vocabulary stabilizes. See `design/sans-io.md`.

#![cfg_attr(docsrs, feature(doc_cfg))]

// Planned modules:
// pub mod clock;      — Clock trait (monotonic Timestamp source)
// pub mod crypto;     — CryptoWorker trait (sign/verify/digest offload)
// pub mod driver;     — actor funnel + effect executor + timer wheel
// pub mod spawn;      — Spawn trait
// pub mod storage;    — Storage trait + in-memory impl + conformance suite
// pub mod telemetry;  — tier-1/tier-3 mapping (feature-gated)
// pub mod transport;  — byte Transport trait
