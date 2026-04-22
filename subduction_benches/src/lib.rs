//! Shared benchmark workloads, fixtures, and harness helpers for the Subduction workspace.
//!
//! # Overview
//!
//! This crate centralises benchmark plumbing that would otherwise be duplicated (and drift)
//! across every `benches/` directory in the workspace. It is `publish = false` and exists
//! solely as a dev-dependency.
//!
//! ## Module layout
//!
//! ```text
//! subduction_benches
//! ├── workload   — deterministic generators (commits, fragments, trees, signers, …)
//! ├── fixtures   — optional filesystem / test-vector fixtures (feature-gated)
//! ├── harness    — shared Criterion / Wasm bench harness helpers (feature-gated)
//! └── scale      — `Scale::{Micro, Medium, Realistic}` + feature gates
//! ```
//!
//! ## Design principles
//!
//! | Principle              | Rationale                                                     |
//! |------------------------|---------------------------------------------------------------|
//! | Deterministic by seed  | Benchmarks need to be reproducible across runs + machines     |
//! | Tiered workloads       | Same bench can run as micro / medium / realistic via features |
//! | `no_std`-friendly core | Workload generators depend only on `std`; no tokio / I/O in core |
//! | Optional heavy deps    | Criterion, Automerge, tempfile gated behind features          |
//!
//! ## Typical usage from a bench
//!
//! ```ignore
//! use subduction_benches::workload::{commits, trees};
//! use subduction_benches::scale::Scale;
//!
//! let tree = trees::synthetic_sedimentree(
//!     Scale::Medium.fragment_count(),
//!     Scale::Medium.commit_count(),
//!     /* seed */ 42,
//! );
//! ```

#![warn(missing_docs)]

pub mod fixtures;
pub mod harness;
pub mod scale;
pub mod workload;
