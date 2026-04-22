//! Shared cross-cutting infrastructure for Subduction benchmarks.
//!
//! # What lives here
//!
//! This crate owns bench machinery that doesn't fit in any single library crate:
//!
//! | Module      | Contents                                                        |
//! |-------------|-----------------------------------------------------------------|
//! | [`scale`]   | `Scale::{Micro, Medium, Realistic}` tier + feature gates        |
//! | [`harness`] | Shared Criterion config (flamegraph profiler, sample rates)     |
//! | [`fixtures`]| egwalker `.am` test vectors, FS tempdir helpers, real-world CAS |
//!
//! # What _doesn't_ live here
//!
//! **Workload generators live with the types they generate**. Each library crate exposes its
//! own bench helpers behind a `test_utils` feature:
//!
//! | For types in…          | Import from…                                           |
//! |------------------------|--------------------------------------------------------|
//! | `sedimentree_core`     | `sedimentree_core::test_utils`                         |
//! | `subduction_crypto`    | `subduction_crypto::test_utils` (feature `test_utils`) |
//! | `subduction_core`      | `subduction_core::test_utils` (feature `test_utils`)   |
//!
//! Cross-crate compositions (e.g. "build a sedimentree, sign all its commits with a
//! deterministic `MemorySigner`, and wrap in `VerifiedMeta`") live in the [`cross_crate`]
//! module below when they're needed — currently empty; populated as benches demand.
//!
//! # Crate status
//!
//! `publish = false`. Dev-only. Never goes to crates.io.

#![warn(missing_docs)]

pub mod cross_crate;
pub mod fixtures;
pub mod harness;
pub mod scale;
