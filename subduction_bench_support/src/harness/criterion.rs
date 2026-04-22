//! Shared Criterion configuration.
//!
//! Centralises the `PProfProfiler` sampling rate and Criterion defaults so every bench in the
//! workspace generates comparable flamegraphs and uses consistent measurement windows.
//!
//! ## Example
//!
//! ```ignore
//! use criterion::{Criterion, criterion_group, criterion_main};
//! use subduction_benches::harness::criterion::default_criterion;
//!
//! criterion_group! {
//!     name = benches;
//!     config = default_criterion();
//!     targets = my_bench
//! }
//! criterion_main!(benches);
//! ```

use ::criterion::Criterion;
use criterion_pprof::criterion::{Output, PProfProfiler};

/// Sampling rate for the pprof profiler (Hz).
///
/// 997 (close to 1 kHz, prime to avoid aliasing with periodic work) matches the convention
/// already in use across `sedimentree_core` / `subduction_core` / `subduction_websocket` benches.
pub const PROFILER_HZ: i32 = 997;

/// A `Criterion` configured with the workspace-standard pprof flamegraph profiler.
#[must_use]
pub fn default_criterion() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(PROFILER_HZ, Output::Flamegraph(None)))
}
