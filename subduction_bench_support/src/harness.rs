//! Shared benchmark-harness helpers.
//!
//! Feature-gated so users of the crate who only need workload generators (e.g., a wasm bench
//! that rolls its own timing loop) aren't forced to pull in Criterion.

#[cfg(feature = "criterion_harness")]
pub mod criterion;
