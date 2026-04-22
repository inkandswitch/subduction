//! Shared benchmark-harness helpers.
//!
//! Feature-gated so users of the crate who only need workload generators (e.g., a wasm bench
//! that rolls its own timing loop) aren't forced to pull in Criterion.
//!
//! | Module        | Cargo feature       | When to use                                         |
//! |---------------|---------------------|-----------------------------------------------------|
//! | [`criterion`] | `criterion_harness` | Native benches driven by Criterion + pprof          |
//! | [`wasm`]      | `wasm_harness`      | Wasm-bindgen-test benches driven by `performance.now()` |

#[cfg(feature = "criterion_harness")]
pub mod criterion;

#[cfg(all(feature = "wasm_harness", target_family = "wasm"))]
pub mod wasm;
