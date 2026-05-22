//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Entry point called when the Wasm module is instantiated.
///
/// Installs the panic hook and a baseline `tracing` subscriber via
/// [`subduction_wasm_bootstrap::init_basic`].
///
/// Both are idempotent and chain-safe: `wasm-bindgen` 0.2.118+ chains
/// every `#[wasm_bindgen(start)]` across all rlibs linked into one
/// cdylib via `__wbindgen_start`. When this crate is consumed as a
/// Rust dep of `automerge_subduction_wasm` (the umbrella with the
/// richer tracing setup), that umbrella's start fires FIRST and
/// installs its own subscriber; this start then becomes a no-op for
/// tracing (the bootstrap checks `dispatcher::has_been_set`) while
/// still ensuring the panic hook is set if it wasn't already. When
/// this crate ships standalone as `@inkandswitch/subduction`, its
/// start is the only one and provides full diagnostic baseline on
/// its own. See `tests/dual_start_supported.rs` for regression
/// coverage of the chaining behavior.
///
/// `start, private` registers this in the `__wbindgen_start` chain
/// without emitting a public Wasm export named `start`. Multiple
/// rlibs all defining `pub fn start()` would otherwise collide on the
/// shared `start` export at link time. Added in wasm-bindgen 0.2.118.
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_subduction_wasm() {
    subduction_wasm_bootstrap::init_basic();
}

/// Deliberately panic, for E2E panic-visibility probes.
///
/// Used by `subduction_wasm/e2e/panic_visibility.spec.ts` to measure
/// what fidelity panic messages reach JS-side surfaces (`console.error`,
/// `try/catch`, `window.onerror`, etc.) in the production-shape Wasm
/// bundle. Empirical result with the panic hook installed:
///
///   - `console.error` receives the full `panicked at ...:LINE:COL: <msg>`.
///   - JS `try/catch` sees only `RuntimeError: unreachable` — the
///     panic text is NOT in `e.message`. Side-channel only.
///
/// Gated behind the `e2e_panic_probe` feature so it doesn't ship in
/// default release builds.
///
/// # Panics
///
/// **Always.** This function's entire purpose is to panic — that is the
/// signal the E2E probe spec measures. If `message` is `Some(s)`, the
/// panic message is `s`; otherwise it is `"default-e2e-panic-marker"`.
#[cfg(feature = "e2e_panic_probe")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = "__testPanicForE2E")]
#[allow(clippy::panic, clippy::needless_pass_by_value)]
pub fn test_panic_for_e2e(message: Option<alloc::string::String>) {
    let msg = message.unwrap_or_else(|| "default-e2e-panic-marker".into());
    panic!("{msg}");
}

pub mod batch_input;
pub mod clock;
pub mod ephemeral;
pub mod error;
pub mod fragment;
pub(crate) mod handler;
pub mod memory_signer;
pub mod peer_id;
pub mod policy;
pub mod remote_heads;
pub mod signer;
pub mod subduction;
pub mod sync_stats;
pub mod timer;
pub mod topic;
pub mod transport;
pub mod wire;
