//! Regression coverage for the multi-`#[wasm_bindgen(start)]` chaining
//! behavior we depend on.
//!
//! `wasm-bindgen` 0.2.118 ([#5081](https://github.com/rustwasm/wasm-bindgen/pull/5081))
//! added support for multiple `#[wasm_bindgen(start)]` functions in
//! one cdylib, chained inside the generated `__wbindgen_start`. We
//! depend on this behavior so that every rlib pulled in by the
//! outermost cdylib (e.g. `automerge_subduction_wasm` pulling in
//! `subduction_wasm`, `sedimentree_wasm`, `automerge_sedimentree_wasm`,
//! and `subduction_wasm_bootstrap`) can install its own startup logic
//! without conflicting with the others.
//!
//! Today, every Wasm cdylib's start function calls
//! [`subduction_wasm_bootstrap::init_basic`] (or, for the umbrella,
//! its richer `init` equivalent), which is idempotent. So all chained
//! starts run, but only the first to do real work succeeds and the
//! rest no-op.
//!
//! This test exists to:
//!   1. Surface a regression if the wasm-bindgen pin moves to a
//!      version that drops or changes the chaining behavior.
//!   2. Confirm that calling the bootstrap entry points directly is
//!      safe and idempotent.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use wasm_bindgen_test::wasm_bindgen_test;

/// Calling the shared bootstrap is idempotent: repeated invocations
/// after the chained start functions have already run must not
/// panic, re-install the hook, or replace the tracing subscriber.
#[wasm_bindgen_test]
fn bootstrap_init_basic_is_idempotent() {
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
}

/// Same for the panic-hook half called on its own.
#[wasm_bindgen_test]
fn bootstrap_install_panic_hook_is_idempotent() {
    subduction_wasm_bootstrap::install_panic_hook();
    subduction_wasm_bootstrap::install_panic_hook();
}

/// And for the tracing half called on its own.
#[wasm_bindgen_test]
fn bootstrap_install_basic_tracing_is_idempotent() {
    subduction_wasm_bootstrap::install_basic_tracing();
    subduction_wasm_bootstrap::install_basic_tracing();
}

/// Sanity: a basic API call from each crate doesn't panic. If either
/// crate's start function panicked (which would happen pre-0.2.118 if
/// two starts somehow got compiled in and conflicted), the test
/// harness would have already aborted before reaching this point.
#[wasm_bindgen_test]
fn module_initialized_cleanly() {
    let bytes = [1u8; 32];
    assert!(subduction_wasm::peer_id::WasmPeerId::new(&bytes).is_ok());
    assert!(sedimentree_wasm::commit_id::WasmCommitId::new(&bytes).is_ok());
}
