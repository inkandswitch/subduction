//! Regression guard for `wasm-bindgen` multi-start chaining and
//! bootstrap idempotency. Surfaces a failure if either the bootstrap
//! entry points stop being safe to call repeatedly or if the
//! `wasm-bindgen` pin moves to a version that changes the chain.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn bootstrap_init_basic_is_idempotent() {
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
}

#[wasm_bindgen_test]
fn bootstrap_install_panic_hook_is_idempotent() {
    subduction_wasm_bootstrap::install_panic_hook();
    subduction_wasm_bootstrap::install_panic_hook();
}

#[wasm_bindgen_test]
fn bootstrap_install_basic_tracing_is_idempotent() {
    subduction_wasm_bootstrap::install_basic_tracing();
    subduction_wasm_bootstrap::install_basic_tracing();
}

#[wasm_bindgen_test]
fn module_initialized_cleanly() {
    let bytes = [1u8; 32];
    assert!(subduction_wasm::peer_id::WasmPeerId::new(&bytes).is_ok());
    assert!(sedimentree_wasm::commit_id::WasmCommitId::new(&bytes).is_ok());
}
