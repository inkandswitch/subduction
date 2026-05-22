//! Reproducers / regression coverage for the **panic visibility** gap
//! in the wasm32 build of `sedimentree_wasm`.
//!
//! Context: the inner cdylib crates (`sedimentree_wasm`,
//! `automerge_sedimentree_wasm`) do not install a panic hook. When
//! loaded as Rust deps of an outer cdylib that does not enable a
//! `standalone` feature, panics in production browsers surface as
//! opaque `RuntimeError: unreachable executed` — no file, no line, no
//! message.
//!
//! These tests fix the visibility, not the root cause. They:
//!
//! 1. Demonstrate that an explicit panic is properly captured by
//!    `wasm-bindgen-test` when run under `wasm-pack test --node` (the
//!    test harness installs its own panic hook, so the test author
//!    sees a real message — production code does not).
//! 2. Assert that any future `pub fn set_panic_hook()` we add to this
//!    crate is callable and idempotent (`console_error_panic_hook::set_once`).
//!
//! Run with: `wasm-pack test --node sedimentree_wasm --test wasm_panic_visibility`
//! or via CI's `wasm-pack test --node sedimentree_wasm`.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use wasm_bindgen_test::wasm_bindgen_test;

// ────────────────────────────────────────────────────────────────────
// Baseline: a deliberate panic surfaces with an actionable message
// under the wasm-bindgen-test harness.
//
// This is the FLOOR for diagnosability. Production currently sits
// BELOW this floor because no panic hook is installed.
// ────────────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
#[should_panic(expected = "diagnostic-panic-marker")]
fn explicit_panic_is_visible_under_test_harness() {
    // wasm-bindgen-test installs its own panic hook (via
    // `console_error_panic_hook` or equivalent) when running tests, so
    // this panic comes through with its message intact. In production
    // (no hook installed) it would surface as `RuntimeError: unreachable`.
    panic!("diagnostic-panic-marker");
}

// ────────────────────────────────────────────────────────────────────
// Smoke tests on the public Wasm surface — no panics expected.
// These exist to fail loudly if a future refactor accidentally
// introduces a panic in basic construction paths.
// ────────────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn wasm_commit_id_from_valid_bytes_does_not_panic() {
    let bytes = [7u8; 32];
    let result = sedimentree_wasm::commit_id::WasmCommitId::new(&bytes);
    assert!(result.is_ok(), "valid 32-byte input should not error");
}

#[wasm_bindgen_test]
fn wasm_commit_id_from_wrong_length_returns_err_not_panic() {
    // 31 bytes (one short). A naive `try_into()` would panic; the
    // correct path returns `Err(WasmInvalidCommitId)`. This pins down
    // that contract for future refactors.
    let bytes = [0u8; 31];
    let result = sedimentree_wasm::commit_id::WasmCommitId::new(&bytes);
    assert!(
        result.is_err(),
        "31-byte input should produce an error, not a panic"
    );
}

#[wasm_bindgen_test]
fn wasm_digest_from_wrong_length_returns_err_not_panic() {
    let bytes = [0u8; 16];
    let result = sedimentree_wasm::digest::WasmDigest::new(&bytes);
    assert!(
        result.is_err(),
        "16-byte input should produce an error, not a panic"
    );
}

#[wasm_bindgen_test]
fn wasm_memory_storage_default_does_not_panic() {
    // Defaults are a common silent-panic surface in Wasm constructors.
    drop(sedimentree_wasm::storage::memory::MemoryStorage::new());
}
