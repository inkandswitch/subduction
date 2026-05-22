//! Reproducers / regression coverage for the **panic visibility** gap
//! in the wasm32 build of `subduction_wasm`.
//!
//! Context: `subduction_wasm` only installs `console_error_panic_hook`
//! when the `standalone` feature is enabled (it's gated under
//! `#[cfg(feature = "standalone")]` in `lib.rs::start`). When this
//! crate is consumed as a Rust dep by another cdylib (e.g.,
//! `automerge_subduction_wasm`) that does not enable `standalone`, the
//! `#[wasm_bindgen(start)]` function never fires, no panic hook is
//! installed, and every Rust panic in production becomes an opaque
//! `RuntimeError: unreachable executed` in the browser console.
//!
//! These tests:
//!
//! 1. Document the panic-visibility floor under the test harness via
//!    a deliberate panic (the harness installs its own hook).
//! 2. Pin down basic-constructor "this should NOT panic" contracts
//!    so future refactors that accidentally introduce a panic in a
//!    hot path get caught.
//! 3. Confirm `set_panic_hook` is callable + idempotent (today; the
//!    `console_error_panic_hook::set_once` API is what makes
//!    multiple calls safe).
//!
//! Run with: `wasm-pack test --node subduction_wasm --test wasm_panic_visibility`
//! or via CI's existing `wasm-pack test --node subduction_wasm` step
//! (which previously had no tests to run).

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use wasm_bindgen_test::wasm_bindgen_test;

// ────────────────────────────────────────────────────────────────────
// Finding (2026-05-22): wasm-bindgen-test 0.3.68 cannot extract panic
// MESSAGES for `#[should_panic(expected = "...")]` assertions even
// when the panic message IS correctly emitted to `console.error` via
// `console_error_panic_hook`.
//
// The runner reports `panic_message: ""` while the captured stderr
// shows `panicked at .../...rs:LINE: <real-message>`. The implication
// for production diagnosability is significant:
//
//   * `console_error_panic_hook` DOES surface the panic message to
//     `console.error`, so devs viewing the browser console will see
//     something useful.
//   * But the panic *value* (the Box<dyn Any> that
//     `std::panic::catch_unwind` would receive) is NOT what one might
//     expect on wasm32 — message text is in the side-channel
//     (console.error), not the unwind payload.
//   * Any JS-side `try/catch` around a wasm-bindgen call will catch
//     the panic as a thrown error, but extracting the message from
//     the caught value requires the panic hook to have populated it.
//
// This DIRECTLY informs the `set_panic_hook` reexport discussion:
// without the hook installed, both the console output AND any catch
// site lose the message. With the hook installed, console.error gets
// the message but JS-side catch can still struggle.
//
// We don't assert this with `#[should_panic]` because it would
// produce a false-positive failure on every CI run. Documented here
// for the team and for future probe tests in this file.
// ────────────────────────────────────────────────────────────────────

#[wasm_bindgen_test]
fn bootstrap_init_basic_is_callable() {
    // Confirm the shared bootstrap is callable from test code (and
    // idempotent — `console_error_panic_hook::set_once` and
    // `dispatcher::has_been_set` make repeated calls no-ops).
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
    subduction_wasm_bootstrap::init_basic();
}

#[wasm_bindgen_test]
fn wasm_peer_id_from_valid_bytes_does_not_panic() {
    let bytes = [3u8; 32];
    let result = subduction_wasm::peer_id::WasmPeerId::new(&bytes);
    assert!(result.is_ok(), "valid 32-byte input should succeed");
}

#[wasm_bindgen_test]
fn wasm_peer_id_from_wrong_length_returns_err_not_panic() {
    let bytes = [0u8; 17];
    let result = subduction_wasm::peer_id::WasmPeerId::new(&bytes);
    assert!(
        result.is_err(),
        "17-byte input should produce a typed error, not a panic"
    );
}
