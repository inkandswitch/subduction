//! # Wasm bindings for Sedimentree.
//!
//! This crate provides JavaScript/Wasm bindings for Sedimentree data structures.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Module entry point. Installs the panic hook and the shared rich tracing
/// stack via [`subduction_wasm_bootstrap::init_rich_from_env`] (initial level
/// from `SUBDUCTION_LOG_LEVEL`, default WARN). Idempotent; the first cdylib to
/// install a subscriber wins.
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_sedimentree_wasm() {
    subduction_wasm_bootstrap::init_rich_from_env();
}

/// Set the log level at runtime. Valid: `"trace"`, `"debug"`, `"info"`,
/// `"warn"`, `"error"`, `"off"`. Persisted to `localStorage`.
///
/// This crate is the single definition site for the workspace's Wasm log
/// controls. Crates that depend on it (`subduction_wasm`, and transitively the
/// umbrella) re-export these via `pub use` rather than redefining them, so the
/// JS export appears exactly once per bundle with no duplicate-symbol clash.
///
/// # Errors
///
/// Returns an error if the level string is invalid or tracing is uninitialized.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = setSubductionLogLevel)]
pub fn set_subduction_log_level(level: &str) -> Result<(), wasm_bindgen::JsValue> {
    subduction_wasm_bootstrap::set_log_level(level).map_err(|e| wasm_bindgen::JsValue::from_str(&e))
}

/// Forward every tracing event to a JavaScript callback
/// `(level, target, message, fields)`. See [`set_subduction_log_level`].
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn set_subduction_logger(callback: js_sys::Function) {
    subduction_wasm_bootstrap::set_subduction_logger(callback);
}

/// Clear the JavaScript logger callback registered via [`set_subduction_logger`].
/// See [`set_subduction_log_level`].
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn clear_subduction_logger() {
    subduction_wasm_bootstrap::clear_subduction_logger();
}

pub mod commit_id;
pub mod depth;
pub mod digest;
pub mod fragment;
pub mod loose_commit;
pub mod sedimentree;
pub mod sedimentree_id;
pub mod signed;
pub mod storage;
