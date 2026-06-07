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
/// Only emitted when this crate is the primary cdylib being built (the
/// standalone `@automerge/sedimentree` bundle); when linked into the umbrella
/// the identical export is inherited from `subduction_wasm`, so gating here
/// avoids a duplicate-symbol link error.
///
/// # Errors
///
/// Returns an error if the level string is invalid or tracing is uninitialized.
#[cfg(all(target_arch = "wasm32", cdylib_primary))]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = setSubductionLogLevel)]
pub fn set_subduction_log_level(level: &str) -> Result<(), wasm_bindgen::JsValue> {
    subduction_wasm_bootstrap::set_log_level(level).map_err(|e| wasm_bindgen::JsValue::from_str(&e))
}

/// Forward every tracing event to a JavaScript callback
/// `(level, target, message, fields)`. Standalone-bundle only (see
/// [`set_subduction_log_level`]).
#[cfg(all(target_arch = "wasm32", cdylib_primary))]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn set_subduction_logger(callback: js_sys::Function) {
    subduction_wasm_bootstrap::set_subduction_logger(callback);
}

/// Clear the JavaScript logger callback registered via [`set_subduction_logger`].
/// Standalone-bundle only (see [`set_subduction_log_level`]).
#[cfg(all(target_arch = "wasm32", cdylib_primary))]
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
