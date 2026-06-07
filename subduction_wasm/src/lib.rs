//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Module entry point. Installs the panic hook and the shared rich tracing
/// stack (reloadable level filter + console layer + JS-callback layer) via
/// [`subduction_wasm_bootstrap::init_rich_from_env`]. The initial level is read
/// from `localStorage` / `process.env` (`SUBDUCTION_LOG_LEVEL`), defaulting to
/// WARN. Idempotent; the first cdylib to install a subscriber wins.
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_subduction_wasm() {
    subduction_wasm_bootstrap::init_rich_from_env();
}

/// Set the log level at runtime. Valid: `"trace"`, `"debug"`, `"info"`,
/// `"warn"`, `"error"`, `"off"`. Persisted to `localStorage`.
///
/// # Errors
///
/// Returns an error if the level string is invalid or tracing is uninitialized.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = setSubductionLogLevel)]
pub fn set_subduction_log_level(level: &str) -> Result<(), wasm_bindgen::JsValue> {
    subduction_wasm_bootstrap::set_log_level(level)
        .map_err(|e| wasm_bindgen::JsValue::from_str(&e))
}

/// Forward every tracing event to a JavaScript callback
/// `(level, target, message, fields)`.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn set_subduction_logger(callback: js_sys::Function) {
    subduction_wasm_bootstrap::set_subduction_logger(callback);
}

/// Clear the JavaScript logger callback registered via [`set_subduction_logger`].
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn clear_subduction_logger() {
    subduction_wasm_bootstrap::clear_subduction_logger();
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
