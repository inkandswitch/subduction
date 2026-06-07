//! # Wasm Bindings for the Subduction/Automerge integration.
//!
//! This crate re-exports all types from `subduction_wasm` and adds the
//! Automerge-specific helpers (e.g. [`commit_id::commit_id_of_base58_id`])
//! to provide a single unified entry point for TypeScript/JavaScript consumers.
//!
//! ## Log Level Configuration
//!
//! The default log level is `warn`. You can change it at runtime or configure
//! it to be read at startup:
//!
//! ### Live adjustment (no reload required)
//!
//! ```js
//! // From the browser console or application code:
//! wasm.setSubductionLogLevel("debug")
//! ```
//!
//! ### Persistent configuration
//!
//! **Browser:** Set `SUBDUCTION_LOG_LEVEL` in `localStorage`:
//!
//! ```js
//! localStorage.setItem("SUBDUCTION_LOG_LEVEL", "debug")
//! // Takes effect on next page load
//! ```
//!
//! **Node.js:** Set the `SUBDUCTION_LOG_LEVEL` environment variable:
//!
//! ```sh
//! SUBDUCTION_LOG_LEVEL=debug node your-app.js
//! ```
//!
//! Valid levels: `trace`, `debug`, `info`, `warn`, `error`, `off`

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)] // wasm_bindgen doens't like const
#![allow(ambiguous_glob_reexports)] // Intentional: umbrella crate for JS consumers

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod commit_id;
pub mod error;

// Re-export everything from subduction_wasm. This includes the shared logging
// API — `setSubductionLogLevel`, `set_subduction_logger`,
// `clear_subduction_logger` — so the umbrella exposes the same surface as every
// other bundle without redefining (and thus duplicating) those exports.
pub use subduction_wasm::*;

use wasm_bindgen::prelude::*;

/// Install the panic hook and the shared rich tracing stack (reloadable level
/// filter + console layer + JS-callback layer). The initial level is read from
/// `SUBDUCTION_LOG_LEVEL` (`localStorage` / `process.env`), defaulting to WARN.
/// Idempotent; whichever subscriber installs first wins.
#[wasm_bindgen]
pub fn init() {
    subduction_wasm_bootstrap::init_rich_from_env();
}

/// Module entry point. Runs [`init`] then logs a startup banner.
#[wasm_bindgen(start, private)]
pub fn start_automerge_subduction_wasm() {
    init();

    tracing::info!(
        "automerge_subduction_wasm v{} ({})",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH")
    );
}
