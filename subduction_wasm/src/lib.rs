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

// The Wasm log controls (`setSubductionLogLevel`, `set_subduction_logger`,
// `clear_subduction_logger`) are defined once in `sedimentree_wasm` (the lowest
// cdylib in the dependency chain) and inherited here via `pub use`, so this
// bundle exports them without a duplicate `#[wasm_bindgen]` definition.
#[cfg(target_arch = "wasm32")]
pub use sedimentree_wasm::{
    clear_subduction_logger, set_subduction_log_level, set_subduction_logger,
};

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
