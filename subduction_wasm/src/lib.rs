//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Install the console error panic hook so that panics produce readable
/// error messages in the browser console.
///
/// Called automatically when the `standalone` feature is enabled.
/// When `subduction_wasm` is used as a dependency of another cdylib crate,
/// call this from that crate's own start function instead.
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Entry point called when the Wasm module is instantiated.
///
/// Only compiled when the `standalone` feature is active. Downstream cdylib
/// crates that define their own `#[wasm_bindgen(start)]` should depend on
/// `subduction_wasm` with `default-features = false` and call
/// [`set_panic_hook`] from their own start function.
#[cfg(feature = "standalone")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn start() {
    set_panic_hook();
}

pub mod clock;
pub mod ephemeral;
pub mod error;
pub mod fragment;
pub mod memory_signer;
pub mod peer_id;
pub mod policy;
pub mod remote_heads;
pub mod signer;
pub mod subduction;
pub mod sync_stats;
pub mod timer;
pub mod transport;
pub mod wire;
