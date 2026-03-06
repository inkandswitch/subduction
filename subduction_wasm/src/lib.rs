//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

use wasm_bindgen::prelude::*;

/// Install the panic hook at module instantiation, before any exported
/// function can be called. This ensures panics always produce readable
/// error messages in the browser console.
#[wasm_bindgen(start)]
pub fn start() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

pub mod connection;
pub mod connection_id;
pub mod error;
pub mod fragment;
pub mod memory_signer;
pub mod peer_id;
pub mod signer;
pub mod subduction;
pub mod sync_stats;
