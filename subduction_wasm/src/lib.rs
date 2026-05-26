//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Module entry point. Installs the panic hook and a baseline
/// `tracing` subscriber via [`subduction_wasm_bootstrap::init_basic`].
/// Idempotent; the umbrella crate's richer subscriber wins if it
/// installs first.
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_subduction_wasm() {
    subduction_wasm_bootstrap::init_basic();
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
