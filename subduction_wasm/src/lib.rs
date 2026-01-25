//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod connection;
pub mod connection_id;
pub mod error;
pub mod fragment;
pub mod peer_id;
pub mod signer;
pub mod subduction;

// Re-export sedimentree types from sedimentree_wasm
pub use sedimentree_wasm::{depth, digest, loose_commit, sedimentree, sedimentree_id, storage};
pub use sedimentree_wasm::fragment as sedimentree_fragment;

pub use signer::WebCryptoSigner;
pub use subduction::WasmSubduction;
