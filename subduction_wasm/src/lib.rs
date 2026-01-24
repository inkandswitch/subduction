//! # Wasm bindings for the Subduction sync protocol.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

pub mod connection;
pub mod connection_id;
pub mod depth;
pub mod digest;
pub mod error;
pub mod fragment;
pub mod loose_commit;
pub mod peer_id;
pub mod sedimentree;
pub mod sedimentree_id;
pub mod signer;
pub mod storage;
pub mod subduction;

pub use signer::WebCryptoSigner;
pub use subduction::WasmSubduction;
