//! # Wasm Bindings for the Subduction sync protocol.

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod connection_id;
pub mod depth;
pub mod digest;
pub mod error;
pub mod fragment;
pub mod loose_commit;
pub mod peer_id;
pub mod sedimentree;
pub mod sedimentree_id;
pub mod storage;
pub mod subduction;
pub mod websocket;

pub(crate) mod connection_callback_reader;

pub use subduction::WasmSubduction;
