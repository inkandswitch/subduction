//! # Wasm bindings for Sedimentree.
//!
//! This crate provides JavaScript/Wasm bindings for Sedimentree data structures.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Module entry point. Installs the panic hook and a baseline
/// `tracing` subscriber via [`subduction_wasm_bootstrap::init_basic`].
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_sedimentree_wasm() {
    subduction_wasm_bootstrap::init_basic();
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
