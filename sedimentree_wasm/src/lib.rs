//! # Wasm bindings for Sedimentree.
//!
//! This crate provides JavaScript/Wasm bindings for Sedimentree data structures.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(clippy::missing_const_for_fn)]

#[cfg(feature = "std")]
extern crate std;

extern crate alloc;

/// Entry point called when the Wasm module is instantiated.
///
/// Installs the panic hook and a baseline `tracing` subscriber via
/// [`subduction_wasm_bootstrap::init_basic`]. Both are idempotent and
/// chain-safe: when this crate is consumed as a Rust dep of a larger
/// cdylib (e.g. `@automerge/subduction`), the outer cdylib's start
/// function runs first and may install a richer subscriber; this
/// start then becomes a no-op for tracing while still ensuring the
/// panic hook is set.
///
/// `start, private` registers this in the `__wbindgen_start` chain
/// without emitting a public Wasm export named `start` (added in
/// wasm-bindgen 0.2.118). Without `private`, multiple cdylib rlibs
/// each emitting `pub fn start()` would collide at link time on the
/// shared `start` export symbol.
// Rust function name is crate-distinct so the wasm-bindgen-generated
// `__wbindgen_describe_*` symbol doesn't collide with the same-named
// start in any other rlib linked into the same cdylib. The `start,
// private` annotation registers this in `__wbindgen_start` chaining
// without emitting a public Wasm `start` export.
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
