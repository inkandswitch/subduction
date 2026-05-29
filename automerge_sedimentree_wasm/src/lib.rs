//! Wasm bindings for `automerge_sedimentree`.
//!
//! The fragment-building bindings (`SedimentreeAutomerge.fragment` and
//! `SedimentreeAutomerge.buildFragmentStore`) that used to live here were
//! removed when the native ingest path migrated to upstream
//! `Automerge::fragments()` / `Automerge::bundle_fragments()`. JS consumers
//! that previously called those should drive the upstream API directly.
//!
//! What remains is small, independent: a utility for computing a
//! base58-encoded ID's commit ID.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

/// Module entry point. Installs the panic hook and a baseline
/// `tracing` subscriber via [`subduction_wasm_bootstrap::init_basic`].
#[wasm_bindgen::prelude::wasm_bindgen(start, private)]
pub fn start_automerge_sedimentree_wasm() {
    subduction_wasm_bootstrap::init_basic();
}

pub mod error;

use base58::FromBase58;
use error::WasmFromBase58Error;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_wasm::commit_id::WasmCommitId;
use wasm_bindgen::prelude::*;

/// Compute the commit ID of a base58-encoded ID string.
///
/// # Errors
///
/// Returns a `WasmFromBase58Error` if the input string is not valid base58.
#[wasm_bindgen(js_name = commitIdOfBase58Id)]
pub fn commit_id_of_base58_id(b58_str: &str) -> Result<WasmCommitId, WasmFromBase58Error> {
    let decoded = b58_str.from_base58()?;
    let raw: [u8; 32] = blake3::hash(&decoded).into();
    Ok(WasmCommitId::from(CommitId::new(raw)))
}
