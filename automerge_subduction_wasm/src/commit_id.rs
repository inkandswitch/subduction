//! Bridges Automerge's base58-encoded document IDs to Subduction's
//! 32-byte [`CommitId`] hashes.

use base58::FromBase58;
use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_wasm::commit_id::WasmCommitId;
use wasm_bindgen::prelude::*;

use crate::error::WasmFromBase58Error;

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
