//! Bridges Automerge's base58-encoded document IDs to Subduction's
//! 32-byte commit hashes, returned as canonical lowercase hex strings.

use alloc::string::String;
use base58::FromBase58;
use sedimentree_wasm::pojo::hex;
use wasm_bindgen::prelude::*;

use crate::error::WasmFromBase58Error;

/// Compute the commit ID of a base58-encoded ID string.
///
/// # Errors
///
/// Returns a `WasmFromBase58Error` if the input string is not valid base58.
#[wasm_bindgen(js_name = commitIdOfBase58Id)]
pub fn commit_id_of_base58_id(b58_str: &str) -> Result<String, WasmFromBase58Error> {
    let decoded = b58_str.from_base58()?;
    let raw: [u8; 32] = blake3::hash(&decoded).into();
    Ok(hex(&raw))
}
