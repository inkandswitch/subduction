//! Wasm bindings for [`CommitId`].

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use base58::FromBase58;
use sedimentree_core::loose_commit::id::CommitId;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A user-supplied opaque identifier for a loose commit.
///
/// Unlike [`Digest`](crate::digest::WasmDigest), which is a content hash
/// computed by the system, `CommitId` is provided by the caller at construction
/// time. This is the identity used for DAG traversal, fragment boundaries,
/// and sync.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_copy_implementations)]
#[wasm_bindgen(js_name = CommitId)]
pub struct WasmCommitId([u8; 32]);

#[wasm_refgen(js_ref = JsCommitId)]
#[wasm_bindgen(js_class = CommitId)]
impl WasmCommitId {
    /// Creates a new commit identifier from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the byte slice is not exactly 32 bytes.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8]) -> Result<WasmCommitId, WasmInvalidCommitId> {
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| WasmInvalidCommitId::WrongLength(bytes.len()))?;
        Ok(WasmCommitId(arr))
    }

    /// Creates a new commit identifier from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the byte slice is not exactly 32 bytes.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<WasmCommitId, WasmInvalidCommitId> {
        Self::new(bytes)
    }

    /// Creates a new commit identifier from its Base58 string representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the string cannot be decoded or is not 32 bytes.
    #[wasm_bindgen(js_name = fromBase58)]
    pub fn from_base58(s: &str) -> Result<WasmCommitId, WasmInvalidCommitId> {
        let bytes: Vec<u8> = s
            .from_base58()
            .map_err(WasmInvalidCommitId::Base58DecodeError)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|v: Vec<u8>| WasmInvalidCommitId::WrongLength(v.len()))?;
        Ok(WasmCommitId(arr))
    }

    /// Creates a new commit identifier from its hexadecimal string representation.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not valid hex or not 32 bytes.
    #[wasm_bindgen(js_name = fromHexString)]
    pub fn from_hex_string(s: &str) -> Result<WasmCommitId, WasmInvalidCommitId> {
        if !s.len().is_multiple_of(2) {
            return Err(WasmInvalidCommitId::InvalidHex);
        }
        if s.len() != 64 {
            return Err(WasmInvalidCommitId::WrongLength(s.len() / 2));
        }

        let mut arr = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks_exact(2).enumerate() {
            let &[hi_byte, lo_byte] = chunk else {
                return Err(WasmInvalidCommitId::InvalidHex);
            };
            let hi = hex_nibble(hi_byte).ok_or(WasmInvalidCommitId::InvalidHex)?;
            let lo = hex_nibble(lo_byte).ok_or(WasmInvalidCommitId::InvalidHex)?;
            if let Some(slot) = arr.get_mut(i) {
                *slot = (hi << 4) | lo;
            }
        }
        Ok(WasmCommitId(arr))
    }

    /// Returns the byte representation of the commit identifier.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Returns the hexadecimal string representation.
    #[must_use]
    #[wasm_bindgen(js_name = toHexString)]
    pub fn to_hex_string(&self) -> String {
        CommitId::new(self.0).to_string()
    }
}

impl From<CommitId> for WasmCommitId {
    fn from(id: CommitId) -> Self {
        Self(*id.as_bytes())
    }
}

impl From<WasmCommitId> for CommitId {
    fn from(id: WasmCommitId) -> Self {
        CommitId::new(id.0)
    }
}

impl From<&WasmCommitId> for CommitId {
    fn from(id: &WasmCommitId) -> Self {
        CommitId::new(id.0)
    }
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// An error indicating an invalid [`CommitId`].
#[derive(Debug, Error)]
pub enum WasmInvalidCommitId {
    /// Wrong byte length (expected 32).
    #[error("expected 32 bytes, got {0}")]
    WrongLength(usize),

    /// The Base58 decoding failed.
    #[error("Base58 decode error: {0:?}")]
    Base58DecodeError(base58::FromBase58Error),

    /// The hex string is invalid.
    #[error("invalid hex string")]
    InvalidHex,
}

impl From<WasmInvalidCommitId> for JsValue {
    fn from(err: WasmInvalidCommitId) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidCommitId");
        err.into()
    }
}
