//! Hash digests.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use base58::FromBase58;
use sedimentree_core::digest::{Digest, InvalidDigest};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A wrapper around digest bytes for use in JavaScript via wasm-bindgen.
///
/// Since JavaScript doesn't have Rust's type system, this stores raw bytes
/// and converts to/from typed `Digest<T>` at the Rust boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_copy_implementations)]
#[wasm_bindgen(js_name = Digest)]
pub struct WasmDigest([u8; 32]);

#[wasm_refgen(js_ref = JsDigest)]
#[wasm_bindgen(js_class = Digest)]
impl WasmDigest {
    /// Creates a new digest from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns a `WasmValue` error if the byte slice is not a valid digest.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8]) -> Result<WasmDigest, WasmInvalidDigest> {
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| InternalWasmInvalidDigest::InvalidDigest(InvalidDigest::WrongLength))?;
        Ok(WasmDigest(arr))
    }

    /// Creates a new digest from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns a `WasmValue` error if the byte slice is not a valid digest.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<WasmDigest, WasmInvalidDigest> {
        Self::new(bytes)
    }

    /// Creates a new digest from its Base58 string representation.
    ///
    /// # Errors
    ///
    /// Returns a `WasmInvalidDigest` error if the string cannot be decoded or is not a valid digest.
    #[wasm_bindgen(js_name = fromBase58)]
    pub fn from_base58(s: &str) -> Result<WasmDigest, WasmInvalidDigest> {
        let bytes: Vec<u8> = s
            .from_base58()
            .map_err(InternalWasmInvalidDigest::Base58DecodeError)?;
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| InternalWasmInvalidDigest::InvalidDigest(InvalidDigest::WrongLength))?;
        Ok(WasmDigest(arr))
    }

    /// Returns the byte representation of the digest.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Creates a new digest from its hexadecimal string representation.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmInvalidDigest`] if the string is not a valid digest.
    #[wasm_bindgen(js_name = fromHexString)]
    pub fn from_hex_string(s: &str) -> Result<WasmDigest, WasmInvalidDigest> {
        // Parse as Digest<()> for validation, then extract bytes
        let digest: Digest<()> = s
            .parse()
            .map_err(InternalWasmInvalidDigest::InvalidDigest)?;
        Ok(WasmDigest(*digest.as_bytes()))
    }

    /// Returns the hexadecimal string representation of the digest.
    #[must_use]
    #[wasm_bindgen(js_name = toHexString)]
    pub fn to_hex_string(&self) -> String {
        // Create a temporary typed digest for formatting
        Digest::<()>::from_bytes(self.0).to_string()
    }

    /// Hash the given data and return the digest.
    #[must_use]
    pub fn hash(data: &[u8]) -> WasmDigest {
        let digest: Digest<()> = Digest::hash_bytes(data);
        WasmDigest(*digest.as_bytes())
    }
}

/// Convert from any typed Digest to WasmDigest.
impl<T> From<Digest<T>> for WasmDigest {
    fn from(digest: Digest<T>) -> Self {
        Self(*digest.as_bytes())
    }
}

/// Convert from WasmDigest to any typed Digest.
impl<T> From<WasmDigest> for Digest<T> {
    fn from(digest: WasmDigest) -> Self {
        Digest::from_bytes(digest.0)
    }
}

/// An error indicating an invalid [`Digest`].
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmInvalidDigest(#[from] InternalWasmInvalidDigest);

/// An internal error indicating an invalid [`Digest`].
#[derive(Debug, Error)]
pub enum InternalWasmInvalidDigest {
    /// The digest is invalid.
    #[error(transparent)]
    InvalidDigest(#[from] InvalidDigest),

    /// The Base58 decoding failed.
    #[error("Base58 decode error: {0:?}")]
    Base58DecodeError(base58::FromBase58Error),
}

impl From<WasmInvalidDigest> for JsValue {
    fn from(err: WasmInvalidDigest) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidDigest");
        err.into()
    }
}
