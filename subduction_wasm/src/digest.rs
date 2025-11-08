//! Hash digests.

use base58::FromBase58;
use sedimentree_core::blob::{error::InvalidDigest, Digest};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A wrapper around [`sedimentree_core::Digest`] for use in JavaScript via wasm-bindgen.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
#[wasm_bindgen(js_name = Digest)]
pub struct WasmDigest(Digest);

#[wasm_refgen(js_ref = JsDigest)]
#[wasm_bindgen(js_class = Digest)]
impl WasmDigest {
    /// Creates a new digest from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns a `WasmValue` error if the byte slice is not a valid digest.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8]) -> Result<WasmDigest, JsValue> {
        let digest = Digest::from_bytes(bytes).map_err(WasmInvalidDigest::from)?;
        Ok(WasmDigest(digest))
    }

    /// Creates a new digest from its byte representation.
    ///
    /// # Errors
    ///
    /// Returns a `WasmValue` error if the byte slice is not a valid digest.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<WasmDigest, JsValue> {
        let digest = Digest::from_bytes(bytes).map_err(WasmInvalidDigest::from)?;
        Ok(WasmDigest(digest))
    }

    #[wasm_bindgen(js_name = fromBase58)]
    pub fn from_base58(s: &str) -> Result<WasmDigest, JsValue> {
        let bytes: Vec<u8> = s.from_base58().expect("FIXME");
        let digest = Digest::from_bytes(&bytes[..32]).map_err(WasmInvalidDigest::from)?;
        Ok(WasmDigest(digest))
    }

    /// Returns the byte representation of the digest.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    #[must_use]
    #[wasm_bindgen(js_name = toHexString)]
    pub fn to_hex_string(&self) -> String {
        self.0.to_string()
    }
}

impl From<Digest> for WasmDigest {
    fn from(digest: Digest) -> Self {
        Self(digest)
    }
}

impl From<WasmDigest> for Digest {
    fn from(digest: WasmDigest) -> Self {
        digest.0
    }
}

/// An error indicating an invalid [`Digest`].
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmInvalidDigest(#[from] InvalidDigest);

impl From<WasmInvalidDigest> for JsValue {
    fn from(err: WasmInvalidDigest) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidDigest");
        err.into()
    }
}
