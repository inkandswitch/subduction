//! Wasm wrapper for request nonces.

use alloc::{string::ToString, vec::Vec};
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A 64-bit nonce represented as big-endian bytes.
#[wasm_bindgen(js_name = Nonce)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmNonce(pub(crate) u64);

#[wasm_bindgen(js_class = Nonce)]
impl WasmNonce {
    /// Create a new [`WasmNonce`] from exactly 8 big-endian bytes.
    ///
    /// # Errors
    ///
    /// Returns [`WasmNonceError`] if the input is not exactly 8 bytes.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: Vec<u8>) -> Result<WasmNonce, WasmNonceError> {
        let arr: [u8; 8] = bytes.try_into().map_err(|_| WasmNonceError)?;
        Ok(Self(u64::from_be_bytes(arr)))
    }

    /// Generate a random nonce.
    ///
    /// # Panics
    ///
    /// Panics if the system random number generator fails.
    #[must_use]
    pub fn random() -> Self {
        let mut bytes = [0u8; 8];
        #[allow(clippy::expect_used)]
        getrandom::getrandom(&mut bytes).expect("getrandom failed");
        Self(u64::from_be_bytes(bytes))
    }

    /// Get the nonce as big-endian bytes.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn bytes(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }
}

/// The nonce must be exactly 8 bytes.
#[derive(Debug, Clone, Copy, Error)]
#[error("Nonce must be exactly 8 bytes")]
pub struct WasmNonceError;

impl From<WasmNonceError> for JsValue {
    fn from(err: WasmNonceError) -> JsValue {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("NonceError");
        js_err.into()
    }
}
