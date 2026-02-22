//! Wasm wrappers for signed types.

use js_sys::Uint8Array;
use sedimentree_core::{codec::error::DecodeError, fragment::Fragment, loose_commit::LooseCommit};
use subduction_crypto::signed::Signed;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use crate::{fragment::WasmFragment, loose_commit::WasmLooseCommit};

/// A Wasm wrapper around `Signed<LooseCommit>`.
#[wasm_bindgen(js_name = SignedLooseCommit)]
#[derive(Debug, Clone)]
pub struct WasmSignedLooseCommit(Signed<LooseCommit>);

#[wasm_refgen(js_ref = JsSignedLooseCommit)]
#[wasm_bindgen(js_class = SignedLooseCommit)]
impl WasmSignedLooseCommit {
    /// Decode a `SignedLooseCommit` from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid signed loose commit.
    #[wasm_bindgen(js_name = tryDecode)]
    pub fn try_decode(bytes: &Uint8Array) -> Result<WasmSignedLooseCommit, JsValue> {
        let data = bytes.to_vec();
        let signed = Signed::<LooseCommit>::try_decode(data)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Self(signed))
    }

    /// Encode this signed loose commit to raw bytes.
    #[must_use]
    #[wasm_bindgen(js_name = encode)]
    pub fn encode(&self) -> Uint8Array {
        Uint8Array::from(self.0.as_bytes())
    }

    /// Get the unsigned payload without re-verifying the signature.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be decoded.
    #[wasm_bindgen(getter)]
    pub fn payload(&self) -> Result<WasmLooseCommit, JsValue> {
        let payload = self
            .0
            .try_decode_trusted_payload()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(payload.into())
    }
}

impl From<Signed<LooseCommit>> for WasmSignedLooseCommit {
    fn from(signed: Signed<LooseCommit>) -> Self {
        Self(signed)
    }
}

impl From<WasmSignedLooseCommit> for Signed<LooseCommit> {
    fn from(wasm: WasmSignedLooseCommit) -> Self {
        wasm.0
    }
}

impl AsRef<Signed<LooseCommit>> for WasmSignedLooseCommit {
    fn as_ref(&self) -> &Signed<LooseCommit> {
        &self.0
    }
}

impl WasmSignedLooseCommit {
    /// Create a `WasmSignedLooseCommit` from raw bytes (non-wasm-bindgen).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid signed loose commit.
    pub fn try_from_vec(bytes: Vec<u8>) -> Result<Self, DecodeError> {
        Ok(Self(Signed::<LooseCommit>::try_decode(bytes)?))
    }

    /// Get the raw bytes of the signed loose commit.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// A Wasm wrapper around `Signed<Fragment>`.
#[wasm_bindgen(js_name = SignedFragment)]
#[derive(Debug, Clone)]
pub struct WasmSignedFragment(Signed<Fragment>);

#[wasm_refgen(js_ref = JsSignedFragment)]
#[wasm_bindgen(js_class = SignedFragment)]
impl WasmSignedFragment {
    /// Decode a `SignedFragment` from raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid signed fragment.
    #[wasm_bindgen(js_name = tryDecode)]
    pub fn try_decode(bytes: &Uint8Array) -> Result<WasmSignedFragment, JsValue> {
        let data = bytes.to_vec();
        let signed =
            Signed::<Fragment>::try_decode(data).map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(Self(signed))
    }

    /// Encode this signed fragment to raw bytes.
    #[must_use]
    #[wasm_bindgen(js_name = encode)]
    pub fn encode(&self) -> Uint8Array {
        Uint8Array::from(self.0.as_bytes())
    }

    /// Get the unsigned payload without re-verifying the signature.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be decoded.
    #[wasm_bindgen(getter)]
    pub fn payload(&self) -> Result<WasmFragment, JsValue> {
        let payload = self
            .0
            .try_decode_trusted_payload()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        Ok(payload.into())
    }
}

impl From<Signed<Fragment>> for WasmSignedFragment {
    fn from(signed: Signed<Fragment>) -> Self {
        Self(signed)
    }
}

impl From<WasmSignedFragment> for Signed<Fragment> {
    fn from(wasm: WasmSignedFragment) -> Self {
        wasm.0
    }
}

impl AsRef<Signed<Fragment>> for WasmSignedFragment {
    fn as_ref(&self) -> &Signed<Fragment> {
        &self.0
    }
}

impl WasmSignedFragment {
    /// Create a `WasmSignedFragment` from raw bytes (non-wasm-bindgen).
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are not a valid signed fragment.
    pub fn try_from_vec(bytes: Vec<u8>) -> Result<Self, DecodeError> {
        Ok(Self(Signed::<Fragment>::try_decode(bytes)?))
    }

    /// Get the raw bytes of the signed fragment.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}
