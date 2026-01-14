//! IDs for individual [`Sedimentree`]s.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use sedimentree_core::SedimentreeId;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

/// A Wasm wrapper around the [`SedimentreeId`] type.
#[wasm_bindgen(js_name = SedimentreeId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmSedimentreeId(SedimentreeId);

#[wasm_refgen(js_ref = JsSedimentreeId)]
#[wasm_bindgen(js_class = SedimentreeId)]
impl WasmSedimentreeId {
    /// Create an ID from a byte array.
    ///
    /// # Errors
    ///
    /// Returns `Not32Bytes` if the provided byte array is not exactly 32 bytes long.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Not32Bytes> {
        let raw: [u8; 32] = bytes.try_into().map_err(|_| Not32Bytes)?;
        Ok(Self(SedimentreeId::from_bytes(raw)))
    }

    /// Returns the string representation of the ID.
    #[must_use]
    #[wasm_bindgen(js_name = toString)]
    pub fn js_to_string(&self) -> String {
        self.0.to_string()
    }
}

impl core::fmt::Display for WasmSedimentreeId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<SedimentreeId> for WasmSedimentreeId {
    fn from(id: SedimentreeId) -> Self {
        Self(id)
    }
}

impl From<WasmSedimentreeId> for SedimentreeId {
    fn from(id: WasmSedimentreeId) -> Self {
        id.0
    }
}

/// Error indicating that the provided byte array is not exactly 32 bytes long.
#[derive(Debug, Error)]
#[error("ID must be exactly 32 bytes")]
#[allow(missing_copy_implementations)]
pub struct Not32Bytes;

impl From<Not32Bytes> for JsValue {
    fn from(err: Not32Bytes) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("Not32Bytes");
        js_err.into()
    }
}

#[wasm_bindgen(js_name = SedimentreeIdsArray)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WasmSedimentreeIdsArray(pub(crate) Vec<WasmSedimentreeId>);

#[wasm_refgen(js_ref = JsSedimentreeIdsArray)]
#[wasm_bindgen(js_class = SedimentreeIdsArray)]
impl WasmSedimentreeIdsArray {}

impl TryFrom<&JsValue> for WasmSedimentreeIdsArray {
    type Error = WasmConvertJsValueToSedimentreeIdArrayError;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(WasmSedimentreeIdsArray(
            try_into_js_sedimentree_ids_array(js_value)
                .map_err(WasmConvertJsValueToSedimentreeIdArrayError)?,
        ))
    }
}

/// An error indicating that a `JsValue` could not be converted into a `SedimentreeId` array.
#[derive(Debug, Error)]
#[error("unable to convert JsValue into SedimentreeId array")]
pub struct WasmConvertJsValueToSedimentreeIdArrayError(JsValue);

impl From<WasmConvertJsValueToSedimentreeIdArrayError> for JsValue {
    fn from(err: WasmConvertJsValueToSedimentreeIdArrayError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("UnableToConvertSedimentreeIdArrayError");
        err.into()
    }
}

#[wasm_bindgen(inline_js = r#"
    export function tryIntoJsSedimentreeIdsArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `WasmSedimentreeId`.
    #[wasm_bindgen(js_name = tryIntoJsSedimentreeIdsArray, catch)]
    pub fn try_into_js_sedimentree_ids_array(
        v: &JsValue,
    ) -> Result<Vec<WasmSedimentreeId>, JsValue>;
}
