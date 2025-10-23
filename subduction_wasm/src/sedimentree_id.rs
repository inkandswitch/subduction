//! IDs for individual [`Sedimentree`]s.

use sedimentree_core::SedimentreeId;
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around the [`SedimentreeId`] type.
#[wasm_bindgen(js_name = SedimentreeId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmSedimentreeId(SedimentreeId);

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

impl std::fmt::Display for WasmSedimentreeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
#[wasm_bindgen]
#[derive(Debug, Error)]
#[error("ID must be exactly 32 bytes")]
#[allow(missing_copy_implementations)]
pub struct Not32Bytes;
