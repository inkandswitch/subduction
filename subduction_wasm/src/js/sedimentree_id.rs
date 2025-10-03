//! IDs for individual [`Sedimentree`]s.

use sedimentree_core::SedimentreeId;
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around the [`SedimentreeId`] type.
#[wasm_bindgen(js_name = SedimentreeId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct JsSedimentreeId(SedimentreeId);

#[wasm_bindgen(js_class = SedimentreeId)]
impl JsSedimentreeId {
    /// Create an ID from a byte array.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Not32Bytes> {
        let raw: [u8; 32] = bytes.try_into().map_err(|_| Not32Bytes)?;
        Ok(Self(SedimentreeId::from_bytes(raw)))
    }

    /// Returns the string representation of the ID.
    #[wasm_bindgen(js_name = toString)]
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl From<SedimentreeId> for JsSedimentreeId {
    fn from(id: SedimentreeId) -> Self {
        Self(id)
    }
}

impl From<JsSedimentreeId> for SedimentreeId {
    fn from(id: JsSedimentreeId) -> Self {
        id.0
    }
}

/// Error indicating that the provided byte array is not exactly 32 bytes long.
#[wasm_bindgen]
#[derive(Debug, Error)]
#[error("ID must be exactly 32 bytes")]
#[allow(missing_copy_implementations)]
pub struct Not32Bytes;
