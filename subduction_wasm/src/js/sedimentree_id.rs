//! IDs for individual [`Sedimentree`]s.

use sedimentree_core::SedimentreeId;
use wasm_bindgen::prelude::*;

/// A Wasm wrapper around the [`SedimentreeId`] type.
#[wasm_bindgen(js_name = SedimentreeId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsSedimentreeId(SedimentreeId);

#[wasm_bindgen(js_class = SedimentreeId)]
impl JsSedimentreeId {
    /// Create an ID from a byte array.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let raw: [u8; 32] = bytes.try_into().map_err(|_| "FIXME".to_string())?; // FIXME
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
