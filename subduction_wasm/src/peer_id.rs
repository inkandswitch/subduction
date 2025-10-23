//! Type safe Peer ID.

use subduction_core::peer::id::PeerId;
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// A JavaScript-compatible wrapper around the Rust `PeerId` type.
#[wasm_bindgen(js_name = PeerId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct WasmPeerId(PeerId);

#[wasm_bindgen(js_class = PeerId)]
impl WasmPeerId {
    /// Creates a new `WasmPeerId` from a `PeerId`.
    ///
    /// # Errors
    ///
    /// Returns a `WasmInvalidPeerId` if the provided byte slice is not exactly 32 bytes long.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8]) -> Result<Self, WasmInvalidPeerId> {
        let arr: [u8; 32] = bytes.try_into().map_err(|_| WasmInvalidPeerId)?;
        Ok(Self(PeerId::new(arr)))
    }
}

impl From<PeerId> for WasmPeerId {
    fn from(id: PeerId) -> Self {
        Self(id)
    }
}

impl From<WasmPeerId> for PeerId {
    fn from(id: WasmPeerId) -> Self {
        id.0
    }
}

/// An error indicating an invalid [`PeerId`].
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error("invalid PeerId, must be exactly 32 bytes")]
pub struct WasmInvalidPeerId;

impl From<WasmInvalidPeerId> for JsValue {
    fn from(err: WasmInvalidPeerId) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidPeerId");
        err.into()
    }
}
