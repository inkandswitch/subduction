//! Type safe Peer ID.

use subduction_core::peer::id::PeerId;
use wasm_bindgen::prelude::*;

/// A JavaScript-compatible wrapper around the Rust `PeerId` type.
#[wasm_bindgen(js_name = PeerId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsPeerId(PeerId);

#[wasm_bindgen(js_class = PeerId)]
impl JsPeerId {
    /// Creates a new `JsPeerId` from a `PeerId`.
    #[wasm_bindgen(constructor)]
    pub fn new(bytes: &[u8]) -> Result<Self, String> {
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "PeerId must be exactly 32 bytes".to_string())?;
        Ok(Self(PeerId::new(arr)))
    }
}

impl From<PeerId> for JsPeerId {
    fn from(id: PeerId) -> Self {
        Self(id)
    }
}

impl From<JsPeerId> for PeerId {
    fn from(id: JsPeerId) -> Self {
        id.0
    }
}
