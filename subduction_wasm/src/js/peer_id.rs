use subduction_core::peer::id::PeerId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = PeerId)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsPeerId(PeerId);

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
