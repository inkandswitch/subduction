use sedimentree_sync_core::peer::id::PeerId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = "PeerId")]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JsPeerId(PeerId);

#[wasm_bindgen(js_class = "PeerId")]
impl JsPeerId {
    #[wasm_bindgen(constructor)]
    pub fn new(s: String) -> Self {
        JsPeerId(PeerId::new(s))
    }

    #[wasm_bindgen(getter)]
    pub fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl std::fmt::Display for JsPeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<PeerId> for JsPeerId {
    fn from(peer_id: PeerId) -> Self {
        JsPeerId(peer_id)
    }
}

impl From<JsPeerId> for PeerId {
    fn from(js_peer_id: JsPeerId) -> Self {
        js_peer_id.0
    }
}
