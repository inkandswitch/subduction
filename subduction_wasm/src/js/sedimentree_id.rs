use sedimentree_core::SedimentreeId;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = SedimentreeId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JsSedimentreeId(SedimentreeId);

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
