use sedimentree_core::Sedimentree;
use wasm_bindgen::prelude::*;

use crate::{fragment::WasmFragment, loose_commit::WasmLooseCommit};

#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Sedimentree)]
pub struct WasmSedimentree(Sedimentree);

#[wasm_bindgen(js_class = Sedimentree)]
impl WasmSedimentree {
    // FIXME don't take ownership of the vectors, needs some boilerplate
    #[wasm_bindgen(constructor)]
    pub fn new(fragments: Vec<WasmFragment>, commits: Vec<WasmLooseCommit>) -> Self {
        let core_fragments = fragments.iter().map(|f| f.clone().into()).collect();
        let core_commits = commits.iter().map(|c| c.clone().into()).collect();
        Sedimentree::new(core_fragments, core_commits).into()
    }

    pub fn empty() -> Self {
        Sedimentree::default().into()
    }
}

impl From<Sedimentree> for WasmSedimentree {
    fn from(value: Sedimentree) -> Self {
        Self(value)
    }
}

impl From<WasmSedimentree> for Sedimentree {
    fn from(value: WasmSedimentree) -> Self {
        value.0
    }
}
