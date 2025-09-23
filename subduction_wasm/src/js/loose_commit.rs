// FIXME move to SedimentreeWasm

use sedimentree_core::LooseCommit;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(js_name = LooseCommit)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JsLooseCommit(LooseCommit);

// FIXME getters

impl From<LooseCommit> for JsLooseCommit {
    fn from(commit: LooseCommit) -> Self {
        Self(commit)
    }
}

impl From<JsLooseCommit> for LooseCommit {
    fn from(commit: JsLooseCommit) -> Self {
        commit.0
    }
}
