//! The main data type for Sedimentree.

use alloc::vec::Vec;
use sedimentree_core::sedimentree::Sedimentree;
use wasm_bindgen::prelude::*;

use crate::{
    fragment::{JsFragment, WasmFragment},
    loose_commit::{JsLooseCommit, WasmLooseCommit},
};

/// The main Sedimentree data structure.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Sedimentree)]
pub struct WasmSedimentree(Sedimentree);

#[wasm_bindgen(js_class = Sedimentree)]
impl WasmSedimentree {
    /// Create a new Sedimentree from fragments and loose commits.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen requires owned types
    pub fn new(fragments: Vec<JsFragment>, commits: Vec<JsLooseCommit>) -> Self {
        let core_fragments = fragments
            .iter()
            .map(|f| WasmFragment::from(f).into())
            .collect();
        let core_commits = commits
            .iter()
            .map(|c| WasmLooseCommit::from(c).into())
            .collect();
        Sedimentree::new(core_fragments, core_commits).into()
    }

    /// Create an empty Sedimentree.
    #[must_use]
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
