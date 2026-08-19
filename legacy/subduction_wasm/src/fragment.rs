//! Subduction-specific fragment types.

use sedimentree_core::loose_commit::id::CommitId;
use sedimentree_wasm::{commit_id::WasmCommitId, depth::WasmDepth};
use subduction_core::subduction::request::FragmentRequested;
use wasm_bindgen::prelude::*;

/// A request for a specific fragment in the Sedimentree system.
#[wasm_bindgen(js_name = FragmentRequested)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct WasmFragmentRequested(FragmentRequested);

#[wasm_bindgen(js_class = FragmentRequested)]
impl WasmFragmentRequested {
    /// Create a new fragment request from the given commit ID.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(commit_id: &WasmCommitId, depth: &WasmDepth) -> Self {
        let commit_id = CommitId::from(commit_id);
        FragmentRequested::new(commit_id, depth.clone().into()).into()
    }

    /// Get the head commit identifier of the requested fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmCommitId {
        WasmCommitId::from(self.0.head())
    }

    /// Get the depth of the requested fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn depth(&self) -> WasmDepth {
        (*self.0.depth()).into()
    }
}

impl From<FragmentRequested> for WasmFragmentRequested {
    fn from(req: FragmentRequested) -> Self {
        Self(req)
    }
}

impl From<WasmFragmentRequested> for FragmentRequested {
    fn from(req: WasmFragmentRequested) -> Self {
        req.0
    }
}
