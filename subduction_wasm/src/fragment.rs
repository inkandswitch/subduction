//! Subduction-specific fragment types.

use sedimentree_core::{
    crypto::digest::Digest,
    loose_commit::{id::CommitId, LooseCommit},
};
use sedimentree_wasm::{depth::WasmDepth, digest::WasmDigest};
use subduction_core::subduction::request::FragmentRequested;
use wasm_bindgen::prelude::*;

/// A request for a specific fragment in the Sedimentree system.
#[wasm_bindgen(js_name = FragmentRequested)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_copy_implementations)]
pub struct WasmFragmentRequested(FragmentRequested);

#[wasm_bindgen(js_class = FragmentRequested)]
impl WasmFragmentRequested {
    /// Create a new fragment request from the given digest.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(digest: &WasmDigest, depth: &WasmDepth) -> Self {
        let commit_id = CommitId::new(Digest::<LooseCommit>::from(digest.clone()).into_bytes());
        FragmentRequested::new(commit_id, depth.clone().into()).into()
    }

    /// Get the head commit identifier of the requested fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmDigest {
        let id = self.0.head();
        WasmDigest::from(Digest::<LooseCommit>::force_from_bytes(*id.as_bytes()))
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
