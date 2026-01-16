//! Sedimentree [`Fragment`](sedimentree_core::Fragment).

use alloc::{string::ToString, vec::Vec};
use sedimentree_core::fragment::Fragment;
use subduction_core::subduction::request::FragmentRequested;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use crate::{
    depth::WasmDepth,
    digest::{JsDigest, WasmDigest},
    loose_commit::WasmBlobMeta,
};

/// A data fragment used in the Sedimentree system.
#[derive(Debug, Clone, PartialEq, Eq)]
#[wasm_bindgen(js_name = Fragment)]
pub struct WasmFragment(Fragment);

#[wasm_refgen(js_ref = JsFragment)]
#[wasm_bindgen(js_class = Fragment)]
impl WasmFragment {
    /// Create a new fragment from the given head, boundary, checkpoints, and blob metadata.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub fn new(
        head: WasmDigest,
        boundary: Vec<JsDigest>,
        checkpoints: Vec<JsDigest>,
        blob_meta: WasmBlobMeta,
    ) -> Self {
        Fragment::new(
            head.into(),
            boundary
                .iter()
                .map(|d| WasmDigest::from(d).into())
                .collect(),
            checkpoints
                .iter()
                .map(|d| WasmDigest::from(d).into())
                .collect(),
            blob_meta.into(),
        )
        .into()
    }

    /// Get the head digest of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmDigest {
        self.0.head().into()
    }

    /// Get the boundary digests of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn boundary(&self) -> Vec<WasmDigest> {
        self.0.boundary().iter().copied().map(Into::into).collect()
    }

    /// Get the checkpoints of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn checkpoints(&self) -> Vec<WasmDigest> {
        self.0
            .checkpoints()
            .iter()
            .copied()
            .map(Into::into)
            .collect()
    }

    /// Get the blob metadata of the fragment.
    #[must_use]
    #[wasm_bindgen(getter, js_name = blobMeta)]
    pub fn blob_meta(&self) -> WasmBlobMeta {
        self.0.summary().blob_meta().into()
    }
}

impl From<Fragment> for WasmFragment {
    fn from(fragment: Fragment) -> Self {
        Self(fragment)
    }
}

impl From<WasmFragment> for Fragment {
    fn from(fragment: WasmFragment) -> Self {
        fragment.0
    }
}

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
        FragmentRequested::new(digest.clone().into(), depth.clone().into()).into()
    }

    /// Get the digest of the requested fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmDigest {
        (*self.0.head()).into()
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

#[wasm_bindgen(js_name = FragmentsArray)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WasmFragmentsArray(pub(crate) Vec<WasmFragment>);

#[wasm_refgen(js_ref = JsFragmentsArray)]
#[wasm_bindgen(js_class = FragmentsArray)]
impl WasmFragmentsArray {}

impl TryFrom<&JsValue> for WasmFragmentsArray {
    type Error = WasmConvertJsValueToFragmentArrayError;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(WasmFragmentsArray(
            try_into_js_fragment_array(js_value).map_err(WasmConvertJsValueToFragmentArrayError)?,
        ))
    }
}

/// An error indicating that a `JsValue` could not be converted into a `Fragment` array.
#[derive(Debug, Error)]
#[error("unable to convert JsValue into Fragment array")]
pub struct WasmConvertJsValueToFragmentArrayError(JsValue);

impl From<WasmConvertJsValueToFragmentArrayError> for JsValue {
    fn from(err: WasmConvertJsValueToFragmentArrayError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("UnableToConvertFragmentArrayError");
        err.into()
    }
}

#[wasm_bindgen(inline_js = r#"
    export function tryIntoJsFragmentArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `WasmFragment`.
    #[wasm_bindgen(js_name = tryIntoJsFragmentArray, catch)]
    pub fn try_into_js_fragment_array(v: &JsValue) -> Result<Vec<WasmFragment>, JsValue>;
}
