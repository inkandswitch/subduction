//! Sedimentree [`Fragment`](sedimentree_core::Fragment).

use alloc::{collections::BTreeSet, string::ToString, vec::Vec};
use sedimentree_core::fragment::Fragment;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use js_sys::Uint8Array;

use crate::{
    checkpoint::{JsCheckpoint, WasmCheckpoint},
    commit_id::{JsCommitId, WasmCommitId},
    loose_commit::WasmBlobMeta,
    sedimentree_id::WasmSedimentreeId,
    signed::WasmSignedFragment,
};

/// A data fragment used in the Sedimentree system.
#[derive(Debug, Clone, PartialEq, Eq)]
#[wasm_bindgen(js_name = Fragment)]
pub struct WasmFragment(Fragment);

#[wasm_refgen(js_ref = JsFragment)]
#[wasm_bindgen(js_class = Fragment)]
impl WasmFragment {
    /// Create a new fragment from the given sedimentree ID, head, boundary, checkpoints, and blob metadata.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub fn new(
        sedimentree_id: &WasmSedimentreeId,
        head: &WasmCommitId,
        boundary: Vec<JsCommitId>,
        checkpoints: Vec<JsCommitId>,
        blob_meta: &WasmBlobMeta,
    ) -> Self {
        let cps: Vec<_> = checkpoints
            .iter()
            .map(|p| WasmCommitId::from(p).into())
            .collect();
        Fragment::new(
            sedimentree_id.into(),
            head.into(),
            boundary
                .iter()
                .map(|p| WasmCommitId::from(p).into())
                .collect(),
            &cps,
            blob_meta.into(),
        )
        .into()
    }

    /// Create a fragment from the 12-byte checkpoints stored on wire.
    ///
    /// All wrapper arguments are borrowed/copied, not consumed. Boundaries and
    /// checkpoints are deduplicated and sorted; no checkpoint is filtered out.
    ///
    /// # Errors
    ///
    /// Throws an `InvalidFragment` error if there are more than 255 unique
    /// boundary IDs or 65535 unique checkpoints (the wire-format count limits).
    #[wasm_bindgen(js_name = fromCheckpointPrefixes)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs Vecs, not slices
    pub fn from_checkpoint_prefixes(
        sedimentree_id: &WasmSedimentreeId,
        head: &WasmCommitId,
        boundary: Vec<JsCommitId>,
        checkpoints: Vec<JsCheckpoint>,
        blob_meta: &WasmBlobMeta,
    ) -> Result<Self, WasmInvalidFragment> {
        let boundary: BTreeSet<_> = boundary
            .iter()
            .map(|id| WasmCommitId::from(id).into())
            .collect();
        let checkpoints: BTreeSet<_> = checkpoints
            .iter()
            .map(|checkpoint| WasmCheckpoint::from(checkpoint).into())
            .collect();
        validate_counts(boundary.len(), checkpoints.len())?;
        Ok(Fragment::from_parts(
            sedimentree_id.into(),
            head.into(),
            boundary,
            checkpoints,
            blob_meta.into(),
        )
        .into())
    }

    /// Get the checkpoints in ascending byte order.
    ///
    /// Each wrapper is independently owned and remains usable after this
    /// fragment is freed. The caller should free each returned checkpoint.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn checkpoints(&self) -> Vec<WasmCheckpoint> {
        self.0
            .checkpoints()
            .iter()
            .copied()
            .map(WasmCheckpoint::from)
            .collect()
    }

    /// Get the actual sedimentree ID in the fragment payload.
    ///
    /// This is an independently owned copy; the caller should free it.
    /// Reading it does not authenticate the fragment or verify a signature.
    #[must_use]
    #[wasm_bindgen(getter, js_name = sedimentreeId)]
    pub fn sedimentree_id(&self) -> WasmSedimentreeId {
        self.0.sedimentree_id().into()
    }

    /// Get the head commit identifier of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn head(&self) -> WasmCommitId {
        WasmCommitId::from(self.0.head())
    }

    /// Get the boundary commit identifiers of the fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn boundary(&self) -> Vec<WasmCommitId> {
        self.0
            .boundary()
            .iter()
            .copied()
            .map(WasmCommitId::from)
            .collect()
    }

    /// Get the blob metadata of the fragment.
    #[must_use]
    #[wasm_bindgen(getter, js_name = blobMeta)]
    pub fn blob_meta(&self) -> WasmBlobMeta {
        self.0.summary().blob_meta().into()
    }
}

/// A fragment's deduplicated metadata exceeds a wire-format count limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WasmInvalidFragment {
    /// The boundary count must fit in a single byte.
    #[error("expected at most 255 unique boundary IDs, got {0}")]
    TooManyBoundaries(usize),
    /// The checkpoint count must fit in two bytes.
    #[error("expected at most 65535 unique checkpoints, got {0}")]
    TooManyCheckpoints(usize),
}

impl From<WasmInvalidFragment> for JsValue {
    fn from(err: WasmInvalidFragment) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidFragment");
        err.into()
    }
}

fn validate_counts(boundaries: usize, checkpoints: usize) -> Result<(), WasmInvalidFragment> {
    if boundaries > usize::from(u8::MAX) {
        return Err(WasmInvalidFragment::TooManyBoundaries(boundaries));
    }
    if checkpoints > usize::from(u16::MAX) {
        return Err(WasmInvalidFragment::TooManyCheckpoints(checkpoints));
    }
    Ok(())
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

/// A fragment stored with its associated blob.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = FragmentWithBlob)]
pub struct WasmFragmentWithBlob {
    signed: WasmSignedFragment,
    blob: Vec<u8>,
}

#[wasm_refgen(js_ref = JsFragmentWithBlob)]
#[wasm_bindgen(js_class = FragmentWithBlob)]
impl WasmFragmentWithBlob {
    /// Create a new fragment with blob.
    #[must_use]
    #[wasm_bindgen(constructor)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen requires owned Uint8Array
    pub fn new(signed: WasmSignedFragment, blob: Uint8Array) -> Self {
        Self {
            signed,
            blob: blob.to_vec(),
        }
    }

    /// Get the signed fragment.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn signed(&self) -> WasmSignedFragment {
        self.signed.clone()
    }

    /// Get the blob.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn blob(&self) -> Uint8Array {
        Uint8Array::from(self.blob.as_slice())
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
