//! Individual/"loose" commits.

use sedimentree_core::{blob::BlobMeta, LooseCommit};
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use super::digest::WasmDigest;

/// A Wasm wrapper around the [`LooseCommit`] type.
#[wasm_bindgen(js_name = LooseCommit)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WasmLooseCommit(LooseCommit);

#[wasm_refgen(js_ref = JsLooseCommit)]
#[wasm_bindgen(js_class = LooseCommit)]
impl WasmLooseCommit {
    /// Get the digest of the commit.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn digest(&self) -> WasmDigest {
        self.0.digest().into()
    }

    /// Get the parent digests of the commit.
    #[wasm_bindgen(getter)]
    pub fn parents(&self) -> Vec<WasmDigest> {
        self.0.parents().iter().copied().map(Into::into).collect()
    }

    /// Get the blob metadata of the commit.
    #[must_use]
    #[wasm_bindgen(getter, js_name = blobMeta)]
    pub fn blob_meta(&self) -> WasmBlobMeta {
        self.0.blob().to_owned().into()
    }
}

impl From<LooseCommit> for WasmLooseCommit {
    fn from(commit: LooseCommit) -> Self {
        Self(commit)
    }
}

impl From<WasmLooseCommit> for LooseCommit {
    fn from(commit: WasmLooseCommit) -> Self {
        commit.0
    }
}

/// A Wasm wrapper around the [`BlobMeta`] type.
#[wasm_bindgen(js_name = BlobMeta)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(missing_copy_implementations)]
pub struct WasmBlobMeta(BlobMeta);

#[wasm_bindgen(js_class = BlobMeta)]
impl WasmBlobMeta {
    /// Get the digest of the blob.
    #[must_use]
    pub fn digest(&self) -> WasmDigest {
        self.0.digest().into()
    }

    /// Get the size of the blob in bytes.
    #[must_use]
    #[wasm_bindgen(getter, js_name = sizeBytes)]
    #[allow(clippy::missing_const_for_fn)]
    pub fn size_bytes(&self) -> u64 {
        self.0.size_bytes()
    }
}

impl From<BlobMeta> for WasmBlobMeta {
    fn from(meta: BlobMeta) -> Self {
        Self(meta)
    }
}

impl From<WasmBlobMeta> for BlobMeta {
    fn from(meta: WasmBlobMeta) -> Self {
        meta.0
    }
}

#[wasm_bindgen(inline_js = r#"
export function tryIntoLooseCommitArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `WasmLooseCommit`.
    #[wasm_bindgen(js_name = tryIntoLooseCommitArray, catch)]
    pub fn try_into_js_loose_commit_array(v: &JsValue) -> Result<Vec<WasmLooseCommit>, JsValue>;
}

pub(crate) struct WasmLooseCommitsArray(pub(crate) Vec<WasmLooseCommit>);

impl TryFrom<&JsValue> for WasmLooseCommitsArray {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(WasmLooseCommitsArray(try_into_js_loose_commit_array(
            js_value,
        )?))
    }
}
