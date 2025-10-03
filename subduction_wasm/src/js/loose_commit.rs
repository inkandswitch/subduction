//! Individual/"loose" commits.

use sedimentree_core::{BlobMeta, LooseCommit};
use wasm_bindgen::prelude::*;

use super::digest::JsDigest;

/// A Wasm wrapper around the [`LooseCommit`] type.
#[wasm_bindgen(js_name = LooseCommit)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JsLooseCommit(LooseCommit);

#[wasm_bindgen(js_class = LooseCommit)]
impl JsLooseCommit {
    // FIXME constrcutor

    /// Get the digest of the commit.
    #[wasm_bindgen(getter)]
    pub fn digest(&self) -> JsDigest {
        self.0.digest().into()
    }

    /// Get the parent digests of the commit.
    #[wasm_bindgen(getter)]
    pub fn parents(&self) -> Vec<JsDigest> {
        self.0.parents().iter().copied().map(Into::into).collect()
    }

    /// Get the blob metadata of the commit.
    #[wasm_bindgen(getter, js_name = blobMeta)]
    pub fn blob_meta(&self) -> JsBlobMeta {
        self.0.blob().clone().into()
    }
}

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

/// A Wasm wrapper around the [`BlobMeta`] type.
#[wasm_bindgen(js_name = BlobMeta)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(missing_copy_implementations)]
pub struct JsBlobMeta(BlobMeta);

#[wasm_bindgen(js_class = BlobMeta)]
impl JsBlobMeta {
    /// Get the digest of the blob.
    pub fn digest(&self) -> JsDigest {
        self.0.digest().into()
    }

    /// Get the size of the blob in bytes.
    #[wasm_bindgen(getter, js_name = sizeBytes)]
    pub fn size_bytes(&self) -> u64 {
        self.0.size_bytes()
    }
}

impl From<BlobMeta> for JsBlobMeta {
    fn from(meta: BlobMeta) -> Self {
        Self(meta)
    }
}

impl From<JsBlobMeta> for BlobMeta {
    fn from(meta: JsBlobMeta) -> Self {
        meta.0
    }
}

#[wasm_bindgen(inline_js = r#"
export function tryIntoLooseCommitArray(xs) { return xs; }
"#)]

extern "C" {
    /// Try to convert a `JsValue` into an array of `JsLooseCommit`.
    #[wasm_bindgen(js_name = tryIntoLooseCommitArray, catch)]
    pub fn try_into_js_loose_commit_array(v: &JsValue) -> Result<Vec<JsLooseCommit>, JsValue>;
}

pub(crate) struct JsLooseCommitsArray(pub(crate) Vec<JsLooseCommit>);

impl TryFrom<&JsValue> for JsLooseCommitsArray {
    type Error = JsValue;

    fn try_from(js_value: &JsValue) -> Result<Self, Self::Error> {
        Ok(JsLooseCommitsArray(try_into_js_loose_commit_array(
            js_value,
        )?))
    }
}
