//! Individual/"loose" commits.

use alloc::{borrow::ToOwned, collections::BTreeSet, vec::Vec};
use sedimentree_core::{blob::BlobMeta, crypto::digest::Digest, loose_commit::LooseCommit};
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use super::digest::{JsDigest, WasmDigest};
use js_sys::Uint8Array;

use crate::{sedimentree_id::WasmSedimentreeId, signed::WasmSignedLooseCommit};

/// A Wasm wrapper around the [`LooseCommit`] type.
#[wasm_bindgen(js_name = LooseCommit)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WasmLooseCommit(LooseCommit);

#[wasm_refgen(js_ref = JsLooseCommit)]
#[wasm_bindgen(js_class = LooseCommit)]
impl WasmLooseCommit {
    /// Create a new `LooseCommit` from the given sedimentree ID, digest, parents, and blob metadata.
    #[wasm_bindgen(constructor)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub fn new(
        sedimentree_id: WasmSedimentreeId,
        digest: &WasmDigest,
        parents: Vec<JsDigest>,
        blob_meta: &WasmBlobMeta,
    ) -> Self {
        let core_parents: BTreeSet<Digest<LooseCommit>> =
            parents.iter().map(|d| WasmDigest::from(d).into()).collect();

        let core_commit = LooseCommit::new(
            sedimentree_id.into(),
            digest.clone().into(),
            core_parents,
            blob_meta.clone().into(),
        );

        Self(core_commit)
    }

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
        self.0.blob_meta().to_owned().into()
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
    /// Create a new `BlobMeta` from the given blob contents.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(blob: &[u8]) -> Self {
        BlobMeta::new(blob).into()
    }

    /// Create a `BlobMeta` from a digest and size.
    ///
    /// This is useful for deserialization when the original blob is not available.
    /// Since this is manual, the caller must ensure the digest and size are correct.
    #[wasm_bindgen(js_name = fromDigestSize)]
    #[must_use]
    pub fn from_digest_size(digest: &WasmDigest, size_bytes: u64) -> Self {
        BlobMeta::from_digest_size(digest.clone().into(), size_bytes).into()
    }

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

/// A commit stored with its associated blob.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = CommitWithBlob)]
pub struct WasmCommitWithBlob {
    signed: WasmSignedLooseCommit,
    blob: Vec<u8>,
}

#[wasm_refgen(js_ref = JsCommitWithBlob)]
#[wasm_bindgen(js_class = CommitWithBlob)]
impl WasmCommitWithBlob {
    /// Create a new commit with blob.
    #[wasm_bindgen(constructor)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen requires owned Uint8Array
    pub fn new(signed: WasmSignedLooseCommit, blob: Uint8Array) -> Self {
        Self {
            signed,
            blob: blob.to_vec(),
        }
    }

    /// Get the signed commit.
    #[wasm_bindgen(getter)]
    pub fn signed(&self) -> WasmSignedLooseCommit {
        self.signed.clone()
    }

    /// Get the blob.
    #[wasm_bindgen(getter)]
    pub fn blob(&self) -> Uint8Array {
        Uint8Array::from(self.blob.as_slice())
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
