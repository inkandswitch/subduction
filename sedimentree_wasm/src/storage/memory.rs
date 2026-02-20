//! In-memory storage for testing and development.

use alloc::{format, string::ToString};
use future_form::Local;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::Blob, crypto::digest::Digest, fragment::Fragment, id::SedimentreeId,
    loose_commit::LooseCommit,
};
use subduction_core::{
    crypto::Signed,
    storage::{memory::MemoryStorage as CoreMemoryStorage, traits::Storage},
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::{
    digest::{JsDigest, WasmDigest},
    sedimentree_id::{JsSedimentreeId, WasmSedimentreeId},
};

/// An in-memory storage implementation for use in tests and development.
///
/// This wraps the core `MemoryStorage` and exposes it via the `SedimentreeStorage` interface.
#[wasm_bindgen]
#[derive(Debug)]
pub struct MemoryStorage {
    inner: CoreMemoryStorage,
}

#[wasm_bindgen]
impl MemoryStorage {
    /// Create a new in-memory storage instance.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: CoreMemoryStorage::new(),
        }
    }

    // ==================== Sedimentree IDs ====================

    /// Save a sedimentree ID.
    #[wasm_bindgen(js_name = saveSedimentreeId)]
    pub fn save_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            Storage::<Local>::save_sedimentree_id(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Delete a sedimentree ID.
    #[wasm_bindgen(js_name = deleteSedimentreeId)]
    pub fn delete_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_sedimentree_id(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Load all sedimentree IDs.
    #[wasm_bindgen(js_name = loadAllSedimentreeIds)]
    pub fn load_all_sedimentree_ids(&self) -> Promise {
        let inner = self.inner.clone();
        future_to_promise(async move {
            let ids = Storage::<Local>::load_all_sedimentree_ids(&inner)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for id in ids {
                result.push(&JsSedimentreeId::from(WasmSedimentreeId::from(id)));
            }
            Ok(result.into())
        })
    }

    // ==================== Commits ====================

    /// Save a commit.
    #[wasm_bindgen(js_name = saveCommit)]
    pub fn save_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        _digest: &WasmDigest,
        signed_commit: &Uint8Array,
        _blob_digest: &WasmDigest,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let bytes = signed_commit.to_vec();
        future_to_promise(async move {
            let signed: Signed<LooseCommit> = minicbor::decode(&bytes)
                .map_err(|e| JsValue::from_str(&format!("CBOR decode error: {e}")))?;
            Storage::<Local>::save_loose_commit(&inner, id, signed)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Load a commit by digest.
    #[wasm_bindgen(js_name = loadCommit)]
    pub fn load_commit(&self, sedimentree_id: &WasmSedimentreeId, digest: &WasmDigest) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let digest: Digest<LooseCommit> = digest.clone().into();
        future_to_promise(async move {
            let result = Storage::<Local>::load_loose_commit(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            match result {
                Some(signed) => {
                    let bytes = minicbor::to_vec(&signed)
                        .map_err(|e| JsValue::from_str(&format!("CBOR encode error: {e}")))?;
                    Ok(Uint8Array::from(bytes.as_slice()).into())
                }
                None => Ok(JsValue::NULL),
            }
        })
    }

    /// List all commit digests for a sedimentree.
    #[wasm_bindgen(js_name = listCommitDigests)]
    pub fn list_commit_digests(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let digests = Storage::<Local>::list_commit_digests(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for d in digests {
                result.push(&JsDigest::from(WasmDigest::from(d)));
            }
            Ok(result.into())
        })
    }

    /// Load all commits for a sedimentree.
    #[wasm_bindgen(js_name = loadAllCommits)]
    pub fn load_all_commits(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let commits = Storage::<Local>::load_loose_commits(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for (digest, signed) in commits {
                let bytes = minicbor::to_vec(&signed)
                    .map_err(|e| JsValue::from_str(&format!("CBOR encode error: {e}")))?;
                let obj = js_sys::Object::new();
                js_sys::Reflect::set(
                    &obj,
                    &"digest".into(),
                    &JsDigest::from(WasmDigest::from(digest)),
                )?;
                js_sys::Reflect::set(&obj, &"signed".into(), &Uint8Array::from(bytes.as_slice()))?;
                result.push(&obj);
            }
            Ok(result.into())
        })
    }

    /// Delete a commit by digest.
    #[wasm_bindgen(js_name = deleteCommit)]
    pub fn delete_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let digest: Digest<LooseCommit> = digest.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_loose_commit(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Delete all commits for a sedimentree.
    #[wasm_bindgen(js_name = deleteAllCommits)]
    pub fn delete_all_commits(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_loose_commits(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    // ==================== Fragments ====================

    /// Save a fragment.
    #[wasm_bindgen(js_name = saveFragment)]
    pub fn save_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        _digest: &WasmDigest,
        signed_fragment: &Uint8Array,
        _blob_digest: &WasmDigest,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let bytes = signed_fragment.to_vec();
        future_to_promise(async move {
            let signed: Signed<Fragment> = minicbor::decode(&bytes)
                .map_err(|e| JsValue::from_str(&format!("CBOR decode error: {e}")))?;
            Storage::<Local>::save_fragment(&inner, id, signed)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Load a fragment by digest.
    #[wasm_bindgen(js_name = loadFragment)]
    pub fn load_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let digest: Digest<Fragment> = digest.clone().into();
        future_to_promise(async move {
            let result = Storage::<Local>::load_fragment(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            match result {
                Some(signed) => {
                    let bytes = minicbor::to_vec(&signed)
                        .map_err(|e| JsValue::from_str(&format!("CBOR encode error: {e}")))?;
                    Ok(Uint8Array::from(bytes.as_slice()).into())
                }
                None => Ok(JsValue::NULL),
            }
        })
    }

    /// List all fragment digests for a sedimentree.
    #[wasm_bindgen(js_name = listFragmentDigests)]
    pub fn list_fragment_digests(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let digests = Storage::<Local>::list_fragment_digests(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for d in digests {
                result.push(&JsDigest::from(WasmDigest::from(d)));
            }
            Ok(result.into())
        })
    }

    /// Load all fragments for a sedimentree.
    #[wasm_bindgen(js_name = loadAllFragments)]
    pub fn load_all_fragments(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let fragments = Storage::<Local>::load_fragments(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for (digest, signed) in fragments {
                let bytes = minicbor::to_vec(&signed)
                    .map_err(|e| JsValue::from_str(&format!("CBOR encode error: {e}")))?;
                let obj = js_sys::Object::new();
                js_sys::Reflect::set(
                    &obj,
                    &"digest".into(),
                    &JsDigest::from(WasmDigest::from(digest)),
                )?;
                js_sys::Reflect::set(&obj, &"signed".into(), &Uint8Array::from(bytes.as_slice()))?;
                result.push(&obj);
            }
            Ok(result.into())
        })
    }

    /// Delete a fragment by digest.
    #[wasm_bindgen(js_name = deleteFragment)]
    pub fn delete_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let digest: Digest<Fragment> = digest.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_fragment(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Delete all fragments for a sedimentree.
    #[wasm_bindgen(js_name = deleteAllFragments)]
    pub fn delete_all_fragments(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_fragments(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    // ==================== Blobs ====================

    /// Save a blob and return its digest.
    #[wasm_bindgen(js_name = saveBlob)]
    pub fn save_blob(&self, id: &WasmSedimentreeId, data: &Uint8Array) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = id.clone().into();
        let bytes = data.to_vec();
        future_to_promise(async move {
            let blob = Blob::from(bytes);
            let digest = Storage::<Local>::save_blob(&inner, id, blob)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsDigest::from(WasmDigest::from(digest)).into())
        })
    }

    /// Load a blob by digest within a sedimentree.
    #[wasm_bindgen(js_name = loadBlob)]
    pub fn load_blob(&self, id: &WasmSedimentreeId, digest: &WasmDigest) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = id.clone().into();
        let digest: Digest<Blob> = digest.clone().into();
        future_to_promise(async move {
            let result = Storage::<Local>::load_blob(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            match result {
                Some(blob) => Ok(Uint8Array::from(blob.as_slice()).into()),
                None => Ok(JsValue::NULL),
            }
        })
    }

    /// Delete a blob by digest within a sedimentree.
    #[wasm_bindgen(js_name = deleteBlob)]
    pub fn delete_blob(&self, id: &WasmSedimentreeId, digest: &WasmDigest) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = id.clone().into();
        let digest: Digest<Blob> = digest.clone().into();
        future_to_promise(async move {
            Storage::<Local>::delete_blob(&inner, id, digest)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
