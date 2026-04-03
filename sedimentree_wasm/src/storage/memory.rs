//! In-memory storage for testing and development.

use alloc::string::ToString;
use future_form::Local;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::Blob,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
};
use subduction_core::storage::{memory::MemoryStorage as CoreMemoryStorage, traits::Storage};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use wasm_bindgen::{convert::TryFromJsValue, prelude::*};
use wasm_bindgen_futures::future_to_promise;

use crate::{
    commit_id::WasmCommitId,
    digest::WasmDigest,
    fragment::WasmFragmentWithBlob,
    loose_commit::WasmCommitWithBlob,
    sedimentree_id::{JsSedimentreeId, WasmSedimentreeId},
    signed::{WasmSignedFragment, WasmSignedLooseCommit},
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

    // ==================== Commits (compound with blob) ====================

    /// Save a commit with its blob.
    ///
    /// The `commit_id` parameter must match the `head()` embedded in
    /// the signed commit payload. Returns an error if they differ.
    ///
    /// # Errors
    ///
    /// Returns a JS error if:
    /// - The signed payload cannot be decoded
    /// - The `commit_id` does not match the embedded `head()`
    #[wasm_bindgen(js_name = saveCommit)]
    pub fn save_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        commit_id: &WasmCommitId,
        signed_commit: &WasmSignedLooseCommit,
        blob: &Uint8Array,
    ) -> Promise {
        let expected_id: CommitId = commit_id.into();
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let signed: Signed<LooseCommit> = signed_commit.clone().into();
        let blob = Blob::new(blob.to_vec());
        future_to_promise(async move {
            // Reconstruct from trusted JS storage without re-verification
            let verified = VerifiedMeta::try_from_trusted(signed, blob)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            if verified.payload().head() != expected_id {
                return Err(JsValue::from_str(
                    "commit_id parameter does not match the embedded head",
                ));
            }
            Storage::<Local>::save_loose_commit(&inner, id, verified)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// List all commit IDs for a sedimentree.
    #[wasm_bindgen(js_name = listCommitIds)]
    pub fn list_commit_ids(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let commit_ids = Storage::<Local>::list_commit_ids(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for cid in commit_ids {
                result.push(&JsValue::from(WasmCommitId::from(cid)));
            }
            Ok(result.into())
        })
    }

    /// Load all commits for a sedimentree, returning `CommitWithBlob[]`.
    #[wasm_bindgen(js_name = loadAllCommits)]
    pub fn load_all_commits(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let commits = Storage::<Local>::load_loose_commits(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for verified in commits {
                let signed = WasmSignedLooseCommit::from(verified.signed().clone());
                let blob = Uint8Array::from(verified.blob().contents().as_slice());
                result.push(&WasmCommitWithBlob::new(signed, blob).into());
            }
            Ok(result.into())
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

    // ==================== Fragments (compound with blob) ====================

    /// Save a fragment with its blob.
    #[wasm_bindgen(js_name = saveFragment)]
    pub fn save_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        _digest: &WasmDigest,
        signed_fragment: &WasmSignedFragment,
        blob: &Uint8Array,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let signed: Signed<Fragment> = signed_fragment.clone().into();
        let blob = Blob::new(blob.to_vec());
        future_to_promise(async move {
            // Reconstruct from trusted JS storage without re-verification
            let verified = VerifiedMeta::try_from_trusted(signed, blob)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Storage::<Local>::save_fragment(&inner, id, verified)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Load a fragment by its identifier, returning `FragmentWithBlob` or null.
    #[wasm_bindgen(js_name = loadFragment)]
    pub fn load_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        fragment_head: &WasmCommitId,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let fragment_head = CommitId::from(fragment_head);
        future_to_promise(async move {
            let result = Storage::<Local>::load_fragment(&inner, id, fragment_head)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            match result {
                Some(verified) => {
                    let signed = WasmSignedFragment::from(verified.signed().clone());
                    let blob = Uint8Array::from(verified.blob().contents().as_slice());
                    Ok(WasmFragmentWithBlob::new(signed, blob).into())
                }
                None => Ok(JsValue::NULL),
            }
        })
    }

    /// List all fragment IDs for a sedimentree.
    #[wasm_bindgen(js_name = listFragmentIds)]
    pub fn list_fragment_ids(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let fragment_ids = Storage::<Local>::list_fragment_ids(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for fid in fragment_ids {
                result.push(&JsValue::from(WasmCommitId::from(fid)));
            }
            Ok(result.into())
        })
    }

    /// Load all fragments for a sedimentree, returning `FragmentWithBlob[]`.
    #[wasm_bindgen(js_name = loadAllFragments)]
    pub fn load_all_fragments(&self, sedimentree_id: &WasmSedimentreeId) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            let fragments = Storage::<Local>::load_fragments(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            let result = js_sys::Array::new();
            for verified in fragments {
                let signed = WasmSignedFragment::from(verified.signed().clone());
                let blob = Uint8Array::from(verified.blob().contents().as_slice());
                result.push(&WasmFragmentWithBlob::new(signed, blob).into());
            }
            Ok(result.into())
        })
    }

    /// Delete a fragment by its identifier.
    #[wasm_bindgen(js_name = deleteFragment)]
    pub fn delete_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        fragment_head: &WasmCommitId,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        let fragment_head = CommitId::from(fragment_head);
        future_to_promise(async move {
            Storage::<Local>::delete_fragment(&inner, id, fragment_head)
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

    // ==================== Batch Operations ====================

    /// Save commits and fragments in a single batch.
    #[wasm_bindgen(js_name = saveBatchAll)]
    pub fn save_batch_all(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        commits: js_sys::Array,
        fragments: js_sys::Array,
    ) -> Promise {
        let inner = self.inner.clone();
        let id: SedimentreeId = sedimentree_id.clone().into();
        future_to_promise(async move {
            Storage::<Local>::save_sedimentree_id(&inner, id)
                .await
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            for item in commits.iter() {
                let signed_val = js_sys::Reflect::get(&item, &JsValue::from_str("signedCommit"))
                    .map_err(|e| JsValue::from_str(&alloc::format!("Reflect error: {e:?}")))?;
                let signed = WasmSignedLooseCommit::try_from_js_value(signed_val)
                    .map_err(|e| JsValue::from_str(&alloc::format!("conversion error: {e:?}")))?;
                let blob_val = js_sys::Reflect::get(&item, &JsValue::from_str("blob"))
                    .map_err(|e| JsValue::from_str(&alloc::format!("Reflect error: {e:?}")))?;
                let blob = Blob::new(Uint8Array::new(&blob_val).to_vec());

                let verified =
                    VerifiedMeta::try_from_trusted(Signed::<LooseCommit>::from(signed), blob)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;
                Storage::<Local>::save_loose_commit(&inner, id, verified)
                    .await
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }

            for item in fragments.iter() {
                let signed_val = js_sys::Reflect::get(&item, &JsValue::from_str("signedFragment"))
                    .map_err(|e| JsValue::from_str(&alloc::format!("Reflect error: {e:?}")))?;
                let signed = WasmSignedFragment::try_from_js_value(signed_val)
                    .map_err(|e| JsValue::from_str(&alloc::format!("conversion error: {e:?}")))?;
                let blob_val = js_sys::Reflect::get(&item, &JsValue::from_str("blob"))
                    .map_err(|e| JsValue::from_str(&alloc::format!("Reflect error: {e:?}")))?;
                let blob = Blob::new(Uint8Array::new(&blob_val).to_vec());

                let verified =
                    VerifiedMeta::try_from_trusted(Signed::<Fragment>::from(signed), blob)
                        .map_err(|e| JsValue::from_str(&e.to_string()))?;
                Storage::<Local>::save_fragment(&inner, id, verified)
                    .await
                    .map_err(|e| JsValue::from_str(&e.to_string()))?;
            }

            Ok(JsValue::from(commits.length() + fragments.length()))
        })
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}
