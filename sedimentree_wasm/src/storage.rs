//! Persistent storage.

#[cfg(feature = "idb")]
pub mod idb;

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use sedimentree_core::collections::Set;

use future_form::{FutureForm, Local};
use futures::future::LocalBoxFuture;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::Blob,
    crypto::digest::Digest,
    fragment::Fragment,
    id::{BadSedimentreeId, SedimentreeId},
    loose_commit::LooseCommit,
};
use subduction_core::storage::{batch_result::BatchResult, traits::Storage};
use subduction_crypto::signed::Signed;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    digest::{JsDigest, WasmDigest},
    sedimentree_id::{
        JsSedimentreeId, WasmConvertJsValueToSedimentreeIdArrayError, WasmSedimentreeId,
        WasmSedimentreeIdsArray,
    },
};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface SedimentreeStorage {
    saveSedimentreeId(sedimentreeId: SedimentreeId): Promise<void>;
    deleteSedimentreeId(sedimentreeId: SedimentreeId): Promise<void>;
    loadAllSedimentreeIds(): Promise<SedimentreeId[]>;

    // CAS operations for commits (keyed by digest)
    // blobDigest is provided so storage bridges can load the blob for event callbacks
    saveCommit(sedimentreeId: SedimentreeId, digest: Digest, signedCommit: Uint8Array, blobDigest: Digest): Promise<void>;
    loadCommit(sedimentreeId: SedimentreeId, digest: Digest): Promise<Uint8Array | null>;
    listCommitDigests(sedimentreeId: SedimentreeId): Promise<Digest[]>;
    loadAllCommits(sedimentreeId: SedimentreeId): Promise<Array<{digest: Digest, signed: Uint8Array}>>;
    deleteCommit(sedimentreeId: SedimentreeId, digest: Digest): Promise<void>;
    deleteAllCommits(sedimentreeId: SedimentreeId): Promise<void>;

    // CAS operations for fragments (keyed by digest)
    // blobDigest is provided so storage bridges can load the blob for event callbacks
    saveFragment(sedimentreeId: SedimentreeId, digest: Digest, signedFragment: Uint8Array, blobDigest: Digest): Promise<void>;
    loadFragment(sedimentreeId: SedimentreeId, digest: Digest): Promise<Uint8Array | null>;
    listFragmentDigests(sedimentreeId: SedimentreeId): Promise<Digest[]>;
    loadAllFragments(sedimentreeId: SedimentreeId): Promise<Array<{digest: Digest, signed: Uint8Array}>>;
    deleteFragment(sedimentreeId: SedimentreeId, digest: Digest): Promise<void>;
    deleteAllFragments(sedimentreeId: SedimentreeId): Promise<void>;

    // Blobs (keyed by sedimentree + digest)
    saveBlob(sedimentreeId: SedimentreeId, data: Uint8Array): Promise<Digest>;
    loadBlob(sedimentreeId: SedimentreeId, digest: Digest): Promise<Uint8Array | null>;
    deleteBlob(sedimentreeId: SedimentreeId, digest: Digest): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed storage backend interface.
    #[wasm_bindgen(js_name = Storage, typescript_type = "SedimentreeStorage")]
    pub type JsSedimentreeStorage;

    // ==================== Sedimentree IDs ====================

    #[wasm_bindgen(method, js_name = saveSedimentreeId)]
    fn js_save_sedimentree_id(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteSedimentreeId)]
    fn js_delete_sedimentree_id(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllSedimentreeIds)]
    fn js_load_all_sedimentree_ids(this: &JsSedimentreeStorage) -> Promise;

    // ==================== Commits (CAS) ====================

    #[wasm_bindgen(method, js_name = saveCommit)]
    fn js_save_commit(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
        signed_commit: &[u8],
        blob_digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadCommit)]
    fn js_load_commit(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = listCommitDigests)]
    fn js_list_commit_digests(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllCommits)]
    fn js_load_all_commits(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteCommit)]
    fn js_delete_commit(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteAllCommits)]
    fn js_delete_all_commits(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    // ==================== Fragments (CAS) ====================

    #[wasm_bindgen(method, js_name = saveFragment)]
    fn js_save_fragment(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
        signed_fragment: &[u8],
        blob_digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadFragment)]
    fn js_load_fragment(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = listFragmentDigests)]
    fn js_list_fragment_digests(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllFragments)]
    fn js_load_all_fragments(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteFragment)]
    fn js_delete_fragment(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteAllFragments)]
    fn js_delete_all_fragments(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    // ==================== Blobs ====================

    #[wasm_bindgen(method, js_name = saveBlob)]
    fn js_save_blob(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        blob: &[u8],
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadBlob)]
    fn js_load_blob(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        blob_digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteBlob)]
    fn js_delete_blob(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        blob_digest: &JsDigest,
    ) -> Promise;
}

impl core::fmt::Debug for JsSedimentreeStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsSedimentreeStorage").finish()
    }
}

/// Compute digest from a signed commit by decoding the payload.
fn commit_digest(
    signed: &Signed<LooseCommit>,
    sedimentree_id: &SedimentreeId,
) -> Option<Digest<LooseCommit>> {
    signed
        .try_decode_payload(sedimentree_id)
        .ok()
        .map(|c| c.digest())
}

/// Compute digest from a signed fragment by decoding the payload.
fn fragment_digest(
    signed: &Signed<Fragment>,
    sedimentree_id: &SedimentreeId,
) -> Option<Digest<Fragment>> {
    signed
        .try_decode_payload(sedimentree_id)
        .ok()
        .map(|f| f.digest())
}

/// Compute blob digest from a signed fragment by decoding the payload.
fn fragment_blob_digest(
    signed: &Signed<Fragment>,
    sedimentree_id: &SedimentreeId,
) -> Option<Digest<Blob>> {
    signed
        .try_decode_payload(sedimentree_id)
        .ok()
        .map(|f| f.summary().blob_meta().digest())
}

/// Parse a JS array of {digest, signed} objects into Rust tuples.
#[allow(clippy::type_complexity)]
fn parse_digest_signed_array<T: sedimentree_core::codec::Codec>(
    js_value: &JsValue,
) -> Result<Vec<(Digest<T>, Signed<T>)>, JsSedimentreeStorageError> {
    let array = js_sys::Array::from(js_value);
    let mut result = Vec::with_capacity(array.length() as usize);

    for i in 0..array.length() {
        let item = array.get(i);
        let digest_val = js_sys::Reflect::get(&item, &JsValue::from_str("digest"))
            .map_err(JsSedimentreeStorageError::ReflectError)?;
        let signed_val = js_sys::Reflect::get(&item, &JsValue::from_str("signed"))
            .map_err(JsSedimentreeStorageError::ReflectError)?;

        let js_digest: JsDigest = JsCast::unchecked_into(digest_val);
        let digest: Digest<T> = WasmDigest::from(&js_digest).into();

        let signed_bytes = Uint8Array::new(&signed_val).to_vec();
        let signed: Signed<T> = minicbor::decode(&signed_bytes)
            .map_err(|e| JsSedimentreeStorageError::CborDecodeError(e.to_string()))?;

        result.push((digest, signed));
    }

    Ok(result)
}

/// Parse a JS array of Digest objects.
#[allow(clippy::unnecessary_wraps)]
fn parse_digest_array<T>(js_value: &JsValue) -> Result<Set<Digest<T>>, JsSedimentreeStorageError> {
    let array = js_sys::Array::from(js_value);
    let mut result = Set::new();

    for i in 0..array.length() {
        let item = array.get(i);
        let js_digest: JsDigest = JsCast::unchecked_into(item);
        let digest: Digest<T> = WasmDigest::from(&js_digest).into();
        result.insert(digest);
    }

    Ok(result)
}

impl Storage<Local> for JsSedimentreeStorage {
    type Error = JsSedimentreeStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsSedimentreeStorage::save_sedimentree_id");
            let js_promise =
                self.js_save_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "JsSedimentreeStorage::delete_sedimentree_id"
            );
            let js_promise =
                self.js_delete_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!("JsSedimentreeStorage::load_all_sedimentree_ids");
            let js_promise = self.js_load_all_sedimentree_ids();
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            let xs: Vec<WasmSedimentreeId> = WasmSedimentreeIdsArray::try_from(&js_value)?.0;
            let mut ids = Set::new();
            for wasm_id in xs {
                ids.insert(wasm_id.into());
            }
            Ok(ids)
        })
    }

    // ==================== Commits (CAS) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: Signed<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<Digest<LooseCommit>, Self::Error>> {
        Local::from_future(async move {
            let digest = commit_digest(&loose_commit, &sedimentree_id)
                .ok_or(JsSedimentreeStorageError::DigestComputationFailed)?;
            // Get the blob digest from the commit's blob metadata
            let blob_digest = loose_commit
                .try_decode_payload(&sedimentree_id)
                .ok()
                .map(|commit| commit.blob_meta().digest())
                .ok_or(JsSedimentreeStorageError::DigestComputationFailed)?;
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "JsSedimentreeStorage::save_loose_commit"
            );

            let signed_bytes = minicbor::to_vec(&loose_commit)
                .map_err(|e| JsSedimentreeStorageError::CborEncodeError(e.to_string()))?;

            let js_promise = self.js_save_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
                &signed_bytes,
                &WasmDigest::from(blob_digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(digest)
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<Option<Signed<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "JsSedimentreeStorage::load_loose_commit"
            );
            let js_promise = self.js_load_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;

            if js_value.is_null() || js_value.is_undefined() {
                return Ok(None);
            }

            let bytes = Uint8Array::new(&js_value).to_vec();
            let signed: Signed<LooseCommit> = minicbor::decode(&bytes)
                .map_err(|e| JsSedimentreeStorageError::CborDecodeError(e.to_string()))?;
            Ok(Some(signed))
        })
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsSedimentreeStorage::list_commit_digests");
            let js_promise =
                self.js_list_commit_digests(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            parse_digest_array(&js_value)
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<(Digest<LooseCommit>, Signed<LooseCommit>)>, Self::Error>>
    {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsSedimentreeStorage::load_loose_commits");
            let js_promise =
                self.js_load_all_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            parse_digest_signed_array(&js_value)
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "JsSedimentreeStorage::delete_loose_commit"
            );
            let js_promise = self.js_delete_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "JsSedimentreeStorage::delete_loose_commits"
            );
            let js_promise =
                self.js_delete_all_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    // ==================== Fragments (CAS) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
    ) -> LocalBoxFuture<'_, Result<Digest<Fragment>, Self::Error>> {
        Local::from_future(async move {
            let digest = fragment_digest(&fragment, &sedimentree_id)
                .ok_or(JsSedimentreeStorageError::DigestComputationFailed)?;
            let blob_digest = fragment_blob_digest(&fragment, &sedimentree_id)
                .ok_or(JsSedimentreeStorageError::DigestComputationFailed)?;
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                ?blob_digest,
                "JsSedimentreeStorage::save_fragment"
            );

            let signed_bytes = minicbor::to_vec(&fragment)
                .map_err(|e| JsSedimentreeStorageError::CborEncodeError(e.to_string()))?;

            let js_promise = self.js_save_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
                &signed_bytes,
                &WasmDigest::from(blob_digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(digest)
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> LocalBoxFuture<'_, Result<Option<Signed<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "JsSedimentreeStorage::load_fragment"
            );
            let js_promise = self.js_load_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;

            if js_value.is_null() || js_value.is_undefined() {
                return Ok(None);
            }

            let bytes = Uint8Array::new(&js_value).to_vec();
            let signed: Signed<Fragment> = minicbor::decode(&bytes)
                .map_err(|e| JsSedimentreeStorageError::CborDecodeError(e.to_string()))?;
            Ok(Some(signed))
        })
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "JsSedimentreeStorage::list_fragment_digests"
            );
            let js_promise =
                self.js_list_fragment_digests(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            parse_digest_array(&js_value)
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<(Digest<Fragment>, Signed<Fragment>)>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsSedimentreeStorage::load_fragments");
            let js_promise =
                self.js_load_all_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            parse_digest_signed_array(&js_value)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?digest,
                "JsSedimentreeStorage::delete_fragment"
            );
            let js_promise = self.js_delete_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsSedimentreeStorage::delete_fragments");
            let js_promise =
                self.js_delete_all_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    // ==================== Blobs ====================

    fn save_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob: Blob,
    ) -> LocalBoxFuture<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(async move {
            let js_id = WasmSedimentreeId::from(sedimentree_id);
            let promise = self.js_save_blob(&js_id.into(), blob.as_slice());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;

            let js_digest: JsDigest = JsCast::unchecked_into(js_value);
            let wasm_digest = WasmDigest::from(&js_digest);
            tracing::debug!(?wasm_digest, "JsSedimentreeStorage::save_blob");
            Ok(wasm_digest.into())
        })
    }

    fn load_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?blob_digest,
                "JsSedimentreeStorage::load_blob"
            );
            let js_id = WasmSedimentreeId::from(sedimentree_id);
            let promise = self.js_load_blob(&js_id.into(), &WasmDigest::from(blob_digest).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;

            if js_value.is_null() || js_value.is_undefined() {
                return Ok(None);
            }

            if js_value.is_instance_of::<Uint8Array>() {
                Ok(Some(Blob::from(Uint8Array::new(&js_value).to_vec())))
            } else {
                Err(JsSedimentreeStorageError::NotBytes)
            }
        })
    }

    fn load_blobs(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digests: &[Digest<Blob>],
    ) -> LocalBoxFuture<'_, Result<Vec<(Digest<Blob>, Blob)>, Self::Error>> {
        let blob_digests = blob_digests.to_vec();
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                count = blob_digests.len(),
                "JsSedimentreeStorage::load_blobs"
            );

            let js_id = WasmSedimentreeId::from(sedimentree_id);
            let mut results = Vec::with_capacity(blob_digests.len());
            for digest in blob_digests {
                let promise =
                    self.js_load_blob(&js_id.clone().into(), &WasmDigest::from(digest).into());
                let js_value = JsFuture::from(promise)
                    .await
                    .map_err(JsSedimentreeStorageError::JsError)?;

                if js_value.is_null() || js_value.is_undefined() {
                    continue; // Skip missing blobs
                }

                if js_value.is_instance_of::<Uint8Array>() {
                    results.push((digest, Blob::from(Uint8Array::new(&js_value).to_vec())));
                } else {
                    return Err(JsSedimentreeStorageError::NotBytes);
                }
            }
            Ok(results)
        })
    }

    fn delete_blob(
        &self,
        sedimentree_id: SedimentreeId,
        blob_digest: Digest<Blob>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                ?blob_digest,
                "JsSedimentreeStorage::delete_blob"
            );
            let js_id = WasmSedimentreeId::from(sedimentree_id);
            let promise = self.js_delete_blob(&js_id.into(), &WasmDigest::from(blob_digest).into());
            JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::JsError)?;
            Ok(())
        })
    }

    // ==================== Convenience Methods ====================

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: Signed<LooseCommit>,
        blob: Blob,
    ) -> LocalBoxFuture<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "JsSedimentreeStorage::save_commit_with_blob"
            );
            let blob_digest = self.save_blob(sedimentree_id, blob).await?;
            self.save_loose_commit(sedimentree_id, commit).await?;
            Ok(blob_digest)
        })
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Signed<Fragment>,
        blob: Blob,
    ) -> LocalBoxFuture<'_, Result<Digest<Blob>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                "JsSedimentreeStorage::save_fragment_with_blob"
            );
            let blob_digest = self.save_blob(sedimentree_id, blob).await?;
            self.save_fragment(sedimentree_id, fragment).await?;
            Ok(blob_digest)
        })
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
    ) -> LocalBoxFuture<'_, Result<BatchResult, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(
                ?sedimentree_id,
                num_commits = commits.len(),
                num_fragments = fragments.len(),
                "JsSedimentreeStorage::save_batch"
            );

            self.save_sedimentree_id(sedimentree_id).await?;

            let mut commit_digests = Vec::with_capacity(commits.len());
            let mut fragment_digests = Vec::with_capacity(fragments.len());

            for (commit, blob) in commits {
                self.save_blob(sedimentree_id, blob).await?;
                let digest = self.save_loose_commit(sedimentree_id, commit).await?;
                commit_digests.push(digest);
            }

            for (fragment, blob) in fragments {
                self.save_blob(sedimentree_id, blob).await?;
                let digest = self.save_fragment(sedimentree_id, fragment).await?;
                fragment_digests.push(digest);
            }

            Ok(BatchResult {
                commit_digests,
                fragment_digests,
            })
        })
    }
}

/// Errors that can occur when using `JsSedimentreeStorage`.
#[derive(Error, Debug)]
pub enum JsSedimentreeStorageError {
    /// A sedimentree ID was not a string.
    #[error("SedimentreeId was not a string: {0:?}")]
    SedimentreeIdNotAString(JsValue),

    /// A sedimentree ID string was invalid.
    #[error(transparent)]
    BadSedimentreeId(#[from] BadSedimentreeId),

    /// A JavaScript error occurred.
    #[error("JavaScript error: {0:?}")]
    JsError(JsValue),

    /// CBOR encoding failed.
    #[error("CBOR encode error: {0}")]
    CborEncodeError(String),

    /// CBOR decoding failed.
    #[error("CBOR decode error: {0}")]
    CborDecodeError(String),

    /// Failed to compute digest from signed payload.
    #[error("Failed to compute digest from signed payload")]
    DigestComputationFailed,

    /// Reflect error accessing JS object property.
    #[error("Reflect error: {0:?}")]
    ReflectError(JsValue),

    /// The `JsValue` could not be converted into bytes.
    #[error("Value was not bytes")]
    NotBytes,

    /// The `JsValue` could not be converted into an array of `SedimentreeId`s.
    #[error("Value was not an array of SedimentreeIds: {0:?}")]
    NotSedimentreeIdArray(#[from] WasmConvertJsValueToSedimentreeIdArrayError),
}

impl From<JsSedimentreeStorageError> for JsValue {
    fn from(err: JsSedimentreeStorageError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("SedimentreeStorageError");
        js_err.into()
    }
}

pub mod memory;
