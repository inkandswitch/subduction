//! [`IndexedDB`] storage backend for Sedimentree.

use alloc::{
    boxed::Box,
    rc::Rc,
    str::FromStr,
    string::{String, ToString},
    vec::Vec,
};
use core::cell::RefCell;
use futures::channel::oneshot;
use js_sys::Uint8Array;
use sedimentree_core::{
    codec::error::DecodeError,
    id::{BadSedimentreeId, SedimentreeId},
};
use thiserror::Error;
use wasm_bindgen::{convert::TryFromJsValue, prelude::*};
use web_sys::{
    Event, IdbDatabase, IdbFactory, IdbObjectStoreParameters, IdbOpenDbRequest, IdbRequest,
    IdbTransactionMode, IdbVersionChangeEvent,
};

use crate::{
    digest::{WasmDigest, WasmInvalidDigest},
    fragment::WasmFragmentWithBlob,
    loose_commit::WasmCommitWithBlob,
    sedimentree_id::WasmSedimentreeId,
    signed::{WasmSignedFragment, WasmSignedLooseCommit},
};

/// The version number of the [`IndexedDB`] database schema.
pub const DB_VERSION: u32 = 2;

/// The name of the [`IndexedDB`] database.
pub const DB_NAME: &str = "@automerge/subduction/db";

/// The name of the object store for sedimentree IDs.
pub const SEDIMENTREE_ID_STORE_NAME: &str = "sedimentree_ids";

/// The name of the object store for loose commits (with embedded blobs).
pub const LOOSE_COMMIT_STORE_NAME: &str = "commits";

/// The name of the object store for fragments (with embedded blobs).
pub const FRAGMENT_STORE_NAME: &str = "fragments";

/// The name of the index for looking up records by Sedimentree ID.
pub const INDEX_BY_SEDIMENTREE_ID: &str = "by_sedimentree_id";

/// The name of the field containing the Sedimentree ID in a record.
pub const RECORD_FIELD_SEDIMENTREE_ID: &str = "sedimentree_id";

/// The name of the field containing the digest in a record.
pub const RECORD_FIELD_DIGEST: &str = "digest";

/// The name of the field containing the signed payload in a record.
pub const RECORD_FIELD_SIGNED: &str = "signed";

/// The name of the field containing the blob data in a record.
pub const RECORD_FIELD_BLOB: &str = "blob";

/// `IndexedDB` storage backend with compound storage (signed + blob together).
#[wasm_bindgen(js_name = IndexedDbStorage)]
#[derive(Debug, Clone)]
pub struct WasmIndexedDbStorage(IdbDatabase);

#[wasm_bindgen(js_class = "IndexedDbStorage")]
impl WasmIndexedDbStorage {
    /// Create a new `IndexedDbStorage` instance, opening (or creating) the database.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the database could not be opened.
    #[wasm_bindgen]
    #[allow(clippy::too_many_lines)]
    pub async fn setup(factory: &IdbFactory) -> Result<Self, JsValue> {
        let span = tracing::debug_span!("IndexedDbStorage::setup");
        let _enter = span.enter();

        tracing::debug!("opening IndexedDB database '{}'", DB_NAME);

        let open_req: IdbOpenDbRequest = factory.open_with_u32(DB_NAME, DB_VERSION)?;

        // Create object stores on first open
        {
            let onupgradeneeded = Closure::wrap(Box::new(move |e: IdbVersionChangeEvent| {
                if let Some(req) = e
                    .target()
                    .and_then(|t| t.dyn_into::<IdbOpenDbRequest>().ok())
                    && let Ok(db_val) = req.result()
                    && let Ok(db) = db_val.dyn_into::<IdbDatabase>()
                {
                    let params = IdbObjectStoreParameters::new();
                    let key_path = js_sys::Array::of2(
                        &RECORD_FIELD_SEDIMENTREE_ID.into(),
                        &RECORD_FIELD_DIGEST.into(),
                    );
                    params.set_key_path(&key_path.into());
                    params.set_auto_increment(false);

                    let names = db.object_store_names();

                    if !names.contains(SEDIMENTREE_ID_STORE_NAME) {
                        match db.create_object_store(SEDIMENTREE_ID_STORE_NAME) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(
                                    "failed to create object store '{}': {:?}",
                                    SEDIMENTREE_ID_STORE_NAME,
                                    e
                                );
                            }
                        }
                    }

                    if !names.contains(LOOSE_COMMIT_STORE_NAME) {
                        match db.create_object_store_with_optional_parameters(
                            LOOSE_COMMIT_STORE_NAME,
                            &params,
                        ) {
                            Ok(store) => {
                                let idx_names = store.index_names();
                                if !idx_names.contains(INDEX_BY_SEDIMENTREE_ID)
                                    && let Err(e) = store.create_index_with_str(
                                        INDEX_BY_SEDIMENTREE_ID,
                                        "sedimentree_id",
                                    )
                                {
                                    tracing::error!(
                                        "failed to create index '{}' on object store '{}': {:?}",
                                        INDEX_BY_SEDIMENTREE_ID,
                                        LOOSE_COMMIT_STORE_NAME,
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "failed to create object store '{}': {:?}",
                                    LOOSE_COMMIT_STORE_NAME,
                                    e
                                );
                            }
                        }
                    }

                    if !names.contains(FRAGMENT_STORE_NAME) {
                        match db.create_object_store_with_optional_parameters(
                            FRAGMENT_STORE_NAME,
                            &params,
                        ) {
                            Ok(store) => {
                                let idx_names = store.index_names();
                                if !idx_names.contains(INDEX_BY_SEDIMENTREE_ID)
                                    && let Err(e) = store.create_index_with_str(
                                        INDEX_BY_SEDIMENTREE_ID,
                                        "sedimentree_id",
                                    )
                                {
                                    tracing::error!(
                                        "failed to create index '{}' on object store '{}': {:?}",
                                        INDEX_BY_SEDIMENTREE_ID,
                                        FRAGMENT_STORE_NAME,
                                        e
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "failed to create object store '{}': {:?}",
                                    FRAGMENT_STORE_NAME,
                                    e
                                );
                            }
                        }
                    }
                }
            }) as Box<dyn FnMut(_)>);
            open_req.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
            onupgradeneeded.forget();
        }

        let db_val = await_idb(&open_req.dyn_into::<IdbRequest>()?).await?;
        let db = db_val.dyn_into::<IdbDatabase>().map_err(|_| {
            JsValue::from(js_sys::TypeError::new(
                "Open returned something other than an `IdbDatabase`",
            ))
        })?;

        Ok(Self(db))
    }

    /// Get the name of the `sedimentreeId` store.
    #[wasm_bindgen(js_name = sedimentreeIdStoreName)]
    #[must_use]
    pub fn sedimentree_id_store_name(&self) -> String {
        SEDIMENTREE_ID_STORE_NAME.to_string()
    }

    /// Get the name of the loose commit store.
    #[wasm_bindgen(js_name = looseCommitStoreName)]
    #[must_use]
    pub fn loose_commit_store_name(&self) -> String {
        LOOSE_COMMIT_STORE_NAME.to_string()
    }

    /// Get the name of the fragment store.
    #[wasm_bindgen(js_name = fragmentStoreName)]
    #[must_use]
    pub fn fragment_store_name(&self) -> String {
        FRAGMENT_STORE_NAME.to_string()
    }

    /// Insert a Sedimentree ID into storage.
    ///
    /// # Errors
    ///
    /// Returns [`WasmSaveSedimentreeIdError`] if saving a [`SedimentreeId`] fails.
    #[wasm_bindgen(js_name = saveSedimentreeId)]
    pub async fn wasm_save_sedimentree_id(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmSaveSedimentreeIdError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmSaveSedimentreeIdError::TransactionError)?;
        let store = tx
            .object_store(SEDIMENTREE_ID_STORE_NAME)
            .map_err(WasmSaveSedimentreeIdError::ObjectStoreError)?;
        let req = store
            .put_with_key(
                &JsValue::from(1u8), // Recommended as smallest unambiguous dummy value for key-as-set-like semantcis
                &JsValue::from_str(&sedimentree_id.to_string()),
            )
            .map_err(WasmSaveSedimentreeIdError::PutError)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Delete a Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns [`WasmDeleteSedimentreeIdError`] if deletion failed.
    #[wasm_bindgen(js_name = deleteSedimentreeId)]
    pub async fn wasm_delete_sedimentree_id(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmDeleteSedimentreeIdError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmDeleteSedimentreeIdError::TransactionError)?;
        let store = tx
            .object_store(SEDIMENTREE_ID_STORE_NAME)
            .map_err(WasmDeleteSedimentreeIdError::ObjectStoreError)?;
        let req = store
            .delete(&JsValue::from_str(&sedimentree_id.to_string()))
            .map_err(WasmDeleteSedimentreeIdError::DeleteError)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Load all Sedimentree IDs from storage.
    ///
    /// # Errors
    ///
    /// Returns [`WasmLoadAllSedimentreeIdsError`] if there was a problem loading.
    #[wasm_bindgen(js_name = loadAllSedimentreeIds)]
    pub async fn wasm_load_all_sedimentree_ids(
        &self,
    ) -> Result<Vec<WasmSedimentreeId>, WasmLoadAllSedimentreeIdsError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readonly)
            .map_err(WasmLoadAllSedimentreeIdsError::TransactionError)?;
        let store = tx
            .object_store(SEDIMENTREE_ID_STORE_NAME)
            .map_err(WasmLoadAllSedimentreeIdsError::ObjectStoreError)?;
        let req = store
            .get_all_keys()
            .map_err(WasmLoadAllSedimentreeIdsError::GetAllKeysError)?;

        let js_value = await_idb(&req).await?;
        let array = js_sys::Array::try_from_js_value_ref(&js_value)
            .ok_or_else(|| WasmLoadAllSedimentreeIdsError::NotAnArray(js_value))?;

        let mut xs = Vec::new();
        for js_val in array.iter() {
            let s = js_val.as_string().ok_or_else(|| {
                WasmLoadAllSedimentreeIdsError::StoredElementNotAString(js_val.clone())
            })?;
            let sedimentree_id = SedimentreeId::from_str(&s)
                .map_err(WasmLoadAllSedimentreeIdsError::BadSedimentreeId)?;
            xs.push(sedimentree_id.into());
        }

        Ok(xs)
    }

    /// Save a commit with its blob to storage (compound storage).
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveCommitError`] if the commit could not be saved.
    #[wasm_bindgen(js_name = saveCommit)]
    pub async fn wasm_save_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
        signed: &WasmSignedLooseCommit,
        blob: &Uint8Array,
    ) -> Result<(), WasmSaveCommitError> {
        let record = Record {
            sedimentree_id: SedimentreeId::from(sedimentree_id.clone()),
            digest: digest.to_hex_string(),
            signed: signed.as_bytes().to_vec(),
            blob: blob.to_vec(),
        };
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmSaveCommitError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME)
            .map_err(WasmSaveCommitError::ObjectStoreError)?
            .put(&record.into())
            .map_err(WasmSaveCommitError::UnableToStore)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Load a commit by digest, returning `CommitWithBlob` or null.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadCommitError`] if the commit could not be loaded.
    #[wasm_bindgen(js_name = loadCommit)]
    pub async fn wasm_load_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Result<Option<WasmCommitWithBlob>, WasmLoadCommitError> {
        let key = js_sys::Array::of2(
            &JsValue::from_str(&sedimentree_id.to_string()),
            &JsValue::from_str(&digest.to_hex_string()),
        );
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readonly)
            .map_err(WasmLoadCommitError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME)
            .map_err(WasmLoadCommitError::ObjectStoreError)?
            .get(&key)
            .map_err(WasmLoadCommitError::UnableToGet)?;

        let js_val = await_idb(&req).await?;
        if js_val.is_undefined() || js_val.is_null() {
            return Ok(None);
        }

        let signed_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_SIGNED.into())
            .map_err(WasmLoadCommitError::ReflectError)?;
        let blob_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_BLOB.into())
            .map_err(WasmLoadCommitError::ReflectError)?;

        let signed_bytes = Uint8Array::new(&signed_val).to_vec();
        let signed = WasmSignedLooseCommit::try_from_vec(signed_bytes)?;
        let blob = Uint8Array::new(&blob_val);

        Ok(Some(WasmCommitWithBlob::new(signed, blob)))
    }

    /// List all commit digests for a sedimentree.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmListDigestsError`] if digests could not be listed.
    #[wasm_bindgen(js_name = listCommitDigests)]
    pub async fn wasm_list_commit_digests(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<Vec<WasmDigest>, WasmListDigestsError> {
        self.list_digests_for_store(sedimentree_id, LOOSE_COMMIT_STORE_NAME)
            .await
    }

    /// Load all commits for a sedimentree, returning `CommitWithBlob[]`.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadAllCommitsError`] if commits could not be loaded.
    #[wasm_bindgen(js_name = loadAllCommits)]
    pub async fn wasm_load_all_commits(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<Vec<WasmCommitWithBlob>, WasmLoadAllCommitsError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readonly)
            .map_err(WasmLoadAllCommitsError::TransactionError)?;
        let store = tx
            .object_store(LOOSE_COMMIT_STORE_NAME)
            .map_err(WasmLoadAllCommitsError::ObjectStoreError)?;
        let idx = store
            .index(INDEX_BY_SEDIMENTREE_ID)
            .map_err(WasmLoadAllCommitsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let req = idx
            .get_all_with_key(&key)
            .map_err(WasmLoadAllCommitsError::UnableToGet)?;

        let vals = await_idb(&req).await?;
        let arr = js_sys::Array::from(&vals);
        let mut result = Vec::with_capacity(arr.length() as usize);

        for js_val in arr.iter() {
            let signed_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_SIGNED.into())
                .map_err(WasmLoadAllCommitsError::ReflectError)?;
            let blob_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_BLOB.into())
                .map_err(WasmLoadAllCommitsError::ReflectError)?;

            let signed_bytes = Uint8Array::new(&signed_val).to_vec();
            let blob = Uint8Array::new(&blob_val);

            let signed = WasmSignedLooseCommit::try_from_vec(signed_bytes)?;

            result.push(WasmCommitWithBlob::new(signed, blob));
        }

        Ok(result)
    }

    /// Delete a commit by digest.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteCommitError`] if the commit could not be deleted.
    #[wasm_bindgen(js_name = deleteCommit)]
    pub async fn wasm_delete_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Result<(), WasmDeleteCommitError> {
        let key = js_sys::Array::of2(
            &JsValue::from_str(&sedimentree_id.to_string()),
            &JsValue::from_str(&digest.to_hex_string()),
        );
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmDeleteCommitError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME)
            .map_err(WasmDeleteCommitError::ObjectStoreError)?
            .delete(&key)
            .map_err(WasmDeleteCommitError::UnableToDelete)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Delete all commits for a sedimentree.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteAllCommitsError`] if commits could not be deleted.
    #[wasm_bindgen(js_name = deleteAllCommits)]
    pub async fn wasm_delete_all_commits(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmDeleteAllCommitsError> {
        self.delete_all_for_sedimentree(sedimentree_id, LOOSE_COMMIT_STORE_NAME)
            .await
            .map_err(|e| match e {
                DeleteAllError::TransactionError(e) => {
                    WasmDeleteAllCommitsError::TransactionError(e)
                }
                DeleteAllError::ObjectStoreError(e) => {
                    WasmDeleteAllCommitsError::ObjectStoreError(e)
                }
                DeleteAllError::KeyRangeError(e) => WasmDeleteAllCommitsError::KeyRangeError(e),
                DeleteAllError::UnableToOpenCursor(e) => {
                    WasmDeleteAllCommitsError::UnableToOpenCursor(e)
                }
                DeleteAllError::CursorError(e) => WasmDeleteAllCommitsError::CursorError(e),
                DeleteAllError::UnableToAdvanceCursor(e) => {
                    WasmDeleteAllCommitsError::UnableToAdvanceCursor(e)
                }
                DeleteAllError::UnableToDelete(e) => WasmDeleteAllCommitsError::UnableToDelete(e),
                DeleteAllError::AwaitIdbError(e) => WasmDeleteAllCommitsError::AwaitIdbError(e),
            })
    }

    /// Save a fragment with its blob to storage (compound storage).
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveFragmentError`] if the fragment could not be saved.
    #[wasm_bindgen(js_name = saveFragment)]
    pub async fn wasm_save_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
        signed: &WasmSignedFragment,
        blob: &Uint8Array,
    ) -> Result<(), WasmSaveFragmentError> {
        let record = Record {
            sedimentree_id: SedimentreeId::from(sedimentree_id.clone()),
            digest: digest.to_hex_string(),
            signed: signed.as_bytes().to_vec(),
            blob: blob.to_vec(),
        };
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmSaveFragmentError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME)
            .map_err(WasmSaveFragmentError::ObjectStoreError)?
            .put(&record.into())
            .map_err(WasmSaveFragmentError::UnableToStore)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Load a fragment by digest, returning `FragmentWithBlob` or null.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadFragmentError`] if the fragment could not be loaded.
    #[wasm_bindgen(js_name = loadFragment)]
    pub async fn wasm_load_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Result<Option<WasmFragmentWithBlob>, WasmLoadFragmentError> {
        let key = js_sys::Array::of2(
            &JsValue::from_str(&sedimentree_id.to_string()),
            &JsValue::from_str(&digest.to_hex_string()),
        );
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readonly)
            .map_err(WasmLoadFragmentError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME)
            .map_err(WasmLoadFragmentError::ObjectStoreError)?
            .get(&key)
            .map_err(WasmLoadFragmentError::UnableToGet)?;

        let js_val = await_idb(&req).await?;
        if js_val.is_undefined() || js_val.is_null() {
            return Ok(None);
        }

        let signed_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_SIGNED.into())
            .map_err(WasmLoadFragmentError::ReflectError)?;
        let blob_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_BLOB.into())
            .map_err(WasmLoadFragmentError::ReflectError)?;

        let signed_bytes = Uint8Array::new(&signed_val).to_vec();
        let blob = Uint8Array::new(&blob_val);

        let signed = WasmSignedFragment::try_from_vec(signed_bytes)?;

        Ok(Some(WasmFragmentWithBlob::new(signed, blob)))
    }

    /// List all fragment digests for a sedimentree.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmListDigestsError`] if digests could not be listed.
    #[wasm_bindgen(js_name = listFragmentDigests)]
    pub async fn wasm_list_fragment_digests(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<Vec<WasmDigest>, WasmListDigestsError> {
        self.list_digests_for_store(sedimentree_id, FRAGMENT_STORE_NAME)
            .await
    }

    /// Load all fragments for a sedimentree, returning `FragmentWithBlob[]`.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadAllFragmentsError`] if fragments could not be loaded.
    #[wasm_bindgen(js_name = loadAllFragments)]
    pub async fn wasm_load_all_fragments(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<Vec<WasmFragmentWithBlob>, WasmLoadAllFragmentsError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readonly)
            .map_err(WasmLoadAllFragmentsError::TransactionError)?;
        let store = tx
            .object_store(FRAGMENT_STORE_NAME)
            .map_err(WasmLoadAllFragmentsError::ObjectStoreError)?;
        let idx = store
            .index(INDEX_BY_SEDIMENTREE_ID)
            .map_err(WasmLoadAllFragmentsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let req = idx
            .get_all_with_key(&key)
            .map_err(WasmLoadAllFragmentsError::UnableToGet)?;

        let vals = await_idb(&req).await?;
        let arr = js_sys::Array::from(&vals);
        let mut result = Vec::with_capacity(arr.length() as usize);

        for js_val in arr.iter() {
            let signed_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_SIGNED.into())
                .map_err(WasmLoadAllFragmentsError::ReflectError)?;
            let blob_val = js_sys::Reflect::get(&js_val, &RECORD_FIELD_BLOB.into())
                .map_err(WasmLoadAllFragmentsError::ReflectError)?;

            let signed_bytes = Uint8Array::new(&signed_val).to_vec();
            let blob = Uint8Array::new(&blob_val);

            let signed = WasmSignedFragment::try_from_vec(signed_bytes)?;

            result.push(WasmFragmentWithBlob::new(signed, blob));
        }

        Ok(result)
    }

    /// Delete a fragment by digest.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteFragmentError`] if the fragment could not be deleted.
    #[wasm_bindgen(js_name = deleteFragment)]
    pub async fn wasm_delete_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Result<(), WasmDeleteFragmentError> {
        let key = js_sys::Array::of2(
            &JsValue::from_str(&sedimentree_id.to_string()),
            &JsValue::from_str(&digest.to_hex_string()),
        );
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(WasmDeleteFragmentError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME)
            .map_err(WasmDeleteFragmentError::ObjectStoreError)?
            .delete(&key)
            .map_err(WasmDeleteFragmentError::UnableToDelete)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Delete all fragments for a sedimentree.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteAllFragmentsError`] if fragments could not be deleted.
    #[wasm_bindgen(js_name = deleteAllFragments)]
    pub async fn wasm_delete_all_fragments(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmDeleteAllFragmentsError> {
        self.delete_all_for_sedimentree(sedimentree_id, FRAGMENT_STORE_NAME)
            .await
            .map_err(|e| match e {
                DeleteAllError::TransactionError(e) => {
                    WasmDeleteAllFragmentsError::TransactionError(e)
                }
                DeleteAllError::ObjectStoreError(e) => {
                    WasmDeleteAllFragmentsError::ObjectStoreError(e)
                }
                DeleteAllError::KeyRangeError(e) => WasmDeleteAllFragmentsError::KeyRangeError(e),
                DeleteAllError::UnableToOpenCursor(e) => {
                    WasmDeleteAllFragmentsError::UnableToOpenCursor(e)
                }
                DeleteAllError::CursorError(e) => WasmDeleteAllFragmentsError::CursorError(e),
                DeleteAllError::UnableToAdvanceCursor(e) => {
                    WasmDeleteAllFragmentsError::UnableToAdvanceCursor(e)
                }
                DeleteAllError::UnableToDelete(e) => WasmDeleteAllFragmentsError::UnableToDelete(e),
                DeleteAllError::AwaitIdbError(e) => WasmDeleteAllFragmentsError::AwaitIdbError(e),
            })
    }

    /// Helper to list digests for a given store.
    async fn list_digests_for_store(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        store_name: &str,
    ) -> Result<Vec<WasmDigest>, WasmListDigestsError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(store_name, IdbTransactionMode::Readonly)
            .map_err(WasmListDigestsError::TransactionError)?;
        let store = tx
            .object_store(store_name)
            .map_err(WasmListDigestsError::ObjectStoreError)?;
        let idx = store
            .index(INDEX_BY_SEDIMENTREE_ID)
            .map_err(WasmListDigestsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let req = idx
            .get_all_with_key(&key)
            .map_err(WasmListDigestsError::UnableToGet)?;

        let vals = await_idb(&req).await?;
        let arr = js_sys::Array::from(&vals);
        let mut out = Vec::new();

        for js_val in arr.iter() {
            let digest_str = js_sys::Reflect::get(&js_val, &RECORD_FIELD_DIGEST.into())
                .map_err(WasmListDigestsError::ReflectError)?;
            if let Some(s) = digest_str.as_string() {
                out.push(WasmDigest::from_hex_string(&s)?);
            }
        }

        Ok(out)
    }

    /// Helper to delete all records for a sedimentree in a given store.
    async fn delete_all_for_sedimentree(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        store_name: &str,
    ) -> Result<(), DeleteAllError> {
        let tx = self
            .0
            .transaction_with_str_and_mode(store_name, IdbTransactionMode::Readwrite)
            .map_err(DeleteAllError::TransactionError)?;
        let store = tx
            .object_store(store_name)
            .map_err(DeleteAllError::ObjectStoreError)?;
        let idx = store
            .index(INDEX_BY_SEDIMENTREE_ID)
            .map_err(DeleteAllError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let range = web_sys::IdbKeyRange::only(&key).map_err(DeleteAllError::KeyRangeError)?;

        let req = idx
            .open_key_cursor_with_range(&range)
            .map_err(DeleteAllError::UnableToOpenCursor)?;

        let mut cursor_val = await_idb(&req).await?;
        if cursor_val.is_undefined() || cursor_val.is_null() {
            return Ok(());
        }

        loop {
            let cursor = cursor_val
                .dyn_into::<web_sys::IdbCursor>()
                .map_err(DeleteAllError::CursorError)?;

            let delete_req = cursor.delete().map_err(DeleteAllError::UnableToDelete)?;
            drop(await_idb(&delete_req).await?);

            cursor
                .continue_()
                .map_err(DeleteAllError::UnableToAdvanceCursor)?;

            cursor_val = await_idb(&req).await?;
            if cursor_val.is_undefined() || cursor_val.is_null() {
                break;
            }
        }

        Ok(())
    }
}

async fn await_idb(req: &IdbRequest) -> Result<JsValue, AwaitIdbError> {
    let (tx, rx) = oneshot::channel::<Result<JsValue, JsValue>>();
    let tx_ref = Rc::new(RefCell::new(Some(tx)));

    let tx_ok = tx_ref.clone();
    let success = {
        let req = req.clone();
        Closure::once(Box::new(move |_e: Event| {
            let js_res = req.result();
            let _ = tx_ok.borrow_mut().take().map(|tx| {
                if let Err(e) = tx.send(js_res) {
                    tracing::error!("failed to send IDB success result: {:?}", e);
                }
            });

            // Unregister handlers
            req.set_onsuccess(None);
            req.set_onerror(None);
        }) as Box<dyn FnOnce(_)>)
    };
    req.set_onsuccess(Some(success.as_ref().unchecked_ref()));
    success.forget();

    let tx_err = tx_ref.clone();
    let error = {
        let req = req.clone();
        Closure::once(Box::new(move |_e: Event| {
            let err = req
                .error()
                .map_or_else(Into::into, |_| js_sys::Error::new("IDB error").into());

            let _ = tx_err.borrow_mut().take().map(|tx| {
                if let Err(e) = tx.send(Err(err)) {
                    tracing::error!("failed to send IDB error result: {:?}", e);
                }
            });

            // Unregister handlers
            req.set_onsuccess(None);
            req.set_onerror(None);
        }) as Box<dyn FnOnce(_)>)
    };
    req.set_onerror(Some(error.as_ref().unchecked_ref()));
    error.forget();

    rx.await
        .map_err(AwaitIdbError::Canceled)?
        .map_err(AwaitIdbError::JsPromiseRejected)
}

/// Errors that can occur while awaiting an `IndexedDB` operation.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum AwaitIdbError {
    /// The channel was canceled.
    #[error("Channel dropped")]
    Canceled(oneshot::Canceled),

    /// The JS Promise was rejected.
    #[error("JS Promise rejected: {0:?}")]
    JsPromiseRejected(JsValue),
}

impl From<AwaitIdbError> for JsValue {
    fn from(err: AwaitIdbError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("AwaitIdbError");
        err.into()
    }
}

/// Error types for `saveSedimentreeId`.
#[derive(Debug, Error)]
pub enum WasmSaveSedimentreeIdError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("saveSedimentreeId IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("saveSedimentreeId IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to put the sedimentree ID into `IndexedDB`.
    #[error("unable to `put` sedimentree ID into IndexedDB: {0:?}")]
    PutError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmSaveSedimentreeIdError> for JsValue {
    fn from(err: WasmSaveSedimentreeIdError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmSaveSedimentreeIdError");
        err.into()
    }
}

/// Error types for `deleteSedimentreeId`.
#[derive(Debug, Error)]
pub enum WasmDeleteSedimentreeIdError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("deleteSedimentreeId IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("deleteSedimentreeId IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to delete the sedimentree ID from `IndexedDB`.
    #[error("unable to `delete` sedimentree ID from IndexedDB: {0:?}")]
    DeleteError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteSedimentreeIdError> for JsValue {
    fn from(err: WasmDeleteSedimentreeIdError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteSedimentreeIdError");
        err.into()
    }
}

/// Error types for `loadAllSedimentreeIds`.
#[derive(Debug, Error)]
pub enum WasmLoadAllSedimentreeIdsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("loadSedimentreeIds IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("loadSedimentreeIds IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get all keys from `IndexedDB`.
    #[error("unable to get sedimentree IDs from IndexedDB: {0:?}")]
    GetAllKeysError(JsValue),

    /// The loaded value is not an array.
    #[error("value loaded via loadSedimentreeIds is not an array: {0:?}")]
    NotAnArray(JsValue),

    /// A stored element is not a string.
    #[error("value loaded via loadSedimentreeIds is not a string: {0:?}")]
    StoredElementNotAString(JsValue),

    /// Invalid sedimentree ID format.
    #[error(transparent)]
    BadSedimentreeId(#[from] BadSedimentreeId),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmLoadAllSedimentreeIdsError> for JsValue {
    fn from(err: WasmLoadAllSedimentreeIdsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadAllSedimentreeIdsError");
        err.into()
    }
}

/// Error types for `saveCommit`.
#[derive(Debug, Error)]
pub enum WasmSaveCommitError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("saveCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("saveCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to store the commit in `IndexedDB`.
    #[error("unable to store commit in IndexedDB: {0:?}")]
    UnableToStore(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmSaveCommitError> for JsValue {
    fn from(err: WasmSaveCommitError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmSaveCommitError");
        err.into()
    }
}

/// Error types for `loadCommit`.
#[derive(Debug, Error)]
pub enum WasmLoadCommitError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("loadCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("loadCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get the commit from `IndexedDB`.
    #[error("unable to get commit from IndexedDB: {0:?}")]
    UnableToGet(JsValue),

    /// Failed to access a property on the JS object.
    #[error("reflect error: {0:?}")]
    ReflectError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),

    /// Failed to decode the signed commit.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl From<WasmLoadCommitError> for JsValue {
    fn from(err: WasmLoadCommitError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadCommitError");
        err.into()
    }
}

/// Error types for `listCommitDigests` and `listFragmentDigests`.
#[derive(Debug, Error)]
pub enum WasmListDigestsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("listDigests IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("listDigests IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get digests from `IndexedDB`.
    #[error("unable to get digests from IndexedDB: {0:?}")]
    UnableToGet(JsValue),

    /// Failed to access a property on the JS object.
    #[error("reflect error: {0:?}")]
    ReflectError(JsValue),

    /// Invalid digest format.
    #[error(transparent)]
    InvalidDigest(#[from] WasmInvalidDigest),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmListDigestsError> for JsValue {
    fn from(err: WasmListDigestsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmListDigestsError");
        err.into()
    }
}

/// Error types for `loadAllCommits`.
#[derive(Debug, Error)]
pub enum WasmLoadAllCommitsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("loadAllCommits IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("loadAllCommits IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get commits from `IndexedDB`.
    #[error("unable to get commits from IndexedDB: {0:?}")]
    UnableToGet(JsValue),

    /// Failed to access a property on the JS object.
    #[error("reflect error: {0:?}")]
    ReflectError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),

    /// Failed to decode the signed commit.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl From<WasmLoadAllCommitsError> for JsValue {
    fn from(err: WasmLoadAllCommitsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadAllCommitsError");
        err.into()
    }
}

/// Error types for `deleteCommit`.
#[derive(Debug, Error)]
pub enum WasmDeleteCommitError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("deleteCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("deleteCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to delete the commit from `IndexedDB`.
    #[error("unable to delete commit from IndexedDB: {0:?}")]
    UnableToDelete(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteCommitError> for JsValue {
    fn from(err: WasmDeleteCommitError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteCommitError");
        err.into()
    }
}

/// Error types for `deleteAllCommits`.
#[derive(Debug, Error)]
pub enum WasmDeleteAllCommitsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("deleteAllCommits IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("deleteAllCommits IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to create key range.
    #[error("deleteAllCommits IndexedDB key range error: {0:?}")]
    KeyRangeError(JsValue),

    /// Failed to open cursor.
    #[error("deleteAllCommits IndexedDB cursor error: {0:?}")]
    UnableToOpenCursor(JsValue),

    /// Cursor operation failed.
    #[error("cursor problem when deleting commits: {0:?}")]
    CursorError(JsValue),

    /// Failed to advance cursor to next record.
    #[error("unable to advance cursor: {0:?}")]
    UnableToAdvanceCursor(JsValue),

    /// Failed to delete the commit.
    #[error("unable to delete commit: {0:?}")]
    UnableToDelete(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteAllCommitsError> for JsValue {
    fn from(err: WasmDeleteAllCommitsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteAllCommitsError");
        err.into()
    }
}

/// Error types for `saveFragment`.
#[derive(Debug, Error)]
pub enum WasmSaveFragmentError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("saveFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("saveFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to store the fragment in `IndexedDB`.
    #[error("unable to store fragment in IndexedDB: {0:?}")]
    UnableToStore(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmSaveFragmentError> for JsValue {
    fn from(err: WasmSaveFragmentError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmSaveFragmentError");
        err.into()
    }
}

/// Error types for `loadFragment`.
#[derive(Debug, Error)]
pub enum WasmLoadFragmentError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("loadFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("loadFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get the fragment from `IndexedDB`.
    #[error("unable to get fragment from IndexedDB: {0:?}")]
    UnableToGet(JsValue),

    /// Failed to access a property on the JS object.
    #[error("reflect error: {0:?}")]
    ReflectError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),

    /// Failed to decode the signed fragment.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl From<WasmLoadFragmentError> for JsValue {
    fn from(err: WasmLoadFragmentError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadFragmentError");
        err.into()
    }
}

/// Error types for `loadAllFragments`.
#[derive(Debug, Error)]
pub enum WasmLoadAllFragmentsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("loadAllFragments IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("loadAllFragments IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to get fragments from `IndexedDB`.
    #[error("unable to get fragments from IndexedDB: {0:?}")]
    UnableToGet(JsValue),

    /// Failed to access a property on the JS object.
    #[error("reflect error: {0:?}")]
    ReflectError(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),

    /// Failed to decode the signed fragment.
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl From<WasmLoadAllFragmentsError> for JsValue {
    fn from(err: WasmLoadAllFragmentsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadAllFragmentsError");
        err.into()
    }
}

/// Error types for `deleteFragment`.
#[derive(Debug, Error)]
pub enum WasmDeleteFragmentError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("deleteFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("deleteFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to delete the fragment from `IndexedDB`.
    #[error("unable to delete fragment from IndexedDB: {0:?}")]
    UnableToDelete(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteFragmentError> for JsValue {
    fn from(err: WasmDeleteFragmentError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteFragmentError");
        err.into()
    }
}

/// Error types for `deleteAllFragments`.
#[derive(Debug, Error)]
pub enum WasmDeleteAllFragmentsError {
    /// Failed to begin `IndexedDB` transaction.
    #[error("deleteAllFragments IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// Failed to access the object store.
    #[error("deleteAllFragments IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// Failed to create key range.
    #[error("deleteAllFragments IndexedDB key range error: {0:?}")]
    KeyRangeError(JsValue),

    /// Failed to open cursor.
    #[error("deleteAllFragments IndexedDB cursor error: {0:?}")]
    UnableToOpenCursor(JsValue),

    /// Cursor operation failed.
    #[error("cursor problem when deleting fragments: {0:?}")]
    CursorError(JsValue),

    /// Failed to advance cursor to next record.
    #[error("unable to advance cursor: {0:?}")]
    UnableToAdvanceCursor(JsValue),

    /// Failed to delete the fragment.
    #[error("unable to delete fragment: {0:?}")]
    UnableToDelete(JsValue),

    /// Error awaiting `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteAllFragmentsError> for JsValue {
    fn from(err: WasmDeleteAllFragmentsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteAllFragmentsError");
        err.into()
    }
}

/// Internal error type for `delete_all_for_sedimentree` helper.
#[derive(Debug, Error)]
enum DeleteAllError {
    #[error("transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("key range error: {0:?}")]
    KeyRangeError(JsValue),

    #[error("unable to open cursor: {0:?}")]
    UnableToOpenCursor(JsValue),

    #[error("cursor error: {0:?}")]
    CursorError(JsValue),

    #[error("unable to advance cursor: {0:?}")]
    UnableToAdvanceCursor(JsValue),

    #[error("unable to delete: {0:?}")]
    UnableToDelete(JsValue),

    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

/// A compound record storing signed data and blob together.
#[derive(Debug, Clone)]
struct Record {
    sedimentree_id: SedimentreeId,
    digest: String,
    signed: Vec<u8>,
    blob: Vec<u8>,
}

impl From<Record> for JsValue {
    fn from(record: Record) -> Self {
        let obj = js_sys::Object::new();
        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(
            &obj,
            &RECORD_FIELD_SEDIMENTREE_ID.into(),
            &record.sedimentree_id.to_string().into(),
        )
        .expect("SedimentreeId to string failed");

        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(&obj, &RECORD_FIELD_DIGEST.into(), &record.digest.into())
            .expect("Digest to string failed");

        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(
            &obj,
            &RECORD_FIELD_SIGNED.into(),
            &js_sys::Uint8Array::from(record.signed.as_slice()),
        )
        .expect("Signed to Uint8Array failed");

        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(
            &obj,
            &RECORD_FIELD_BLOB.into(),
            &js_sys::Uint8Array::from(record.blob.as_slice()),
        )
        .expect("Blob to Uint8Array failed");

        obj.into()
    }
}
