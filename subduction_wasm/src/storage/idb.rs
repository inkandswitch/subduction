//! [`IndexedDB`] storage backend for Sedimentree.

use alloc::{boxed::Box, rc::Rc, str::FromStr, string::{String, ToString}, vec::Vec};
use core::{cell::RefCell};
use futures::{channel::oneshot};
use js_sys::Uint8Array;
use sedimentree_core::{
    blob::Digest,
    fragment::Fragment,
    id::{BadSedimentreeId, SedimentreeId},
    loose_commit::LooseCommit,
};
use thiserror::Error;
use wasm_bindgen::{convert::TryFromJsValue, prelude::*};
use web_sys::{
    Event, IdbDatabase,IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransactionMode,
    IdbVersionChangeEvent, IdbObjectStoreParameters, 
};

use crate::{digest::WasmDigest, fragment::WasmFragment, loose_commit::WasmLooseCommit, sedimentree_id::WasmSedimentreeId};

/// The version number of the [`IndexedDB`] database schema.
pub const DB_VERSION: u32 = 1;

/// The name of the [`IndexedDB`] database.
pub const DB_NAME: &str = "@automerge/subduction/db";

/// The name of the object store for blobs.
pub const SEDIMENTREE_ID_STORE_NAME: &str = "sedimentree_ids";

/// The name of the object store for blobs.
pub const BLOB_STORE_NAME: &str = "blobs";

/// The name of the object store for loose commits.
pub const LOOSE_COMMIT_STORE_NAME: &str = "commits";

/// The name of the object store for fragments.
pub const FRAGMENT_STORE_NAME: &str = "fragments";

/// The name of the index for looking up records by Sedimentree ID.
pub const INDEX_BY_SEDIMENTREE_ID: &str = "by_sedimentree_id";

/// The name of the field containing the Sedimentree ID in a record.
pub const RECORD_FIELD_SEDIMENTREE_ID: &str = "sedimentree_id";

/// The name of the field containing the digest in a record.
pub const RECORD_FIELD_DIGEST: &str = "digest";

/// The name of the field containing the payload in a record.
pub const RECORD_FIELD_PAYLOAD: &str = "payload";

/// `IndexedDB` storage backend.
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
                    && let Ok(db) = db_val.dyn_into::<IdbDatabase>() {
                        let params = IdbObjectStoreParameters::new();
                        let key_path = js_sys::Array::of2(&RECORD_FIELD_SEDIMENTREE_ID.into(), &RECORD_FIELD_DIGEST.into());
                        params.set_key_path(&key_path.into());
                        params.set_auto_increment(false);

                        let names = db.object_store_names();

                        if !names.contains(SEDIMENTREE_ID_STORE_NAME) {
                            match db.create_object_store(SEDIMENTREE_ID_STORE_NAME) {
                                Ok(_) => {},
                                Err(e) => {
                                    tracing::error!(
                                        "failed to create object store '{}': {:?}",
                                        SEDIMENTREE_ID_STORE_NAME,
                                        e
                                    );
                                }
                            }
                        }

                        if !names.contains(BLOB_STORE_NAME) {
                            match db.create_object_store(BLOB_STORE_NAME) {
                                Ok(_) => {},
                                Err(e) => {
                                    tracing::error!(
                                        "failed to create object store '{}': {:?}",
                                        BLOB_STORE_NAME,
                                        e
                                    );
                                }
                            }
                        }

                        if !names.contains(LOOSE_COMMIT_STORE_NAME) {
                            match db.create_object_store_with_optional_parameters(LOOSE_COMMIT_STORE_NAME, &params) {
                                Ok(store) => {
                                    let idx_names = store.index_names();
                                    if !idx_names.contains(INDEX_BY_SEDIMENTREE_ID)
                                        && let Err(e) = store.create_index_with_str(INDEX_BY_SEDIMENTREE_ID, "sedimentree_id") {
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
                            match db.create_object_store_with_optional_parameters(FRAGMENT_STORE_NAME, &params) {
                                Ok(store) => {
                                    let idx_names = store.index_names();
                                    if !idx_names.contains(INDEX_BY_SEDIMENTREE_ID)
                                        && let Err(e) = store.create_index_with_str(INDEX_BY_SEDIMENTREE_ID, "sedimentree_id") {
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

    /// Get the name of the blob store.
    #[wasm_bindgen(js_name = blobStoreName)]
    #[must_use]
    pub fn blob_store_name(&self) -> String {
        BLOB_STORE_NAME.to_string()
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
    pub async fn wasm_save_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) -> Result<(), WasmSaveSedimentreeIdError> {
        let tx = self.0.transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveSedimentreeIdError::TransactionError)?;
        let store = tx.object_store(SEDIMENTREE_ID_STORE_NAME).map_err(WasmSaveSedimentreeIdError::ObjectStoreError)?;
        let req = store
            .put_with_key(
                &JsValue::from(1u8), // Recommended as smallest unambiguous dummy value for key-as-set-like semantcis
                &JsValue::from_str(&sedimentree_id.to_string()),
            ).map_err(WasmSaveSedimentreeIdError::PutError)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Delete a Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns [`WasmDeleteSedimentreeIdError`] if deletion failed.
    #[wasm_bindgen(js_name = deleteSedimentreeId)]
    pub async fn wasm_delete_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) -> Result<(), WasmDeleteSedimentreeIdError> {
        let tx = self.0.transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmDeleteSedimentreeIdError::TransactionError)?;
        let store = tx.object_store(SEDIMENTREE_ID_STORE_NAME).map_err(WasmDeleteSedimentreeIdError::ObjectStoreError)?;
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
    pub async fn wasm_load_all_sedimentree_ids(&self) -> Result<Vec<WasmSedimentreeId>, WasmLoadAllSedimentreeIdsError> {
        let tx = self.0.transaction_with_str_and_mode(SEDIMENTREE_ID_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadAllSedimentreeIdsError::TransactionError)?;
        let store = tx.object_store(SEDIMENTREE_ID_STORE_NAME).map_err(WasmLoadAllSedimentreeIdsError::ObjectStoreError)?;
        let req = store.get_all_keys().map_err(WasmLoadAllSedimentreeIdsError::GetAllKeysError)?;

        let js_value = await_idb(&req).await?;
        let array = js_sys::Array::try_from_js_value_ref(&js_value).ok_or_else(||WasmLoadAllSedimentreeIdsError::NotAnArray(js_value))?;

        let mut xs = Vec::new();
        for js_val in array.iter() {
            let s = js_val.as_string().ok_or_else(|| WasmLoadAllSedimentreeIdsError::StoredElementNotAString(js_val.clone()))?;
            let sedimentree_id = SedimentreeId::from_str(&s).map_err(WasmLoadAllSedimentreeIdsError::BadSedimentreeId)?;
            xs.push(sedimentree_id.into());
        }

        Ok(xs)
    }

    /// Save a loose commit to storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveLooseCommitError`] if the loose commit could not be saved.
    ///
    /// # Panics
    ///
    /// Panics if the fragment is not serializable to CBOR.
    #[wasm_bindgen( js_name = saveLooseCommit)]
    pub async fn wasm_save_loose_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        loose_commit: &WasmLooseCommit,
    ) -> Result<(), WasmSaveLooseCommitError> {
        let core_commit = LooseCommit::from(loose_commit.clone());
        let digest = core_commit.digest();

        #[allow(clippy::expect_used)]
        let bytes = minicbor::to_vec(&core_commit).expect("CBOR serialization should be infallible");

        let record = Record {
            sedimentree_id: SedimentreeId::from(sedimentree_id.clone()),
            digest,
            payload: bytes,
        };
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveLooseCommitError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmSaveLooseCommitError::ObjectStoreError)?
            .put(&record.into())
            .map_err(WasmSaveLooseCommitError::UnableToStoreLooseCommit)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Load all loose commits from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadLooseCommitsError`] if loose commits could not be loaded.
    #[wasm_bindgen( js_name = loadLooseCommits)]
    pub async fn wasm_load_loose_commits(&self, sedimentree_id: &WasmSedimentreeId) -> Result<Vec<WasmLooseCommit>, WasmLoadLooseCommitsError> {
        let tx = self.0
                     .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadLooseCommitsError::TransactionError)?;
        let store = tx.object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmLoadLooseCommitsError::ObjectStoreError)?;
        let idx = store.index(INDEX_BY_SEDIMENTREE_ID).map_err(WasmLoadLooseCommitsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let req = idx.get_all_with_key(&key).map_err(WasmLoadLooseCommitsError::UnableToGetLooseCommits)?;

        let vals = await_idb(&req).await?;
        let arr = js_sys::Array::from(&vals);

        let mut out: Vec<WasmLooseCommit> = Vec::new();
        for js_val in arr.iter() {
            let js_opaque = js_sys::Reflect::get(&js_val, &RECORD_FIELD_PAYLOAD.into())
                .map_err(WasmLoadLooseCommitsError::IndexError)?;
            let bytes: Vec<u8> = Uint8Array::new(&js_opaque).to_vec();
            let commit: LooseCommit = minicbor::decode(&bytes)
                .map_err(|_| WasmLoadLooseCommitsError::DecodeError)?;
            out.push(commit.into());
        }

        Ok(out)
    }

    /// Delete all loose commits for a given Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteLooseCommitsError`] if loose commits could not be deleted.
    #[wasm_bindgen( js_name = deleteLooseCommits)]
    pub async fn wasm_delete_loose_commits(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmDeleteLooseCommitsError> {
        let tx = self.0
                     .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmDeleteLooseCommitsError::TransactionError)?;
        let store = tx.object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmDeleteLooseCommitsError::ObjectStoreError)?;
        let idx = store.index(INDEX_BY_SEDIMENTREE_ID).map_err(WasmDeleteLooseCommitsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let range = web_sys::IdbKeyRange::only(&key).map_err(WasmDeleteLooseCommitsError::KeyRangeError)?;

        let req = idx.open_key_cursor_with_range(&range)
                     .map_err(WasmDeleteLooseCommitsError::UnableToOpenCursor)?;

        let mut cursor_val = await_idb(&req).await?;
        if cursor_val.is_undefined() || cursor_val.is_null() {
            return Ok(());
        }

        loop {
            let cursor = cursor_val
                .dyn_into::<web_sys::IdbCursor>()
                .map_err(WasmDeleteLooseCommitsError::CursorError)?;

            let delete_req = cursor
                .delete()
                .map_err(WasmDeleteLooseCommitsError::UnableToDeleteLooseCommit)?;
            drop(await_idb(&delete_req).await?);

            cursor
                .continue_()
                .map_err(WasmDeleteLooseCommitsError::UnableToAdvanceCursor)?;

            cursor_val = await_idb(&req).await?;
            if cursor_val.is_undefined() || cursor_val.is_null() {
                break;
            }
        }

        Ok(())
    }

    /// Save a fragment to storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveFragmentError`] if the fragment could not be saved.
    ///
    /// # Panics
    ///
    /// Panics if the fragment is not serializable to CBOR.
    #[wasm_bindgen(js_name = saveFragment)]
   pub async fn wasm_save_fragment(&self, sedimentree_id: &WasmSedimentreeId, fragment: &WasmFragment) -> Result<(), WasmSaveFragmentError> {
        let core_fragment = Fragment::from(fragment.clone());
        let digest = core_fragment.digest();

       #[allow(clippy::expect_used)]
       let bytes = minicbor::to_vec(&core_fragment).expect("CBOR serialization should be infallible");

        let record = Record {
            sedimentree_id: SedimentreeId::from(sedimentree_id.clone()),
            digest,
            payload: bytes,
        };
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveFragmentError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME).map_err(WasmSaveFragmentError::ObjectStoreError)?
            .put(&record.into())
            .map_err(WasmSaveFragmentError::UnableToStoreFragment)?;

        drop(await_idb(&req).await?);
        Ok(())
    }

    /// Load all fragments from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadFragmentsError`] if fragments could not be loaded.
    #[wasm_bindgen(js_name = loadFragments)]
    pub async fn wasm_load_fragments(&self, sedimentree_id: &WasmSedimentreeId) -> Result<Vec<WasmFragment>, WasmLoadFragmentsError> {
        let tx = self.0
                     .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadFragmentsError::TransactionError)?;
        let store = tx.object_store(FRAGMENT_STORE_NAME).map_err(WasmLoadFragmentsError::ObjectStoreError)?;
        let idx = store.index(INDEX_BY_SEDIMENTREE_ID).map_err(WasmLoadFragmentsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let req = idx.get_all_with_key(&key).map_err(WasmLoadFragmentsError::UnableToGetFragments)?;

        let vals = await_idb(&req).await?;
        let arr = js_sys::Array::from(&vals);

        let mut out: Vec<WasmFragment> = Vec::new();
        for js_val in arr.iter() {
            let js_opaque = js_sys::Reflect::get(&js_val, &RECORD_FIELD_PAYLOAD.into())
                .map_err(WasmLoadFragmentsError::IndexError)?;
            let bytes: Vec<u8> = Uint8Array::new(&js_opaque).to_vec();
            let commit: Fragment = minicbor::decode(&bytes)
                .map_err(|_| WasmLoadFragmentsError::DecodeError)?;
            out.push(commit.into());
        }

        Ok(out)
    }

    /// Delete all fragments for a given Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteFragmentsError`] if loose commits could not be deleted.
    #[wasm_bindgen( js_name = deleteFragments)]
    pub async fn wasm_delete_fragments(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Result<(), WasmDeleteFragmentsError> {
        let tx = self.0
                     .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmDeleteFragmentsError::TransactionError)?;
        let store = tx.object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmDeleteFragmentsError::ObjectStoreError)?;
        let idx = store.index(INDEX_BY_SEDIMENTREE_ID).map_err(WasmDeleteFragmentsError::ObjectStoreError)?;

        let key = JsValue::from_str(&sedimentree_id.to_string());
        let range = web_sys::IdbKeyRange::only(&key).map_err(WasmDeleteFragmentsError::KeyRangeError)?;

        let req = idx.open_key_cursor_with_range(&range)
                     .map_err(WasmDeleteFragmentsError::UnableToOpenCursor)?;

        let mut cursor_val = await_idb(&req).await?;
        if cursor_val.is_undefined() || cursor_val.is_null() {
            return Ok(());
        }

        loop {
            let cursor = cursor_val
                .dyn_into::<web_sys::IdbCursor>()
                .map_err(WasmDeleteFragmentsError::CursorError)?;

            let delete_req = cursor
                .delete()
                .map_err(WasmDeleteFragmentsError::UnableToDeleteLooseCommit)?;
            drop(await_idb(&delete_req).await?);

            cursor
                .continue_()
                .map_err(WasmDeleteFragmentsError::UnableToAdvanceCursor)?;

            cursor_val = await_idb(&req).await?;
            if cursor_val.is_undefined() || cursor_val.is_null() {
                break;
            }
        }

        Ok(())
    }

    /// Save a blob to the database, returning its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the JS transaction could not be opened,
    /// or if the blob could not be saved.
    #[wasm_bindgen(js_name = saveBlob)]
    pub async fn wasm_save_blob(&self, bytes: &[u8]) -> Result<WasmDigest, WasmSaveBlobError> {
        let digest = Digest::hash(bytes);
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveBlobError::TransactionError)?
            .object_store(BLOB_STORE_NAME).map_err(WasmSaveBlobError::ObjectStoreError)?
            .put_with_key(
                &Uint8Array::from(bytes).into(),
                &JsValue::from_str(&digest.to_string()), // Hex
            ).map_err(WasmSaveBlobError::UnableToStoreBlob)?;

        drop(await_idb(&req).await?);
        Ok(digest.into())
    }

    /// Load a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the blob could not be loaded.
    #[wasm_bindgen(js_name = loadBlob)]
    pub async fn wasm_load_blob(&self, wasm_digest: WasmDigest) -> Result<Option<Vec<u8>>, WasmLoadBlobError> {
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadBlobError::TransactionError)?
            .object_store(BLOB_STORE_NAME).map_err(WasmLoadBlobError::ObjectStoreError)?
            .get(&JsValue::from_str(&wasm_digest.to_hex_string())).map_err(WasmLoadBlobError::UnableToGetBlob)?;

        let js_value = await_idb(&req).await?;
        if js_value.is_undefined() || js_value.is_null() {
            return Ok(None);
        }

        let u8s = if js_value.is_instance_of::<Uint8Array>() || js_value.is_instance_of::<js_sys::ArrayBuffer>() {
            Uint8Array::new(&js_value)
        } else {
            return Err(WasmLoadBlobError::NotABlob);
        };

        Ok(Some(u8s.to_vec()))
    }

    /// Delete a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteBlobError`] if the blob could not be deleted.
    #[wasm_bindgen(js_name = deleteBlob)]
    pub async fn wasm_delete_blob(&self, wasm_digest: WasmDigest) -> Result<(), WasmDeleteBlobError> {
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmDeleteBlobError::TransactionError)?
            .object_store(BLOB_STORE_NAME).map_err(WasmDeleteBlobError::ObjectStoreError)?
            .delete(&JsValue::from_str(&wasm_digest.to_hex_string())).map_err(WasmDeleteBlobError::UnableToDeleteBlob)?;

        drop(await_idb(&req).await?);
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

    rx.await.map_err(AwaitIdbError::Canceled)?.map_err(AwaitIdbError::JsPromiseRejected)
}

/// Error indicating that a `JsValue` was expected to be a `Uint8Array` but was not.
#[wasm_bindgen]
#[derive(Debug, Clone, Error)]
#[error("expected a Uint8Array but got something else")]
#[allow(missing_copy_implementations)]
pub struct NotBytes;

/// Error indicating that a value from [`IndexedDB`] was expected to be a
/// `Uint8Array` or `null`/`undefined` but was not.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone, Error)]
#[error("expected Uint8Array or null/undefined from IndexedDB")]
pub struct InvalidIndexedDbValue;

impl From<InvalidIndexedDbValue> for JsValue {
    fn from(err: InvalidIndexedDbValue) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("InvalidIndexedDbValue");
        err.into()
    }
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

/// Error types for `saveBlob`.
#[derive(Debug, Error)]
pub enum WasmSaveBlobError {
    /// An error indicating that there was an issue during the transaction.
    #[error("saveBlob IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("saveBlob IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the blob could not be stored in `IndexedDB`.
    #[error("unable to store fragment(s) from IndexedDB: {0:?}")]
    UnableToStoreBlob(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmSaveBlobError> for JsValue {
    fn from(err: WasmSaveBlobError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmSaveBlobError");
        err.into()
    }
}

/// Error types for `loadBlob`.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum WasmLoadBlobError {
    /// An error indicating that there was an issue during the transaction.
    #[error("loadBlob IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("loadBlob IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the blob could not be retrieved from `IndexedDB`.
    #[error("unable to get blob from IndexedDB: {0:?}")]
    UnableToGetBlob(JsValue),

    /// An error indicating that the loaded JS value is not a `Uint8Array` blob.
    #[error("value loaded via loadBlob is not a Uint8Array")]
    NotABlob,

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmLoadBlobError> for JsValue {
    fn from(err: WasmLoadBlobError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadBlobError");
        err.into()
    }
}

/// Error types for `deleteBlob`.
#[derive(Debug, Error)]
pub enum WasmDeleteBlobError {
    /// An error indicating that there was an issue during the transaction.
    #[error("deleteBlob IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("deleteBlob IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the blob could not be deleted from `IndexedDB`.
    #[error("unable to delete blob from IndexedDB: {0:?}")]
    UnableToDeleteBlob(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteBlobError> for JsValue {
    fn from(err: WasmDeleteBlobError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteBlobError");
        err.into()
    }
}


/// Error types for `saveSedimentreeId`.
#[derive(Debug, Error)]
pub enum WasmSaveSedimentreeIdError {
    /// An error indicating that there was an issue during the transaction.
    #[error("saveSedimentreeId IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("saveSedimentreeId IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the sedimentree ID could not be saveed into `IndexedDB`.
    #[error("unable to `put` sedimentree ID into IndexedDB: {0:?}")]
    PutError(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
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

/// Error types for `saveSedimentreeId`.
#[derive(Debug, Error)]
pub enum WasmDeleteSedimentreeIdError {
    /// An error indicating that there was an issue during the transaction.
    #[error("deleteSedimentreeId IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("deleteSedimentreeId IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the sedimentree ID could not be deleted from `IndexedDB`.
    #[error("unable to `delete` sedimentree ID into IndexedDB: {0:?}")]
    DeleteError(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
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
    /// An error indicating that there was an issue during the transaction.
    #[error("loadSedimentreeIds IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("loadSedimentreeIds IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the sedimentree IDs could not be made into a valid request.
    #[error("unable to get sedimentree IDs from IndexedDB: {0:?}")]
    GetAllKeysError(JsValue),

    /// An error indicating that the loaded value is not an array.
    #[error("value loaded via loadSedimentreeIds is not an array: {0:?}")]
    NotAnArray(JsValue),

    /// An error indicating that a stored sedimentree ID is not a string.
    #[error("value loaded via loadSedimentreeIds is not a string: {0:?}")]
    StoredElementNotAString(JsValue),

    /// An error indicating that a stored sedimentree ID is invalid.
    #[error(transparent)]
    BadSedimentreeId(#[from] BadSedimentreeId),

    /// An error occurred while awaiting an `IndexedDB` operation.
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

/// Error types for `saveFragment`.
#[derive(Debug, Error)]
pub enum WasmSaveFragmentError {
    /// An error indicating that there was an issue during the transaction.
    #[error("saveFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("saveFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that fragments could not be stored in `IndexedDB`.
    #[error("unable to store fragment(s) from IndexedDB: {0:?}")]
    UnableToStoreFragment(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
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
pub enum WasmLoadFragmentsError {
    /// An error indicating that there was an issue during the transaction.
    #[error("loadFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("loadFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that fragments could not be retrieved from `IndexedDB`.
    #[error("unable to get fragment(s) from IndexedDB: {0:?}")]
    UnableToGetFragments(JsValue),

    /// An error indicating that the loaded value is not a `Uint8Array`.
    #[error("value loaded via loadFragment is not a Uint8Array: {0:?}")]
    NotUint8Array(JsValue),

    /// An error indicating that the loaded value is not an array of fragments.
    #[error("value loaded via loadFragment is not a Fragment array: {0:?}")]
    IndexError(JsValue),

    /// An error occurred while decoding a fragment from `IndexedDB`.
    #[error("error decoding blob to fragment from IndexedDB")]
    DecodeError,

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmLoadFragmentsError> for JsValue {
    fn from(err: WasmLoadFragmentsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadFragmentError");
        err.into()
    }
}

/// Error types for `deleteFragments`.
#[derive(Debug, Error)]
pub enum WasmDeleteFragmentsError {
    /// An error indicating that there was an issue during the transaction.
    #[error("deletelooseCommits IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("deletelooseCommits IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the cursor could not be opened.
    #[error("deletelooseCommits IndexedDB cursor error: {0:?}")]
    UnableToOpenCursor(JsValue),

    /// An error indicating that there was an issue creating the key range.
    #[error("deletelooseCommits IndexedDB key range error: {0:?}")]
    KeyRangeError(JsValue),

    /// A cursor error occurred when deleting loose commits.
    #[error("cursor problem when deleting loose commit(s) from IndexedDB: {0:?}")]
    CursorError(JsValue),

    /// Unable to delete a loose commit via the cursor.
    #[error("unable to advance cursor when deleting loose commit(s) from IndexedDB: {0:?}")]
    UnableToAdvanceCursor(JsValue),

    /// Unable to delete a loose commit.
    #[error("unable to delete loose commit(s) from IndexedDB: {0:?}")]
    UnableToDeleteLooseCommit(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteFragmentsError> for JsValue {
    fn from(err: WasmDeleteFragmentsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteFragmentsError");
        err.into()
    }
}

/// Error types for `saveLooseCommit`.
#[derive(Debug, Error)]
pub enum WasmSaveLooseCommitError {
    /// An error indicating that there was an issue during the transaction.
    #[error("saveLooseCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("saveLooseCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that loose commits could not be stored in `IndexedDB`.
    #[error("unable to store looseCommit(s) from IndexedDB: {0:?}")]
    UnableToStoreLooseCommit(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmSaveLooseCommitError> for JsValue {
    fn from(err: WasmSaveLooseCommitError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmSaveLooseCommitError");
        err.into()
    }
}

/// Error types for `loadLooseCommits`.
#[derive(Debug, Error)]
pub enum WasmLoadLooseCommitsError {
    /// An error indicating that there was an issue during the transaction.
    #[error("loadlooseCommits IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("loadlooseCommits IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that loose commits could not be retrieved from `IndexedDB`.
    #[error("unable to get loose commit(s) from IndexedDB: {0:?}")]
    UnableToGetLooseCommits(JsValue),

    /// An error indicating that the loaded value is not a `Uint8Array`.
    #[error("value loaded via loadlooseCommits is not a Uint8Array: {0:?}")]
    NotUint8Array(JsValue),

    /// An error indicating that the loaded value is not an array of loose commits.
    #[error("value loaded via loadlooseCommits is not a looseCommit array: {0:?}")]
    IndexError(JsValue),

    /// An error occurred while decoding a loose commit from `IndexedDB`.
    #[error("error decoding blob to loose commit from IndexedDB")]
    DecodeError,

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmLoadLooseCommitsError> for JsValue {
    fn from(err: WasmLoadLooseCommitsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmLoadLooseCommitError");
        err.into()
    }
}

/// Error types for `deleteLooseCommits`.
#[derive(Debug, Error)]
pub enum WasmDeleteLooseCommitsError {
    /// An error indicating that there was an issue during the transaction.
    #[error("deletelooseCommits IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    /// An error indicating that there was an issue accessing the object store.
    #[error("deletelooseCommits IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    /// An error indicating that the cursor could not be opened.
    #[error("deletelooseCommits IndexedDB cursor error: {0:?}")]
    UnableToOpenCursor(JsValue),

    /// An error indicating that there was an issue creating the key range.
    #[error("deletelooseCommits IndexedDB key range error: {0:?}")]
    KeyRangeError(JsValue),

    /// A cursor error occurred when deleting loose commits.
    #[error("cursor problem when deleting loose commit(s) from IndexedDB: {0:?}")]
    CursorError(JsValue),

    /// Unable to delete a loose commit via the cursor.
    #[error("unable to advance cursor when deleting loose commit(s) from IndexedDB: {0:?}")]
    UnableToAdvanceCursor(JsValue),

    /// Unable to delete a loose commit.
    #[error("unable to delete loose commit(s) from IndexedDB: {0:?}")]
    UnableToDeleteLooseCommit(JsValue),

    /// An error occurred while awaiting an `IndexedDB` operation.
    #[error("error awaiting IndexedDB operation: {0:?}")]
    AwaitIdbError(#[from] AwaitIdbError),
}

impl From<WasmDeleteLooseCommitsError> for JsValue {
    fn from(err: WasmDeleteLooseCommitsError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WasmDeleteLooseCommitsError");
        err.into()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Record {
    sedimentree_id: SedimentreeId,
    digest: Digest,
    payload: Vec<u8>,
}

impl From<Record> for JsValue {
    fn from(record: Record) -> Self {
        let obj = js_sys::Object::new();
        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(&obj, &RECORD_FIELD_SEDIMENTREE_ID.into(), &record.sedimentree_id.to_string().into()).expect("SedimentreeId to string failed");

        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(&obj, &RECORD_FIELD_DIGEST.into(), &record.digest.to_string().into()).expect("Digest to bytes failed");

        #[allow(clippy::expect_used)]
        js_sys::Reflect::set(&obj, &RECORD_FIELD_PAYLOAD.into(), &js_sys::Uint8Array::from(record.payload.as_slice())).expect("Payload to Uint8Array failed");
        obj.into()
    }
}
