//! [`IndexedDB`] storage backend for Sedimentree.

use futures::channel::oneshot;
use js_sys::Uint8Array;
use sedimentree_core::{blob::Digest, Fragment, LooseCommit};
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use web_sys::{
    Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransactionMode,
    IdbVersionChangeEvent,
};

use crate::{digest::WasmDigest, fragment::{JsFragment, WasmFragment}, loose_commit::{JsLooseCommit, WasmLooseCommit}};

/// The version number of the [`IndexedDB`] database schema.
pub const DB_VERSION: u32 = 1;

/// The name of the [`IndexedDB`] database.
pub const DB_NAME: &str = "@automerge/subduction/db";

/// The name of the object store for blobs.
pub const BLOB_STORE_NAME: &str = "blobs";

/// The name of the object store for loose commits.
pub const LOOSE_COMMIT_STORE_NAME: &str = "commits";

/// The name of the object store for fragments.
pub const FRAGMENT_STORE_NAME: &str = "fragments";

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
                        let names = db.object_store_names();
                        if !names.contains(BLOB_STORE_NAME)
                            && let Err(e) = db.create_object_store(BLOB_STORE_NAME) {
                                tracing::error!(
                                    "failed to create object store '{}': {:?}",
                                    BLOB_STORE_NAME,
                                    e
                                );
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

    #[wasm_bindgen(js_name = looseCommitStoreName)]
    pub fn loose_commit_store_name(&self) -> String {
        LOOSE_COMMIT_STORE_NAME.to_string()
    }

    #[wasm_bindgen(js_name = fragmentStoreName)]
    pub fn fragment_store_name(&self) -> String {
        FRAGMENT_STORE_NAME.to_string()
    }

    #[wasm_bindgen(js_name = blobStoreName)]
    pub fn blob_store_name(&self) -> String {
        BLOB_STORE_NAME.to_string()
    }

    /// Save a loose commit to storage.
    #[wasm_bindgen( js_name = saveLooseCommit)]
    pub async fn wasm_save_loose_commit(
        &self,
        loose_commit: &WasmLooseCommit,
    ) -> Result<(), WasmSaveLooseCommitError> {
        let core_commit = LooseCommit::from(loose_commit.clone());
        let digest = core_commit.digest().clone();
        let bytes: Vec<u8> = bincode::serde::encode_to_vec(core_commit, bincode::config::standard())?;

        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveLooseCommitError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmSaveLooseCommitError::ObjectStoreError)?
            .put_with_key(
                &bytes.into(),
                &digest.as_bytes().to_vec().into()
            ).map_err(WasmSaveLooseCommitError::UnableToStoreLooseCommit)?;

        let key = await_idb(&req).await?;
        drop(key);

        Ok(())
    }

    /// Load all loose commits from storage.
    #[wasm_bindgen( js_name = loadLooseCommits)]
    pub async fn wasm_load_loose_commits(&self) -> Result<Vec<JsLooseCommit>, WasmLoadLooseCommitsError> {
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadLooseCommitsError::TransactionError)?
            .object_store(LOOSE_COMMIT_STORE_NAME).map_err(WasmLoadLooseCommitsError::ObjectStoreError)?
            .get_all().map_err(WasmLoadLooseCommitsError::UnableToGetLooseCommits)?;

        let js_value = await_idb(&req).await?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(Vec::new())
        } else if js_value.is_instance_of::<js_sys::Array>() {
            let mut xs = Vec::new();
            for js_item in js_sys::Array::from(&js_value).iter() {
                let bytes = js_item.dyn_into::<Uint8Array>().map_err(WasmLoadLooseCommitsError::NotUint8Array)?.to_vec();
                let (fragment, _size) = bincode::serde::decode_from_slice::<LooseCommit, _>(
                    &bytes,
                    bincode::config::standard(),
                )?;
                xs.push(WasmLooseCommit::from(fragment).into());
            }
            Ok(xs)
        } else {
            Err(WasmLoadLooseCommitsError::NotALooseCommitArray)
        }
    }

    /// Save a fragment to storage.
    #[wasm_bindgen(js_name = saveFragment)]
   pub  async fn wasm_save_fragment(&self, fragment: &WasmFragment) -> Result<(), WasmSaveFragmentError> {
        let core_fragment = Fragment::from(fragment.clone());
        let digest = core_fragment.digest().clone();
        let value: Vec<u8> = bincode::serde::encode_to_vec(core_fragment, bincode::config::standard())?;

        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(WasmSaveFragmentError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME).map_err(WasmSaveFragmentError::ObjectStoreError)?
            .put_with_key(
                &value.into(),
                &digest.as_bytes().to_vec().into()
            ).map_err(WasmSaveFragmentError::UnableToStoreFragment)?;

        let key = await_idb(&req).await?;
        drop(key);

        Ok(())
    }

    /// Load all fragments from storage.
    #[wasm_bindgen(js_name = loadFragments)]
    pub async fn wasm_load_fragments(&self) -> Result<Vec<JsFragment>, WasmLoadFragmentsError> {
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadFragmentsError::TransactionError)?
            .object_store(FRAGMENT_STORE_NAME).map_err(WasmLoadFragmentsError::ObjectStoreError)?
            .get_all().map_err(WasmLoadFragmentsError::UnableToGetFragments)?;

        let js_value = await_idb(&req).await?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(Vec::new())
        } else if js_value.is_instance_of::<js_sys::Array>() {
            let mut xs = Vec::new();
            for js_item in js_sys::Array::from(&js_value).iter() {
                let bytes = js_item.dyn_into::<Uint8Array>().map_err(WasmLoadFragmentsError::NotUint8Array)?.to_vec();
                let (fragment, _size) = bincode::serde::decode_from_slice::<Fragment, _>(
                    &bytes,
                    bincode::config::standard())?;
                xs.push(WasmFragment::from(fragment).into());
            }
            Ok(xs)
        } else {
            Err(WasmLoadFragmentsError::NotAFragmentArray)
        }
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
                &JsValue::from(bytes.to_vec()),
                &JsValue::from_str(&digest.to_string()),
            ).map_err(WasmSaveBlobError::UnableToStoreBlob)?;

        let key = await_idb(&req).await?;
        drop(key);

        Ok(digest.into())
    }

    /// Load a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the blob could not be loaded.
    #[wasm_bindgen(js_name = loadBlob)]
    pub async fn wasm_load_blob(&self, digest: WasmDigest) -> Result<Option<Vec<u8>>, WasmLoadBlobError> {
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readonly).map_err(WasmLoadBlobError::TransactionError)?
            .object_store(BLOB_STORE_NAME).map_err(WasmLoadBlobError::ObjectStoreError)?
            .get(&JsValue::from_str(&Digest::from(digest).to_string())).map_err(WasmLoadBlobError::UnableToGetBlob)?;

        let js_value = await_idb(&req).await?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(None)
        } else if js_value.is_instance_of::<Uint8Array>() {
            Ok(Some(Uint8Array::new(&js_value).to_vec()))
        } else {
            Err(WasmLoadBlobError::NotABlob)
        }
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

    Ok(rx.await?.map_err(AwaitIdbError::JsPromiseRejected)?)
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

#[derive(Debug, Clone, PartialEq, Error)]
pub enum AwaitIdbError {
    #[error("Channel dropped")]
    Canceled(#[from] oneshot::Canceled),

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

#[derive(Debug, Error)]
pub enum WasmSaveBlobError {
    #[error("saveBlob IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("saveBlob IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to store fragment(s) from IndexedDB: {0:?}")]
    UnableToStoreBlob(JsValue),

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

#[derive(Debug, Clone, PartialEq, Error)]
pub enum WasmLoadBlobError {
    #[error("loadBlob IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("loadBlob IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to get blob from IndexedDB: {0:?}")]
    UnableToGetBlob(JsValue),

    #[error("value loaded via loadBlob is not a Uint8Array")]
    NotABlob,

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

#[derive(Debug, Error)]
pub enum WasmSaveFragmentError {
    #[error("saveFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("saveFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to store fragment(s) from IndexedDB: {0:?}")]
    UnableToStoreFragment(JsValue),

    #[error("error encoding blob from IndexedDB: {0:?}")]
    EncodeError(#[from] bincode::error::EncodeError),

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

#[derive(Debug, Error)]
pub enum WasmLoadFragmentsError {
    #[error("loadFragment IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("loadFragment IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to get fragment(s) from IndexedDB: {0:?}")]
    UnableToGetFragments(JsValue),

    #[error("value loaded via loadFragment is not a Uint8Array: {0:?}")]
    NotUint8Array(JsValue),

    #[error("value loaded via loadFragment is not a Uint8Array")]
    NotAFragmentArray,

    #[error("error decoding blob from IndexedDB: {0:?}")]
    DecodeError(#[from] bincode::error::DecodeError),

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


#[derive(Debug, Error)]
pub enum WasmSaveLooseCommitError {
    #[error("saveLooseCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("saveLooseCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to store looseCommit(s) from IndexedDB: {0:?}")]
    UnableToStoreLooseCommit(JsValue),

    #[error("error encoding blob from IndexedDB: {0:?}")]
    EncodeError(#[from] bincode::error::EncodeError),

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

#[derive(Debug, Error)]
pub enum WasmLoadLooseCommitsError {
    #[error("loadLooseCommit IndexedDB transaction error: {0:?}")]
    TransactionError(JsValue),

    #[error("loadLooseCommit IndexedDB object store error: {0:?}")]
    ObjectStoreError(JsValue),

    #[error("unable to get looseCommit(s) from IndexedDB: {0:?}")]
    UnableToGetLooseCommits(JsValue),

    #[error("value loaded via loadLooseCommit is not a Uint8Array: {0:?}")]
    NotUint8Array(JsValue),

    #[error("value loaded via loadLooseCommit is not a Uint8Array")]
    NotALooseCommitArray,

    #[error("error decoding blob from IndexedDB: {0:?}")]
    DecodeError(#[from] bincode::error::DecodeError),

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
