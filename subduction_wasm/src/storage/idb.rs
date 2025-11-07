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
    #[wasm_bindgen( js_name = saveWasmLooseCommit)]
    pub async fn wasm_save_loose_commit(
        &self,
        loose_commit: &WasmLooseCommit,
    ) -> Result<(), FIXME> {
        let core_commit = LooseCommit::from(loose_commit.clone());
        let digest = core_commit.digest().clone();
        let value: JsValue = bincode::serde::encode_to_vec(core_commit, bincode::config::standard())
            .map_err(|_| FIXME)?.into();

        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(|_| FIXME)?
            .object_store(LOOSE_COMMIT_STORE_NAME).map_err(|_| FIXME)?
            .put_with_key(
                &value,
                &digest.as_bytes().to_vec().into()
            ).map_err(|_| FIXME)?;

        let key = await_idb(&req).await.map_err(|_| FIXME)?;
        drop(key);

        Ok(())
    }

    /// Load all loose commits from storage.
    #[wasm_bindgen( js_name = loadWasmLooseCommits)]
    pub async fn wasm_load_loose_commits(&self) -> Result<Vec<JsLooseCommit>, FIXME> {
        let req = self
            .0
            .transaction_with_str_and_mode(LOOSE_COMMIT_STORE_NAME, IdbTransactionMode::Readonly).map_err(|_| FIXME)?
            .object_store(LOOSE_COMMIT_STORE_NAME).map_err(|_| FIXME)?
            .get_all().map_err(|_| FIXME)?;

        let js_value = await_idb(&req).await.map_err(|_| FIXME)?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(Vec::new())
        } else if js_value.is_instance_of::<js_sys::Array>() {
            let mut xs = Vec::new();
            for js_item in js_sys::Array::from(&js_value).iter() {
                let bytes = js_item.dyn_into::<Uint8Array>().map_err(|_| FIXME)?.to_vec();
                let (fragment, _size) = bincode::serde::decode_from_slice::<LooseCommit, _>(
                    &bytes,
                    bincode::config::standard(),
                ).map_err(|_| FIXME)?;
                xs.push(WasmLooseCommit::from(fragment).into());
            }
            Ok(xs)
        } else {
            Err(FIXME)
            // FIXME Err(JsValue::from(InvalidIndexedDbValue))
        }
    }

    /// Save a fragment to storage.
    #[wasm_bindgen(js_name = saveWasmFragment)]
   pub  async fn wasm_save_fragment(&self, fragment: &WasmFragment) -> Result<(), FIXME> {
        let core_fragment = Fragment::from(fragment.clone());
        let digest = core_fragment.digest().clone();
        let value: JsValue = bincode::serde::encode_to_vec(core_fragment, bincode::config::standard())
            .map_err(|_| FIXME)?.into();

        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readwrite).map_err(|_| FIXME)?
            .object_store(FRAGMENT_STORE_NAME).map_err(|_| FIXME)?
            .put_with_key(
                &value,
                &digest.as_bytes().to_vec().into()
            ).map_err(|_| FIXME)?;

        let key = await_idb(&req).await.map_err(|_| FIXME)?;
        drop(key);

        Ok(())
    }

    /// Load all fragments from storage.
    #[wasm_bindgen(js_name = loadWasmFragments)]
    pub async fn wasm_load_fragments(&self) -> Result<Vec<JsFragment>, FIXME> {
        let req = self
            .0
            .transaction_with_str_and_mode(FRAGMENT_STORE_NAME, IdbTransactionMode::Readonly).map_err(|_| FIXME)?
            .object_store(FRAGMENT_STORE_NAME).map_err(|_| FIXME)?
            .get_all().map_err(|_| FIXME)?;

        let js_value = await_idb(&req).await.map_err(|_| FIXME)?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(Vec::new())
        } else if js_value.is_instance_of::<js_sys::Array>() {
            let mut xs = Vec::new();
            for js_item in js_sys::Array::from(&js_value).iter() {
                let bytes = js_item.dyn_into::<Uint8Array>().map_err(|_| FIXME)?.to_vec();
                let (fragment, _size) = bincode::serde::decode_from_slice::<Fragment, _>(
                    &bytes,
                    bincode::config::standard(),
                ).map_err(|_| FIXME)?;
                xs.push(WasmFragment::from(fragment).into());
            }
            Ok(xs)
        } else {
            Err(FIXME)
            // FIXME Err(JsValue::from(InvalidIndexedDbValue))
        }
    }

    /// Save a blob to the database, returning its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the JS transaction could not be opened,
    /// or if the blob could not be saved.
    #[wasm_bindgen(js_name = saveBlob)]
    pub async fn wasm_save_blob(&self, bytes: &[u8]) -> Result<WasmDigest, FIXME> {
        let digest = Digest::hash(bytes);
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readwrite).map_err(|_| FIXME)?
            .object_store(BLOB_STORE_NAME).map_err(|_| FIXME)?
            .put_with_key(
                &JsValue::from(bytes.to_vec()),
                &JsValue::from_str(&digest.to_string()),
            ).map_err(|_| FIXME)?;

        let key = await_idb(&req).await.map_err(|_| FIXME)?;
        drop(key);

        Ok(digest.into())
    }

    /// Load a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the blob could not be loaded.
    #[wasm_bindgen(js_name = loadBlob)]
    pub async fn wasm_load_blob(&self, digest: WasmDigest) -> Result<Option<Vec<u8>>, FIXME> {
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readonly).map_err(|_| FIXME)?
            .object_store(BLOB_STORE_NAME).map_err(|_| FIXME)?
            .get(&JsValue::from_str(&Digest::from(digest).to_string())).map_err(|_| FIXME)?;

        let js_value = await_idb(&req).await.map_err(|_| FIXME)?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(None)
        } else if js_value.is_instance_of::<Uint8Array>() {
            Ok(Some(Uint8Array::new(&js_value).to_vec()))
        } else {
            Err(FIXME)
            // FIXME Err(JsValue::from(InvalidIndexedDbValue))
        }
    }
}

async fn await_idb(req: &IdbRequest) -> Result<JsValue, JsValue> {
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
        .map_err(|_| JsValue::from(js_sys::Error::new("Channel dropped")))?
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

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("FIXME")]
pub struct FIXME;

impl From<FIXME> for JsValue {
    fn from(err: FIXME) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("FIXME");
        err.into()
    }
}
