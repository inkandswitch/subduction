//! [`IndexedDB`] storage backend for Sedimentree.

use futures::channel::oneshot;
use js_sys::Uint8Array;
use sedimentree_core::Digest;
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use web_sys::{Event, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransactionMode};

use crate::js::digest::JsDigest;

/// The version number of the [`IndexedDB`] database schema.
pub const DB_VERSION: u32 = 0;

/// The name of the [`IndexedDB`] database.
pub const DB_NAME: &str = "@automerge/subduction/db";

/// The name of the object store for blobs.
pub const BLOB_STORE_NAME: &str = "blobs";

/// IndexedDB storage backend.
#[wasm_bindgen(js_name = IndexedDbStorage)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedDbStorage(IdbDatabase);

#[wasm_bindgen(js_class = "IndexedDbStorage")]
impl IndexedDbStorage {
    /// Create a new `IndexedDbStorage` instance, opening (or creating) the database.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the database could not be opened.
    #[wasm_bindgen]
    pub async fn setup(factory: &IdbFactory) -> Result<Self, JsValue> {
        let open_req: IdbOpenDbRequest = factory.open_with_u32(DB_NAME, DB_VERSION)?;

        // Create object stores on first open
        {
            let onupgradeneeded = Closure::once(Box::new(move |e: Event| {
                if let Some(req) = e
                    .target()
                    .and_then(|t| t.dyn_into::<IdbOpenDbRequest>().ok())
                {
                    if let Ok(db_val) = req.result() {
                        if let Ok(db) = db_val.dyn_into::<IdbDatabase>() {
                            let ensured = db.create_object_store(DB_NAME).map_err(|e| {
                                tracing::error!("Failed to create object store: {:?}", e);
                            });
                            drop(ensured);
                        }
                    }
                }
            }) as Box<dyn FnOnce(_)>);
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

    /// Load a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the blob could not be loaded.
    #[wasm_bindgen(js_name = loadBlob)]
    pub async fn load_blob(&self, digest: JsDigest) -> Result<Option<Vec<u8>>, JsValue> {
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readonly)?
            .object_store(BLOB_STORE_NAME)?
            .get(&JsValue::from_str(&Digest::from(digest).to_string()))?;

        let js_value = await_idb(&req).await?;
        if js_value.is_undefined() || js_value.is_null() {
            Ok(None)
        } else if js_value.is_instance_of::<Uint8Array>() {
            Ok(Some(Uint8Array::new(&js_value).to_vec()))
        } else {
            Err(JsValue::from(InvalidIndexedDbValue))
        }
    }

    /// Save a blob to the database, returning its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the JS transaction could not be opened,
    /// or if the blob could not be saved.
    #[wasm_bindgen(js_name = saveBlob)]
    pub async fn save_blob(&self, bytes: &[u8]) -> Result<JsDigest, JsValue> {
        let digest = Digest::hash(bytes);
        let req = self
            .0
            .transaction_with_str_and_mode(BLOB_STORE_NAME, IdbTransactionMode::Readwrite)?
            .object_store(BLOB_STORE_NAME)?
            .put_with_key(
                &JsValue::from(bytes.to_vec()),
                &JsValue::from_str(&digest.to_string()),
            )?;

        let key = await_idb(&req).await?;
        drop(key);

        Ok(digest.into())
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
