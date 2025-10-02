use std::{cell::RefCell, rc::Rc};

use futures::{channel::oneshot, future::LocalBoxFuture, FutureExt};
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{future::Local, storage::Storage, Blob, Chunk, Digest, LooseCommit};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use web_sys::{
    Event, IdbDatabase, IdbOpenDbRequest, IdbRequest, IdbTransactionMode, ServiceWorkerGlobalScope,
    WorkerGlobalScope,
};

use crate::js::{
    chunk::JsChunk,
    digest::JsDigest,
    loose_commit::{JsLooseCommit, JsLooseCommitsArray},
};

use super::chunk::JsChunksArray;

pub const DB_VERSION: u32 = 0;
pub const DB_NAME: &str = "@automerge/subduction/db";
pub const BLOB_STORE_NAME: &str = "blobs";

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Storage {
    saveJsLooseCommit(commit: LooseCommit): Promise<void>;
    saveJsChunk(chunk: Chunk): Promise<void>;
    saveBlob(data: Uint8Array): Promise<Digest>;

    loadJsLooseCommits(): Promise<LooseCommit[]>;
    loadJsChunks(): Promise<Chunk[]>;
    loadBlob(digest: Digest): Promise<Uint8Array | null>;
}
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = Storage, typescript_type = "Storage")]
    pub type JsStorage;

    /// Save a blob to storage.
    #[wasm_bindgen(method, catch, js_name = saveBlob)]
    fn js_save_blob(this: &JsStorage, blob: &[u8]) -> Result<Promise, JsValue>;

    /// Load all loose commits from storage.
    #[wasm_bindgen(method, catch, js_name = loadJsLooseCommits)]
    fn js_load_loose_commits(this: &JsStorage) -> Result<Promise, JsValue>;

    /// Save a loose commit to storage.
    #[wasm_bindgen(method, catch, js_name = saveJsLooseCommit)]
    fn js_save_loose_commit(
        this: &JsStorage,
        loose_commit: JsLooseCommit,
    ) -> Result<Promise, JsValue>;

    /// Save a chunk to storage.
    #[wasm_bindgen(method, catch, js_name = saveJsChunk)]
    fn js_save_chunk(this: &JsStorage, chunk: JsChunk) -> Result<Promise, JsValue>;

    /// Load all chunks from storage.
    #[wasm_bindgen(method, catch, js_name = loadJsChunks)]
    fn js_load_chunks(this: &JsStorage) -> Result<Promise, JsValue>;

    /// Load a blob from storage.
    #[wasm_bindgen(method, catch, js_name = loadBlob)]
    fn js_load_blob(this: &JsStorage, blob_digest: JsDigest) -> Result<Promise, JsValue>;
}

impl std::fmt::Debug for JsStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("JsStorage").finish()
    }
}

impl Storage<Local> for JsStorage {
    // FIXME error type
    type Error = JsErr;

    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let js_loose_commit: JsLooseCommit = loose_commit.into();
            let promise = self.js_save_loose_commit(js_loose_commit)?;
            wasm_bindgen_futures::JsFuture::from(promise).await?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_chunk(&self, chunk: Chunk) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let js_chunk: JsChunk = chunk.into();
            let promise = self.js_save_chunk(js_chunk)?;
            wasm_bindgen_futures::JsFuture::from(promise).await?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, JsErr>> {
        async move {
            let promise = self.js_save_blob(blob.as_slice())?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise).await?;
            let js_digest = JsDigest::try_from(&js_value)?;
            Ok(js_digest.into())
        }
        .boxed_local()
    }

    fn load_loose_commits(&self) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, JsErr>> {
        async move {
            let promise = self.js_load_loose_commits()?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise).await?;
            let js_loose_commits = JsLooseCommitsArray::try_from(&js_value)?;
            Ok(js_loose_commits.0.into_iter().map(|jc| jc.into()).collect())
        }
        .boxed_local()
    }

    fn load_chunks(&self) -> LocalBoxFuture<'_, Result<Vec<Chunk>, Self::Error>> {
        async move {
            let promise = self.js_load_chunks()?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise).await?;
            let js_chunks = JsChunksArray::try_from(&js_value)?;
            Ok(js_chunks.0.into_iter().map(|jc| jc.into()).collect())
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let promise = self.js_load_blob(blob_digest.into())?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise).await?;

            let maybe_blob = if js_value.is_null() || js_value.is_undefined() {
                None
            } else {
                if js_value.is_instance_of::<Uint8Array>() {
                    Some(Blob::from(Uint8Array::new(&js_value).to_vec()))
                } else {
                    // FIXME
                    return Err(JsErr(JsValue::from_str(
                        "Expected Uint8Array or null/undefined",
                    )));
                }
            };

            Ok(maybe_blob)
        }
        .boxed_local()
    }
}

//////////////////////

// FIXME better error
#[wasm_bindgen]
#[derive(Debug, Error)]
#[error("JavaScript error: {0:?}")]
pub struct JsErr(JsValue);

impl From<JsValue> for JsErr {
    fn from(value: JsValue) -> Self {
        JsErr(value)
    }
}

#[wasm_bindgen(js_name = IndexedDbStorage)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedDbStorage(IdbDatabase);

#[wasm_bindgen(js_class = "IndexedDbStorage")]
impl IndexedDbStorage {
    pub async fn new() -> Result<Self, JsValue> {
        let global = js_sys::global();

        let factory = if global.is_instance_of::<web_sys::Window>() {
            let win: web_sys::Window = global.dyn_into()?;
            win.indexed_db()?.ok_or_else(|| {
                JsValue::from(js_sys::Error::new("IndexedDB not available in Window"))
            })
        } else if global.is_instance_of::<ServiceWorkerGlobalScope>() {
            let sw: ServiceWorkerGlobalScope = global.dyn_into()?;
            let worker: WorkerGlobalScope = sw.unchecked_into();
            worker.indexed_db()?.ok_or_else(|| {
                js_sys::Error::new("IndexedDB not available in ServiceWorker").into()
            })
        } else if global.is_instance_of::<WorkerGlobalScope>() {
            let worker: WorkerGlobalScope = global.dyn_into()?;
            worker
                .indexed_db()?
                .ok_or_else(|| js_sys::Error::new("IndexedDB not available in Worker").into())
        } else {
            Err(js_sys::Error::new("Unsupported JS global context (not Window/Worker)").into())
        }?;

        let open_req: IdbOpenDbRequest = factory.open_with_u32(DB_NAME, DB_VERSION)?;

        // Optional upgrade step to create object stores on first open
        {
            let onupgradeneeded = Closure::once(Box::new(move |e: Event| {
                if let Some(req) = e
                    .target()
                    .and_then(|t| t.dyn_into::<IdbOpenDbRequest>().ok())
                {
                    if let Ok(db_val) = req.result() {
                        if let Ok(db) = db_val.dyn_into::<IdbDatabase>() {
                            let ensured = db.create_object_store(DB_NAME).expect("FIXME");
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
            Err(JsValue::from_str(
                "Expected Uint8Array or null/undefined from IndexedDB", // FIXME better error
            ))
        }
    }

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
            let res = req.result().map_err(|e| e.into());
            let _ = tx_ok.borrow_mut().take().map(|tx| {
                tx.send(res).expect("FIXME");
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
                .map(|dom_exc| dom_exc.into())
                .unwrap_or_else(|_| js_sys::Error::new("IDB error").into());

            let _ = tx_err.borrow_mut().take().map(|tx| {
                tx.send(Err(err)).expect("FIXME");
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
