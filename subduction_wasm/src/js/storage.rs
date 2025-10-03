//! Persistent storage.

pub mod idb;

use futures::{future::LocalBoxFuture, FutureExt};
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{future::Local, storage::Storage, Blob, Chunk, Digest, LooseCommit};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::js::{
    chunk::{JsChunk, JsChunksArray},
    digest::JsDigest,
    loose_commit::{JsLooseCommit, JsLooseCommitsArray},
};

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
    /// A duck-typed storage backend interface.
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
    type Error = JsStorageError;

    fn save_loose_commit(
        &self,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let js_loose_commit: JsLooseCommit = loose_commit.into();
            let promise = self
                .js_save_loose_commit(js_loose_commit)
                .map_err(JsStorageError::SaveLooseCommitError)?;
            wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_chunk(&self, chunk: Chunk) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let js_chunk: JsChunk = chunk.into();
            let promise = self
                .js_save_chunk(js_chunk)
                .map_err(JsStorageError::SaveChunkError)?;
            wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let promise = self
                .js_save_blob(blob.as_slice())
                .map_err(JsStorageError::SaveBlobError)?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            let js_digest = JsDigest::try_from(&js_value).map_err(JsStorageError::NotDigest)?;
            Ok(js_digest.into())
        }
        .boxed_local()
    }

    fn load_loose_commits(&self) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let promise = self
                .js_load_loose_commits()
                .map_err(JsStorageError::LoadLooseCommitsError)?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            let js_loose_commits = JsLooseCommitsArray::try_from(&js_value)
                .map_err(JsStorageError::NotLooseCommitArray)?;
            Ok(js_loose_commits.0.into_iter().map(|jc| jc.into()).collect())
        }
        .boxed_local()
    }

    fn load_chunks(&self) -> LocalBoxFuture<'_, Result<Vec<Chunk>, Self::Error>> {
        async move {
            let promise = self
                .js_load_chunks()
                .map_err(JsStorageError::LoadChunksError)?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            let js_chunks =
                JsChunksArray::try_from(&js_value).map_err(JsStorageError::NotChunksArray)?;
            Ok(js_chunks.0.into_iter().map(|jc| jc.into()).collect())
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let promise = self
                .js_load_blob(blob_digest.into())
                .map_err(JsStorageError::LoadBlobError)?;

            let js_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;

            let maybe_blob = if js_value.is_null() || js_value.is_undefined() {
                None
            } else {
                if js_value.is_instance_of::<Uint8Array>() {
                    Some(Blob::from(Uint8Array::new(&js_value).to_vec()))
                } else {
                    return Err(JsStorageError::NotBytes);
                }
            };

            Ok(maybe_blob)
        }
        .boxed_local()
    }
}

/// Errors that can occur when using `JsStorage`.
#[derive(Error, Debug)]
pub enum JsStorageError {
    /// An error occurred while saving a blob.
    #[error("JavaScript save blob error: {0:?}")]
    SaveBlobError(JsValue),

    /// An error occurred while saving a loose commit.
    #[error("JavaScript save loose commits error: {0:?}")]
    SaveLooseCommitError(JsValue),

    /// An error occurred while saving a chunk.
    #[error("JavaScript save chunks error: {0:?}")]
    SaveChunkError(JsValue),

    /// An error occurred while loading a blob.
    #[error("JavaScript load blob error: {0:?}")]
    LoadBlobError(JsValue),

    /// An error occurred while loading loose commits.
    #[error("JavaScript load loose commits error: {0:?}")]
    LoadLooseCommitsError(JsValue),

    /// An error occurred while loading chunks.
    #[error("JavaScript load chunks error: {0:?}")]
    LoadChunksError(JsValue),

    /// An error occurred while converting a `Promise` result to a Rust future.
    #[error("Promise conversion error: {0:?}")]
    ConvertFromJsPromiseError(JsValue),

    /// The `JsValue` could not be converted into bytes.
    #[error("Value was not bytes")]
    NotBytes,

    /// The `JsValue` could not be converted into an array of `JsLooseCommit`.
    #[error("Value was not an array of LooseCommits: {0:?}")]
    NotLooseCommitArray(JsValue),

    /// The `JsValue` could not be converted into an array of `JsChunk`.
    #[error("Value was not an array of Chunks: {0:?}")]
    NotChunksArray(JsValue),

    /// The `JsValue` could not be converted into a `JsDigest`.
    #[error("Value was not a Digest: {0:?}")]
    NotDigest(JsValue),
}
