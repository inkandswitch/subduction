//! Persistent storage.

pub mod idb;

use futures::{future::LocalBoxFuture, FutureExt};
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{future::Local, storage::Storage, Blob, Digest, Fragment, LooseCommit};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::js::{
    digest::JsDigest,
    fragment::{JsFragment, JsFragmentsArray},
    loose_commit::{JsLooseCommit, JsLooseCommitsArray},
};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Storage {
    saveJsLooseCommit(commit: LooseCommit): Promise<void>;
    saveJsFragment(fragment: Fragment): Promise<void>;
    saveBlob(data: Uint8Array): Promise<Digest>;

    loadJsLooseCommits(): Promise<LooseCommit[]>;
    loadJsFragments(): Promise<Fragment[]>;
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

    /// Save a fragment to storage.
    #[wasm_bindgen(method, catch, js_name = saveJsFragment)]
    fn js_save_fragment(this: &JsStorage, fragment: JsFragment) -> Result<Promise, JsValue>;

    /// Load all fragments from storage.
    #[wasm_bindgen(method, catch, js_name = loadJsFragments)]
    fn js_load_fragments(this: &JsStorage) -> Result<Promise, JsValue>;

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

    fn save_fragment(&self, fragment: Fragment) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let js_fragment: JsFragment = fragment.into();
            let promise = self
                .js_save_fragment(js_fragment)
                .map_err(JsStorageError::SaveFragmentError)?;
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

    fn load_fragments(&self) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let promise = self
                .js_load_fragments()
                .map_err(JsStorageError::LoadFragmentsError)?;
            let js_value = wasm_bindgen_futures::JsFuture::from(promise)
                .await
                .map_err(JsStorageError::ConvertFromJsPromiseError)?;
            let js_fragments =
                JsFragmentsArray::try_from(&js_value).map_err(JsStorageError::NotFragmentsArray)?;
            Ok(js_fragments.0.into_iter().map(|jc| jc.into()).collect())
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

    /// An error occurred while saving a fragment.
    #[error("JavaScript save fragments error: {0:?}")]
    SaveFragmentError(JsValue),

    /// An error occurred while loading a blob.
    #[error("JavaScript load blob error: {0:?}")]
    LoadBlobError(JsValue),

    /// An error occurred while loading loose commits.
    #[error("JavaScript load loose commits error: {0:?}")]
    LoadLooseCommitsError(JsValue),

    /// An error occurred while loading fragments.
    #[error("JavaScript load fragments error: {0:?}")]
    LoadFragmentsError(JsValue),

    /// An error occurred while converting a `Promise` result to a Rust future.
    #[error("Promise conversion error: {0:?}")]
    ConvertFromJsPromiseError(JsValue),

    /// The `JsValue` could not be converted into bytes.
    #[error("Value was not bytes")]
    NotBytes,

    /// The `JsValue` could not be converted into an array of `JsLooseCommit`.
    #[error("Value was not an array of LooseCommits: {0:?}")]
    NotLooseCommitArray(JsValue),

    /// The `JsValue` could not be converted into an array of `JsFragment`.
    #[error("Value was not an array of Fragments: {0:?}")]
    NotFragmentsArray(JsValue),

    /// The `JsValue` could not be converted into a `JsDigest`.
    #[error("Value was not a Digest: {0:?}")]
    NotDigest(JsValue),
}
