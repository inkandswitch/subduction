//! Persistent storage.

pub mod idb;

use from_js_ref::FromJsRef;
use futures::{future::LocalBoxFuture, FutureExt};
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::{Blob, Digest},
    future::Local,
    storage::Storage,
    Fragment, LooseCommit, SedimentreeId,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    digest::{JsDigest, WasmDigest},
    fragment::{
        JsFragment, WasmConvertJsValueToFragmentArrayError, WasmFragment, WasmFragmentsArray,
    },
    loose_commit::{JsLooseCommit, WasmLooseCommit, WasmLooseCommitsArray},
    sedimentree_id::{JsSedimentreeId, WasmSedimentreeId},
};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Storage {
    saveWasmLooseCommit(sedimentreeId: SedimentreeId, commit: LooseCommit): Promise<void>;
    saveWasmFragment(sedimentreeId: SedimentreeId, fragment: Fragment): Promise<void>;
    saveBlob(data: Uint8Array): Promise<Digest>;

    loadWasmLooseCommits(sedimentreeId: SedimentreeId): Promise<LooseCommit[]>;
    loadWasmFragments(sedimentreeId: SedimentreeId): Promise<Fragment[]>;
    loadBlob(digest: Digest): Promise<Uint8Array | null>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed storage backend interface.
    #[wasm_bindgen(js_name = Storage, typescript_type = "Storage")]
    pub type JsStorage;

    /// Save a blob to storage.
    #[wasm_bindgen(method, js_name = saveBlob)]
    fn js_save_blob(this: &JsStorage, blob: &[u8]) -> Promise;

    /// Save a loose commit to storage.
    #[wasm_bindgen(method, js_name = saveWasmLooseCommit)]
    fn js_save_loose_commit(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        loose_commit: &JsLooseCommit,
    ) -> Promise;

    /// Save a fragment to storage.
    #[wasm_bindgen(method, js_name = saveWasmFragment)]
    fn js_save_fragment(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        fragment: &JsFragment,
    ) -> Promise;

    /// Load all loose commits from storage.
    #[wasm_bindgen(method, js_name = loadWasmLooseCommits)]
    fn js_load_loose_commits(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    /// Load all fragments from storage.
    #[wasm_bindgen(method, js_name = loadWasmFragments)]
    fn js_load_fragments(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    /// Load a blob from storage.
    #[wasm_bindgen(method, js_name = loadBlob)]
    fn js_load_blob(this: &JsStorage, blob_digest: &JsDigest) -> Promise;
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
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::save_loose_commit");
            let _enter = span.enter();

            tracing::debug!("saving loose commit {:?}", loose_commit.digest());
            let wasm_loose_commit: WasmLooseCommit = loose_commit.into();
            let js_promise = self.js_save_loose_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &wasm_loose_commit.into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::SaveLooseCommitError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::save_fragment");
            let _enter = span.enter();

            tracing::debug!(
                "saving fragment {:?}",
                fragment.summary().blob_meta().digest()
            );
            let js_fragment: WasmFragment = fragment.into();
            let promise = self.js_save_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &js_fragment.into(),
            );
            JsFuture::from(promise)
                .await
                .map_err(JsStorageError::SaveFragmentError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::save_blob");
            let _enter = span.enter();

            let promise = self.js_save_blob(blob.as_slice());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsStorageError::SaveBlobError)?;
            let wasm_digest =
                WasmDigest::try_from_js_value(&js_value).ok_or(JsStorageError::NotDigest)?;
            Ok(wasm_digest.into())
        }
        .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::load_loose_commits");
            let _enter = span.enter();

            let promise =
                self.js_load_loose_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsStorageError::LoadLooseCommitsError)?;
            let js_loose_commits = WasmLooseCommitsArray::try_from(&js_value)
                .map_err(JsStorageError::NotLooseCommitArray)?;
            Ok(js_loose_commits.0.into_iter().map(Into::into).collect())
        }
        .boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::load_fragments");
            let _enter = span.enter();

            let promise = self.js_load_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsStorageError::LoadFragmentsError)?;
            let js_fragments = WasmFragmentsArray::try_from(&js_value)
                .map_err(JsStorageError::NotFragmentsArray)?;
            Ok(js_fragments.0.into_iter().map(Into::into).collect())
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsStorage::load_blob");
            let _enter = span.enter();

            tracing::debug!("blob {blob_digest}");

            let promise = self.js_load_blob(&WasmDigest::from(blob_digest).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsStorageError::LoadBlobError)?;

            let maybe_blob = if js_value.is_null() || js_value.is_undefined() {
                None
            } else if js_value.is_instance_of::<Uint8Array>() {
                Some(Blob::from(Uint8Array::new(&js_value).to_vec()))
            } else {
                return Err(JsStorageError::NotBytes);
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

    /// The `JsValue` could not be converted into bytes.
    #[error("Value was not bytes")]
    NotBytes,

    /// The `JsValue` could not be converted into an array of `WasmLooseCommit`.
    #[error("Value was not an array of LooseCommits: {0:?}")]
    NotLooseCommitArray(JsValue),

    /// The `JsValue` could not be converted into an array of `WasmFragment`.
    #[error("Value was not an array of Fragments: {0:?}")]
    NotFragmentsArray(WasmConvertJsValueToFragmentArrayError),

    /// The `JsValue` could not be converted into a `WasmDigest`.
    #[error("Value was not a Digest")]
    NotDigest,
}

impl From<JsStorageError> for JsValue {
    fn from(err: JsStorageError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("StorageError");
        js_err.into()
    }
}
