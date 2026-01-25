//! Persistent storage.

#[cfg(feature = "idb")]
pub mod idb;
pub mod memory;

use alloc::{string::ToString, vec::Vec};
use sedimentree_core::collections::Set;

use futures::{FutureExt, future::LocalBoxFuture};
use futures_kind::Local;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::{Blob, Digest},
    fragment::Fragment,
    id::{BadSedimentreeId, SedimentreeId},
    loose_commit::LooseCommit,
    storage::{BatchResult, Storage},
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

    saveLooseCommit(sedimentreeId: SedimentreeId, commit: LooseCommit): Promise<void>;
    saveFragment(sedimentreeId: SedimentreeId, fragment: Fragment): Promise<void>;
    saveBlob(data: Uint8Array): Promise<Digest>;

    loadLooseCommits(sedimentreeId: SedimentreeId): Promise<LooseCommit[]>;
    loadFragments(sedimentreeId: SedimentreeId): Promise<Fragment[]>;
    loadBlob(digest: Digest): Promise<Uint8Array | null>;

    deleteLooseCommits(sedimentreeId: SedimentreeId): Promise<void>;
    deleteFragments(sedimentreeId: SedimentreeId): Promise<void>;
    deleteBlob(digest: Digest): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed storage backend interface.
    #[wasm_bindgen(js_name = Storage, typescript_type = "SedimentreeStorage")]
    pub type JsSedimentreeStorage;

    /// Insert a sedimentree ID to know which sedimentrees have data stored.
    #[wasm_bindgen(method, js_name = saveSedimentreeId)]
    fn js_save_sedimentree_id(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    /// Delete a sedimentree ID and all associated data.
    #[wasm_bindgen(method, js_name = deleteSedimentreeId)]
    fn js_delete_sedimentree_id(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    /// Get all sedimentree IDs that have loose commits stored.
    #[wasm_bindgen(method, js_name = loadAllSedimentreeIds)]
    fn js_load_all_sedimentree_ids(this: &JsSedimentreeStorage) -> Promise;

    /// Save a blob to storage.
    #[wasm_bindgen(method, js_name = saveBlob)]
    fn js_save_blob(this: &JsSedimentreeStorage, blob: &[u8]) -> Promise;

    /// Save a loose commit to storage.
    #[wasm_bindgen(method, js_name = saveLooseCommit)]
    fn js_save_loose_commit(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        loose_commit: &JsLooseCommit,
    ) -> Promise;

    /// Save a fragment to storage.
    #[wasm_bindgen(method, js_name = saveFragment)]
    fn js_save_fragment(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
        fragment: &JsFragment,
    ) -> Promise;

    /// Load all loose commits from storage.
    #[wasm_bindgen(method, js_name = loadLooseCommits)]
    fn js_load_loose_commits(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    /// Load all fragments from storage.
    #[wasm_bindgen(method, js_name = loadFragments)]
    fn js_load_fragments(this: &JsSedimentreeStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    /// Load a blob from storage.
    #[wasm_bindgen(method, js_name = loadBlob)]
    fn js_load_blob(this: &JsSedimentreeStorage, blob_digest: &JsDigest) -> Promise;

    /// Delete all loose commits from storage.
    #[wasm_bindgen(method, js_name = deleteLooseCommits)]
    fn js_delete_loose_commits(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    /// Delete all fragments from storage.
    #[wasm_bindgen(method, js_name = deleteFragments)]
    fn js_delete_fragments(
        this: &JsSedimentreeStorage,
        sedimentree_id: &JsSedimentreeId,
    ) -> Promise;

    /// Delete a blob from storage.
    #[wasm_bindgen(method, js_name = deleteBlob)]
    fn js_delete_blob(this: &JsSedimentreeStorage, blob_digest: &JsDigest) -> Promise;
}

impl core::fmt::Debug for JsSedimentreeStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsSedimentreeStorage").finish()
    }
}

impl Storage<Local> for JsSedimentreeStorage {
    type Error = JsSedimentreeStorageError;

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_sedimentree_id");
            let _enter = span.enter();

            tracing::debug!("inserting sedimentree id {:?}", sedimentree_id);
            let js_promise =
                self.js_save_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::SaveLooseCommitError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::delete_sedimentree_id");
            let _enter = span.enter();

            tracing::debug!("deleting sedimentree id {:?}", sedimentree_id);
            let js_promise =
                self.js_delete_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::SaveLooseCommitError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::load_all_sedimentree_ids");
            let _enter = span.enter();

            let js_promise = self.js_load_all_sedimentree_ids();
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadLooseCommitsError)?;
            let xs: Vec<WasmSedimentreeId> = WasmSedimentreeIdsArray::try_from(&js_value)?.0;
            let mut sedimentree_ids_set = Set::new();
            for wasm_id in xs {
                sedimentree_ids_set.insert(wasm_id.into());
            }
            Ok(sedimentree_ids_set)
        }
        .boxed_local()
    }

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        loose_commit: LooseCommit,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_loose_commit");
            let _enter = span.enter();

            tracing::debug!("saving loose commit {:?}", loose_commit.digest());
            let wasm_loose_commit: WasmLooseCommit = loose_commit.into();
            let js_promise = self.js_save_loose_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &wasm_loose_commit.into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsSedimentreeStorageError::SaveLooseCommitError)?;
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
            let span = tracing::debug_span!("JsSedimentreeStorage::save_fragment");
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
                .map_err(JsSedimentreeStorageError::SaveFragmentError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_blob(&self, blob: Blob) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_blob");
            let _enter = span.enter();

            let promise = self.js_save_blob(blob.as_slice());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::SaveBlobError)?;

            let js_digest: JsDigest = JsCast::unchecked_into(js_value); // What could possibly go wrong?
            let wasm_digest = WasmDigest::from(&js_digest);
            tracing::debug!("saved blob {:?}", wasm_digest);
            Ok(wasm_digest.into())
        }
        .boxed_local()
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::load_loose_commits");
            let _enter = span.enter();

            let promise =
                self.js_load_loose_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadLooseCommitsError)?;
            let js_loose_commits = WasmLooseCommitsArray::try_from(&js_value)
                .map_err(JsSedimentreeStorageError::NotLooseCommitArray)?;
            Ok(js_loose_commits.0.into_iter().map(Into::into).collect())
        }
        .boxed_local()
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::load_fragments");
            let _enter = span.enter();

            let promise = self.js_load_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadFragmentsError)?;
            let js_fragments = WasmFragmentsArray::try_from(&js_value)
                .map_err(JsSedimentreeStorageError::NotFragmentsArray)?;
            Ok(js_fragments.0.into_iter().map(Into::into).collect())
        }
        .boxed_local()
    }

    fn load_blob(
        &self,
        blob_digest: Digest,
    ) -> LocalBoxFuture<'_, Result<Option<Blob>, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::load_blob");
            let _enter = span.enter();

            tracing::debug!("blob {blob_digest}");

            let promise = self.js_load_blob(&WasmDigest::from(blob_digest).into());
            let js_value = JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadBlobError)?;

            let maybe_blob = if js_value.is_null() || js_value.is_undefined() {
                None
            } else if js_value.is_instance_of::<Uint8Array>() {
                Some(Blob::from(Uint8Array::new(&js_value).to_vec()))
            } else {
                return Err(JsSedimentreeStorageError::NotBytes);
            };

            Ok(maybe_blob)
        }
        .boxed_local()
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::delete_loose_commits");
            let _enter = span.enter();

            let promise =
                self.js_delete_loose_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadLooseCommitsError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::delete_fragments");
            let _enter = span.enter();

            let promise = self.js_delete_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadFragmentsError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn delete_blob(&self, blob_digest: Digest) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::delete_blob");
            let _enter = span.enter();

            let promise = self.js_delete_blob(&WasmDigest::from(blob_digest).into());
            JsFuture::from(promise)
                .await
                .map_err(JsSedimentreeStorageError::LoadBlobError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn save_commit_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_commit_with_blob");
            let _enter = span.enter();

            let digest = self.save_blob(blob).await?;
            self.save_loose_commit(sedimentree_id, commit).await?;
            Ok(digest)
        }
        .boxed_local()
    }

    fn save_fragment_with_blob(
        &self,
        sedimentree_id: SedimentreeId,
        fragment: Fragment,
        blob: Blob,
    ) -> LocalBoxFuture<'_, Result<Digest, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_fragment_with_blob");
            let _enter = span.enter();

            let digest = self.save_blob(blob).await?;
            self.save_fragment(sedimentree_id, fragment).await?;
            Ok(digest)
        }
        .boxed_local()
    }

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> LocalBoxFuture<'_, Result<BatchResult, Self::Error>> {
        async move {
            let span = tracing::debug_span!("JsSedimentreeStorage::save_batch");
            let _enter = span.enter();

            let mut blob_digests = Vec::with_capacity(commits.len() + fragments.len());

            self.save_sedimentree_id(sedimentree_id).await?;

            for (commit, blob) in commits {
                let digest = self.save_blob(blob).await?;
                self.save_loose_commit(sedimentree_id, commit).await?;
                blob_digests.push(digest);
            }

            for (fragment, blob) in fragments {
                let digest = self.save_blob(blob).await?;
                self.save_fragment(sedimentree_id, fragment).await?;
                blob_digests.push(digest);
            }

            Ok(BatchResult { blob_digests })
        }
        .boxed_local()
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
    NotFragmentsArray(#[from] WasmConvertJsValueToFragmentArrayError),

    /// The `JsValue` could not be converted into an array of `SedimentreeId`s.
    #[error("Value was not an array of SedimentreeIds: {0:?}")]
    NotSedimentreeIdArray(#[from] WasmConvertJsValueToSedimentreeIdArrayError),

    /// The `JsValue` could not be converted into a `WasmDigest`.
    #[error("Value was not a Digest: {0:?}")]
    NotDigest(JsValue),
}

impl From<JsSedimentreeStorageError> for JsValue {
    fn from(err: JsSedimentreeStorageError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("SedimentreeStorageError");
        js_err.into()
    }
}
