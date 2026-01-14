//! In-memory storage adapter.

use crate::{
    digest::WasmDigest, fragment::WasmFragment, loose_commit::WasmLooseCommit,
    sedimentree_id::WasmSedimentreeId,
};
use alloc::vec::Vec;
use sedimentree_core::{
    future::Local,
    storage::{MemoryStorage, Storage},
};
use wasm_bindgen::prelude::*;

/// In-memory storage adapter
#[derive(Debug, Clone, Default)]
#[wasm_bindgen(js_name = MemoryStorage)]
pub struct WasmMemoryStorage(MemoryStorage);

#[wasm_bindgen(js_class = MemoryStorage)]
impl WasmMemoryStorage {
    /// Create a fresh, blank [`WasmMemoryStorage`].
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(MemoryStorage::new())
    }

    /// Insert a Sedimentree ID into storage.
    #[wasm_bindgen(js_name = saveSedimentreeId)]
    pub async fn wasm_save_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) {
        let result = <MemoryStorage as Storage<Local>>::save_sedimentree_id(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Delete a Sedimentree ID from storage.
    #[wasm_bindgen(js_name = deleteSedimentreeId)]
    pub async fn wasm_delete_sedimentree_id(&self, sedimentree_id: &WasmSedimentreeId) {
        let result = <MemoryStorage as Storage<Local>>::delete_sedimentree_id(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Load all Sedimentree IDs from storage.
    #[wasm_bindgen(js_name = loadAllSedimentreeIds)]
    pub async fn wasm_load_all_sedimentree_ids(&self) -> Vec<WasmSedimentreeId> {
        let result = <MemoryStorage as Storage<Local>>::load_all_sedimentree_ids(&self.0).await;

        match result {
            Err(e) => match e {},
            Ok(hashmap) => hashmap.into_iter().map(Into::into).collect(),
        }
    }

    /// Save a loose commit to storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveLooseCommitError`] if the loose commit could not be saved.
    #[wasm_bindgen(js_name = saveLooseCommit)]
    pub async fn wasm_save_loose_commit(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        loose_commit: &WasmLooseCommit,
    ) {
        let result = <MemoryStorage as Storage<Local>>::save_loose_commit(
            &self.0,
            sedimentree_id.clone().into(),
            loose_commit.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Load all loose commits from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadLooseCommitsError`] if loose commits could not be loaded.
    #[wasm_bindgen(js_name = loadLooseCommits)]
    pub async fn wasm_load_loose_commits(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Vec<WasmLooseCommit> {
        let result = <MemoryStorage as Storage<Local>>::load_loose_commits(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Err(e) => match e {},
            Ok(commits) => commits.into_iter().map(Into::into).collect(),
        }
    }

    /// Delete all loose commits for a given Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteLooseCommitsError`] if loose commits could not be deleted.
    #[wasm_bindgen(js_name = deleteLooseCommits)]
    pub async fn wasm_delete_loose_commits(&self, sedimentree_id: &WasmSedimentreeId) {
        let result = <MemoryStorage as Storage<Local>>::delete_loose_commits(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Save a fragment to storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmSaveFragmentError`] if the fragment could not be saved.
    #[wasm_bindgen(js_name = saveFragment)]
    pub async fn wasm_save_fragment(
        &self,
        sedimentree_id: &WasmSedimentreeId,
        fragment: &WasmFragment,
    ) {
        let result = <MemoryStorage as Storage<Local>>::save_fragment(
            &self.0,
            sedimentree_id.clone().into(),
            fragment.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Load all fragments from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmLoadFragmentsError`] if fragments could not be loaded.
    #[wasm_bindgen(js_name = loadFragments)]
    pub async fn wasm_load_fragments(
        &self,
        sedimentree_id: &WasmSedimentreeId,
    ) -> Vec<WasmFragment> {
        let result = <MemoryStorage as Storage<Local>>::load_fragments(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Err(e) => match e {},
            Ok(frags) => frags.into_iter().map(Into::into).collect(),
        }
    }

    /// Delete all fragments for a given Sedimentree ID from storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteFragmentsError`] if loose commits could not be deleted.
    #[wasm_bindgen( js_name = deleteFragments)]
    pub async fn wasm_delete_fragments(&self, sedimentree_id: &WasmSedimentreeId) {
        let result = <MemoryStorage as Storage<Local>>::delete_fragments(
            &self.0,
            sedimentree_id.clone().into(),
        )
        .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }

    /// Save a blob to the database, returning its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the JS transaction could not be opened,
    /// or if the blob could not be saved.
    #[wasm_bindgen(js_name = saveBlob)]
    pub async fn wasm_save_blob(&self, bytes: &[u8]) -> WasmDigest {
        let result =
            <MemoryStorage as Storage<Local>>::save_blob(&self.0, bytes.to_vec().into()).await;

        match result {
            Err(e) => match e {},
            Ok(digest) => digest.into(),
        }
    }

    /// Load a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a `JsValue` if the blob could not be loaded.
    #[wasm_bindgen(js_name = loadBlob)]
    pub async fn wasm_load_blob(&self, wasm_digest: WasmDigest) -> Option<Vec<u8>> {
        let result =
            <MemoryStorage as Storage<Local>>::load_blob(&self.0, wasm_digest.into()).await;

        match result {
            Err(e) => match e {},
            Ok(None) => None,
            Ok(Some(blob)) => Some(blob.into()),
        }
    }

    /// Delete a blob from the database by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDeleteBlobError`] if the blob could not be deleted.
    #[wasm_bindgen(js_name = deleteBlob)]
    pub async fn wasm_delete_blob(&self, wasm_digest: WasmDigest) {
        let result =
            <MemoryStorage as Storage<Local>>::delete_blob(&self.0, wasm_digest.clone().into())
                .await;

        match result {
            Ok(()) => (),
            Err(e) => match e {},
        }
    }
}
