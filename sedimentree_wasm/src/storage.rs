//! Persistent storage.

#[cfg(feature = "idb")]
pub mod idb;

use alloc::vec::Vec;
use sedimentree_core::collections::Set;

use future_form::{FutureForm, Local};
use futures::future::LocalBoxFuture;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::{
    blob::Blob,
    codec::{decode::Decode, encode::Encode, error::DecodeError},
    crypto::digest::Digest,
    fragment::Fragment,
    id::{BadSedimentreeId, SedimentreeId},
    loose_commit::LooseCommit,
};
use subduction_core::storage::traits::Storage;
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    digest::{JsDigest, WasmDigest},
    fragment::{JsFragmentWithBlob, WasmFragmentWithBlob},
    loose_commit::{JsCommitWithBlob, WasmCommitWithBlob},
    sedimentree_id::{
        JsSedimentreeId, WasmConvertJsValueToSedimentreeIdArrayError, WasmSedimentreeId,
        WasmSedimentreeIdsArray,
    },
    signed::{JsSignedFragment, JsSignedLooseCommit, WasmSignedFragment, WasmSignedLooseCommit},
};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface SedimentreeStorage {
    saveSedimentreeId(sedimentreeId: SedimentreeId): Promise<void>;
    deleteSedimentreeId(sedimentreeId: SedimentreeId): Promise<void>;
    loadAllSedimentreeIds(): Promise<SedimentreeId[]>;

    // Compound storage for commits (signed data + blob stored together)
    saveCommit(sedimentreeId: SedimentreeId, digest: Digest, signedCommit: SignedLooseCommit, blob: Uint8Array): Promise<void>;
    loadCommit(sedimentreeId: SedimentreeId, digest: Digest): Promise<CommitWithBlob | null>;
    listCommitDigests(sedimentreeId: SedimentreeId): Promise<Digest[]>;
    loadAllCommits(sedimentreeId: SedimentreeId): Promise<CommitWithBlob[]>;
    deleteCommit(sedimentreeId: SedimentreeId, digest: Digest): Promise<void>;
    deleteAllCommits(sedimentreeId: SedimentreeId): Promise<void>;

    // Compound storage for fragments (signed data + blob stored together)
    saveFragment(sedimentreeId: SedimentreeId, digest: Digest, signedFragment: SignedFragment, blob: Uint8Array): Promise<void>;
    loadFragment(sedimentreeId: SedimentreeId, digest: Digest): Promise<FragmentWithBlob | null>;
    listFragmentDigests(sedimentreeId: SedimentreeId): Promise<Digest[]>;
    loadAllFragments(sedimentreeId: SedimentreeId): Promise<FragmentWithBlob[]>;
    deleteFragment(sedimentreeId: SedimentreeId, digest: Digest): Promise<void>;
    deleteAllFragments(sedimentreeId: SedimentreeId): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed storage backend interface.
    #[wasm_bindgen(js_name = Storage, typescript_type = "SedimentreeStorage")]
    pub type JsStorage;

    // ==================== Sedimentree IDs ====================

    #[wasm_bindgen(method, js_name = saveSedimentreeId)]
    fn js_save_sedimentree_id(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = deleteSedimentreeId)]
    fn js_delete_sedimentree_id(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllSedimentreeIds)]
    fn js_load_all_sedimentree_ids(this: &JsStorage) -> Promise;

    // ==================== Commits (compound with blob) ====================

    #[wasm_bindgen(method, js_name = saveCommit)]
    fn js_save_commit(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
        signed_commit: &JsSignedLooseCommit,
        blob: &Uint8Array,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadCommit)]
    fn js_load_commit(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = listCommitDigests)]
    fn js_list_commit_digests(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllCommits)]
    fn js_load_all_commits(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = deleteCommit)]
    fn js_delete_commit(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteAllCommits)]
    fn js_delete_all_commits(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    // ==================== Fragments (compound with blob) ====================

    #[wasm_bindgen(method, js_name = saveFragment)]
    fn js_save_fragment(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
        signed_fragment: &JsSignedFragment,
        blob: &Uint8Array,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = loadFragment)]
    fn js_load_fragment(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = listFragmentDigests)]
    fn js_list_fragment_digests(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = loadAllFragments)]
    fn js_load_all_fragments(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;

    #[wasm_bindgen(method, js_name = deleteFragment)]
    fn js_delete_fragment(
        this: &JsStorage,
        sedimentree_id: &JsSedimentreeId,
        digest: &JsDigest,
    ) -> Promise;

    #[wasm_bindgen(method, js_name = deleteAllFragments)]
    fn js_delete_all_fragments(this: &JsStorage, sedimentree_id: &JsSedimentreeId) -> Promise;
}

impl core::fmt::Debug for JsStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsStorage").finish()
    }
}

/// Parse a JS array of digests.
fn parse_digest_array<T: Encode + Decode>(js_value: &JsValue) -> Set<Digest<T>> {
    let array = js_sys::Array::from(js_value);
    let mut result = Set::new();

    for i in 0..array.length() {
        let item = array.get(i);
        let js_digest: JsDigest = JsCast::unchecked_into(item);
        let digest: Digest<T> = WasmDigest::from(&js_digest).into();
        result.insert(digest);
    }

    result
}

impl Storage<Local> for JsStorage {
    type Error = JsStorageError;

    // ==================== Sedimentree IDs ====================

    fn save_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::save_sedimentree_id");
            let js_promise =
                self.js_save_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_sedimentree_id(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::delete_sedimentree_id");
            let js_promise =
                self.js_delete_sedimentree_id(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!("JsStorage::load_all_sedimentree_ids");
            let js_promise = self.js_load_all_sedimentree_ids();
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            let xs: Vec<WasmSedimentreeId> = WasmSedimentreeIdsArray::try_from(&js_value)?.0;
            let mut ids = Set::new();
            for wasm_id in xs {
                ids.insert(wasm_id.into());
            }
            Ok(ids)
        })
    }

    // ==================== Commits (compound with blob) ====================

    fn save_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let digest = Digest::hash(verified.payload());
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::save_loose_commit");

            let signed: WasmSignedLooseCommit = verified.signed().clone().into();
            let blob = Uint8Array::from(verified.blob().contents().as_slice());

            let js_promise = self.js_save_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
                &signed.into(),
                &blob,
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn load_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::load_loose_commit");
            let js_promise = self.js_load_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;

            if js_value.is_null() || js_value.is_undefined() {
                return Ok(None);
            }

            let commit_with_blob: JsCommitWithBlob = js_value.unchecked_into();
            let wasm_commit: WasmCommitWithBlob = (&commit_with_blob).into();
            let signed: Signed<LooseCommit> = wasm_commit.signed().into();
            let blob = Blob::new(wasm_commit.blob().to_vec());

            // Reconstruct from trusted storage without re-verification
            Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
        })
    }

    fn list_commit_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<Digest<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::list_commit_digests");
            let js_promise =
                self.js_list_commit_digests(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(parse_digest_array(&js_value))
        })
    }

    fn load_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::load_loose_commits");
            let js_promise =
                self.js_load_all_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;

            let array = js_sys::Array::from(&js_value);
            let mut result = Vec::with_capacity(array.length() as usize);

            for i in 0..array.length() {
                let item = array.get(i);
                let commit_with_blob: JsCommitWithBlob = item.unchecked_into();
                let wasm_commit: WasmCommitWithBlob = (&commit_with_blob).into();
                let signed: Signed<LooseCommit> = wasm_commit.signed().into();
                let blob = Blob::new(wasm_commit.blob().to_vec());
                result.push(VerifiedMeta::try_from_trusted(signed, blob)?);
            }

            Ok(result)
        })
    }

    fn delete_loose_commit(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::delete_loose_commit");
            let js_promise = self.js_delete_commit(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_loose_commits(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::delete_loose_commits");
            let js_promise =
                self.js_delete_all_commits(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    // ==================== Fragments (compound with blob) ====================

    fn save_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let digest = Digest::hash(verified.payload());
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::save_fragment");

            let signed: WasmSignedFragment = verified.signed().clone().into();
            let blob = Uint8Array::from(verified.blob().contents().as_slice());

            let js_promise = self.js_save_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
                &signed.into(),
                &blob,
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn load_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::load_fragment");
            let js_promise = self.js_load_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;

            if js_value.is_null() || js_value.is_undefined() {
                return Ok(None);
            }

            let fragment_with_blob: JsFragmentWithBlob = js_value.unchecked_into();
            let wasm_fragment: WasmFragmentWithBlob = (&fragment_with_blob).into();
            let signed: Signed<Fragment> = wasm_fragment.signed().into();
            let blob = Blob::new(wasm_fragment.blob().to_vec());

            // Reconstruct from trusted storage without re-verification
            Ok(Some(VerifiedMeta::try_from_trusted(signed, blob)?))
        })
    }

    fn list_fragment_digests(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<Digest<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::list_fragment_digests");
            let js_promise =
                self.js_list_fragment_digests(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(parse_digest_array(&js_value))
        })
    }

    fn load_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::load_fragments");
            let js_promise =
                self.js_load_all_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            let js_value = JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;

            let array = js_sys::Array::from(&js_value);
            let mut result = Vec::with_capacity(array.length() as usize);

            for i in 0..array.length() {
                let item = array.get(i);
                let fragment_with_blob: JsFragmentWithBlob = item.unchecked_into();
                let wasm_fragment: WasmFragmentWithBlob = (&fragment_with_blob).into();
                let signed: Signed<Fragment> = wasm_fragment.signed().into();
                let blob = Blob::new(wasm_fragment.blob().to_vec());
                result.push(VerifiedMeta::try_from_trusted(signed, blob)?);
            }

            Ok(result)
        })
    }

    fn delete_fragment(
        &self,
        sedimentree_id: SedimentreeId,
        digest: Digest<Fragment>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, ?digest, "JsStorage::delete_fragment");
            let js_promise = self.js_delete_fragment(
                &WasmSedimentreeId::from(sedimentree_id).into(),
                &WasmDigest::from(digest).into(),
            );
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    fn delete_fragments(
        &self,
        sedimentree_id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            tracing::debug!(?sedimentree_id, "JsStorage::delete_fragments");
            let js_promise =
                self.js_delete_all_fragments(&WasmSedimentreeId::from(sedimentree_id).into());
            JsFuture::from(js_promise)
                .await
                .map_err(JsStorageError::JsError)?;
            Ok(())
        })
    }

    // ==================== Batch Operations ====================

    fn save_batch(
        &self,
        sedimentree_id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> LocalBoxFuture<'_, Result<usize, Self::Error>> {
        Local::from_future(async move {
            let num_commits = commits.len();
            let num_fragments = fragments.len();
            tracing::debug!(
                ?sedimentree_id,
                num_commits,
                num_fragments,
                "JsStorage::save_batch"
            );

            self.save_sedimentree_id(sedimentree_id).await?;

            for verified in commits {
                Storage::<Local>::save_loose_commit(self, sedimentree_id, verified).await?;
            }

            for verified in fragments {
                Storage::<Local>::save_fragment(self, sedimentree_id, verified).await?;
            }

            Ok(num_commits + num_fragments)
        })
    }
}

/// Errors that can occur when using `JsStorage`.
#[derive(Error, Debug)]
pub enum JsStorageError {
    /// A sedimentree ID was not a string.
    #[error("SedimentreeId was not a string: {0:?}")]
    SedimentreeIdNotAString(JsValue),

    /// A sedimentree ID string was invalid.
    #[error(transparent)]
    BadSedimentreeId(#[from] BadSedimentreeId),

    /// A JavaScript error occurred.
    #[error("JavaScript error: {0:?}")]
    JsError(JsValue),

    /// Decoding failed.
    #[error(transparent)]
    Decode(#[from] DecodeError),

    /// Failed to compute digest from signed payload.
    #[error("Failed to compute digest from signed payload")]
    DigestComputationFailed,

    /// Reflect error accessing JS object property.
    #[error("Reflect error: {0:?}")]
    ReflectError(JsValue),

    /// The `JsValue` could not be converted into bytes.
    #[error("Value was not bytes")]
    NotBytes,

    /// The `JsValue` could not be converted into an array of `SedimentreeId`s.
    #[error("Value was not an array of SedimentreeIds: {0:?}")]
    NotSedimentreeIdArray(#[from] WasmConvertJsValueToSedimentreeIdArrayError),
}

impl From<JsStorageError> for JsValue {
    fn from(err: JsStorageError) -> Self {
        let js_err = js_sys::Error::new(&alloc::string::ToString::to_string(&err));
        js_err.set_name("SedimentreeStorageError");
        js_err.into()
    }
}

pub mod memory;
