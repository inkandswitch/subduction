//! Structural JavaScript storage interface. Identifiers are hex strings and
//! records contain signed envelope bytes plus blob bytes, never Wasm wrappers.

use crate::pojo as p;
use alloc::{string::ToString, vec::Vec};
use future_form::{FutureForm, Local};
use futures::future::LocalBoxFuture;
use js_sys::{Array, Function, Promise, Uint8Array};
use sedimentree_core::{
    blob::{Blob, has_meta::HasBlobMeta},
    codec::{decode::DecodeFields, encode::EncodeFields, error::DecodeError, schema::Schema},
    collections::Set,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use subduction_core::storage::traits::{
    Storage, load_fragment_metas_via_full, load_loose_commit_metas_via_full,
};
use subduction_crypto::{
    signed::Signed,
    verified_meta::{BlobMismatch, VerifiedMeta},
    verified_signature::VerifiedSignature,
};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    /// Any object implementing `SedimentreeStorage`.
    #[wasm_bindgen(typescript_type = "SedimentreeStorage")]
    pub type JsStorage;
}

impl core::fmt::Debug for JsStorage {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsStorage").finish()
    }
}

impl JsStorage {
    async fn call(&self, name: &str, args: &[JsValue]) -> Result<JsValue, JsStorageError> {
        let method = p::field(self.as_ref(), name).map_err(JsStorageError::JsError)?;
        let method: Function = method
            .dyn_into()
            .map_err(|_| JsStorageError::JsError(p::invalid(name, "expected a function")))?;
        let args: Array = args.iter().collect();
        let value = method
            .apply(self.as_ref(), &args)
            .map_err(JsStorageError::JsError)?;
        JsFuture::from(Promise::resolve(&value))
            .await
            .map_err(JsStorageError::JsError)
    }
}

fn id_value(id: SedimentreeId) -> JsValue {
    p::hex(id.as_bytes()).into()
}
fn head_value(id: CommitId) -> JsValue {
    p::hex(id.as_bytes()).into()
}

/// Reload an envelope that was signed locally or verified on network admission
/// before being persisted. This follows the core `Storage` contract: storage is
/// part of the trusted computing base, not an untrusted network input. A custom
/// backend must preserve those envelopes; arbitrary externally supplied records
/// must be signature-verified before entering this path.
///
/// We therefore skip *re-verifying* signatures (`try_from_trusted`), but still
/// decode the envelope and check that the separately stored blob's digest and
/// length match its signed metadata (`VerifiedMeta::new`). This catches mismatched
/// record/blob pairs, not malicious edits to both metadata and blob or signatures.
fn load_record<T: HasBlobMeta + Schema + EncodeFields + DecodeFields>(
    value: &JsValue,
) -> Result<VerifiedMeta<T>, JsStorageError> {
    let parse = || -> Result<_, JsValue> {
        p::record(value, "stored")?;
        Ok((
            p::bytes(&p::field(value, "signed")?, "stored.signed")?,
            p::bytes(&p::field(value, "blob")?, "stored.blob")?,
        ))
    };
    let (signed, blob) = parse().map_err(JsStorageError::JsError)?;
    let signed = Signed::<T>::try_decode(&signed)?;
    let verified = VerifiedSignature::try_from_trusted(signed)?;
    Ok(VerifiedMeta::new(verified, Blob::new(blob))?)
}

fn load_records<T: HasBlobMeta + Schema + EncodeFields + DecodeFields>(
    value: &JsValue,
) -> Result<Vec<VerifiedMeta<T>>, JsStorageError> {
    p::array(value, "stored")
        .map_err(JsStorageError::JsError)?
        .iter()
        .map(load_record)
        .collect()
}

fn load_ids(value: &JsValue) -> Result<Set<CommitId>, JsStorageError> {
    p::ids(value, "ids")
        .map(|ids| ids.into_iter().collect())
        .map_err(JsStorageError::JsError)
}

impl Storage<Local> for JsStorage {
    type Error = JsStorageError;

    fn save_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("saveSedimentreeId", &[id_value(id)]).await?;
            Ok(())
        })
    }
    fn delete_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("deleteSedimentreeId", &[id_value(id)]).await?;
            Ok(())
        })
    }
    fn load_all_sedimentree_ids(
        &self,
    ) -> LocalBoxFuture<'_, Result<Set<SedimentreeId>, Self::Error>> {
        Local::from_future(async move {
            let value = self.call("loadAllSedimentreeIds", &[]).await?;
            p::array(&value, "ids")
                .map_err(JsStorageError::JsError)?
                .iter()
                .map(|v| p::tree(v, "ids[]").map_err(JsStorageError::JsError))
                .collect()
        })
    }
    fn contains_sedimentree_id(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<bool, Self::Error>> {
        Local::from_future(async move {
            let method = p::field(self.as_ref(), "containsSedimentreeId")
                .map_err(JsStorageError::JsError)?;
            if method.is_undefined() {
                return Ok(self.load_all_sedimentree_ids().await?.contains(&id));
            }
            self.call("containsSedimentreeId", &[id_value(id)])
                .await?
                .as_bool()
                .ok_or_else(|| {
                    JsStorageError::JsError(p::invalid(
                        "containsSedimentreeId",
                        "expected a boolean",
                    ))
                })
        })
    }
    fn save_loose_commit(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<LooseCommit>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call(
                "saveCommit",
                &[
                    id_value(id),
                    head_value(verified.payload().head()),
                    Uint8Array::from(verified.signed().as_bytes()).into(),
                    Uint8Array::from(verified.blob().as_slice()).into(),
                ],
            )
            .await?;
            // Register only after the item is durable.
            self.save_sedimentree_id(id).await
        })
    }
    fn list_commit_ids(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(
            async move { load_ids(&self.call("listCommitIds", &[id_value(id)]).await?) },
        )
    }
    fn load_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            load_records(&self.call("loadAllCommits", &[id_value(id)]).await?)
        })
    }
    fn load_loose_commit_metas(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<LooseCommit>, Self::Error>> {
        Local::from_future(load_loose_commit_metas_via_full(self, id))
    }
    fn load_loose_commit(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<LooseCommit>>, Self::Error>> {
        Local::from_future(async move {
            let value = self
                .call("loadCommit", &[id_value(id), head_value(head)])
                .await?;
            if value.is_null() || value.is_undefined() {
                Ok(None)
            } else {
                load_record(&value).map(Some)
            }
        })
    }
    fn delete_loose_commit(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("deleteCommit", &[id_value(id), head_value(head)])
                .await?;
            Ok(())
        })
    }
    fn delete_loose_commits(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("deleteAllCommits", &[id_value(id)]).await?;
            Ok(())
        })
    }
    fn save_fragment(
        &self,
        id: SedimentreeId,
        verified: VerifiedMeta<Fragment>,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call(
                "saveFragment",
                &[
                    id_value(id),
                    head_value(verified.payload().head()),
                    Uint8Array::from(verified.signed().as_bytes()).into(),
                    Uint8Array::from(verified.blob().as_slice()).into(),
                ],
            )
            .await?;
            self.save_sedimentree_id(id).await
        })
    }
    fn list_fragment_ids(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Set<CommitId>, Self::Error>> {
        Local::from_future(async move {
            load_ids(&self.call("listFragmentIds", &[id_value(id)]).await?)
        })
    }
    fn load_fragments(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            load_records(&self.call("loadAllFragments", &[id_value(id)]).await?)
        })
    }
    fn load_fragment_metas(
        &self,
        id: SedimentreeId,
    ) -> LocalBoxFuture<'_, Result<Vec<Fragment>, Self::Error>> {
        Local::from_future(load_fragment_metas_via_full(self, id))
    }
    fn load_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> LocalBoxFuture<'_, Result<Option<VerifiedMeta<Fragment>>, Self::Error>> {
        Local::from_future(async move {
            let value = self
                .call("loadFragment", &[id_value(id), head_value(head)])
                .await?;
            if value.is_null() || value.is_undefined() {
                Ok(None)
            } else {
                load_record(&value).map(Some)
            }
        })
    }
    fn delete_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
    ) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("deleteFragment", &[id_value(id), head_value(head)])
                .await?;
            Ok(())
        })
    }
    fn delete_fragments(&self, id: SedimentreeId) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.call("deleteAllFragments", &[id_value(id)]).await?;
            Ok(())
        })
    }
    fn save_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<VerifiedMeta<LooseCommit>>,
        fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> LocalBoxFuture<'_, Result<usize, Self::Error>> {
        Local::from_future(async move {
            let count = commits.len() + fragments.len();
            let commits = p::values(commits.iter().map(|v| {
                p::object(&[
                    ("commitId", head_value(v.payload().head())),
                    (
                        "signedCommit",
                        Uint8Array::from(v.signed().as_bytes()).into(),
                    ),
                    ("blob", Uint8Array::from(v.blob().as_slice()).into()),
                ])
            }));
            let fragments = p::values(fragments.iter().map(|v| {
                p::object(&[
                    ("fragmentHead", head_value(v.payload().head())),
                    (
                        "signedFragment",
                        Uint8Array::from(v.signed().as_bytes()).into(),
                    ),
                    ("blob", Uint8Array::from(v.blob().as_slice()).into()),
                ])
            }));
            // Preserve the storage contract and existing registration order.
            self.save_sedimentree_id(id).await?;
            let reported = self
                .call("saveBatchAll", &[id_value(id), commits, fragments])
                .await?;
            if !js_sys::Number::is_safe_integer(&reported)
                || reported.as_f64().is_none_or(|n| n < 0.0)
            {
                return Err(JsStorageError::JsError(p::invalid(
                    "saveBatchAll return value",
                    "expected a nonnegative safe integer",
                )));
            }
            // Preserve the core's existing input-count semantics, not a backend's
            // potentially different notion of newly inserted records.
            Ok(count)
        })
    }
}

/// Errors from a supplied storage implementation or its returned data.
#[derive(Debug, thiserror::Error)]
pub enum JsStorageError {
    /// A callback failed or returned an invalid value.
    #[error("JavaScript error: {0:?}")]
    JsError(JsValue),
    /// Signed envelope decoding failed.
    #[error(transparent)]
    Decode(#[from] DecodeError),
    /// The blob does not match its signed metadata.
    #[error(transparent)]
    BlobMismatch(#[from] BlobMismatch),
}

impl From<JsStorageError> for JsValue {
    fn from(error: JsStorageError) -> Self {
        let js = js_sys::Error::new(&error.to_string());
        js.set_name("SedimentreeStorageError");
        if let JsStorageError::JsError(cause) = error {
            let descriptor = js_sys::Object::new();
            js_sys::Reflect::set(&descriptor, &"value".into(), &cause).ok();
            js_sys::Reflect::set(&descriptor, &"writable".into(), &JsValue::TRUE).ok();
            js_sys::Reflect::set(&descriptor, &"configurable".into(), &JsValue::TRUE).ok();
            js_sys::Object::define_property(&js, &"cause".into(), &descriptor);
        }
        js.into()
    }
}
