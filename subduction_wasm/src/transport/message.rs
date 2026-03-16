//! Wasm wrapper for [`SyncMessage`].

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use js_sys::Uint8Array;
use sedimentree_core::{blob::Blob, codec::error::DecodeError};
use subduction_core::connection::message::SyncMessage;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use sedimentree_wasm::{
    digest::WasmDigest, fragment::WasmFragment, loose_commit::WasmLooseCommit,
    sedimentree_id::WasmSedimentreeId,
};

use super::{WasmBatchSyncRequest, WasmBatchSyncResponse};

/// Wasm wrapper for [`SyncMessage`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = SyncMessage)]
pub struct WasmMessage(pub(crate) SyncMessage);

#[wasm_refgen(js_ref = JsMessage)]
#[wasm_bindgen(js_class = SyncMessage)]
impl WasmMessage {
    /// Serialize the message to bytes.
    #[must_use]
    #[wasm_bindgen(js_name = toBytes)]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.encode()
    }

    /// Deserialize a message from bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`JsMessageDeserializationError`] if deserialization fails.
    #[wasm_bindgen(js_name = fromBytes)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, JsMessageDeserializationError> {
        let msg = SyncMessage::try_decode(bytes).map_err(JsMessageDeserializationError)?;
        Ok(msg.into())
    }

    /// Create a [`SyncMessage::BlobsRequest`] message.
    #[wasm_bindgen(js_name = blobsRequest)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn blobs_request(id: &WasmSedimentreeId, digests: Vec<WasmDigest>) -> Self {
        SyncMessage::BlobsRequest {
            id: id.clone().into(),
            digests: digests.into_iter().map(Into::into).collect(),
        }
        .into()
    }

    /// Create a [`SyncMessage::BlobsResponse`] message.
    #[wasm_bindgen(js_name = blobsResponse)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn blobs_response(id: &WasmSedimentreeId, blobs: Vec<Uint8Array>) -> Self {
        SyncMessage::BlobsResponse {
            id: id.clone().into(),
            blobs: blobs.iter().map(|b| Blob::from(b.to_vec())).collect(),
        }
        .into()
    }

    /// Create a [`SyncMessage::BatchSyncRequest`] message.
    #[wasm_bindgen(js_name = batchSyncRequest)]
    #[must_use]
    pub fn batch_sync_request(request: &WasmBatchSyncRequest) -> Self {
        SyncMessage::BatchSyncRequest(request.clone().into()).into()
    }

    /// Create a [`SyncMessage::BatchSyncResponse`] message.
    #[wasm_bindgen(js_name = batchSyncResponse)]
    #[must_use]
    pub fn batch_sync_response(response: &WasmBatchSyncResponse) -> Self {
        SyncMessage::BatchSyncResponse(response.clone().into()).into()
    }

    /// The message variant name.
    #[wasm_bindgen(getter, js_name = type)]
    #[must_use]
    pub fn msg_type(&self) -> String {
        self.0.variant_name().into()
    }

    /// The [`SedimentreeId`] associated with this message, if any.
    #[wasm_bindgen(getter, js_name = sedimentreeId)]
    #[must_use]
    pub fn sedimentree_id(&self) -> Option<WasmSedimentreeId> {
        self.0.sedimentree_id().map(WasmSedimentreeId::from)
    }

    /// The [`LooseCommit`] for a [`SyncMessage::LooseCommit`], if applicable.
    ///
    /// Decodes the signed payload to extract the underlying commit.
    #[wasm_bindgen(getter, js_name = commit)]
    #[must_use]
    pub fn commit(&self) -> Option<WasmLooseCommit> {
        match &self.0 {
            SyncMessage::LooseCommit { commit, .. } => commit
                .try_decode_trusted_payload()
                .ok()
                .map(WasmLooseCommit::from),
            SyncMessage::Fragment { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The [`Fragment`] for a [`SyncMessage::Fragment`], if applicable.
    ///
    /// Decodes the signed payload to extract the underlying fragment.
    #[wasm_bindgen(getter, js_name = fragment)]
    #[must_use]
    pub fn fragment(&self) -> Option<WasmFragment> {
        match &self.0 {
            SyncMessage::Fragment { fragment, .. } => fragment
                .try_decode_trusted_payload()
                .ok()
                .map(WasmFragment::from),
            SyncMessage::LooseCommit { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The [`Blob`] for commit or fragment messages, if applicable.
    #[wasm_bindgen(getter, js_name = blob)]
    #[must_use]
    pub fn blob(&self) -> Option<Uint8Array> {
        match &self.0 {
            SyncMessage::LooseCommit { blob, .. } | SyncMessage::Fragment { blob, .. } => {
                Some(Uint8Array::from(blob.as_slice()))
            }
            SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The requested [`Digest`]s for a [`SyncMessage::BlobsRequest`], if applicable.
    #[wasm_bindgen(getter, js_name = digests)]
    #[must_use]
    pub fn digests(&self) -> Option<Vec<WasmDigest>> {
        match &self.0 {
            SyncMessage::BlobsRequest { digests, .. } => {
                Some(digests.iter().copied().map(WasmDigest::from).collect())
            }
            SyncMessage::LooseCommit { .. }
            | SyncMessage::Fragment { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The [`Blob`]s for a [`SyncMessage::BlobsResponse`], if applicable.
    #[wasm_bindgen(getter, js_name = blobs)]
    #[must_use]
    pub fn blobs(&self) -> Option<Vec<Uint8Array>> {
        match &self.0 {
            SyncMessage::BlobsResponse { blobs, .. } => Some(
                blobs
                    .iter()
                    .map(|b| Uint8Array::from(b.as_slice()))
                    .collect(),
            ),
            SyncMessage::LooseCommit { .. }
            | SyncMessage::Fragment { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The [`BatchSyncRequest`] for a [`SyncMessage::BatchSyncRequest`], if applicable.
    #[wasm_bindgen(getter, js_name = request)]
    #[must_use]
    pub fn request(&self) -> Option<WasmBatchSyncRequest> {
        match &self.0 {
            SyncMessage::BatchSyncRequest(req) => Some(req.clone().into()),
            SyncMessage::LooseCommit { .. }
            | SyncMessage::Fragment { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncResponse(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    /// The [`BatchSyncResponse`] for a [`SyncMessage::BatchSyncResponse`], if applicable.
    #[wasm_bindgen(getter, js_name = response)]
    #[must_use]
    pub fn response(&self) -> Option<WasmBatchSyncResponse> {
        match &self.0 {
            SyncMessage::BatchSyncResponse(resp) => Some(resp.clone().into()),
            SyncMessage::LooseCommit { .. }
            | SyncMessage::Fragment { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }
}

impl From<SyncMessage> for WasmMessage {
    fn from(msg: SyncMessage) -> Self {
        Self(msg)
    }
}

impl From<WasmMessage> for SyncMessage {
    fn from(msg: WasmMessage) -> Self {
        msg.0
    }
}

/// An error indicating a failure to deserialize a [`SyncMessage`].
#[derive(Debug, Clone, Copy, Error)]
#[error("failed to deserialize SyncMessage: {0}")]
pub struct JsMessageDeserializationError(DecodeError);

impl From<JsMessageDeserializationError> for JsValue {
    fn from(err: JsMessageDeserializationError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("MessageDeserializationError");
        js_err.into()
    }
}
