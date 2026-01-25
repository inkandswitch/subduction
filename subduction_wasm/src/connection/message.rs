//! Wasm wrapper for [`Message`].

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use js_sys::Uint8Array;
use sedimentree_core::blob::Blob;
use subduction_core::connection::message::Message;
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_refgen::wasm_refgen;

use crate::{
    digest::WasmDigest, loose_commit::WasmLooseCommit,
    sedimentree_fragment::WasmFragment, sedimentree_id::WasmSedimentreeId,
};

use super::{WasmBatchSyncRequest, WasmBatchSyncResponse};

/// Wasm wrapper for [`Message`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Message)]
pub struct WasmMessage(pub(crate) Message);

#[wasm_refgen(js_ref = JsMessage)]
#[wasm_bindgen(js_class = Message)]
impl WasmMessage {
    /// Serialize the message to CBOR bytes.
    ///
    /// # Panics
    ///
    /// Panics if serialization of the Subduction protocol message fails.
    #[must_use]
    #[wasm_bindgen(js_name = toCborBytes)]
    pub fn to_cbor_bytes(&self) -> Vec<u8> {
        #[allow(clippy::expect_used)]
        minicbor::to_vec(&self.0).expect("serialization should be infallible")
    }

    /// Deserialize a message from CBOR bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`JsMessageDeserializationError`] if deserialization fails.
    #[wasm_bindgen(js_name = fromCborBytes)]
    pub fn from_cbor_bytes(bytes: &[u8]) -> Result<Self, JsMessageDeserializationError> {
        let msg: Message = minicbor::decode(bytes).map_err(JsMessageDeserializationError)?;
        Ok(msg.into())
    }

    /// Create a [`Message::LooseCommit`] message.
    #[wasm_bindgen(js_name = looseCommit)]
    #[must_use]
    pub fn loose_commit(
        id: &WasmSedimentreeId,
        commit: &WasmLooseCommit,
        blob: &Uint8Array,
    ) -> Self {
        Message::LooseCommit {
            id: id.clone().into(),
            commit: commit.clone().into(),
            blob: Blob::from(blob.to_vec()),
        }
        .into()
    }

    /// Create a [`Message::Fragment`] message.
    #[wasm_bindgen(js_name = newFragment)]
    #[must_use]
    pub fn new_fragment(
        id: &WasmSedimentreeId,
        fragment: &WasmFragment,
        blob: &Uint8Array,
    ) -> Self {
        Message::Fragment {
            id: id.clone().into(),
            fragment: fragment.clone().into(),
            blob: Blob::from(blob.to_vec()),
        }
        .into()
    }

    /// Create a [`Message::BlobsRequest`] message.
    #[wasm_bindgen(js_name = blobsRequest)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn blobs_request(digests: Vec<WasmDigest>) -> Self {
        Message::BlobsRequest(digests.into_iter().map(Into::into).collect()).into()
    }

    /// Create a [`Message::BlobsResponse`] message.
    #[wasm_bindgen(js_name = blobsResponse)]
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn blobs_response(blobs: Vec<Uint8Array>) -> Self {
        Message::BlobsResponse(blobs.iter().map(|b| Blob::from(b.to_vec())).collect()).into()
    }

    /// Create a [`Message::BatchSyncRequest`] message.
    #[wasm_bindgen(js_name = batchSyncRequest)]
    #[must_use]
    pub fn batch_sync_request(request: &WasmBatchSyncRequest) -> Self {
        Message::BatchSyncRequest(request.clone().into()).into()
    }

    /// Create a [`Message::BatchSyncResponse`] message.
    #[wasm_bindgen(js_name = batchSyncResponse)]
    #[must_use]
    pub fn batch_sync_response(response: &WasmBatchSyncResponse) -> Self {
        Message::BatchSyncResponse(response.clone().into()).into()
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

    /// The [`LooseCommit`] for a [`Message::LooseCommit`], if applicable.
    #[wasm_bindgen(getter, js_name = commit)]
    #[must_use]
    pub fn commit(&self) -> Option<WasmLooseCommit> {
        match &self.0 {
            Message::LooseCommit { commit, .. } => Some(commit.clone().into()),
            Message::Fragment { .. }
            | Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::BatchSyncRequest(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The [`Fragment`] for a [`Message::Fragment`], if applicable.
    #[wasm_bindgen(getter, js_name = fragment)]
    #[must_use]
    pub fn fragment(&self) -> Option<WasmFragment> {
        match &self.0 {
            Message::Fragment { fragment, .. } => Some(fragment.clone().into()),
            Message::LooseCommit { .. }
            | Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::BatchSyncRequest(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The [`Blob`] for commit or fragment messages, if applicable.
    #[wasm_bindgen(getter, js_name = blob)]
    #[must_use]
    pub fn blob(&self) -> Option<Uint8Array> {
        match &self.0 {
            Message::LooseCommit { blob, .. } | Message::Fragment { blob, .. } => {
                Some(Uint8Array::from(blob.as_slice()))
            }
            Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::BatchSyncRequest(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The requested [`Digest`]s for a [`Message::BlobsRequest`], if applicable.
    #[wasm_bindgen(getter, js_name = digests)]
    #[must_use]
    pub fn digests(&self) -> Option<Vec<WasmDigest>> {
        match &self.0 {
            Message::BlobsRequest(digests) => {
                Some(digests.iter().copied().map(WasmDigest::from).collect())
            }
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsResponse(_)
            | Message::BatchSyncRequest(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The [`Blob`]s for a [`Message::BlobsResponse`], if applicable.
    #[wasm_bindgen(getter, js_name = blobs)]
    #[must_use]
    pub fn blobs(&self) -> Option<Vec<Uint8Array>> {
        match &self.0 {
            Message::BlobsResponse(blobs) => Some(
                blobs
                    .iter()
                    .map(|b| Uint8Array::from(b.as_slice()))
                    .collect(),
            ),
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest(_)
            | Message::BatchSyncRequest(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The [`BatchSyncRequest`] for a [`Message::BatchSyncRequest`], if applicable.
    #[wasm_bindgen(getter, js_name = request)]
    #[must_use]
    pub fn request(&self) -> Option<WasmBatchSyncRequest> {
        match &self.0 {
            Message::BatchSyncRequest(req) => Some(req.clone().into()),
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::BatchSyncResponse(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }

    /// The [`BatchSyncResponse`] for a [`Message::BatchSyncResponse`], if applicable.
    #[wasm_bindgen(getter, js_name = response)]
    #[must_use]
    pub fn response(&self) -> Option<WasmBatchSyncResponse> {
        match &self.0 {
            Message::BatchSyncResponse(resp) => Some(resp.clone().into()),
            Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest(_)
            | Message::BlobsResponse(_)
            | Message::BatchSyncRequest(_)
            | Message::RemoveSubscriptions(_) => None,
        }
    }
}

impl From<Message> for WasmMessage {
    fn from(msg: Message) -> Self {
        Self(msg)
    }
}

impl From<WasmMessage> for Message {
    fn from(msg: WasmMessage) -> Self {
        msg.0
    }
}

/// An error indicating a failure to deserialize a [`Message`] from CBOR.
#[derive(Debug, Error)]
#[error("failed to deserialize Message: {0}")]
pub struct JsMessageDeserializationError(minicbor::decode::Error);

impl From<JsMessageDeserializationError> for JsValue {
    fn from(err: JsMessageDeserializationError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("MessageDeserializationError");
        js_err.into()
    }
}
