//! JS [`Connection`] interface for Subduction.

pub mod websocket;

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::time::Duration;
use wasm_refgen::wasm_refgen;

use futures::{future::LocalBoxFuture, FutureExt};
use futures_kind::Local;
use js_sys::{Promise, Uint8Array};
use sedimentree_core::blob::Blob;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::id::PeerId,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    digest::WasmDigest, fragment::WasmFragment, loose_commit::WasmLooseCommit, peer_id::WasmPeerId,
    sedimentree_id::WasmSedimentreeId,
};

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Connection {
    peerId(): PeerId;
    disconnect(): Promise<void>;
    send(message: Message): Promise<void>;
    recv(): Promise<Message>;
    nextRequestId(): Promise<RequestId>;
    call(request: BatchSyncRequest, timeoutMs: number | null): Promise<BatchSyncResponse>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A duck-typed connection interface.
    #[wasm_bindgen(js_name = Connection, typescript_type = "Connection")]
    pub type JsConnection;

    /// Get the peer ID of the remote peer.
    #[wasm_bindgen(method, js_name = peerId)]
    fn js_peer_id(this: &JsConnection) -> WasmPeerId;

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(method, js_name = disconnect)]
    fn js_disconnect(this: &JsConnection) -> Promise;

    /// Send a message.
    #[wasm_bindgen(method, js_name = send)]
    fn js_send(this: &JsConnection, message: WasmMessage) -> Promise;

    /// Receive a message.
    #[wasm_bindgen(method, js_name = recv)]
    fn js_recv(this: &JsConnection) -> Promise;

    /// Get the next request ID.
    #[wasm_bindgen(method, js_name = nextRequestId)]
    fn js_next_request_id(this: &JsConnection) -> Promise;

    /// Make a synchronous call to the peer.
    #[wasm_bindgen(method, js_name = call)]
    fn js_call(
        this: &JsConnection,
        request: WasmBatchSyncRequest,
        timeout_ms: Option<f64>,
    ) -> Promise;
}

impl core::fmt::Debug for JsConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsConnection").finish()
    }
}

impl Clone for JsConnection {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}

impl PartialEq for JsConnection {
    fn eq(&self, other: &Self) -> bool {
        JsValue::from(self) == JsValue::from(other)
    }
}

impl Connection<Local> for JsConnection {
    type DisconnectionError = JsConnectionError;
    type SendError = JsConnectionError;
    type RecvError = JsConnectionError;
    type CallError = JsConnectionError;

    fn peer_id(&self) -> PeerId {
        self.js_peer_id().into()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            JsFuture::from(self.js_disconnect())
                .await
                .map_err(JsConnectionError::Disconnect)?;
            Ok(())
        }
        .boxed_local()
    }

    fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let wasm_msg = WasmMessage::from(message);
            JsFuture::from(self.js_send(wasm_msg))
                .await
                .map_err(JsConnectionError::Send)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async move {
            let js_value = JsFuture::from(self.js_recv())
                .await
                .map_err(JsConnectionError::Recv)?;
            let js_msg: JsMessage = JsCast::unchecked_into(js_value);
            let wasm_msg = WasmMessage::from(&js_msg);
            Ok(wasm_msg.into())
        }
        .boxed_local()
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        async move {
            #[allow(clippy::expect_used)]
            let js_value = JsFuture::from(self.js_next_request_id())
                .await
                .expect("nextRequestId should not fail");
            let js_req_id: JsRequestId = js_value.unchecked_into();
            let wasm_req_id = WasmRequestId::from(&js_req_id);
            wasm_req_id.into()
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            let wasm_req = WasmBatchSyncRequest::from(req);
            let timeout_ms = timeout.map(|d| d.as_millis() as f64);
            let js_value = JsFuture::from(self.js_call(wasm_req, timeout_ms))
                .await
                .map_err(JsConnectionError::Call)?;
            let js_resp: JsBatchSyncResponse = JsCast::unchecked_into(js_value);
            let wasm_resp = WasmBatchSyncResponse::from(&js_resp);
            Ok(wasm_resp.into())
        }
        .boxed_local()
    }
}

/// Errors from the JS connection.
#[derive(Error, Debug, Clone)]
pub enum JsConnectionError {
    /// An error that occurred while disconnecting.
    #[error("Disconnect error")]
    Disconnect(JsValue),

    /// An error that occurred while sending a message.
    #[error("Send error")]
    Send(JsValue),

    /// An error that occurred while receiving a message.
    #[error("Recv error")]
    Recv(JsValue),

    /// An error that occurred during a synchronous call.
    #[error("Call error")]
    Call(JsValue),
}

impl From<JsConnectionError> for JsValue {
    fn from(err: JsConnectionError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ConnectionError");
        js_err.into()
    }
}

/// WASM wrapper for [`Message`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = Message)]
pub struct WasmMessage(Message);

#[wasm_refgen(js_ref = JsMessage)]
#[wasm_bindgen(js_class = Message)]
impl WasmMessage {
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
            _ => None,
        }
    }

    /// The [`Fragment`] for a [`Message::Fragment`], if applicable.
    #[wasm_bindgen(getter, js_name = fragment)]
    #[must_use]
    pub fn fragment(&self) -> Option<WasmFragment> {
        match &self.0 {
            Message::Fragment { fragment, .. } => Some(fragment.clone().into()),
            _ => None,
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
            _ => None,
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
            _ => None,
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
            _ => None,
        }
    }

    /// The [`BatchSyncRequest`] for a [`Message::BatchSyncRequest`], if applicable.
    #[wasm_bindgen(getter, js_name = request)]
    #[must_use]
    pub fn request(&self) -> Option<WasmBatchSyncRequest> {
        match &self.0 {
            Message::BatchSyncRequest(req) => Some(req.clone().into()),
            _ => None,
        }
    }

    /// The [`BatchSyncResponse`] for a [`Message::BatchSyncResponse`], if applicable.
    #[wasm_bindgen(getter, js_name = response)]
    #[must_use]
    pub fn response(&self) -> Option<WasmBatchSyncResponse> {
        match &self.0 {
            Message::BatchSyncResponse(resp) => Some(resp.clone().into()),
            _ => None,
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

// =============================================================================
// WasmRequestId
// =============================================================================

/// WASM wrapper for [`RequestId`].
#[wasm_bindgen(js_name = RequestId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmRequestId(RequestId);

#[wasm_refgen(js_ref = JsRequestId)]
#[wasm_bindgen(js_class = RequestId)]
impl WasmRequestId {
    /// Create a new [`RequestId`] from a requestor peer ID and nonce.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(requestor: &WasmPeerId, nonce: f64) -> Self {
        RequestId {
            requestor: requestor.clone().into(),
            nonce: nonce as u64,
        }
        .into()
    }

    /// The peer ID of the requestor.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn requestor(&self) -> WasmPeerId {
        self.0.requestor.into()
    }

    /// The request nonce.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn nonce(&self) -> f64 {
        self.0.nonce as f64
    }
}

impl From<RequestId> for WasmRequestId {
    fn from(id: RequestId) -> Self {
        Self(id)
    }
}

impl From<WasmRequestId> for RequestId {
    fn from(id: WasmRequestId) -> Self {
        id.0
    }
}

// =============================================================================
// WasmBatchSyncRequest
// =============================================================================

/// WASM wrapper for [`BatchSyncRequest`].
#[wasm_bindgen(js_name = BatchSyncRequest)]
#[derive(Debug, Clone)]
pub struct WasmBatchSyncRequest(BatchSyncRequest);

impl From<BatchSyncRequest> for WasmBatchSyncRequest {
    fn from(req: BatchSyncRequest) -> Self {
        Self(req)
    }
}

impl From<WasmBatchSyncRequest> for BatchSyncRequest {
    fn from(req: WasmBatchSyncRequest) -> Self {
        req.0
    }
}

// =============================================================================
// WasmBatchSyncResponse
// =============================================================================

/// WASM wrapper for [`BatchSyncResponse`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = BatchSyncResponse)]
pub struct WasmBatchSyncResponse(BatchSyncResponse);

#[wasm_refgen(js_ref = JsBatchSyncResponse)]
#[wasm_bindgen(js_class = BatchSyncResponse)]
impl WasmBatchSyncResponse {}

impl From<BatchSyncResponse> for WasmBatchSyncResponse {
    fn from(resp: BatchSyncResponse) -> Self {
        Self(resp)
    }
}

impl From<WasmBatchSyncResponse> for BatchSyncResponse {
    fn from(resp: WasmBatchSyncResponse) -> Self {
        resp.0
    }
}
