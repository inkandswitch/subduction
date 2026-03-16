//! JS [`Transport`] interface for Subduction.

pub mod fetch_client;
pub mod handshake;
pub mod longpoll;
pub mod message;
pub mod message_port;
pub mod nonce;
pub mod websocket;

use alloc::{string::ToString, vec::Vec};
use core::time::Duration;
use wasm_refgen::wasm_refgen;

use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use js_sys::{self, Promise};
use subduction_core::{
    connection::{
        Roundtrip,
        authenticated::Authenticated,
        handshake::{self as hs, audience::Audience},
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
        multiplexer::Multiplexer,
        transport::{MessageTransport, Transport},
    },
    timestamp::TimestampSeconds,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{
    connection::{longpoll::JsTimeout, nonce::WasmNonce},
    error::WasmHandshakeError,
    peer_id::WasmPeerId,
    signer::JsSigner,
};
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;

/// Type alias for the `MessageTransport`-wrapped `JsTransport`.
///
/// This is the connection type used by `WasmSubductionCore` and error aliases.
pub type WasmJsConnection = MessageTransport<JsTransport>;

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
export interface Transport {
    sendBytes(bytes: Uint8Array): Promise<void>;
    recvBytes(): Promise<Uint8Array>;
    disconnect(): Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// Byte-oriented transport interface.
    #[wasm_bindgen(js_name = Transport, typescript_type = "Transport")]
    pub type JsTransport;

    /// Send raw bytes over the transport.
    #[wasm_bindgen(method, js_name = sendBytes)]
    fn js_send_bytes(this: &JsTransport, bytes: &js_sys::Uint8Array) -> Promise;

    /// Receive the next message frame as raw bytes.
    #[wasm_bindgen(method, js_name = recvBytes)]
    fn js_recv_bytes(this: &JsTransport) -> Promise;

    /// Disconnect from the peer gracefully.
    #[wasm_bindgen(method, js_name = disconnect)]
    fn js_disconnect(this: &JsTransport) -> Promise;
}

impl core::fmt::Debug for JsTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsTransport").finish()
    }
}

impl Clone for JsTransport {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}

impl PartialEq for JsTransport {
    fn eq(&self, other: &Self) -> bool {
        JsValue::from(self) == JsValue::from(other)
    }
}

impl Transport<Local> for JsTransport {
    type SendError = JsConnectionError;
    type RecvError = JsConnectionError;
    type DisconnectionError = JsConnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let uint8_array = js_sys::Uint8Array::from(bytes);
        async move {
            JsFuture::from(self.js_send_bytes(&uint8_array))
                .await
                .map_err(JsConnectionError::Send)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async move {
            let js_value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(JsConnectionError::Recv)?;
            let uint8_array = js_sys::Uint8Array::new(&js_value);
            Ok(uint8_array.to_vec())
        }
        .boxed_local()
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
}

/// Default timeout for roundtrip calls via `JsTransport`.
const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(30);

impl Roundtrip<Local, BatchSyncRequest, BatchSyncResponse> for JsTransport {
    type CallError = JsConnectionError;

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        // Generate a locally-random request ID.
        // We can't use a Multiplexer here since JsTransport is a JS extern type
        // without interior state. Use a global-ish random nonce instead.
        async move {
            let mut buf = [0u8; 8];
            #[allow(clippy::expect_used)]
            getrandom::getrandom(&mut buf).expect("platform CSPRNG unavailable");
            let nonce = u64::from_le_bytes(buf);
            // Requestor is zeroed — it will be filled in by the caller (Subduction)
            // using the authenticated peer_id.
            RequestId {
                requestor: subduction_core::peer::id::PeerId::new([0u8; 32]),
                nonce,
            }
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            // Encode request as bytes
            let bytes = Multiplexer::<JsTimeout>::encode_request(&req);

            // Send via the transport
            self.send_bytes(&bytes)
                .await
                .map_err(|_| JsConnectionError::Call(JsValue::from_str("send failed")))?;

            // Wait for a response matching our request ID
            let req_id = req.req_id;
            let deadline = timeout.unwrap_or(DEFAULT_CALL_TIMEOUT);
            let start = js_sys::Date::now();

            #[allow(clippy::cast_possible_truncation)]
            let deadline_ms = deadline.as_millis().min(u128::from(u64::MAX)) as u64;

            loop {
                #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                let elapsed_ms = (js_sys::Date::now() - start) as u64;
                if elapsed_ms >= deadline_ms {
                    return Err(JsConnectionError::Call(JsValue::from_str("call timed out")));
                }

                let resp_bytes = self.recv_bytes().await?;

                if let Ok(SyncMessage::BatchSyncResponse(resp)) =
                    SyncMessage::try_decode(&resp_bytes)
                    && resp.req_id == req_id
                {
                    return Ok(resp);
                }

                // Not our response — in a real multiplexed setup this
                // would be dispatched elsewhere. For now, loop and
                // discard non-matching messages.
                tracing::debug!("JsTransport::call: received non-matching message, retrying");
            }
        }
        .boxed_local()
    }
}

/// Errors from the JS transport.
#[derive(Error, Debug, Clone)]
pub enum JsConnectionError {
    /// An error that occurred while disconnecting.
    #[error("Disconnect error")]
    Disconnect(JsValue),

    /// An error that occurred while sending bytes.
    #[error("Send error")]
    Send(JsValue),

    /// An error that occurred while receiving bytes.
    #[error("Recv error")]
    Recv(JsValue),

    /// An error that occurred during a roundtrip call.
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

/// Wasm wrapper for [`RequestId`].
#[wasm_bindgen(js_name = RequestId)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WasmRequestId(RequestId);

#[wasm_refgen(js_ref = JsRequestId)]
#[wasm_bindgen(js_class = RequestId)]
impl WasmRequestId {
    /// Create a new [`RequestId`] from a requestor peer ID and nonce.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(requestor: &WasmPeerId, nonce: &WasmNonce) -> Self {
        RequestId {
            requestor: requestor.clone().into(),
            nonce: nonce.0,
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
    pub fn nonce(&self) -> WasmNonce {
        WasmNonce(self.0.nonce)
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

/// Wasm wrapper for [`BatchSyncRequest`].
#[wasm_bindgen(js_name = BatchSyncRequest)]
#[derive(Debug, Clone)]
pub struct WasmBatchSyncRequest(BatchSyncRequest);

#[wasm_bindgen(js_class = BatchSyncRequest)]
impl WasmBatchSyncRequest {
    /// The sedimentree ID this request corresponds to.
    #[must_use]
    pub fn id(&self) -> WasmSedimentreeId {
        self.0.id.into()
    }

    /// The request ID for this request.
    #[must_use]
    pub fn request_id(&self) -> WasmRequestId {
        self.0.req_id.into()
    }

    /// Whether this request subscribes to future updates.
    #[must_use]
    pub fn subscribe(&self) -> bool {
        self.0.subscribe
    }
}

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

/// Wasm wrapper for [`BatchSyncResponse`].
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = BatchSyncResponse)]
pub struct WasmBatchSyncResponse(BatchSyncResponse);

#[wasm_bindgen(js_class = BatchSyncResponse)]
impl WasmBatchSyncResponse {
    /// The sedimentree ID this response corresponds to.
    #[must_use]
    pub fn id(&self) -> WasmSedimentreeId {
        self.0.id.into()
    }

    /// The request ID this response corresponds to.
    #[must_use]
    pub fn request_id(&self) -> WasmRequestId {
        self.0.req_id.into()
    }
}

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

/// A transport-erased authenticated connection.
///
/// Wraps an [`Authenticated<WasmJsConnection>`] and is the common type
/// accepted by [`addConnection`](crate::subduction::WasmSubduction::add_connection).
///
/// # Construction
///
/// There are three ways to obtain an `AuthenticatedTransport`:
///
/// 1. **Custom transport** — implement [`HandshakeConnection`](handshake::JsHandshakeTransport)
///    (a `Transport` with `sendBytes`/`recvBytes`/`disconnect`) and call [`setup`](Self::setup):
///
///    ```js
///    const auth = await AuthenticatedTransport.setup(myConn, signer, peerId);
///    ```
///
/// 2. **From WebSocket** — authenticate via [`SubductionWebSocket`] then convert:
///
///    ```js
///    const wsAuth = await SubductionWebSocket.tryConnect(url, signer, peerId, timeout);
///    const auth = wsAuth.toConnection();
///    ```
///
/// 3. **From HTTP long-poll** — same pattern via [`SubductionLongPoll`]:
///
///    ```js
///    const lpAuth = await SubductionLongPoll.tryConnect(url, signer, peerId, timeout);
///    const auth = lpAuth.toConnection();
///    ```
#[wasm_bindgen(js_name = AuthenticatedTransport)]
#[derive(Debug)]
pub struct WasmAuthenticatedTransport {
    inner: Authenticated<WasmJsConnection, Local>,
}

impl WasmAuthenticatedTransport {
    /// Access the inner `Authenticated` connection.
    pub(crate) fn inner(&self) -> &Authenticated<WasmJsConnection, Local> {
        &self.inner
    }

    /// Construct from an `Authenticated<WasmJsConnection>`.
    ///
    /// Used by [`WasmAuthenticatedWebSocket::to_connection`] and
    /// [`WasmAuthenticatedLongPoll::to_connection`] to wrap transport-specific
    /// authenticated connections.
    pub(crate) fn from_authenticated(inner: Authenticated<WasmJsConnection, Local>) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = AuthenticatedTransport)]
impl WasmAuthenticatedTransport {
    /// Run the Subduction handshake over a custom transport, producing an
    /// authenticated connection.
    ///
    /// The `connection` object must implement `Transport`
    /// (`sendBytes`/`recvBytes`/`disconnect`).
    /// The same object is used for both the handshake phase and post-handshake
    /// communication.
    ///
    /// # Arguments
    ///
    /// * `connection` - A `HandshakeConnection` (extends `Transport`)
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn setup(
        connection: handshake::JsHandshakeTransport,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, peer_id) = hs::initiate::<Local, _, _, _, _>(
            connection,
            |hs_conn, peer_id| {
                let transport: JsTransport = wasm_bindgen::JsValue::from(hs_conn).unchecked_into();
                (MessageTransport::new(transport), peer_id)
            },
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Handshake complete: authenticated peer {peer_id}");

        Ok(Self {
            inner: authenticated,
        })
    }

    /// Accept an incoming handshake over a custom transport (responder side).
    ///
    /// This is the counterpart to [`setup`](Self::setup). The initiator calls
    /// `setup`, and the responder calls `accept` over the same underlying channel.
    ///
    /// # Arguments
    ///
    /// * `connection` - A `HandshakeConnection` (extends `Transport`)
    /// * `signer` - The responder's signer for authentication
    /// * `max_drift_seconds` - Maximum acceptable clock drift in seconds (default: 600)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn accept(
        connection: handshake::JsHandshakeTransport,
        signer: &JsSigner,
        max_drift_seconds: Option<u32>,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        use core::time::Duration;
        use subduction_core::connection::nonce_cache::NonceCache;

        let our_peer_id = signer.verifying_key().into();
        let nonce_cache = NonceCache::default();
        let max_drift = Duration::from_secs(u64::from(max_drift_seconds.unwrap_or(600)));

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);

        let (authenticated, peer_id) = hs::respond::<Local, _, _, _, _>(
            connection,
            |hs_conn, peer_id| {
                let transport: JsTransport = wasm_bindgen::JsValue::from(hs_conn).unchecked_into();
                (MessageTransport::new(transport), peer_id)
            },
            signer,
            &nonce_cache,
            our_peer_id,
            None,
            now,
            max_drift,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Handshake complete (responder): authenticated peer {peer_id}");

        Ok(Self {
            inner: authenticated,
        })
    }

    /// The verified peer identity.
    #[must_use]
    #[wasm_bindgen(getter, js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        self.inner.peer_id().into()
    }
}
