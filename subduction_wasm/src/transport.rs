//! JS [`Transport`] interface for Subduction.

pub mod fetch_client;
pub mod longpoll;
pub mod message;
pub mod message_port;
pub mod nonce;
pub mod websocket;

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use wasm_refgen::wasm_refgen;

use future_form::Local;
use futures::{FutureExt, future::LocalBoxFuture};
use js_sys::{self, Promise};
use subduction_core::{
    authenticated::Authenticated,
    connection::message::{BatchSyncRequest, BatchSyncResponse, RequestId},
    handshake::{self as hs, Handshake, audience::Audience},
    timestamp::TimestampSeconds,
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use crate::{error::WasmHandshakeError, peer_id::WasmPeerId, signer::JsSigner};

use self::nonce::WasmNonce;
use sedimentree_wasm::sedimentree_id::WasmSedimentreeId;

/// Default service name for local (non-network) discovery handshakes.
///
/// Used by [`connectTransport`](crate::subduction::WasmSubduction::connect_transport)
/// and [`acceptTransport`](crate::subduction::WasmSubduction::accept_transport) when
/// no explicit service name is provided.
pub const DEFAULT_LOCAL_SERVICE_NAME: &str = "subduction:local";



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
    type SendError = JsTransportError;
    type RecvError = JsTransportError;
    type DisconnectionError = JsTransportError;

    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let uint8_array = js_sys::Uint8Array::from(bytes);
        async move {
            JsFuture::from(self.js_send_bytes(&uint8_array))
                .await
                .map_err(JsTransportError::Send)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async move {
            let js_value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(JsTransportError::Recv)?;
            let uint8_array = js_sys::Uint8Array::new(&js_value);
            Ok(uint8_array.to_vec())
        }
        .boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async move {
            JsFuture::from(self.js_disconnect())
                .await
                .map_err(JsTransportError::Disconnect)?;
            Ok(())
        }
        .boxed_local()
    }
}

impl Handshake<Local> for JsTransport {
    type Error = crate::error::WasmHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let array = js_sys::Uint8Array::from(bytes.as_slice());
        async move {
            JsFuture::from(self.js_send_bytes(&array))
                .await
                .map_err(|e| crate::error::WasmHandshakeError::WebSocket(format!("{e:?}")))?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        async move {
            let value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(|e| crate::error::WasmHandshakeError::WebSocket(format!("{e:?}")))?;

            let array: js_sys::Uint8Array = value.dyn_into().map_err(|v| {
                crate::error::WasmHandshakeError::WebSocket(format!(
                    "expected Uint8Array, got {v:?}"
                ))
            })?;

            Ok(array.to_vec())
        }
        .boxed_local()
    }
}

// Request-response multiplexing is handled by `ManagedConnection`,
// which pairs the connection with a `Multiplexer` and routes responses
// in the `Subduction` listen loop.

/// Errors from the JS transport.
#[derive(Error, Debug, Clone)]
pub enum JsTransportError {
    /// An error that occurred while disconnecting.
    #[error("Disconnect error")]
    Disconnect(JsValue),

    /// An error that occurred while sending bytes.
    #[error("Send error")]
    Send(JsValue),

    /// An error that occurred while receiving bytes.
    #[error("Recv error")]
    Recv(JsValue),
}

impl From<JsTransportError> for JsValue {
    fn from(err: JsTransportError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("TransportError");
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

/// A transport-erased authenticated transport.
///
/// Wraps an [`Authenticated<MessageTransport<JsTransport>>`] and is the common type
/// accepted by [`addConnection`](crate::subduction::WasmSubduction::add_connection).
///
/// # Construction
///
/// There are three ways to obtain an `AuthenticatedTransport`:
///
/// 1. **Custom transport** — implement the `Transport` interface
///    (`sendBytes`/`recvBytes`/`disconnect`) and call [`setup`](Self::setup):
///
///    ```js
///    const auth = await AuthenticatedTransport.setup(myTransport, signer, peerId);
///    ```
///
/// 2. **From WebSocket** — authenticate via [`SubductionWebSocket`] then convert:
///
///    ```js
///    const wsAuth = await SubductionWebSocket.tryConnect(url, signer, peerId, timeout);
///    const auth = wsAuth.toTransport();
///    ```
///
/// 3. **From HTTP long-poll** — same pattern via [`SubductionLongPoll`]:
///
///    ```js
///    const lpAuth = await SubductionLongPoll.tryConnect(url, signer, peerId, timeout);
///    const auth = lpAuth.toTransport();
///    ```
#[wasm_bindgen(js_name = AuthenticatedTransport)]
#[derive(Debug)]
pub struct WasmAuthenticatedTransport {
    inner: Authenticated<MessageTransport<JsTransport>, Local>,
}

impl WasmAuthenticatedTransport {
    /// Access the inner `Authenticated` transport.
    pub(crate) fn inner(&self) -> &Authenticated<MessageTransport<JsTransport>, Local> {
        &self.inner
    }

    /// Consume and return the inner `Authenticated` transport.
    pub(crate) fn into_inner(self) -> Authenticated<MessageTransport<JsTransport>, Local> {
        self.inner
    }

    /// Construct from an `Authenticated<MessageTransport<JsTransport>>`.
    pub(crate) fn from_authenticated(
        inner: Authenticated<MessageTransport<JsTransport>, Local>,
    ) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen(js_class = AuthenticatedTransport)]
impl WasmAuthenticatedTransport {
    /// Run the Subduction handshake over a custom transport, producing an
    /// authenticated transport.
    ///
    /// The `transport` object must implement the `Transport` interface
    /// (`sendBytes`/`recvBytes`/`disconnect`).
    /// The same object is used for both the handshake phase and post-handshake
    /// communication.
    ///
    /// # Arguments
    ///
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn setup(
        transport: JsTransport,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        let audience = Audience::known(expected_peer_id.clone().into());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, peer_id) = hs::initiate::<Local, _, _, _, _>(
            transport,
            |transport, peer_id| (MessageTransport::new(transport), peer_id),
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

    /// Run the Subduction handshake over a custom transport using discovery
    /// mode, producing an authenticated transport.
    ///
    /// Unlike [`setup`](Self::setup) which requires a known peer ID,
    /// this method discovers the peer's identity during the handshake
    /// using a shared service name.
    ///
    /// # Arguments
    ///
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`
    /// * `signer` - The client's signer for authentication
    /// * `service_name` - Shared service name for discovery.
    ///   Defaults to [`DEFAULT_LOCAL_SERVICE_NAME`] (`"subduction:local"`) if omitted.
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen(js_name = setupDiscover)]
    pub async fn setup_discover(
        transport: JsTransport,
        signer: &JsSigner,
        service_name: Option<String>,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        let service_name = service_name.unwrap_or_else(|| DEFAULT_LOCAL_SERVICE_NAME.into());
        let audience = Audience::discover(service_name.as_bytes());
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let nonce = Nonce::random();

        let (authenticated, peer_id) = hs::initiate::<Local, _, _, _, _>(
            transport,
            |transport, peer_id| (MessageTransport::new(transport), peer_id),
            signer,
            audience,
            now,
            nonce,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Discovery handshake complete: authenticated peer {peer_id}");

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
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`
    /// * `signer` - The responder's signer for authentication
    /// * `max_drift_seconds` - Maximum acceptable clock drift in seconds (default: 600)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails.
    #[wasm_bindgen]
    pub async fn accept(
        transport: JsTransport,
        signer: &JsSigner,
        max_drift_seconds: Option<u32>,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        use core::time::Duration;
        use subduction_core::nonce_cache::NonceCache;

        let our_peer_id = signer.verifying_key().into();
        let nonce_cache = NonceCache::default();
        let max_drift = Duration::from_secs(u64::from(max_drift_seconds.unwrap_or(600)));

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);

        let (authenticated, peer_id) = hs::respond::<Local, _, _, _, _>(
            transport,
            |transport, peer_id| (MessageTransport::new(transport), peer_id),
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

    /// Accept an incoming discovery handshake, verifying the service name.
    ///
    /// Like [`accept`](Self::accept) but filters by the expected
    /// [`Audience::discover`] service name. Rejects handshakes
    /// targeting a different service.
    ///
    /// # Arguments
    ///
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`
    /// * `signer` - The responder's signer for authentication
    /// * `service_name` - Expected service name (must match the initiator's)
    ///
    /// # Errors
    ///
    /// Returns a [`HandshakeError`](WasmHandshakeError) if the handshake fails
    /// or the service name doesn't match.
    pub(crate) async fn accept_discover(
        transport: JsTransport,
        signer: &JsSigner,
        service_name: String,
    ) -> Result<WasmAuthenticatedTransport, WasmHandshakeError> {
        use core::time::Duration;
        use subduction_core::nonce_cache::NonceCache;

        let our_peer_id = signer.verifying_key().into();
        let nonce_cache = NonceCache::default();
        let max_drift = Duration::from_secs(600);
        let audience = Audience::discover(service_name.as_bytes());

        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let now = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);

        let (authenticated, peer_id) = hs::respond::<Local, _, _, _, _>(
            transport,
            |transport, peer_id| (MessageTransport::new(transport), peer_id),
            signer,
            &nonce_cache,
            our_peer_id,
            Some(audience),
            now,
            max_drift,
        )
        .await
        .map_err(|e| WasmHandshakeError::WebSocket(e.to_string()))?;

        tracing::info!("Discovery handshake complete (responder): authenticated peer {peer_id}");

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
