//! Transport-erased authenticated transport wrapper.
//!
//! [`WasmAuthenticatedTransport`] is the common type accepted by
//! [`addConnection`](crate::subduction::WasmSubduction::add_connection).
//! All transport types (WebSocket, HTTP long-poll, custom JS) converge
//! to this type before being registered with Subduction.

use alloc::string::{String, ToString};

use future_form::Local;
use subduction_core::{
    authenticated::Authenticated,
    handshake::{self as hs, audience::Audience},
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use wasm_bindgen::prelude::*;

use crate::{error::WasmHandshakeError, peer_id::WasmPeerId, signer::JsSigner};

use super::{DEFAULT_LOCAL_SERVICE_NAME, JsTransport};

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
///    (`sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`) and call
///    [`setup`](Self::setup):
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
    /// (`sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`).
    /// The same object is used for both the handshake phase and post-handshake
    /// communication.
    ///
    /// # Arguments
    ///
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`
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
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`
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
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`
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
    /// * `transport` - A `Transport` implementing `sendBytes`/`recvBytes`/`disconnect`/`onDisconnect`
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

    /// Register a callback that fires when the underlying transport disconnects.
    ///
    /// The callback receives the peer's [`PeerId`] as its sole argument.
    /// Works for any transport type (WebSocket, long-poll, custom).
    ///
    /// ```js
    /// const auth = await AuthenticatedTransport.setup(transport, signer, peerId);
    /// auth.onDisconnect((peerId) => {
    ///   console.log(`peer ${peerId} disconnected`);
    /// });
    /// ```
    #[wasm_bindgen(js_name = onDisconnect)]
    pub fn on_disconnect(&self, callback: js_sys::Function) {
        let peer_id: JsValue = WasmPeerId::from(self.inner.peer_id()).into();
        let wrapper = wasm_bindgen::closure::Closure::<dyn Fn()>::new(move || {
            if let Err(e) = callback.call1(&JsValue::NULL, &peer_id) {
                tracing::error!("onDisconnect callback threw: {e:?}");
            }
        });
        let func: js_sys::Function = wrapper.as_ref().unchecked_ref::<js_sys::Function>().clone();
        wrapper.forget();
        self.inner.inner().inner().js_on_disconnect(&func);
    }
}
