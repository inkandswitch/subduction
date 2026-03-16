//! Wasm bindings for the HTTP long-poll transport.
//!
//! Provides [`SubductionLongPoll`] (JS name) for connecting to a server
//! via HTTP long-poll from browser/worker environments.

use alloc::string::{String, ToString};
use core::time::Duration;

use future_form::Local;
use futures::FutureExt;
use subduction_core::{
    authenticated::Authenticated,
    timeout::{TimedOut, Timeout},
    transport::Transport,
};
use subduction_http_longpoll::{
    client::HttpLongPollClient, session::SessionId, transport::HttpLongPollTransport,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

use super::fetch_client::FetchHttpClient;
use crate::{peer_id::WasmPeerId, signer::JsSigner, timer};

// ---------------------------------------------------------------------------
// Timeout: direct `globalThis.setTimeout` binding
// ---------------------------------------------------------------------------

/// A [`Timeout<Local>`] backed by `globalThis.setTimeout`.
///
/// Works in browsers, web workers, service workers, Node.js, Deno, and Bun.
#[derive(Debug, Clone, Copy)]
pub struct JsTimeout;

impl Timeout<Local> for JsTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: futures::future::LocalBoxFuture<'a, T>,
    ) -> futures::future::LocalBoxFuture<'a, Result<T, TimedOut>> {
        use futures::{
            future::{Either, select},
            pin_mut,
        };

        async move {
            let ms = i32::try_from(dur.as_millis()).unwrap_or(i32::MAX);
            let delay = js_sys::Promise::new(&mut |resolve, _| {
                timer::set_timeout(&resolve, ms);
            });
            let delay_fut = JsFuture::from(delay);
            pin_mut!(fut, delay_fut);
            match select(fut, delay_fut).await {
                Either::Left((val, _)) => Ok(val),
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed_local()
    }
}

/// Type alias for the long-poll transport used in Wasm.
pub type WasmLongPollTransport = HttpLongPollTransport<JsTimeout>;

/// JS-facing wrapper around [`WasmLongPollTransport`] that exposes the
/// byte-oriented [`Transport`](super::JsTransport) interface
/// (`sendBytes`/`recvBytes`/`disconnect`) so it can be used as a
/// duck-typed `JsTransport` from JavaScript.
#[wasm_bindgen(js_name = SubductionHttpLongPoll)]
#[derive(Debug, Clone)]
pub struct WasmHttpLongPoll(WasmLongPollTransport);

impl WasmHttpLongPoll {
    /// Wrap a raw long-poll transport for JS exposure.
    pub(crate) fn new(transport: WasmLongPollTransport) -> Self {
        Self(transport)
    }

    /// Access the inner transport.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &WasmLongPollTransport {
        &self.0
    }
}

/// Error from a long-poll transport method exposed to JS.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct WasmHttpLongPollError(String);

impl From<WasmHttpLongPollError> for JsValue {
    fn from(err: WasmHttpLongPollError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("HttpLongPollError");
        js_err.into()
    }
}

#[wasm_bindgen(js_class = SubductionHttpLongPoll)]
impl WasmHttpLongPoll {
    /// Send raw bytes over the transport.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound channel is closed.
    #[wasm_bindgen(js_name = sendBytes)]
    pub async fn send_bytes(&self, bytes: &[u8]) -> Result<(), WasmHttpLongPollError> {
        Transport::<Local>::send_bytes(&self.0, bytes)
            .await
            .map_err(|e| WasmHttpLongPollError(e.to_string()))
    }

    /// Receive the next message frame as raw bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the inbound channel is closed.
    #[wasm_bindgen(js_name = recvBytes)]
    pub async fn recv_bytes(&self) -> Result<js_sys::Uint8Array, WasmHttpLongPollError> {
        let bytes = Transport::<Local>::recv_bytes(&self.0)
            .await
            .map_err(|e| WasmHttpLongPollError(e.to_string()))?;
        Ok(js_sys::Uint8Array::from(bytes.as_slice()))
    }

    /// Disconnect from the peer gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if the disconnect fails.
    #[wasm_bindgen(js_name = disconnect)]
    pub async fn disconnect(&self) -> Result<(), WasmHttpLongPollError> {
        Transport::<Local>::disconnect(&self.0)
            .await
            .map_err(|e| WasmHttpLongPollError(e.to_string()))
    }
}

/// An authenticated HTTP long-poll connection.
///
/// This wrapper proves that the connection has completed the Subduction handshake
/// and the peer identity has been cryptographically verified.
///
/// Obtain via [`SubductionLongPoll::tryConnect`] or [`SubductionLongPoll::tryDiscover`].
#[wasm_bindgen(js_name = AuthenticatedLongPoll)]
#[derive(Debug)]
pub struct WasmAuthenticatedLongPoll {
    inner: Authenticated<WasmLongPollTransport, Local>,
    session_id: SessionId,
}

impl WasmAuthenticatedLongPoll {
    /// Access the inner `Authenticated` connection.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &Authenticated<WasmLongPollTransport, Local> {
        &self.inner
    }
}

#[wasm_bindgen(js_class = AuthenticatedLongPoll)]
impl WasmAuthenticatedLongPoll {
    /// The verified peer identity.
    #[must_use]
    #[wasm_bindgen(getter, js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        self.inner.peer_id().into()
    }

    /// The session ID assigned by the server.
    #[must_use]
    #[wasm_bindgen(getter, js_name = sessionId)]
    pub fn session_id(&self) -> String {
        self.session_id.to_hex()
    }

    /// Convert to a transport-erased [`AuthenticatedConnection`](super::WasmAuthenticatedTransport).
    #[must_use]
    #[wasm_bindgen(js_name = toConnection)]
    pub fn to_connection(self) -> super::WasmAuthenticatedTransport {
        let peer_id = self.inner.peer_id();
        super::WasmAuthenticatedTransport::from_authenticated(self.inner.map(|lp| {
            let transport: super::JsTransport =
                wasm_bindgen::JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into();
            super::make_connection(transport, peer_id)
        }))
    }
}

// ---------------------------------------------------------------------------
// Connection factory
// ---------------------------------------------------------------------------

/// Build an [`HttpLongPollClient`] configured for the browser.
fn make_client(
    base_url: &str,
    default_time_limit: Duration,
) -> HttpLongPollClient<FetchHttpClient, JsTimeout> {
    HttpLongPollClient::new(
        base_url,
        FetchHttpClient::new(),
        JsTimeout,
        default_time_limit,
    )
}

/// Get the current timestamp from JS `Date.now()`.
fn js_now() -> subduction_core::timestamp::TimestampSeconds {
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    subduction_core::timestamp::TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64)
}

/// HTTP long-poll connection factory for browser/worker environments.
///
/// Analogous to [`SubductionWebSocket`] but uses HTTP long-poll instead of WebSocket.
#[wasm_bindgen(js_name = SubductionLongPoll)]
#[derive(Debug, Clone, Copy)]
pub struct WasmLongPoll;

#[wasm_bindgen(js_class = SubductionLongPoll)]
impl WasmLongPoll {
    /// Connect to a server with a known peer ID.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    ///
    /// # Errors
    ///
    /// Returns [`LongPollConnectionError`] if connection or handshake fails.
    #[wasm_bindgen(js_name = tryConnect)]
    pub async fn try_connect(
        base_url: &str,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmAuthenticatedLongPoll, LongPollConnectionError> {
        let timeout_ms = timeout_milliseconds.unwrap_or(30_000);
        let default_time_limit = Duration::from_millis(timeout_ms.into());
        let client = make_client(base_url, default_time_limit);

        let result = client
            .connect(signer, expected_peer_id.clone().into(), js_now())
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        // Spawn background tasks
        wasm_bindgen_futures::spawn_local(result.poll_task);
        wasm_bindgen_futures::spawn_local(result.send_task);

        Ok(WasmAuthenticatedLongPoll {
            inner: result.authenticated,
            session_id: result.session_id,
        })
    }

    /// Connect to a server using discovery mode.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `signer` - The client's signer for authentication
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    /// * `service_name` - The service name for discovery. If omitted, the base URL is used.
    ///
    /// # Errors
    ///
    /// Returns [`LongPollConnectionError`] if connection or handshake fails.
    #[wasm_bindgen(js_name = tryDiscover)]
    pub async fn try_discover(
        base_url: &str,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<WasmAuthenticatedLongPoll, LongPollConnectionError> {
        let timeout_ms = timeout_milliseconds.unwrap_or(30_000);
        let default_time_limit = Duration::from_millis(timeout_ms.into());
        let client = make_client(base_url, default_time_limit);
        let service_name = service_name.unwrap_or_else(|| base_url.to_string());

        let result = client
            .connect_discover(signer, &service_name, js_now())
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        // Spawn background tasks
        wasm_bindgen_futures::spawn_local(result.poll_task);
        wasm_bindgen_futures::spawn_local(result.send_task);

        Ok(WasmAuthenticatedLongPoll {
            inner: result.authenticated,
            session_id: result.session_id,
        })
    }

    /// Connect and return the raw `Authenticated` connection (for internal use).
    pub(crate) async fn connect_authenticated(
        base_url: &str,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<(Authenticated<WasmLongPollTransport, Local>, SessionId), LongPollConnectionError>
    {
        let default_time_limit = Duration::from_millis(timeout_milliseconds.into());
        let client = make_client(base_url, default_time_limit);

        let result = client
            .connect(signer, expected_peer_id.clone().into(), js_now())
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        // Spawn background tasks
        wasm_bindgen_futures::spawn_local(result.poll_task);
        wasm_bindgen_futures::spawn_local(result.send_task);

        Ok((result.authenticated, result.session_id))
    }

    /// Connect using discovery and return the raw `Authenticated` connection.
    pub(crate) async fn connect_discover_authenticated(
        base_url: &str,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<(Authenticated<WasmLongPollTransport, Local>, SessionId), LongPollConnectionError>
    {
        let timeout_ms = timeout_milliseconds.unwrap_or(30_000);
        let default_time_limit = Duration::from_millis(timeout_ms.into());
        let client = make_client(base_url, default_time_limit);
        let service_name = service_name.unwrap_or_else(|| base_url.to_string());

        let result = client
            .connect_discover(signer, &service_name, js_now())
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        // Spawn background tasks
        wasm_bindgen_futures::spawn_local(result.poll_task);
        wasm_bindgen_futures::spawn_local(result.send_task);

        Ok((result.authenticated, result.session_id))
    }
}

/// Error connecting via HTTP long-poll.
#[derive(Debug, Error)]
pub enum LongPollConnectionError {
    /// Connection or handshake failed.
    #[error("long-poll connection failed: {0}")]
    Connection(String),
}

impl From<LongPollConnectionError> for JsValue {
    fn from(err: LongPollConnectionError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("LongPollConnectionError");
        js_err.into()
    }
}
