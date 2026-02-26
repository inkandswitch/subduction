//! Wasm bindings for the HTTP long-poll transport.
//!
//! Provides [`SubductionLongPoll`] (JS name) for connecting to a server
//! via HTTP long-poll from browser/worker environments.

use alloc::string::{String, ToString};
use core::time::Duration;

use future_form::Local;
use subduction_core::connection::{
    Connection,
    authenticated::Authenticated,
    message::{BatchSyncRequest, Message},
    timeout::{TimedOut, Timeout},
};
use subduction_http_longpoll::{
    client::HttpLongPollClient, connection::HttpLongPollConnection, session::SessionId,
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::{
    WasmBatchSyncRequest, WasmBatchSyncResponse, WasmRequestId, fetch_client::FetchHttpClient,
    message::WasmMessage,
};
use crate::{peer_id::WasmPeerId, signer::JsSigner};

// ---------------------------------------------------------------------------
// Timeout: use FuturesTimerTimeout which works on both native and wasm
// ---------------------------------------------------------------------------

/// A [`Timeout<Local>`] implementation using [`futures_timer::Delay`].
///
/// On wasm targets, `futures-timer` uses browser `setTimeout` internally.
#[derive(Debug, Clone, Copy)]
pub struct FuturesTimerTimeout;

impl Timeout<Local> for FuturesTimerTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: futures::future::LocalBoxFuture<'a, T>,
    ) -> futures::future::LocalBoxFuture<'a, Result<T, TimedOut>> {
        use futures::{
            FutureExt,
            future::{Either, select},
        };
        async move {
            match select(fut, futures_timer::Delay::new(dur)).await {
                Either::Left((val, _)) => Ok(val),
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed_local()
    }
}

/// Type alias for the long-poll connection used in wasm.
pub type WasmLongPollConnection = HttpLongPollConnection<FuturesTimerTimeout>;

/// JS-facing wrapper around [`WasmLongPollConnection`] that exposes the
/// [`Connection`](super::JsConnection) interface so it can be used as a
/// duck-typed `JsConnection` from JavaScript.
#[wasm_bindgen(js_name = SubductionLongPollConnection)]
#[derive(Debug, Clone)]
pub struct WasmLongPollConn(WasmLongPollConnection);

impl WasmLongPollConn {
    /// Wrap a raw long-poll connection for JS exposure.
    pub(crate) fn new(conn: WasmLongPollConnection) -> Self {
        Self(conn)
    }

    /// Access the inner connection.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &WasmLongPollConnection {
        &self.0
    }
}

/// Error from a long-poll connection method exposed to JS.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct WasmLongPollConnError(String);

impl From<WasmLongPollConnError> for JsValue {
    fn from(err: WasmLongPollConnError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("LongPollConnectionError");
        js_err.into()
    }
}

#[wasm_bindgen(js_class = SubductionLongPollConnection)]
impl WasmLongPollConn {
    /// Get the peer ID of the remote peer.
    #[must_use]
    #[wasm_bindgen(js_name = peerId)]
    pub fn peer_id(&self) -> WasmPeerId {
        Connection::<Local>::peer_id(&self.0).into()
    }

    /// Disconnect from the peer gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`WasmLongPollConnError`] if the disconnect fails.
    #[wasm_bindgen(js_name = disconnect)]
    pub async fn disconnect(&self) -> Result<(), WasmLongPollConnError> {
        Connection::<Local>::disconnect(&self.0)
            .await
            .map_err(|e| WasmLongPollConnError(e.to_string()))
    }

    /// Send a message.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound channel is closed.
    #[wasm_bindgen(js_name = send)]
    pub async fn send(&self, message: WasmMessage) -> Result<(), WasmLongPollConnError> {
        let msg: Message = message.into();
        Connection::<Local>::send(&self.0, &msg)
            .await
            .map_err(|e| WasmLongPollConnError(e.to_string()))
    }

    /// Receive a message.
    ///
    /// # Errors
    ///
    /// Returns an error if the inbound channel is closed.
    #[wasm_bindgen(js_name = recv)]
    pub async fn recv(&self) -> Result<WasmMessage, WasmLongPollConnError> {
        Connection::<Local>::recv(&self.0)
            .await
            .map(Into::into)
            .map_err(|e| WasmLongPollConnError(e.to_string()))
    }

    /// Get the next request ID.
    #[wasm_bindgen(js_name = nextRequestId)]
    pub async fn next_request_id(&self) -> WasmRequestId {
        Connection::<Local>::next_request_id(&self.0).await.into()
    }

    /// Make a synchronous call to the peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails or times out.
    #[wasm_bindgen(js_name = call)]
    pub async fn call(
        &self,
        request: WasmBatchSyncRequest,
        timeout_ms: Option<f64>,
    ) -> Result<WasmBatchSyncResponse, WasmLongPollConnError> {
        let req: BatchSyncRequest = request.into();
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let timeout = timeout_ms.map(|ms| Duration::from_millis(ms as u64));
        Connection::<Local>::call(&self.0, req, timeout)
            .await
            .map(Into::into)
            .map_err(|e| WasmLongPollConnError(e.to_string()))
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
    inner: Authenticated<WasmLongPollConnection, Local>,
    session_id: SessionId,
}

impl WasmAuthenticatedLongPoll {
    /// Access the inner `Authenticated` connection.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &Authenticated<WasmLongPollConnection, Local> {
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
}

// ---------------------------------------------------------------------------
// Connection factory
// ---------------------------------------------------------------------------

/// Build an [`HttpLongPollClient`] configured for the browser.
fn make_client(
    base_url: &str,
    default_time_limit: Duration,
) -> HttpLongPollClient<FetchHttpClient, FuturesTimerTimeout> {
    HttpLongPollClient::new(
        base_url,
        FetchHttpClient::new(),
        FuturesTimerTimeout,
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
    ) -> Result<(Authenticated<WasmLongPollConnection, Local>, SessionId), LongPollConnectionError>
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
    ) -> Result<(Authenticated<WasmLongPollConnection, Local>, SessionId), LongPollConnectionError>
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
