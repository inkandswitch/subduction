//! Wasm bindings for the HTTP long-poll transport.
//!
//! Provides [`SubductionLongPoll`] (JS name) for connecting to a server
//! via HTTP long-poll from browser/worker environments.

use alloc::string::{String, ToString};
use core::time::Duration;

use future_form::Local;
use subduction_core::connection::authenticated::Authenticated;
use subduction_http_longpoll::{
    connection::HttpLongPollConnection,
    session::SessionId,
    wasm_client::{WasmHttpLongPollClient, WasmLongPollTimeout},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::{peer_id::WasmPeerId, signer::JsSigner};

/// Type alias for the long-poll connection used in wasm.
pub type WasmLongPollConnection = HttpLongPollConnection<WasmLongPollTimeout>;

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
    #[wasm_bindgen(js_name = tryConnect)]
    pub async fn try_connect(
        base_url: &str,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmAuthenticatedLongPoll, LongPollConnectionError> {
        let timeout_ms = timeout_milliseconds.unwrap_or(30_000);
        let default_time_limit = Duration::from_millis(timeout_ms.into());
        let client = WasmHttpLongPollClient::new(base_url, default_time_limit);

        // Cancel channel — in the future this could be tied to a close() method.
        let (_cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

        let (authenticated, session_id) = client
            .connect(signer, expected_peer_id.clone().into(), cancel_rx)
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        Ok(WasmAuthenticatedLongPoll {
            inner: authenticated,
            session_id,
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
    #[wasm_bindgen(js_name = tryDiscover)]
    pub async fn try_discover(
        base_url: &str,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<WasmAuthenticatedLongPoll, LongPollConnectionError> {
        let timeout_ms = timeout_milliseconds.unwrap_or(30_000);
        let default_time_limit = Duration::from_millis(timeout_ms.into());
        let client = WasmHttpLongPollClient::new(base_url, default_time_limit);
        let service_name = service_name.unwrap_or_else(|| base_url.to_string());

        let (_cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

        let (authenticated, session_id) = client
            .connect_discover(signer, &service_name, cancel_rx)
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))?;

        Ok(WasmAuthenticatedLongPoll {
            inner: authenticated,
            session_id,
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
        let client = WasmHttpLongPollClient::new(base_url, default_time_limit);
        let (_cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

        client
            .connect(signer, expected_peer_id.clone().into(), cancel_rx)
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))
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
        let client = WasmHttpLongPollClient::new(base_url, default_time_limit);
        let service_name = service_name.unwrap_or_else(|| base_url.to_string());
        let (_cancel_tx, cancel_rx) = async_channel::bounded::<()>(1);

        client
            .connect_discover(signer, &service_name, cancel_rx)
            .await
            .map_err(|e| LongPollConnectionError::Connection(e.to_string()))
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
