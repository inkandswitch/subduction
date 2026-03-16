//! Error types.

use crate::policy::JsPolicyDenied;
use alloc::string::{String, ToString};
use future_form::Local;
use subduction_core::{
    connection::message::SyncMessage,
    subduction::error::{AddConnectionError, HydrationError, IoError, ListenError, WriteError},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::transport::{
    longpoll::LongPollTransportError, websocket::WebSocketAuthenticatedTransportError,
    JsTransportError, WasmTransport,
};
use sedimentree_wasm::storage::JsStorage;

/// A Wasm wrapper around the [`HydrationError`] type.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmHydrationError(#[from] HydrationError<Local, JsStorage>);

impl From<WasmHydrationError> for JsValue {
    fn from(err: WasmHydrationError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("HydrationError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`IoError`] type.
///
/// This includes errors related to I/O operations,
/// such as networking or storage issues.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmIoError(#[from] IoError<Local, JsStorage, WasmTransport, SyncMessage>);

impl From<WasmIoError> for JsValue {
    fn from(err: WasmIoError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("IoError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`WriteError`] type.
///
/// This includes errors related to write operations,
/// including policy rejections.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmWriteError(
    #[from] WriteError<Local, JsStorage, WasmTransport, SyncMessage, JsPolicyDenied>,
);

impl From<WasmWriteError> for JsValue {
    fn from(err: WasmWriteError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("WriteError");
        js_err.into()
    }
}

/// Error connecting to a peer (handshake + add connection).
#[derive(Debug, Error)]
pub enum WasmConnectError {
    /// WebSocket transport or handshake failed.
    #[error("transport failed: {0}")]
    Transport(#[from] WebSocketAuthenticatedTransportError),

    /// Adding the connection failed after successful handshake.
    #[error("add connection failed: {0}")]
    AddConnection(#[from] AddConnectionError<JsPolicyDenied>),
}

impl From<WasmConnectError> for JsValue {
    fn from(err: WasmConnectError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ConnectError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`ListenError`] type.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmListenError(#[from] ListenError<Local, JsStorage, WasmTransport, SyncMessage>);

impl From<WasmListenError> for JsValue {
    fn from(err: WasmListenError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ListenError");
        js_err.into()
    }
}

/// An error that occurred when adding a connection.
#[derive(Debug, Clone, Error, PartialEq, Eq, Hash)]
#[error(transparent)]
#[allow(missing_copy_implementations)]
pub struct WasmAddConnectionError(AddConnectionError<JsPolicyDenied>);

impl From<AddConnectionError<JsPolicyDenied>> for WasmAddConnectionError {
    fn from(err: AddConnectionError<JsPolicyDenied>) -> Self {
        WasmAddConnectionError(err)
    }
}

impl From<WasmAddConnectionError> for JsValue {
    fn from(err: WasmAddConnectionError) -> Self {
        let js_err = js_sys::Error::new(&err.0.to_string());
        js_err.set_name("AddConnectionError");
        js_err.into()
    }
}

/// An error that occurred during disconnection.
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmDisconnectionError(#[from] JsTransportError);

impl From<WasmDisconnectionError> for JsValue {
    fn from(err: WasmDisconnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("DisconnectionError");
        err.into()
    }
}

/// Error connecting via HTTP long-poll (handshake + add connection).
#[derive(Debug, Error)]
pub enum WasmLongPollConnectError {
    /// Long-poll transport or handshake failed.
    #[error("transport failed: {0}")]
    Transport(#[from] LongPollTransportError),

    /// Adding the connection failed after successful handshake.
    #[error("add connection failed: {0}")]
    AddConnection(#[from] AddConnectionError<JsPolicyDenied>),
}

impl From<WasmLongPollConnectError> for JsValue {
    fn from(err: WasmLongPollConnectError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("LongPollConnectError");
        js_err.into()
    }
}

/// Error returned when a handshake fails.
#[derive(Debug, Error)]
pub enum WasmHandshakeError {
    /// WebSocket error during handshake.
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Invalid message received during handshake.
    #[error("invalid handshake message: {0}")]
    InvalidMessage(String),

    /// Server rejected the handshake.
    #[error("handshake rejected: {0}")]
    Rejected(String),

    /// Signature verification failed.
    #[error("signature verification failed")]
    InvalidSignature,

    /// Response doesn't match our challenge.
    #[error("response doesn't match challenge")]
    ChallengeMismatch,
}

impl From<WasmHandshakeError> for JsValue {
    fn from(err: WasmHandshakeError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("HandshakeError");
        js_err.into()
    }
}
