//! Error types.

use alloc::string::{String, ToString};
use core::convert::Infallible;
use future_form::Local;
use subduction_core::{
    connection::{Connection, ConnectionDisallowed},
    subduction::error::{
        AttachError, HydrationError, IoError, ListenError, RegistrationError, WriteError,
    },
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::connection::{
    transport::WasmUnifiedTransport,
    websocket::{CallError, WebSocketAuthenticatedConnectionError},
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
pub struct WasmIoError(#[from] IoError<Local, JsStorage, WasmUnifiedTransport>);

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
pub struct WasmWriteError(#[from] WriteError<Local, JsStorage, WasmUnifiedTransport, Infallible>);

impl From<WasmWriteError> for JsValue {
    fn from(err: WasmWriteError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("WriteError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`AttachError`] type.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmAttachError(#[from] AttachError<Local, JsStorage, WasmUnifiedTransport, Infallible>);

impl From<WasmAttachError> for JsValue {
    fn from(err: WasmAttachError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("AttachError");
        js_err.into()
    }
}

/// Error connecting to a peer (handshake + registration).
#[derive(Debug, Error)]
pub enum WasmConnectError {
    /// WebSocket connection or handshake failed.
    #[error("connection failed: {0}")]
    Connection(#[from] WebSocketAuthenticatedConnectionError),

    /// Registration failed after successful handshake.
    #[error("registration failed: {0}")]
    Registration(#[from] RegistrationError<Infallible>),
}

impl From<WasmConnectError> for JsValue {
    fn from(err: WasmConnectError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ConnectError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`ConnectionDisallowed`] type.
#[derive(Debug, Clone, Error)]
#[error(transparent)]
#[allow(missing_copy_implementations)]
pub struct WasmConnectionDisallowed(#[from] ConnectionDisallowed);

impl From<WasmConnectionDisallowed> for JsValue {
    fn from(err: WasmConnectionDisallowed) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ConnectionDisallowed");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`ListenError`] type.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmListenError(#[from] ListenError<Local, JsStorage, WasmUnifiedTransport>);

impl From<WasmListenError> for JsValue {
    fn from(err: WasmListenError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("ListenError");
        js_err.into()
    }
}

/// A Wasm wrapper around the [`CallError`] type.
#[derive(Debug, Clone, Error)]
#[error(transparent)]
pub struct WasmCallError(#[from] WasmCallErrorInner);

impl From<WasmCallError> for js_sys::Error {
    fn from(err: WasmCallError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CallError");
        js_err
    }
}

impl From<CallError> for WasmCallError {
    fn from(err: CallError) -> Self {
        WasmCallError(err.into())
    }
}

impl From<&CallError> for WasmCallError {
    fn from(err: &CallError) -> Self {
        WasmCallError((err).into())
    }
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Clone, Error)]
pub enum WasmCallErrorInner {
    /// Problem encoding message.
    #[error("Problem encoding message: {0}")]
    Encoding(String),

    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),

    /// Tried to read from a canceled channel.
    #[error("Channel canceled")]
    ChannelCanceled,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    TimedOut,
}

impl From<CallError> for WasmCallErrorInner {
    fn from(err: CallError) -> Self {
        match err {
            CallError::SocketSend(e) => Self::SocketSend(e),
            CallError::ChannelCanceled => Self::ChannelCanceled,
            CallError::TimedOut => Self::TimedOut,
        }
    }
}

impl From<&CallError> for WasmCallErrorInner {
    fn from(err: &CallError) -> Self {
        match err {
            CallError::SocketSend(e) => Self::SocketSend(e.clone()),
            CallError::ChannelCanceled => Self::ChannelCanceled,
            CallError::TimedOut => Self::TimedOut,
        }
    }
}

/// An error that occurred during registration.
#[derive(Debug, Clone, Error, PartialEq, Eq, Hash)]
#[error(transparent)]
#[allow(missing_copy_implementations)]
pub struct WasmRegistrationError(RegistrationError<Infallible>);

impl From<RegistrationError<Infallible>> for WasmRegistrationError {
    fn from(err: RegistrationError<Infallible>) -> Self {
        WasmRegistrationError(err)
    }
}

impl From<WasmRegistrationError> for JsValue {
    fn from(err: WasmRegistrationError) -> Self {
        let js_err = js_sys::Error::new(&err.0.to_string());
        js_err.set_name("RegistrationError");
        js_err.into()
    }
}

/// An error that occurred during disconnection.
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmDisconnectionError(
    #[from] <WasmUnifiedTransport as Connection<Local>>::DisconnectionError,
);

impl From<WasmDisconnectionError> for JsValue {
    fn from(err: WasmDisconnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("DisconnectionError");
        err.into()
    }
}

/// Error connecting via HTTP long-poll (handshake + registration).
#[derive(Debug, Error)]
pub enum WasmLongPollConnectError {
    /// Long-poll connection or handshake failed.
    #[error("connection failed: {0}")]
    Connection(#[from] crate::connection::longpoll::LongPollConnectionError),

    /// Registration failed after successful handshake.
    #[error("registration failed: {0}")]
    Registration(#[from] RegistrationError<Infallible>),
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
