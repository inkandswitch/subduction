//! Error types.

use alloc::string::{String, ToString};
use sedimentree_core::future::Local;
use subduction_core::{
    connection::{Connection, ConnectionDisallowed},
    subduction::error::{HydrationError, IoError, ListenError, RegistrationError},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::{
    connection_callback_reader::WasmConnectionCallbackReader,
    storage::JsStorage,
    websocket::{CallError, WasmWebSocket},
};

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
pub struct WasmIoError(
    #[from] IoError<Local, JsStorage, WasmConnectionCallbackReader<WasmWebSocket>>,
);

impl From<WasmIoError> for JsValue {
    fn from(err: WasmIoError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("IoError");
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
pub struct WasmListenError(
    #[from] ListenError<Local, JsStorage, WasmConnectionCallbackReader<WasmWebSocket>>,
);

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

impl From<WasmCallError> for JsValue {
    fn from(err: WasmCallError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CallError");
        js_err.into()
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
            CallError::Encoding(e) => Self::Encoding(e.to_string()),
            CallError::SocketSend(e) => Self::SocketSend(e),
            CallError::ChannelCanceled => Self::ChannelCanceled,
            CallError::TimedOut => Self::TimedOut,
        }
    }
}

impl From<&CallError> for WasmCallErrorInner {
    fn from(err: &CallError) -> Self {
        match err {
            CallError::Encoding(e) => Self::Encoding(e.to_string()),
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
pub struct WasmRegistrationError(RegistrationError);

impl From<RegistrationError> for WasmRegistrationError {
    fn from(err: RegistrationError) -> Self {
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
    #[from] <WasmConnectionCallbackReader<WasmWebSocket> as Connection<Local>>::DisconnectionError,
);

impl From<WasmDisconnectionError> for JsValue {
    fn from(err: WasmDisconnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("DisconnectionError");
        err.into()
    }
}
