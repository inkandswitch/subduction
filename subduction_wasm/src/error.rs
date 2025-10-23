//! Error types.

use sedimentree_core::future::Local;
use subduction_core::{
    connection::ConnectionDisallowed,
    subduction::error::{IoError, ListenError},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::{
    connection_callback_reader::WasmConnectionCallbackReader,
    storage::JsStorage,
    websocket::{CallError, WasmWebSocket},
};

/// A Wasm wrapper around the [`IoError`] type.
///
/// This includes errors related to I/O operations,
/// such as networking or storage issues.
#[wasm_bindgen(js_name = IoError)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmIoError(
    #[from] IoError<Local, JsStorage, WasmConnectionCallbackReader<WasmWebSocket>>,
);

/// A Wasm wrapper around the [`ConnectionDisallowed`] type.
#[wasm_bindgen(js_name = ConnectionDisallowed)]
#[derive(Debug, Clone, Error)]
#[error(transparent)]
#[allow(missing_copy_implementations)]
pub struct WasmConnectionDisallowed(#[from] ConnectionDisallowed);

/// A Wasm wrapper around the [`ListenError`] type.
#[wasm_bindgen(js_name = ListenError)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmListenError(
    #[from] ListenError<Local, JsStorage, WasmConnectionCallbackReader<WasmWebSocket>>,
);

/// A Wasm wrapper around the [`CallError`] type.
#[wasm_bindgen(js_name = CallError)]
#[derive(Debug, Clone, Error)]
#[error(transparent)]
pub struct WasmCallError(#[from] WasmCallErrorInner);

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

    /// Tried to read from a cancelled channel.
    #[error("Channel cancelled")]
    ChannelCancelled,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    TimedOut,
}

impl From<CallError> for WasmCallErrorInner {
    fn from(err: CallError) -> Self {
        match err {
            CallError::Encoding(e) => Self::Encoding(e.to_string()),
            CallError::SocketSend(e) => Self::SocketSend(e),
            CallError::ChannelCancelled => Self::ChannelCancelled,
            CallError::TimedOut => Self::TimedOut,
        }
    }
}

impl From<&CallError> for WasmCallErrorInner {
    fn from(err: &CallError) -> Self {
        match err {
            CallError::Encoding(e) => Self::Encoding(e.to_string()),
            CallError::SocketSend(e) => Self::SocketSend(e.clone()),
            CallError::ChannelCancelled => Self::ChannelCancelled,
            CallError::TimedOut => Self::TimedOut,
        }
    }
}
