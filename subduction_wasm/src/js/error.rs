//! Error types.

use sedimentree_core::{future::Local, storage::MemoryStorage};
use subduction_core::{
    connection::ConnectionDisallowed,
    subduction::error::{IoError, ListenError},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::websocket::JsWebSocket;

/// A Wasm wrapper around the [`IoError`] type.
///
/// This includes errors related to I/O operations,
/// such as networking or storage issues.
#[wasm_bindgen(js_name = IoError)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct JsIoError(#[from] IoError<Local, MemoryStorage, JsWebSocket>);

/// A Wasm wrapper around the [`ConnectionDisallowed`] type.
#[wasm_bindgen(js_name = ConnectionDisallowed)]
#[derive(Debug, Copy, Clone, Error)]
#[error(transparent)]
pub struct JsConnectionDisallowed(#[from] ConnectionDisallowed);

/// A Wasm wrapper around the [`ListenError`] type.
#[wasm_bindgen(js_name = ListenError)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct JsListenError(#[from] ListenError<Local, MemoryStorage, JsWebSocket>);
