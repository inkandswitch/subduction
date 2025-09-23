use sedimentree_core::{future::Local, storage::MemoryStorage};
use subduction_core::subduction::error::IoError;
use thiserror::Error;
use wasm_bindgen::prelude::*;

use super::websocket::JsWebSocket;

#[wasm_bindgen(js_name = IoError)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct JsIoError(IoError<Local, MemoryStorage, JsWebSocket>);

impl From<IoError<Local, MemoryStorage, JsWebSocket>> for JsIoError {
    fn from(e: IoError<Local, MemoryStorage, JsWebSocket>) -> Self {
        Self(e)
    }
}

impl From<JsIoError> for IoError<Local, MemoryStorage, JsWebSocket> {
    fn from(e: JsIoError) -> Self {
        e.0
    }
}
