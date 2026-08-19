//! Errors.

use alloc::string::ToString;
use base58::FromBase58Error;
use thiserror::Error;
use wasm_bindgen::prelude::*;

/// An error while unpacking a base58 string to binary.
#[derive(Debug, Error)]
#[error("FromBase58Error: {0:?}")]
pub struct WasmFromBase58Error(FromBase58Error);

impl From<FromBase58Error> for WasmFromBase58Error {
    fn from(err: FromBase58Error) -> Self {
        Self(err)
    }
}

impl From<WasmFromBase58Error> for FromBase58Error {
    fn from(err: WasmFromBase58Error) -> Self {
        err.0
    }
}

impl From<WasmFromBase58Error> for JsValue {
    fn from(err: WasmFromBase58Error) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("FromBase58Error");
        js_err.into()
    }
}
