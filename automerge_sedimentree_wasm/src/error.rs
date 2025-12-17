//! Errors

use alloc::{string::ToString, vec::Vec};
use base58::FromBase58Error;
use sedimentree_core::commit::FragmentError;
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::WasmSedimentreeAutomerge;

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

/// An error that can occur while computing fragment states.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmFragmentError(#[from] FragmentError<'static, WasmSedimentreeAutomerge>);

impl From<WasmFragmentError> for FragmentError<'static, WasmSedimentreeAutomerge> {
    fn from(err: WasmFragmentError) -> Self {
        err.0
    }
}

impl From<WasmFragmentError> for JsValue {
    fn from(err: WasmFragmentError) -> Self {
        let js_err = js_sys::Error::new(&err.0.to_string());
        js_err.set_name("FragmentError");
        js_err.into()
    }
}

/// An error that can occur when looking up a change's metadata in Wasm.
#[derive(Debug, Error)]
pub enum WasmLookupError {
    /// An invalid hash length was encountered.
    #[error("invalid hash length: expected 32 bytes, got {0:?}")]
    InvalidHashLength(Vec<u8>),

    /// A byte value was out of the valid range (0-255).
    #[error("byte value out of range: expected 0-255, got {0}")]
    ByteValueOutOfRange(f64),

    /// The expected hash was not a byte array.
    #[error("expected hash to be a byte array: got {0:?}")]
    ExpectedHashNotByteArray(JsValue),

    /// An invalid hex string was encountered.
    #[error("invalid hex string")]
    InvalidHexString,

    /// A non-numeric value was encountered where a numeric value was expected.
    #[error("expected numeric value: got {0:?}")]
    UnexpectedNonNumericValue(JsValue),

    /// Expected the `deps` field to be an array, but it wasn't.
    #[error("change metadata `deps` field is not an array")]
    DepsAreNotArray,

    /// The change metadata object is missing the `deps` method.
    #[error("object missing `deps` field")]
    NoDepsMethod,

    /// There was a problem calling `getChangeMetaByHash`.
    #[error("problem calling `getChangeMetaByHash`: {0:?}")]
    ProblemCallingGetChangeMetaByHash(JsValue),

    /// The value returned by `getChangeMetaByHash` should be an object but isn't.
    #[error("value returned by `getChangeMetaByHash` should be an object but isn't")]
    MetaShouldBeObject,
}

impl From<WasmLookupError> for JsValue {
    fn from(err: WasmLookupError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("LookupError");
        js_err.into()
    }
}
