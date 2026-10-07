//! A `JsStorageError` crossing into JS keeps the backend's original exception.
//!
//! Run with `wasm-pack test --node sedimentree_wasm --test storage_error_wasm`.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs, clippy::expect_used)]

use js_sys::{BigInt, Error, JSON, Object, Reflect};
use sedimentree_wasm::storage::JsStorageError;
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_test::wasm_bindgen_test;

fn cause(err: &JsValue) -> JsValue {
    Reflect::get(err, &JsValue::from_str("cause")).expect("Reflect::get on an Error cannot fail")
}

#[wasm_bindgen_test]
fn thrown_exception_becomes_cause() {
    let original = Error::new("quota exceeded");
    let err: JsValue = JsStorageError::JsError(original.clone().into()).into();

    let name = err
        .dyn_ref::<Error>()
        .expect("JsStorageError converts to js_sys::Error")
        .name();
    assert_eq!(String::from(name), "SedimentreeStorageError");
    assert!(Object::is(&cause(&err), &original));
}

/// Matches `new Error(msg, { cause })`: serializing the error skips the
/// cause, so a cause `JSON.stringify` rejects (here a `BigInt`) is harmless.
#[wasm_bindgen_test]
fn cause_is_not_enumerable() {
    let err: JsValue = JsStorageError::JsError(BigInt::from(1).into()).into();
    let keys = Object::keys(err.unchecked_ref::<Object>());
    assert!(!keys.includes(&JsValue::from_str("cause"), 0));
    JSON::stringify(&err).expect("a non-enumerable cause is not serialized");
}

#[wasm_bindgen_test]
fn internal_failure_has_no_cause() {
    let err: JsValue = JsStorageError::NotBytes.into();
    assert!(cause(&err).is_undefined());
}
