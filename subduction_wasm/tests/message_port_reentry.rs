//! Regression coverage for the `MessagePortTransport::on_disconnect`
//! re-entrancy fix (Cell instead of RefCell).
//!
//! Pre-fix, `disconnect()` held a `RefMut` on `on_disconnect` across
//! the JS callback's invocation (Rust 2024 let-chain temporary scope
//! extends to the end of the surrounding `if` block). A JS callback
//! that re-entered the same `MessagePortTransport` (to re-register
//! via `onDisconnect(...)`, or to call `disconnect()` recursively)
//! would trip a `BorrowMutError` panic, surfacing in production as
//! `RuntimeError: unreachable executed`.
//!
//! Post-fix, the field is `Cell<Option<Function>>` and `disconnect`
//! uses `Cell::take` (which doesn't borrow). Re-entry is
//! structurally impossible to trigger that bug class.
//!
//! These tests construct a real Node `MessageChannel`, wrap one port
//! in `MessagePortTransport`, register an `onDisconnect` callback
//! that itself calls back into the same transport, and assert no
//! panic occurs.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use js_sys::{Function, Reflect};
use subduction_wasm::transport::message_port::WasmMessagePortTransport;
use wasm_bindgen::{JsCast, JsValue, prelude::Closure};
use wasm_bindgen_test::wasm_bindgen_test;

/// Obtain `globalThis.MessageChannel`, available in Node ≥15 and all
/// modern browsers. Returns the constructed channel with `port1` and
/// `port2` properties.
fn make_message_channel() -> JsValue {
    let global = js_sys::global();
    let ctor = Reflect::get(&global, &JsValue::from_str("MessageChannel"))
        .expect("globalThis.MessageChannel must exist (Node ≥15 / modern browsers)");
    let ctor_fn: &Function = ctor
        .dyn_ref()
        .expect("MessageChannel must be a constructor function");
    js_sys::Reflect::construct(ctor_fn, &js_sys::Array::new())
        .expect("MessageChannel constructor must succeed")
        .into()
}

fn port_of(channel: &JsValue, name: &str) -> JsValue {
    Reflect::get(channel, &JsValue::from_str(name))
        .unwrap_or_else(|_| panic!("MessageChannel.{name} should be a MessagePort"))
}

/// Baseline: disconnect with a registered callback fires the callback
/// and doesn't panic.
#[wasm_bindgen_test]
fn disconnect_fires_callback() {
    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = WasmMessagePortTransport::new(port1);

    // A simple closure that flips a flag in a JS object. We can't
    // observe Rust-side state from a JS callback easily, so we mutate
    // a JS-side object.
    let observed = js_sys::Object::new();
    let observed_clone = observed.clone();
    let cb_closure: Closure<dyn FnMut()> = Closure::new(move || {
        Reflect::set(&observed_clone, &"fired".into(), &JsValue::TRUE).ok();
    });
    let cb_fn: &Function = cb_closure.as_ref().unchecked_ref();
    transport.on_disconnect(cb_fn);
    cb_closure.forget();

    let _promise = transport.disconnect();

    let fired = Reflect::get(&observed, &"fired".into()).unwrap_or(JsValue::UNDEFINED);
    assert_eq!(fired, JsValue::TRUE, "disconnect should fire the callback");
}

/// The bug fix: an `onDisconnect` callback that re-enters the
/// transport via `onDisconnect(otherCb)` to re-register must not
/// panic. Pre-fix this tripped `BorrowMutError`.
#[wasm_bindgen_test]
fn disconnect_callback_can_reregister_without_panic() {
    use std::{cell::RefCell, rc::Rc};

    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    // Rc so we can share with the callback. The transport itself is
    // not Clone, but the JS side holds it via wasm-bindgen anyway.
    let transport = Rc::new(WasmMessagePortTransport::new(port1));

    // Set up the re-registration target so the callback can call it.
    let transport_for_cb = Rc::clone(&transport);
    let second_called = Rc::new(RefCell::new(false));
    let second_called_clone = Rc::clone(&second_called);

    // A second-stage callback registered from inside the first.
    let second_cb: Closure<dyn FnMut()> = Closure::new(move || {
        *second_called_clone.borrow_mut() = true;
    });
    let second_fn: Function = second_cb.as_ref().unchecked_ref::<Function>().clone();
    second_cb.forget();

    let first_cb: Closure<dyn FnMut()> = Closure::new(move || {
        // Re-enter the transport's onDisconnect to register a new
        // callback. Pre-fix this would panic with BorrowMutError
        // because disconnect() held a RefMut across this call.
        transport_for_cb.on_disconnect(&second_fn);
    });
    transport.on_disconnect(first_cb.as_ref().unchecked_ref::<Function>());
    first_cb.forget();

    // Should not panic.
    let _first_disconnect = transport.disconnect();

    // First callback fired (and re-registered second). Second callback
    // is registered but not yet fired — that requires another disconnect.
    assert!(
        !*second_called.borrow(),
        "second callback should be registered but not yet invoked"
    );

    // A second disconnect fires the re-registered callback. Also
    // must not panic.
    let _second_disconnect = transport.disconnect();
    assert!(
        *second_called.borrow(),
        "second callback should fire on second disconnect"
    );
}

/// Re-entrant `disconnect()` from inside the callback. Idempotent on
/// the JS side (the port is already closed); the important property
/// is that we don't panic.
#[wasm_bindgen_test]
fn disconnect_callback_can_call_disconnect_recursively_without_panic() {
    use std::rc::Rc;

    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = Rc::new(WasmMessagePortTransport::new(port1));

    let transport_for_cb = Rc::clone(&transport);
    let cb: Closure<dyn FnMut()> = Closure::new(move || {
        // Re-enter disconnect. Cell::take has already cleared the
        // callback slot (we're inside the only invocation), so this
        // is a no-op for callback firing. It must not panic.
        let _inner = transport_for_cb.disconnect();
    });
    transport.on_disconnect(cb.as_ref().unchecked_ref::<Function>());
    cb.forget();

    // Must not panic.
    let _outer = transport.disconnect();
}

/// Disconnect with no callback registered is a no-op and must not
/// panic. (Regression guard: Cell::take returns None cleanly.)
#[wasm_bindgen_test]
fn disconnect_with_no_callback_does_not_panic() {
    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = WasmMessagePortTransport::new(port1);

    let _promise = transport.disconnect();
}

/// Calling disconnect twice (with a callback registered the first
/// time, none the second time) must not panic. The first call
/// `take`s the callback; the second call finds an empty slot.
#[wasm_bindgen_test]
fn disconnect_twice_does_not_panic() {
    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = WasmMessagePortTransport::new(port1);

    let cb: Closure<dyn FnMut()> = Closure::new(|| {});
    transport.on_disconnect(cb.as_ref().unchecked_ref::<Function>());
    cb.forget();

    let _first = transport.disconnect();
    let _second = transport.disconnect();
}
