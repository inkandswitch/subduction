//! Baseline coverage for `MessagePortTransport::on_disconnect`.

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]

use js_sys::{Function, Reflect};
use subduction_wasm::transport::message_port::WasmMessagePortTransport;
use wasm_bindgen::{JsCast, JsValue, prelude::Closure};
use wasm_bindgen_test::wasm_bindgen_test;

fn make_message_channel() -> JsValue {
    let global = js_sys::global();
    let ctor = Reflect::get(&global, &JsValue::from_str("MessageChannel"))
        .expect("globalThis.MessageChannel must exist (Node ≥15 / modern browsers)");
    let ctor_fn: &Function = ctor
        .dyn_ref()
        .expect("MessageChannel must be a constructor function");
    js_sys::Reflect::construct(ctor_fn, &js_sys::Array::new())
        .expect("MessageChannel constructor must succeed")
}

fn port_of(channel: &JsValue, name: &str) -> JsValue {
    Reflect::get(channel, &JsValue::from_str(name))
        .unwrap_or_else(|_| panic!("MessageChannel.{name} should be a MessagePort"))
}

#[wasm_bindgen_test]
fn disconnect_fires_callback() {
    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = WasmMessagePortTransport::new(port1);

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

#[wasm_bindgen_test]
fn disconnect_with_no_callback_does_not_panic() {
    let channel = make_message_channel();
    let port1 = port_of(&channel, "port1");
    let transport = WasmMessagePortTransport::new(port1);

    let _promise = transport.disconnect();
}

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
