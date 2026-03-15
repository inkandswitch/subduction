//! [`MessagePort`]-based [`HandshakeConnection`] adapter.
//!
//! Provides [`makeMessagePortConnection`] — a JS-callable factory that wraps a
//! [`MessagePort`] (or any object with the same `postMessage` / `onmessage`
//! interface) into a [`HandshakeConnection`] suitable for peer-to-peer
//! authentication via [`AuthenticatedConnection.setup`] / `.accept`.
//!
//! # Example (JavaScript)
//!
//! ```js
//! const { MessageChannel } = globalThis;
//! const { makeMessagePortConnection, AuthenticatedConnection } = await import("subduction_wasm");
//!
//! const channel = new MessageChannel();
//! const connA = makeMessagePortConnection(channel.port1);
//! const connB = makeMessagePortConnection(channel.port2);
//!
//! const [authA, authB] = await Promise.all([
//!   AuthenticatedConnection.setup(connA, signerA, 5000),
//!   AuthenticatedConnection.accept(connB, signerB, nonceCache, 5000),
//! ]);
//! ```

use alloc::{rc::Rc, vec::Vec};
use core::cell::RefCell;

use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// MessagePort bindings (works with any object that has the same interface)
// ---------------------------------------------------------------------------

#[wasm_bindgen]
extern "C" {
    /// A `MessagePort`-like JS object.
    type Port;

    #[wasm_bindgen(method, js_name = postMessage)]
    fn post_message(this: &Port, data: &JsValue);

    #[wasm_bindgen(method)]
    fn close(this: &Port);

    #[wasm_bindgen(method, setter, js_name = onmessage)]
    fn set_onmessage(this: &Port, handler: &Closure<dyn FnMut(JsValue)>);
}

#[wasm_bindgen(typescript_custom_section)]
const TS: &str = r#"
/**
 * Wrap a `MessagePort` (or any object with `postMessage`/`onmessage`) into a
 * `HandshakeConnection` for use with `AuthenticatedConnection.setup` / `.accept`.
 */
export function makeMessagePortConnection(port: MessagePort): HandshakeConnection;
"#;

// ---------------------------------------------------------------------------
// Shared message queue
// ---------------------------------------------------------------------------

struct MessageQueue {
    incoming: Vec<JsValue>,
    waiters: Vec<Function>,
}

type SharedQueue = Rc<RefCell<MessageQueue>>;

fn dequeue_or_wait(queue: &SharedQueue) -> Promise {
    let mut q = queue.borrow_mut();
    if !q.incoming.is_empty() {
        return Promise::resolve(&q.incoming.remove(0));
    }
    drop(q);

    let queue = Rc::clone(queue);
    Promise::new(&mut |resolve, _reject| {
        queue.borrow_mut().waiters.push(resolve);
    })
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Wrap a `MessagePort` into a [`HandshakeConnection`].
///
/// The returned object implements the full `HandshakeConnection` interface
/// (`sendBytes`, `recvBytes`, `send`, `recv`, `disconnect`) using the
/// port's `postMessage` / `onmessage` for transport.
///
/// `nextRequestId` and `call` return rejected promises — `MessagePort`
/// connections are used for peer-to-peer handshakes, not RPC-style sync.
/// After the handshake, the [`Authenticated`] wrapper provides the sync API.
///
/// # Errors
///
/// Returns a JS error if building the connection object fails.
#[wasm_bindgen(js_name = makeMessagePortConnection)]
pub fn make_message_port_connection(port: JsValue) -> Result<JsValue, JsValue> {
    let port: Port = port.unchecked_into();
    let queue: SharedQueue = Rc::new(RefCell::new(MessageQueue {
        incoming: Vec::new(),
        waiters: Vec::new(),
    }));

    // Install onmessage handler
    let q = Rc::clone(&queue);
    let onmessage: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
        let data =
            js_sys::Reflect::get(&event, &JsValue::from_str("data")).unwrap_or(JsValue::UNDEFINED);

        let mut guard = q.borrow_mut();
        if guard.waiters.is_empty() {
            guard.incoming.push(data);
        } else {
            let resolve = guard.waiters.remove(0);
            drop(guard);
            drop(resolve.call1(&JsValue::NULL, &data));
        }
    });
    port.set_onmessage(&onmessage);
    onmessage.forget();

    let conn = js_sys::Object::new();
    let port = Rc::new(port);

    // sendBytes(bytes: Uint8Array) -> Promise<void>
    let p = Rc::clone(&port);
    set_method(
        &conn,
        "sendBytes",
        Closure::<dyn FnMut(JsValue) -> JsValue>::new(move |bytes: JsValue| {
            let buffer =
                js_sys::Reflect::get(&bytes, &JsValue::from_str("buffer")).unwrap_or(bytes);
            p.post_message(&buffer);
            resolved_void()
        }),
    )?;

    // recvBytes() -> Promise<Uint8Array>
    let q = Rc::clone(&queue);
    set_method(
        &conn,
        "recvBytes",
        Closure::<dyn FnMut() -> JsValue>::new(move || {
            let mut guard = q.borrow_mut();
            if !guard.incoming.is_empty() {
                let data = guard.incoming.remove(0);
                return Promise::resolve(&Uint8Array::new(&data)).into();
            }
            drop(guard);

            let q_inner = Rc::clone(&q);
            Promise::new(&mut |resolve, _reject| {
                let resolve_with_u8arr: Closure<dyn FnMut(JsValue)> =
                    Closure::new(move |data: JsValue| {
                        drop(resolve.call1(&JsValue::NULL, &Uint8Array::new(&data)));
                    });
                q_inner.borrow_mut().waiters.push(
                    resolve_with_u8arr
                        .as_ref()
                        .unchecked_ref::<Function>()
                        .clone(),
                );
                resolve_with_u8arr.forget();
            })
            .into()
        }),
    )?;

    // disconnect() -> Promise<void>
    let p = Rc::clone(&port);
    set_method(
        &conn,
        "disconnect",
        Closure::<dyn FnMut() -> JsValue>::new(move || {
            p.close();
            resolved_void()
        }),
    )?;

    // send(message) -> Promise<void>
    let p = Rc::clone(&port);
    set_method(
        &conn,
        "send",
        Closure::<dyn FnMut(JsValue) -> JsValue>::new(move |message: JsValue| {
            p.post_message(&message);
            resolved_void()
        }),
    )?;

    // recv() -> Promise<any>
    let q = Rc::clone(&queue);
    set_method(
        &conn,
        "recv",
        Closure::<dyn FnMut() -> JsValue>::new(move || dequeue_or_wait(&q).into()),
    )?;

    // nextRequestId() -> Promise (rejects)
    set_method(
        &conn,
        "nextRequestId",
        Closure::<dyn FnMut() -> JsValue>::new(|| {
            rejected("nextRequestId is not supported on MessagePort connections")
        }),
    )?;

    // call() -> Promise (rejects)
    set_method(
        &conn,
        "call",
        Closure::<dyn FnMut() -> JsValue>::new(|| {
            rejected("call is not supported on MessagePort connections")
        }),
    )?;

    Ok(conn.into())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolved_void() -> JsValue {
    Promise::resolve(&JsValue::UNDEFINED).into()
}

fn rejected(msg: &str) -> JsValue {
    Promise::reject(&JsValue::from_str(msg)).into()
}

fn set_method<F: wasm_bindgen::closure::WasmClosure + ?Sized>(
    obj: &js_sys::Object,
    name: &str,
    closure: Closure<F>,
) -> Result<(), JsValue> {
    js_sys::Reflect::set(obj, &JsValue::from_str(name), closure.as_ref())?;
    closure.forget();
    Ok(())
}
