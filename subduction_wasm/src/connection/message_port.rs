//! [`MessagePort`]-based [`HandshakeConnection`] adapter.
//!
//! Provides [`MessagePortConnection`] — a Wasm-exported class that wraps a
//! [`MessagePort`] (or any object with the same `postMessage` / `onmessage`
//! interface) into a [`HandshakeConnection`] suitable for peer-to-peer
//! authentication via [`AuthenticatedConnection.setup`] / `.accept`.
//!
//! # Example (JavaScript)
//!
//! ```js
//! const { MessagePortConnection, AuthenticatedConnection } = await import("subduction_wasm");
//!
//! const channel = new MessageChannel();
//! const connA = new MessagePortConnection(channel.port1);
//! const connB = new MessagePortConnection(channel.port2);
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
// MessagePort bindings
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

fn resolved_void() -> Promise {
    Promise::resolve(&JsValue::UNDEFINED)
}

fn rejected(msg: &str) -> Promise {
    Promise::reject(&JsValue::from_str(msg))
}

// ---------------------------------------------------------------------------
// Exported struct
// ---------------------------------------------------------------------------

/// A [`HandshakeConnection`] backed by a `MessagePort` (or any object with
/// `postMessage` / `onmessage` / `close`).
///
/// Implements the full `HandshakeConnection` interface (`sendBytes`,
/// `recvBytes`, `send`, `recv`, `disconnect`) using the port for transport.
///
/// `nextRequestId` and `call` return rejected promises — `MessagePort`
/// connections are used for peer-to-peer handshakes, not RPC-style sync.
/// After the handshake, the [`Authenticated`] wrapper provides the sync API.
#[wasm_bindgen(js_name = MessagePortConnection)]
pub struct WasmMessagePortConnection {
    port: Rc<Port>,
    queue: SharedQueue,
    _onmessage: Closure<dyn FnMut(JsValue)>,
}

impl core::fmt::Debug for WasmMessagePortConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessagePortConnection")
            .finish_non_exhaustive()
    }
}

#[wasm_bindgen(js_class = MessagePortConnection)]
impl WasmMessagePortConnection {
    /// Create a new connection wrapping the given `MessagePort`.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(port: JsValue) -> Self {
        let port: Port = port.unchecked_into();

        let queue: SharedQueue = Rc::new(RefCell::new(MessageQueue {
            incoming: Vec::new(),
            waiters: Vec::new(),
        }));

        let q = Rc::clone(&queue);
        let onmessage: Closure<dyn FnMut(JsValue)> = Closure::new(move |event: JsValue| {
            let data = js_sys::Reflect::get(&event, &JsValue::from_str("data"))
                .unwrap_or(JsValue::UNDEFINED);

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

        Self {
            port: Rc::new(port),
            queue,
            _onmessage: onmessage,
        }
    }

    /// Send raw bytes (for the handshake phase).
    #[wasm_bindgen(js_name = sendBytes)]
    pub fn send_bytes(&self, bytes: &[u8]) -> Promise {
        let buffer = Uint8Array::from(bytes).buffer();
        self.port.post_message(&buffer);
        resolved_void()
    }

    /// Receive raw bytes (for the handshake phase).
    #[wasm_bindgen(js_name = recvBytes)]
    pub fn recv_bytes(&self) -> Promise {
        let mut guard = self.queue.borrow_mut();
        if !guard.incoming.is_empty() {
            let data = guard.incoming.remove(0);
            let arr: JsValue = Uint8Array::new(&data).into();
            return Promise::resolve(&arr);
        }
        drop(guard);

        let q = Rc::clone(&self.queue);
        Promise::new(&mut |resolve, _reject| {
            let resolve_with_u8arr: Closure<dyn FnMut(JsValue)> =
                Closure::new(move |data: JsValue| {
                    drop(resolve.call1(&JsValue::NULL, &Uint8Array::new(&data)));
                });
            q.borrow_mut().waiters.push(
                resolve_with_u8arr
                    .as_ref()
                    .unchecked_ref::<Function>()
                    .clone(),
            );
            resolve_with_u8arr.forget();
        })
    }

    /// Disconnect (close the port).
    pub fn disconnect(&self) -> Promise {
        self.port.close();
        resolved_void()
    }

    /// Send a structured message (post-handshake).
    pub fn send(&self, message: &JsValue) -> Promise {
        self.port.post_message(message);
        resolved_void()
    }

    /// Receive the next message.
    pub fn recv(&self) -> Promise {
        dequeue_or_wait(&self.queue)
    }

    /// Not supported on `MessagePort` connections.
    #[wasm_bindgen(js_name = nextRequestId)]
    pub fn next_request_id(&self) -> Promise {
        rejected("nextRequestId is not supported on MessagePort connections")
    }

    /// Not supported on `MessagePort` connections.
    pub fn call(&self, _request: &JsValue, _timeout_ms: Option<f64>) -> Promise {
        rejected("call is not supported on MessagePort connections")
    }
}

/// Convenience factory — equivalent to `new MessagePortConnection(port)`.
#[must_use]
#[wasm_bindgen(js_name = makeMessagePortConnection)]
pub fn make_message_port_connection(port: JsValue) -> WasmMessagePortConnection {
    WasmMessagePortConnection::new(port)
}
