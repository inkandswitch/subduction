//! [`MessagePort`]-based [`Transport`] adapter.
//!
//! Provides [`MessagePortTransport`] — a Wasm-exported class that wraps a
//! [`MessagePort`] (or any object with the same `postMessage` / `onmessage`
//! interface) into a `Transport` suitable for peer-to-peer authentication
//! via [`AuthenticatedTransport.setup`] / `.accept`.
//!
//! # Example (JavaScript)
//!
//! ```js
//! const { MessagePortTransport, AuthenticatedTransport } = await import("subduction_wasm");
//!
//! const channel = new MessageChannel();
//! const transportA = new MessagePortTransport(channel.port1);
//! const transportB = new MessagePortTransport(channel.port2);
//!
//! const [authA, authB] = await Promise.all([
//!   AuthenticatedTransport.setup(transportA, signerA, 5000),
//!   AuthenticatedTransport.accept(transportB, signerB, nonceCache, 5000),
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

fn resolved_void() -> Promise {
    Promise::resolve(&JsValue::UNDEFINED)
}

// ---------------------------------------------------------------------------
// Exported struct
// ---------------------------------------------------------------------------

/// A `Transport` backed by a `MessagePort` (or any object with
/// `postMessage` / `onmessage` / `close`).
///
/// Implements the byte-oriented `Transport` interface (`sendBytes`,
/// `recvBytes`, `disconnect`) using the port as the underlying channel.
/// After the handshake, the [`Authenticated`] wrapper provides the sync API.
#[wasm_bindgen(js_name = MessagePortTransport)]
pub struct WasmMessagePortTransport {
    port: Rc<Port>,
    queue: SharedQueue,
    _onmessage: Closure<dyn FnMut(JsValue)>,
}

impl core::fmt::Debug for WasmMessagePortTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessagePortTransport")
            .finish_non_exhaustive()
    }
}

#[wasm_bindgen(js_class = MessagePortTransport)]
impl WasmMessagePortTransport {
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
}

/// Convenience factory — equivalent to `new MessagePortTransport(port)`.
#[must_use]
#[wasm_bindgen(js_name = makeMessagePortTransport)]
pub fn make_message_port_transport(port: JsValue) -> WasmMessagePortTransport {
    WasmMessagePortTransport::new(port)
}
