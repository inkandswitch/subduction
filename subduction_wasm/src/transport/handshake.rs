//! Wasm-specific handshake implementations.
//!
//! This module provides [`Handshake`] trait implementations for:
//! - [`WasmWebSocketHandshake`] — operates on a raw `web_sys::WebSocket`
//! - [`JsHandshakeTransport`] — operates on a user-provided JS object that
//!   implements `HandshakeConnection` (extends `Transport` with `sendBytes`/`recvBytes`)

use alloc::{format, string::String, vec::Vec};

use future_form::{FutureForm, Local};
use futures::{channel::oneshot, future::LocalBoxFuture};
use js_sys::{Promise, Uint8Array};
use subduction_core::handshake::Handshake;
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use wasm_bindgen_futures::JsFuture;
use web_sys::{MessageEvent, WebSocket, js_sys};

use crate::error::WasmHandshakeError;

// ── WebSocket handshake ──────────────────────────────────────────────────────

/// Wrapper around `web_sys::WebSocket` implementing the [`Handshake`] trait.
///
/// This enables using [`handshake::initiate`] from `subduction_core` with
/// Wasm WebSocket connections.
#[derive(Debug)]
pub(super) struct WasmWebSocketHandshake {
    ws: WebSocket,
}

impl WasmWebSocketHandshake {
    /// Wrap a `web_sys::WebSocket` for handshake.
    ///
    /// The WebSocket must be in the OPEN state.
    pub(super) fn new(ws: WebSocket) -> Self {
        Self { ws }
    }
}

impl Handshake<Local> for WasmWebSocketHandshake {
    type Error = WasmHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            self.ws
                .send_with_u8_array(&bytes)
                .map_err(|e| WasmHandshakeError::WebSocket(format!("{e:?}")))?;
            Ok(())
        })
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Local::from_future(async move {
            // Set up oneshot channel to receive the response
            let (tx, rx) = oneshot::channel::<Result<Vec<u8>, String>>();
            let tx_cell = core::cell::RefCell::new(Some(tx));

            let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
                if let Some(tx) = tx_cell.borrow_mut().take() {
                    if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                        let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                        drop(tx.send(Ok(bytes)));
                    } else {
                        drop(tx.send(Err("expected binary message".into())));
                    }
                }
            });

            // Temporarily replace the onmessage handler
            let old_onmessage = self.ws.onmessage();
            self.ws
                .set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

            // Wait for response
            let result = rx
                .await
                .map_err(|_| WasmHandshakeError::WebSocket("channel canceled".into()))?
                .map_err(WasmHandshakeError::WebSocket)?;

            // Restore original handler (will be set up properly by setup())
            self.ws.set_onmessage(old_onmessage.as_ref());

            // Keep closure alive until we're done
            drop(onmessage);

            Ok(result)
        })
    }
}

// ── Generic JS handshake connection ──────────────────────────────────────────

#[wasm_bindgen]
extern "C" {
    /// A JS `Transport` used for the handshake phase.
    ///
    /// Any object that implements the `Transport` interface (`sendBytes`,
    /// `recvBytes`, `disconnect`) can be passed to
    /// [`AuthenticatedTransport.setup`] or [`.accept`].
    /// The same object is reused as the post-handshake transport.
    #[wasm_bindgen(js_name = Transport, typescript_type = "Transport")]
    pub type JsHandshakeTransport;

    /// Send raw bytes over the transport (handshake phase).
    #[wasm_bindgen(method, js_name = sendBytes)]
    fn js_send_bytes(this: &JsHandshakeTransport, bytes: Uint8Array) -> Promise;

    /// Receive raw bytes from the transport (handshake phase).
    #[wasm_bindgen(method, js_name = recvBytes)]
    fn js_recv_bytes(this: &JsHandshakeTransport) -> Promise;
}

impl core::fmt::Debug for JsHandshakeTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsHandshakeTransport").finish()
    }
}

impl Handshake<Local> for JsHandshakeTransport {
    type Error = WasmHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        Local::from_future(async move {
            let array = Uint8Array::from(bytes.as_slice());
            JsFuture::from(self.js_send_bytes(array))
                .await
                .map_err(|e| WasmHandshakeError::WebSocket(format!("{e:?}")))?;
            Ok(())
        })
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Local::from_future(async move {
            let value = JsFuture::from(self.js_recv_bytes())
                .await
                .map_err(|e| WasmHandshakeError::WebSocket(format!("{e:?}")))?;

            let array: Uint8Array = value.dyn_into().map_err(|v| {
                WasmHandshakeError::WebSocket(format!("expected Uint8Array, got {v:?}"))
            })?;

            Ok(array.to_vec())
        })
    }
}
