//! Wasm-specific handshake implementation for WebSocket connections.
//!
//! This module provides the [`WasmWebSocketHandshake`] type which implements
//! the [`Handshake`] trait for Wasm WebSocket connections.

use alloc::{format, string::String, vec::Vec};

use future_form::{FutureForm, Local};
use futures::{channel::oneshot, future::LocalBoxFuture};
use subduction_core::connection::handshake::Handshake;
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{MessageEvent, WebSocket, js_sys};

use crate::error::WasmHandshakeError;

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
