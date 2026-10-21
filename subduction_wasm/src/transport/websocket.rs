//! Private WebSocket byte-transport driver. Authentication belongs to the node,
//! using core witnesses; there are no JS authenticated-transport/value classes.
use crate::transport::OnDisconnect;
use alloc::{rc::Rc, string::ToString, vec::Vec};
use core::cell::RefCell;
use future_form::Local;
use futures::{FutureExt, channel::oneshot, future::LocalBoxFuture};
use subduction_core::transport::Transport;
use thiserror::Error;
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};
use web_sys::{BinaryType, Event, MessageEvent, Url, WebSocket, js_sys};

/// Private driver holding the socket, buffers, and owned event callbacks.
#[wasm_bindgen(js_name = SubductionWebSocket, private, skip_typescript)]
#[derive(Debug, Clone)]
pub struct WasmWebSocket {
    socket: WebSocket,
    inbound_reader: async_channel::Receiver<Vec<u8>>,
    inbound_closer: Rc<async_channel::Sender<Vec<u8>>>,
    on_disconnect: OnDisconnect,
    callbacks: Rc<SocketCallbacks>,
}
#[derive(Debug)]
struct SocketCallbacks {
    socket: WebSocket,
    _message: Closure<dyn FnMut(MessageEvent)>,
    _close: Closure<dyn FnMut(Event)>,
}
impl SocketCallbacks {
    fn detach(&self) {
        self.socket.set_onmessage(None);
        self.socket.set_onclose(None);
    }
}
impl Drop for SocketCallbacks {
    fn drop(&mut self) {
        self.detach();
        drop(self.socket.close());
    }
}
struct OpeningSocket {
    socket: WebSocket,
    opened: bool,
}
impl Drop for OpeningSocket {
    fn drop(&mut self) {
        self.socket.set_onopen(None);
        self.socket.set_onerror(None);
        if !self.opened {
            drop(self.socket.close());
        }
    }
}
impl WasmWebSocket {
    /// Cancellation-safe opening before authentication and registration.
    pub(crate) async fn open(address: &Url) -> Result<Self, WebSocketAuthenticatedTransportError> {
        let ws = WebSocket::new(&address.href())
            .map_err(WebSocketAuthenticatedTransportError::SocketCreationFailed)?;
        Ok(Self::from_open_socket(Self::wait_for_open(ws).await?))
    }
    fn from_open_socket(ws: WebSocket) -> Self {
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Vec<u8>>(64);
        let inbound_writer = Rc::new(inbound_writer);
        let on_disconnect = OnDisconnect::default();
        let msg_writer = inbound_writer.clone();
        let overflow_socket = ws.clone();
        let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes = js_sys::Uint8Array::new(&buf).to_vec();
                // Browser callbacks cannot await backpressure. Disconnect on
                // overflow rather than silently losing frames or spawning tasks.
                if msg_writer.try_send(bytes).is_err() {
                    msg_writer.close();
                    drop(overflow_socket.close());
                }
            } else {
                tracing::error!(
                    "unexpected message event (not ArrayBuffer): {:?}",
                    event.data()
                );
            }
        });
        let close_writer = inbound_writer.clone();
        let close_callback = on_disconnect.clone();
        let onclose = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            close_writer.close();
            close_callback.take_and_fire();
        });
        ws.set_binary_type(BinaryType::Arraybuffer);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
        let callbacks = Rc::new(SocketCallbacks {
            socket: ws.clone(),
            _message: onmessage,
            _close: onclose,
        });
        Self {
            socket: ws,
            inbound_reader,
            inbound_closer: inbound_writer,
            on_disconnect,
            callbacks,
        }
    }
    async fn wait_for_open(
        ws: WebSocket,
    ) -> Result<WebSocket, WebSocketAuthenticatedTransportError> {
        let (open_tx, open_rx) = oneshot::channel::<Result<(), WebSocketTransportFailed>>();
        let open_tx_cell = Rc::new(RefCell::new(Some(open_tx)));
        let open_tx_clone = open_tx_cell.clone();
        let onopen = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = open_tx_clone.borrow_mut().take() {
                let _ = tx.send(Ok(()));
            }
        });
        let onerror_tx = open_tx_cell.clone();
        let onerror = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            if let Some(tx) = onerror_tx.borrow_mut().take() {
                let _ = tx.send(Err(WebSocketTransportFailed));
            }
        });
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
        if ws.ready_state() == WebSocket::OPEN
            && let Some(tx) = open_tx_cell.borrow_mut().take()
        {
            let _ = tx.send(Ok(()));
        }
        // Detach before dropping closures, and close a cancelled dial.
        let mut guard = OpeningSocket {
            socket: ws.clone(),
            opened: false,
        };
        open_rx
            .await
            .map_err(|_| WebSocketAuthenticatedTransportError::Canceled)??;
        guard.opened = true;
        drop(guard);
        drop(onopen);
        drop(onerror);
        Ok(ws)
    }
}
#[wasm_bindgen(js_class = SubductionWebSocket)]
impl WasmWebSocket {
    /// Send a frame.
    /// # Errors
    /// Propagates socket send failures.
    #[wasm_bindgen(js_name = sendBytes)]
    pub async fn send_bytes(&self, bytes: &[u8]) -> Result<(), WasmSendError> {
        Transport::<Local>::send_bytes(self, bytes).await?;
        Ok(())
    }
    /// Receive a frame.
    /// # Errors
    /// Returns an error when the channel closes.
    #[wasm_bindgen(js_name = recvBytes)]
    pub async fn recv_bytes(&self) -> Result<js_sys::Uint8Array, ReadFromClosedChannel> {
        let bytes = Transport::<Local>::recv_bytes(self).await?;
        Ok(js_sys::Uint8Array::from(bytes.as_slice()))
    }
    /// Disconnect and notify the runtime observer.
    #[wasm_bindgen(js_name = disconnect)]
    pub async fn wasm_disconnect(&self) {
        let _ = Transport::<Local>::disconnect(self).await;
    }
    /// Install the runtime's disconnect observer.
    #[wasm_bindgen(js_name = onDisconnect)]
    pub fn on_disconnect(&self, callback: js_sys::Function) {
        self.on_disconnect.set(Closure::<dyn Fn()>::new(move || {
            if let Err(e) = callback.call0(&JsValue::NULL) {
                tracing::error!("onDisconnect callback threw: {e:?}");
            }
        }));
    }
}
impl PartialEq for WasmWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.socket == other.socket
    }
}
impl Transport<Local> for WasmWebSocket {
    type SendError = SendError;
    type RecvError = ReadFromClosedChannel;
    type DisconnectionError = DisconnectionError;
    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let data = bytes.to_vec();
        async move {
            self.socket
                .send_with_u8_array(&data)
                .map_err(SendError::SocketSend)?;
            Ok(())
        }
        .boxed_local()
    }
    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        async {
            self.inbound_reader
                .recv()
                .await
                .map_err(|_| ReadFromClosedChannel)
        }
        .boxed_local()
    }
    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async {
            self.callbacks.detach();
            if let Err(e) = self.socket.close() {
                tracing::debug!("WebSocket::close() failed (may already be closed): {e:?}");
            }
            self.inbound_closer.close();
            // Notify immediately, not on a later browser onclose event.
            self.on_disconnect.take_and_fire();
            Ok(())
        }
        .boxed_local()
    }
}
/// Socket send failure.
#[derive(Debug, Error)]
pub enum SendError {
    /// Browser rejected a send.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),
}
/// Socket disconnect failure (currently infallible).
#[derive(Debug, Clone, Copy, Error)]
#[error("WebSocket disconnection error (should not occur)")]
pub struct DisconnectionError;
/// Attempted to read from a closed inbound channel.
#[derive(Debug, Clone, Copy, Error)]
#[error("Attempted to read from closed channel")]
pub struct ReadFromClosedChannel;
impl From<ReadFromClosedChannel> for JsValue {
    fn from(err: ReadFromClosedChannel) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name("ReadFromClosedChannel");
        error.into()
    }
}
/// Failure to open the socket used for an authenticated connection.
#[derive(Debug, Error)]
pub enum WebSocketAuthenticatedTransportError {
    /// Browser rejected construction.
    #[error("WebSocket creation failed: {0:?}")]
    SocketCreationFailed(JsValue),
    /// Socket failed to open.
    #[error(transparent)]
    TransportFailed(#[from] WebSocketTransportFailed),
    /// Setup cancelled.
    #[error("transport setup canceled")]
    Canceled,
}
impl From<WebSocketAuthenticatedTransportError> for JsValue {
    fn from(err: WebSocketAuthenticatedTransportError) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name("WebSocketAuthenticatedTransportError");
        error.into()
    }
}
/// Socket failed during opening.
#[derive(Debug, Clone, Copy, Error)]
#[error("WebSocket transport failed")]
pub struct WebSocketTransportFailed;
/// JS error adapter for socket sends, not a wasm-bindgen value class.
#[derive(Debug, Error)]
#[error(transparent)]
pub struct WasmSendError(#[from] SendError);
impl From<WasmSendError> for JsValue {
    fn from(err: WasmSendError) -> Self {
        let error = js_sys::Error::new(&err.to_string());
        error.set_name("SendError");
        error.into()
    }
}
