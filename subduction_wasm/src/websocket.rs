//! JS [`WebSocket`] connection implementation for Subduction.

use alloc::{
    boxed::Box,
    collections::BTreeMap,
    rc::Rc,
    string::ToString,
    sync::Arc,
    vec::Vec
};
use core::{
    cell::RefCell,
    convert::Infallible,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration
};

use async_lock::Mutex;
use futures::{
    channel::oneshot::{self, Canceled},
    future::LocalBoxFuture,
    FutureExt,
};
use futures_kind::Local;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use thiserror::Error;
use wasm_bindgen::{closure::Closure, prelude::*, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    js_sys::{self, Promise}, BinaryType, Event, MessageEvent, Url, WebSocket
};

use super::peer_id::WasmPeerId;

/// A WebSocket connection with internal wiring for [`Subduction`] message handling.
#[wasm_bindgen(js_name = SubductionWebSocket)]
#[derive(Debug, Clone)]
pub struct WasmWebSocket {
    peer_id: PeerId,
    timeout_ms: u32,

    request_id_counter: Arc<AtomicU64>,
    socket: WebSocket,

    pending: Arc<Mutex<BTreeMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: async_channel::Receiver<Message>,
}

#[wasm_bindgen(js_class = SubductionWebSocket)]
impl WasmWebSocket {
    /// Create a new [`WasmWebSocket`] instance.
    ///
    /// # Errors
    ///
    /// Returns [`WasmWebSocketSetupCanceled`] if the setup was canceled.
    #[allow(clippy::too_many_lines)]
    #[wasm_bindgen]
    pub async fn setup(peer_id: &WasmPeerId, ws: &WebSocket, timeout_milliseconds: u32) -> Result<Self, WasmWebSocketSetupCanceled> {
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Message>(64);

        let pending = Arc::new(Mutex::new(BTreeMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let closure_pending = pending.clone();

        let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            tracing::debug!("WS message event received");
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                if let Ok(msg) = ciborium::de::from_reader::<Message, &[u8]>(&bytes) {
                    tracing::info!("WS message received that's {} bytes long", bytes.len());
                    let inner_pending = closure_pending.clone();
                    let inner_inbound_writer = inbound_writer.clone();

                    wasm_bindgen_futures::spawn_local(async move {
                        match msg {
                            Message::BatchSyncResponse(resp) => {
                                let req_id = resp.req_id;
                                let removed = { inner_pending.lock().await.remove(&req_id) };
                                if let Some(waiting) = removed {
                                    tracing::info!("dispatching to waiter {:?}", req_id);
                                    let result = waiting.send(resp);
                                    debug_assert!(result.is_ok());
                                    if result.is_err() {
                                        tracing::error!(
                                                "oneshot channel closed before sending response for req_id {:?}",
                                                req_id
                                            );
                                    }
                                } else {
                                    tracing::info!(
                                        "dispatching to inbound channel {:?}",
                                        resp.req_id
                                    );
                                    if let Err(e) = inner_inbound_writer.clone().send(Message::BatchSyncResponse(resp)).await {
                                            tracing::error!("failed to send inbound message: {e}");
                                        }
                                }
                            }
                            other @ (Message::LooseCommit { .. } | Message::Fragment { .. } | Message::BlobsRequest(_) | Message::BlobsResponse(_) | Message::BatchSyncRequest(_)) => {
                                    if let Err(e) = inner_inbound_writer.clone().send(other).await {
                                        tracing::error!("failed to send inbound message: {e}");
                                    }
                                }
                            }
                    });
                } else {
                    tracing::error!("failed to decode message: {:?}", event.data());
                }
            } else {
                tracing::error!("unexpected message event: {:?}", event.data());
            }
        });

        let onclose = Closure::<dyn FnMut(_)>::new(move |event: Event| {
            tracing::warn!("WebSocket connection closed: {:?}", event);
        });
        
        let ws_clone = ws.clone();
        let (tx, rx) = oneshot::channel();
        let maybe_tx = Rc::new(RefCell::new(Some(tx)));
        let maybe_tx_clone = maybe_tx.clone();

        // HACK: keeps the `onopen` closure alive until called
        #[allow(clippy::type_complexity)]
        let keep_closure_alive: Rc<RefCell<Option<Closure<dyn FnMut(Event)>>>> = Rc::new(RefCell::new(None));
        let keep_closure_alive_clone = keep_closure_alive.clone();
        let onopen = Closure::<dyn FnMut(_)>::new(move |_event: Event| {
            tracing::info!("WebSocket connection opened");
            if let Some(tx) = maybe_tx_clone.borrow_mut().take() {
                let _ = tx.send(());
            }
            ws_clone.set_onopen(None);
            keep_closure_alive_clone.borrow_mut().take();
        });

        ws.set_binary_type(BinaryType::Arraybuffer);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));

        onmessage.forget();
        onclose.forget();
        // NOTE no onopen.forget() because we only want it to fire once,
        // so we're doing manual handling with the `keep_alive` slots.

        if ws.ready_state() == WebSocket::CONNECTING {
            ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
            *keep_closure_alive.borrow_mut() = Some(onopen);

            // Re-check to close the race where it opened between lines above
            if ws.ready_state() == WebSocket::OPEN {
                if let Some(tx) = maybe_tx.borrow_mut().take() {
                    let _ = tx.send(());
                }
                ws.set_onopen(None);
                keep_closure_alive.borrow_mut().take();
            }
        } else if ws.ready_state() == WebSocket::OPEN {
            // already open
            if let Some(tx) = maybe_tx.borrow_mut().take() {
                let _ = tx.send(());
            }
        } else {
            // CLOSING/CLOSED
            if let Some(tx) = maybe_tx.borrow_mut().take() {
                let _ = tx.send(());
            }
        }

        rx.await?;

        Ok(Self {
            peer_id: peer_id.clone().into(),
            timeout_ms: timeout_milliseconds,

            request_id_counter: Arc::new(AtomicU64::new(0)),
            socket: ws.clone(),

            pending,
            inbound_reader,
        })
    }

    /// Connect to a WebSocket server at the given address.
    ///
    /// # Errors
    ///
    /// Returns [`WebSocketConnectionError`] if establishing the connection fails.
    pub async fn connect(
        address: &Url,
        peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<Self, WebSocketConnectionError> {
        Ok(Self::setup(
            peer_id,
            &WebSocket::new(&address.href())
                .map_err(WebSocketConnectionError::SocketCreationFailed)?,
            timeout_milliseconds,
        ).await?)
    }
}

impl PartialEq for WasmWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.socket == other.socket
    }
}

impl Connection<Local> for WasmWebSocket {
    type SendError = SendError;
    type RecvError = ReadFromClosedChannel;
    type CallError = CallError;
    type DisconnectionError = Infallible;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        let counter = self.request_id_counter.clone();
        async move {
            let counter = counter.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("generated message id {:?}", counter);
            RequestId {
                requestor: self.peer_id,
                nonce: counter,
            }
        }
        .boxed_local()
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            tracing::debug!("sending outbound message id {:?}", message.request_id());
            
            let mut msg_bytes = Vec::new();
            #[allow(clippy::expect_used)]
            ciborium::ser::into_writer(&message, &mut msg_bytes).expect("should be Infallible");

            tracing::debug!(
                "sending outbound message id {:?} that's {} bytes long",
                message.request_id(),
                msg_bytes.len()
            );

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(SendError::SocketSend)?;

            tracing::debug!("sent outbound message id {:?}", message.request_id());

            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("waiting for inbound message");
            let msg = self.inbound_reader.recv().await.map_err(|_| ReadFromClosedChannel)?;
            tracing::info!("received inbound message id {:?}", msg.request_id());
            Ok(msg)
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            { self.pending.lock().await.insert(req_id, tx); }

            let mut msg_bytes = Vec::new();
            #[allow(clippy::expect_used)]
            ciborium::ser::into_writer(&Message::BatchSyncRequest(req), &mut msg_bytes).expect("should be Infallible");

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(CallError::SocketSend)?;

            tracing::info!("sent request {:?}", req_id);

            let req_timeout =
                override_timeout.unwrap_or(Duration::from_millis(self.timeout_ms.into()));

            // await response with timeout & cleanup
            match timeout(req_timeout, rx).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    Err(CallError::ChannelCanceled)
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    Err(CallError::TimedOut)
                }
            }
        }
        .boxed_local()
    }
}

impl Reconnect<Local> for WasmWebSocket {
    type ConnectError = WebSocketConnectionError;

    fn reconnect(&mut self) -> LocalBoxFuture<'_, Result<(), Self::ConnectError>> {
        async {
            let address = self.socket.url();
            let peer_id = self.peer_id;

            *self = WasmWebSocket::connect(
                &Url::new(&address).map_err(WebSocketConnectionError::InvalidUrl)?,
                &peer_id.into(),
                self.timeout_ms,
            ).await?;
            Ok(())
        }
        .boxed_local()
    }
}

#[derive(Debug)]
struct WasmTimeout {
    id: JsValue, // Numeric in browsers, special Timeout type in e.g. Deno.
    // Keep the closure alive so the timer can call it
    _closure: Option<Closure<dyn FnMut()>>,
}

impl WasmTimeout {
    /// Creates a Promise that resolves after `ms` and a handle you can cancel.
    fn new(ms: i32) -> (JsFuture, Self) {
        let mut out_id: JsValue = JsValue::UNDEFINED;
        let mut out_closure: Option<Closure<dyn FnMut()>> = None;

        let promise = Promise::new(&mut |resolve, reject| {
            // NOTE this `global` strategy looks ugly,
            // BUT it abstracts over both `window` and `worker` contexts.
            let global = js_sys::global();

            let js_value = match js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout")) {
                Ok(v) => v,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            let set_timeout = match js_value.dyn_into::<js_sys::Function>() {
                Ok(set_timeout) => set_timeout,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            let callback_reject = reject.clone();
            let callback = Closure::wrap(Box::new(move || match resolve.call0(&JsValue::NULL) {
                Err(e) => {
                    drop(callback_reject.call1(&JsValue::NULL, &e));
                }
                Ok(_val) => (),
            }) as Box<dyn FnMut()>);

            out_id = match set_timeout.call2(
                &global,
                callback.as_ref().unchecked_ref(),
                &JsValue::from(ms),
            ) {
                Ok(id) => id,
                Err(e) => {
                    drop(reject.call1(&JsValue::NULL, &e));
                    return;
                }
            };

            out_closure = Some(callback);
        });

        (
            JsFuture::from(promise),
            WasmTimeout {
                id: out_id,
                _closure: out_closure,
            },
        )
    }

    /// Cancel the timer (prevents the callback from firing).
    fn cancel(self) {
        let global = js_sys::global();
        if let Ok(clear_timeout) = js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
            .and_then(JsCast::dyn_into::<js_sys::Function>)
            && let Err(e) = clear_timeout.call1(&global, &self.id) {
                tracing::error!("failed to clear timeout: {:?}", e);
            }
    }
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    let ms = dur.as_millis().try_into().unwrap_or(i32::MAX);
    let (sleep, handle) = WasmTimeout::new(ms);
    match futures_util::future::select(fut, sleep).await {
        futures_util::future::Either::Left((val, _sleep_future)) => {
            handle.cancel();
            Ok(val)
        }
        futures_util::future::Either::Right((_done, _fut)) => Err(TimedOut),
    }
}

/// Problem while sending a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),
}

/// Attempted to read from a closed channel.
#[derive(Debug, Clone, Copy, Error)]
#[error("Attempted to read from closed channel")]
pub struct ReadFromClosedChannel;

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),

    /// Tried to read from a canceled channel.
    #[error("Channel canceled")]
    ChannelCanceled,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    TimedOut,
}

/// Problem while attempting to connect or reconnect the WebSocket.
#[derive(Debug, Clone, Error)]
pub enum WebSocketConnectionError {
    /// Problem creating the WebSocket.
    #[error("WebSocket creation failed: {0:?}")]
    SocketCreationFailed(JsValue),

    /// Problem creating the URL.
    #[error("invalid URL: {0:?}")]
    InvalidUrl(JsValue),

    /// WebSocket setup was canceled.
    #[error(transparent)]
    WasmSetupCanceled(#[from] WasmWebSocketSetupCanceled),
}

impl From<WebSocketConnectionError> for JsValue {
    fn from(err: WebSocketConnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("WebSocketConnectionError");
        err.into()
    }
}

/// WebSocket setup was canceled.
#[derive(Debug, Clone, Error)]
#[error("WebSocket setup was canceled")]
#[allow(missing_copy_implementations)]
pub struct WasmWebSocketSetupCanceled(Canceled);

impl From<Canceled> for WasmWebSocketSetupCanceled {
    fn from(err: Canceled) -> Self {
        Self(err)
    }
}

impl From<WasmWebSocketSetupCanceled> for JsValue {
    fn from(err: WasmWebSocketSetupCanceled) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CancelWebSocketSetup");
        js_err.into()
    }
}
