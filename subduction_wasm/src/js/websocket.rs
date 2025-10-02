//! JS [`WebSocket`] connection implementation for Subduction.

use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};

use futures::{
    channel::{mpsc, oneshot},
    future::LocalBoxFuture,
    lock::Mutex,
    FutureExt, SinkExt, StreamExt,
};
use sedimentree_core::future::Local;
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
    js_sys::{self, Promise},
    MessageEvent, Url, WebSocket,
};

use super::peer_id::JsPeerId;

/// A WebSocket connection with internal wiring for [`Subduction`] message handling.
#[wasm_bindgen(js_name = SubductionWebSocket)]
#[derive(Debug, Clone)]
pub struct JsWebSocket {
    peer_id: PeerId,
    timeout: Duration,

    request_id_counter: Arc<Mutex<u128>>,
    socket: web_sys::WebSocket,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
    // FIXME callbacks: Option<Arc<Mutex<js_sys::Function>>>,
}

#[wasm_bindgen(js_class = SubductionWebSocket)]
impl JsWebSocket {
    /// Create a new [`JsWebSocket`] instance.
    #[wasm_bindgen(constructor)]
    pub fn new(peer_id: JsPeerId, ws: &WebSocket, timeout_milliseconds: u32) -> Self {
        let (inbound_writer, raw_inbound_reader) = mpsc::unbounded();
        let inbound_reader = Arc::new(Mutex::new(raw_inbound_reader));

        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let closure_pending = pending.clone();

        let onmessage = Closure::<dyn FnMut(_)>::new(move |event: MessageEvent| {
            if let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() {
                let bytes: Vec<u8> = js_sys::Uint8Array::new(&buf).to_vec();
                if let Ok((msg, _size)) = bincode::serde::decode_from_slice::<Message, _>(
                    &bytes,
                    bincode::config::standard(),
                ) {
                    tracing::info!("WS message received that's {} bytes long", bytes.len());
                    let inner_pending = closure_pending.clone();
                    let inner_inbound_writer = inbound_writer.clone();

                    wasm_bindgen_futures::spawn_local(async move {
                        match msg {
                            Message::BatchSyncResponse(resp) => {
                                let req_id = resp.req_id;
                                if let Some(waiting) =
                                    inner_pending.clone().lock().await.remove(&req_id)
                                {
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
                                    let _ = inner_inbound_writer
                                        .clone()
                                        .send(Message::BatchSyncResponse(resp))
                                        .await
                                        .map_err(|e| {
                                            tracing::error!("Failed to send inbound message: {e}");
                                            e
                                        });
                                }
                            }
                            other => {
                                let _ =
                                    inner_inbound_writer.clone().send(other).await.map_err(|e| {
                                        tracing::error!("Failed to send inbound message: {e}");
                                        e
                                    });
                            }
                        }
                    });
                } else {
                    tracing::error!("Failed to decode message: {:?}", event.data());
                }
            } else {
                tracing::error!("Unexpected message event: {:?}", event.data());
            }
        });

        let socket = ws.clone();
        socket.set_binary_type(web_sys::BinaryType::Arraybuffer);
        socket.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        Self {
            peer_id: peer_id.into(),
            timeout: Duration::from_millis(timeout_milliseconds as u64),

            request_id_counter: Arc::new(Mutex::new(0)),
            socket,

            pending,
            inbound_reader,
        }
    }

    /// Connect to a WebSocket server at the given address.
    pub fn connect(
        address: Url,
        peer_id: JsPeerId,
        timeout_milliseconds: u32,
    ) -> Result<Self, WebsocketConnectionError> {
        Ok(Self::new(
            peer_id,
            &WebSocket::new(&address.href()).map_err(WebsocketConnectionError)?,
            timeout_milliseconds,
        ))
    }
}

impl PartialEq for JsWebSocket {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.socket == other.socket
    }
}

impl Connection<Local> for JsWebSocket {
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
            let mut counter = counter.lock().await;
            *counter += 1;
            tracing::debug!("generated message id {:?}", *counter);
            RequestId {
                requestor: self.peer_id,
                nonce: *counter,
            }
        }
        .boxed_local()
    }

    fn disconnect(&mut self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(&self, message: Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            let msg_bytes = bincode::serde::encode_to_vec(&message, bincode::config::standard())
                .map_err(SendError::Encoding)?;

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(SendError::SocketSend)?;

            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Waiting for inbound message");
            let mut chan = self.inbound_reader.lock().await;
            let msg = chan.next().await.ok_or(ReadFromClosedChannel)?;
            tracing::info!("Received inbound message id {:?}", msg.request_id());
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
            self.pending.lock().await.insert(req_id, tx);

            let msg_bytes = bincode::serde::encode_to_vec(
                Message::BatchSyncRequest(req),
                bincode::config::standard(),
            )
            .map_err(CallError::Encoding)?;

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .map_err(CallError::SocketSend)?;

            tracing::info!("sent request {:?}", req_id);

            let req_timeout = override_timeout.unwrap_or(self.timeout);

            // await response with timeout & cleanup
            match timeout(req_timeout, rx).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    Err(CallError::ChannelCancelled)
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

impl Reconnect<Local> for JsWebSocket {
    type ConnectError = WebsocketConnectionError;
    type RunError = WebsocketConnectionError;

    fn reconnect(&mut self) -> LocalBoxFuture<'_, Result<(), Self::ConnectError>> {
        async {
            let address = self.socket.url();
            let peer_id = self.peer_id;
            let timeout = self.timeout.as_millis() as u32;

            *self = JsWebSocket::connect(
                Url::new(&address).expect("existing URL should be valid URL"),
                peer_id.into(),
                timeout,
            )?;
            Ok(())
        }
        .boxed_local()
    }

    /// Run the connection send/receive loop.
    fn run(&mut self) -> LocalBoxFuture<'_, Result<(), Self::RunError>> {
        async move {
            loop {
                self.reconnect().await?;
            }
        }
        .boxed_local()
    }
}

#[derive(Debug)]
struct JsTimeout {
    id: JsValue, // Numeric in browsers, special Timeout type in e.g. Deno.
    // Keep the closure alive so the timer can call it
    _closure: Option<Closure<dyn FnMut()>>,
}

impl JsTimeout {
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
                    return;
                }
                Ok(_val) => (),
            }) as Box<dyn FnMut()>);

            let id = match set_timeout.call2(
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

            out_id = id.into();
            out_closure = Some(callback);
        });

        (
            JsFuture::from(promise),
            JsTimeout {
                id: out_id,
                _closure: out_closure,
            },
        )
    }

    /// Cancel the timer (prevents the callback from firing).
    fn cancel(self) {
        let global = js_sys::global();
        if let Ok(clear_timeout) = js_sys::Reflect::get(&global, &JsValue::from_str("clearTimeout"))
            .and_then(|v| v.dyn_into::<js_sys::Function>().map_err(|e| e.into()))
        {
            if let Err(e) = clear_timeout.call1(&global, &self.id) {
                tracing::error!("Failed to clear timeout: {:?}", e);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    let ms = dur.as_millis().try_into().unwrap_or(i32::MAX);
    let (sleep, handle) = JsTimeout::new(ms);
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
    /// Problem encoding message.
    #[error("Problem encoding message: {0}")]
    Encoding(bincode::error::EncodeError),

    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),
}

/// Attempted to read from a closed channel.
#[wasm_bindgen]
#[derive(Debug, Clone, Copy, Error)]
#[error("Attempted to read from closed channel")]
pub struct ReadFromClosedChannel;

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// Problem encoding message.
    #[error("Problem encoding message: {0}")]
    Encoding(bincode::error::EncodeError),

    /// WebSocket error while sending.
    #[error("WebSocket error while sending: {0:?}")]
    SocketSend(JsValue),

    /// Tried to read from a cancelled channel.
    #[error("Channel cancelled")]
    ChannelCancelled,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    TimedOut,
}

/// Problem while attempting to connect or reconnect the WebSocket.
#[wasm_bindgen]
#[derive(Debug, Clone, Error)]
#[error("WebSocket connection error: {0:?}")]
pub struct WebsocketConnectionError(JsValue);
