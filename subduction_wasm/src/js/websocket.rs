use std::{collections::HashMap, sync::Arc, time::Duration};

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
        Connection,
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

#[wasm_bindgen(js_name = JsWebSocket)]
#[derive(Debug, Clone)]
pub struct JsWebSocket {
    address: Url,
    peer_id: PeerId,
    timeout: Duration,

    request_id_counter: Arc<Mutex<u128>>,
    socket: web_sys::WebSocket,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
}

#[wasm_bindgen(js_class = JsWebSocket)]
impl JsWebSocket {
    /// Create a new [`JsWebSocket`] instance.
    #[wasm_bindgen(constructor)]
    pub fn new(
        address: &Url,
        peer_id: JsPeerId,
        ws: &WebSocket,
        timeout_milliseconds: u32,
    ) -> Self {
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
            address: address.clone(),
            peer_id: peer_id.into(),
            timeout: Duration::from_millis(timeout_milliseconds as u64),

            request_id_counter: Arc::new(Mutex::new(0)),
            socket,

            pending,
            inbound_reader,
        }
    }

    pub fn connect(
        address: Url,
        peer_id: JsPeerId,
        timeout_milliseconds: u32,
    ) -> Result<Self, String> {
        Ok(Self::new(
            &address,
            peer_id,
            &WebSocket::new(&address.href()).expect("FIXME"),
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
    type DisconnectionError = Fixme;
    type SendError = Fixme;
    type RecvError = Fixme;
    type CallError = Fixme;

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
                .expect("FIXME");
            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .expect("FIXME");
            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Waiting for inbound message");
            let mut chan = self.inbound_reader.lock().await;
            let msg = chan.next().await.expect("FIXME"); //ok_or(RecvError::ReadFromClosed)?;
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
            .expect("FIXME");

            self.socket
                .send_with_u8_array(msg_bytes.as_slice())
                .expect("FIXME");

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
                    todo!()
                    // FIXME Err(CallError::ChanCanceled(e))
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    todo!()
                    // FIXME Err(CallError::Timeout)
                }
            }
        }
        .boxed_local()
    }
}

#[derive(Debug, Error)]
#[error("Not implemented")]
pub struct Fixme;

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

        let promise = Promise::new(&mut |resolve, _reject| {
            // NOTE this `global` strategy looks ugly,
            // BUT it abstracts over both `window` and `worker` contexts.
            let global = js_sys::global();
            let set_timeout = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout"))
                .expect("FIXME")
                .dyn_into::<js_sys::Function>()
                .expect("FIXME");

            let callback = Closure::wrap(Box::new(move || {
                resolve
                    .call0(&JsValue::NULL)
                    .expect("FIXME but also maybe okay to fail silently?");
            }) as Box<dyn FnMut()>);

            let id = set_timeout
                .call2(
                    &global,
                    callback.as_ref().unchecked_ref(),
                    &JsValue::from(ms),
                )
                .expect("setTimeout call ok");

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
            clear_timeout.call1(&global, &self.id).expect("FIXME"); // MAYBE silently failing is ok?
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
