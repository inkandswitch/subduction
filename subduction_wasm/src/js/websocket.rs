use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::{
    channel::{mpsc, oneshot},
    future::LocalBoxFuture,
    lock::Mutex,
    FutureExt, SinkExt,
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
use web_sys::{js_sys, MessageEvent, WebSocket};

use super::peer_id::JsPeerId;

#[wasm_bindgen(js_name = JsWebSocket)]
#[derive(Debug, Clone)]
pub struct JsWebSocket {
    peer_id: PeerId,
    request_id_counter: Arc<Mutex<u128>>,
    socket: web_sys::WebSocket,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,
    inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
}

#[wasm_bindgen(js_class = JsWebSocket)]
impl JsWebSocket {
    /// Create a new [`JsWebSocket`] instance.
    #[wasm_bindgen(constructor)]
    pub fn new(peer_id: JsPeerId, ws: &WebSocket) -> Self {
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
                    tracing::info!("WS message received with {} bytes length", bytes.len());
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
            request_id_counter: Arc::new(Mutex::new(0)),
            socket,

            pending,
            inbound_reader,
        }
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
            todo!("Implement sending logic using WebSocket");
        }
        .boxed_local()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async {
            todo!("Implement sending logic using WebSocket");
        }
        .boxed_local()
    }
}

#[derive(Debug, Error)]
#[error("Not implemented")]
pub struct Fixme;
