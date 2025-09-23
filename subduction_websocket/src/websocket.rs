//! # Generic WebSocket connection for Subduction

use crate::error::{CallError, DisconnectionError, RecvError, RunError, SendError};
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use futures::{
    channel::{mpsc, oneshot},
    future::{self, BoxFuture, LocalBoxFuture},
    lock::Mutex,
    FutureExt, SinkExt,
};
use futures_timer::Delay;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use sedimentree_core::future::{Local, Sendable};
use std::{collections::HashMap, sync::Arc, time::Duration};
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection,
    },
    peer::id::PeerId,
};

/// A WebSocket implementation for [`Connection`].
#[derive(Debug)]
pub struct WebSocket<T: AsyncRead + AsyncWrite + Unpin> {
    pub(crate) peer_id: PeerId,

    pub(crate) req_id_counter: Arc<Mutex<u128>>,
    pub(crate) timeout: Duration,

    pub(crate) ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,
    pub(crate) outbound: Arc<Mutex<WebSocketSender<T>>>,

    pub(crate) pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    pub(crate) inbound_writer: mpsc::UnboundedSender<Message>,
    pub(crate) inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
}

impl<T: AsyncRead + AsyncWrite + Unpin> WebSocket<T> {
    /// Create a new WebSocket connection.
    pub fn new(ws: WebSocketStream<T>, timeout: Duration, peer_id: PeerId) -> Self {
        let (ws_writer, ws_reader) = ws.split();
        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_rx) = mpsc::unbounded();
        let starting_counter = rand::random::<u128>();

        Self {
            peer_id,

            req_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound: Arc::new(Mutex::new(ws_writer)),
            pending,
            inbound_writer,
            inbound_reader: Arc::new(Mutex::new(inbound_rx)),
        }
    }

    /// Listen for incoming messages and dispatch them appropriately.
    ///
    /// # Errors
    ///
    /// If there is an error reading from the WebSocket or processing messages.
    pub async fn listen(&self) -> Result<(), RunError> {
        while let Some(msg) = self.ws_reader.lock().await.next().await {
            tracing::debug!("received ws message");
            match msg {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    let (msg, _size): (Message, usize) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;

                    match msg {
                        Message::BatchSyncResponse(resp) => {
                            let req_id = resp.req_id;
                            if let Some(waiting) = self.pending.lock().await.remove(&req_id) {
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
                                tracing::info!("dispatching to inbound channel {:?}", resp.req_id);
                                self.inbound_writer
                                    .clone()
                                    .send(Message::BatchSyncResponse(resp))
                                    .await?;
                            }
                        }
                        other => {
                            self.inbound_writer.clone().send(other).await?;
                        }
                    }
                }
                Ok(tungstenite::Message::Text(text)) => {
                    tracing::warn!("unexpected text message: {}", text);
                }
                Ok(tungstenite::Message::Ping(p)) => {
                    tracing::info!("received ping: {:x?}", p);
                    self.outbound
                        .lock()
                        .await
                        .send(tungstenite::Message::Pong(p))
                        .await
                        .unwrap_or_else(|_| {
                            tracing::error!("failed to send pong");
                        });
                }
                Ok(tungstenite::Message::Pong(p)) => {
                    tracing::warn!("unexpected pong message: {:x?}", p);
                }
                Ok(tungstenite::Message::Frame(f)) => {
                    tracing::warn!("unexpected frame: {:x?}", f);
                }
                Ok(tungstenite::Message::Close(_)) => {
                    // fail all pending
                    std::mem::take(&mut *self.pending.lock().await);
                    break;
                }
                Err(e) => Err(e)?,
            }
        }

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Clone for WebSocket<T> {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            req_id_counter: self.req_id_counter.clone(),
            timeout: self.timeout,
            ws_reader: self.ws_reader.clone(),
            outbound: self.outbound.clone(),
            pending: self.pending.clone(),
            inbound_writer: self.inbound_writer.clone(),
            inbound_reader: self.inbound_reader.clone(),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection<Local> for WebSocket<T> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        async {
            let mut counter = self.req_id_counter.lock().await;
            *counter = counter.wrapping_add(1);
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
            tracing::debug!("sending outbound message id {:?}", message.request_id());
            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
                ))
                .await?;

            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Waiting for inbound message");
            let mut chan = self.inbound_reader.lock().await;
            let msg = chan.next().await.ok_or(RecvError::ReadFromClosed)?;
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

            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(
                        Message::BatchSyncRequest(req),
                        bincode::config::standard(),
                    )
                    .map_err(CallError::Serialization)?
                    .into(),
                ))
                .await?;

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
                    Err(CallError::ChanCanceled(e))
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed_local()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection<Sendable> for WebSocket<T> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async {
            let mut counter = self.req_id_counter.lock().await;
            *counter = counter.wrapping_add(1);
            tracing::debug!("generated message id {:?}", *counter);
            RequestId {
                requestor: self.peer_id,
                nonce: *counter,
            }
        }
        .boxed()
    }

    fn disconnect(&mut self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            tracing::debug!("sending outbound message id {:?}", message.request_id());
            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
                ))
                .await?;

            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Waiting for inbound message");
            let mut chan = self.inbound_reader.lock().await;
            let msg = chan.next().await.ok_or(RecvError::ReadFromClosed)?;
            tracing::info!("Received inbound message id {:?}", msg.request_id());
            Ok(msg)
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(req_id, tx);

            self.outbound
                .lock()
                .await
                .send(tungstenite::Message::Binary(
                    bincode::serde::encode_to_vec(
                        Message::BatchSyncRequest(req),
                        bincode::config::standard(),
                    )
                    .map_err(CallError::Serialization)?
                    .into(),
                ))
                .await?;

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
                    Err(CallError::ChanCanceled(e))
                }
                Err(TimedOut) => {
                    tracing::error!("request {:?} timed out", req_id);
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    match future::select(fut, Delay::new(dur)).await {
        future::Either::Left((val, _delay)) => Ok(val),
        future::Either::Right(_) => Err(TimedOut),
    }
}
