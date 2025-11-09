//! # Generic WebSocket connection for Subduction

use crate::error::{CallError, DisconnectionError, RecvError, RunError, SendError};
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use futures::{
    channel::oneshot,
    future::{self, BoxFuture, LocalBoxFuture},
    lock::Mutex,
    FutureExt,
};
use futures_timer::Delay;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use sedimentree_core::future::{Local, Sendable};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
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
    chan_id: u64,
    peer_id: PeerId,
    req_id_counter: Arc<AtomicU64>,
    timeout: Duration,

    ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,
    outbound: Arc<Mutex<WebSocketSender<T>>>,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    inbound_writer: async_channel::Sender<Message>,
    inbound_reader: async_channel::Receiver<Message>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> Connection<Local> for WebSocket<T> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        async {
            let counter = self.req_id_counter.fetch_add(1, Ordering::Relaxed);
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
            tracing::debug!(
                "ws: sending outbound with message id {:?} to peer_id {}",
                message.request_id(),
                self.peer_id
            );
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
        let chan = self.inbound_reader.clone();
        tracing::debug!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);

        async move {
            let msg = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel {} closed unexpectedly", self.chan_id);
                RecvError::ReadFromClosed
            })?;

            tracing::debug!("recv: inbound message {msg:?}");
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

            tracing::info!(
                chan_id = self.chan_id,
                "sent WebSocket request {:?}",
                req_id
            );

            let req_timeout = override_timeout.unwrap_or(self.timeout);

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

impl<T: AsyncRead + AsyncWrite + Unpin> WebSocket<T> {
    /// Create a new WebSocket connection.
    pub fn new(ws: WebSocketStream<T>, timeout: Duration, peer_id: PeerId) -> Self {
        tracing::info!("new WebSocket connection for peer {:?}", peer_id);
        let (ws_writer, ws_reader) = ws.split();
        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_reader) = async_channel::bounded(1024);
        let starting_counter = rand::random::<u64>();
        let chan_id = rand::random::<u64>();

        Self {
            peer_id,
            chan_id,

            req_id_counter: Arc::new(starting_counter.into()),
            timeout,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound: Arc::new(Mutex::new(ws_writer)),
            pending,
            inbound_writer,
            inbound_reader,
        }
    }

    /// The timeout for requests.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Get the [`PeerId`] associated with this connection.
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Listen for incoming messages and dispatch them appropriately.
    ///
    /// # Errors
    ///
    /// If there is an error reading from the WebSocket or processing messages.
    pub async fn listen(&self) -> Result<(), RunError> {
        tracing::info!("starting WebSocket listener for peer {:?}", self.peer_id);
        let mut in_chan = self.ws_reader.lock().await;
        while let Some(ws_msg) = in_chan.next().await {
            tracing::debug!(
                "received WebSocket message for peer {} on channel {}",
                self.peer_id,
                self.chan_id
            );

            match ws_msg {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    let (msg, _size): (Message, usize) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;

                    tracing::debug!(
                        "decoded inbound message id {:?} from peer {:?}, message: {:?}",
                        msg.request_id(),
                        self.peer_id,
                        &msg
                    );

                    match msg {
                        Message::BatchSyncResponse(resp) => {
                            tracing::info!(
                                "received BatchSyncResponse for req_id {:?}",
                                resp.req_id
                            );
                            let req_id = resp.req_id;
                            if let Some(waiting) = self.pending.lock().await.remove(&req_id) {
                                tracing::info!("dispatching to waiter {:?}", req_id);
                                let result = waiting.send(resp);
                                if result.is_err() {
                                    tracing::error!(
                                        "oneshot channel closed before sending response for req_id {:?}",
                                        req_id
                                    );
                                }
                            } else {
                                self.inbound_writer
                                    .send(Message::BatchSyncResponse(resp))
                                    .await
                                    .map_err(|e| {
                                        tracing::error!(
                                            "failed to send inbound message to channel {}: {}",
                                            self.chan_id,
                                            e
                                        );
                                        e
                                    })?;
                            }
                        }
                        other => {
                            self.inbound_writer.send(other).await.map_err(|e| {
                                tracing::error!(
                                    "failed to send inbound message to channel {}: {}",
                                    self.chan_id,
                                    e
                                );
                                e
                            })?;
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
                    tracing::info!("received close message, shutting down listener");
                    break;
                }
                Err(e) => {
                    tracing::error!("error reading from websocket: {}", e);
                    Err(e)?
                }
            }
        }

        Ok(())
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
            let counter = self.req_id_counter.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("generated message id {:?}", counter);
            RequestId {
                requestor: self.peer_id,
                nonce: counter,
            }
        }
        .boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async move {
            tracing::debug!(
                "sending outbound message id {:?} / {message:?}",
                message.request_id()
            );
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
        let chan = self.inbound_reader.clone();
        tracing::debug!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);

        async move {
            let msg = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel {} closed unexpectedly", self.chan_id);
                RecvError::ReadFromClosed
            })?;

            tracing::debug!("recv: inbound message {msg:?}");
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

            tracing::info!(
                chan_id = self.chan_id,
                "sent WebSocket request {:?}",
                req_id
            );

            let req_timeout = override_timeout.unwrap_or(self.timeout);

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

impl<T: AsyncRead + AsyncWrite + Unpin> Clone for WebSocket<T> {
    fn clone(&self) -> Self {
        Self {
            chan_id: self.chan_id,
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

impl<T: AsyncRead + AsyncWrite + Unpin> PartialEq for WebSocket<T> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
            && Arc::ptr_eq(&self.outbound, &other.outbound)
            && Arc::ptr_eq(&self.pending, &other.pending)
            && self.inbound_writer.same_channel(&other.inbound_writer)
            && self.inbound_reader.same_channel(&other.inbound_reader)
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
