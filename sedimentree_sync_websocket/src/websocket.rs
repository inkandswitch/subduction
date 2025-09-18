//! # Sedimentree Sync WebSocket Connection

use crate::error::{CallError, DisconnectionError, RecvError, RunError, SendError};
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use futures::{
    channel::{mpsc, oneshot},
    future,
    lock::Mutex,
    SinkExt,
};
use futures_timer::Delay;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use sedimentree_sync_core::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionId, Message, RequestId,
    },
    peer::id::PeerId,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// A WebSocket implementation for [`Connection`].
#[derive(Debug)]
pub struct WebSocket<T: AsyncRead + AsyncWrite + Unpin> {
    pub(crate) conn_id: ConnectionId,
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
    pub fn new(
        ws: WebSocketStream<T>,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Self {
        let (ws_writer, ws_reader_owned) = ws.split();
        let pending = Arc::new(Mutex::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_rx) = mpsc::unbounded();
        let ws_reader = Arc::new(Mutex::new(ws_reader_owned));
        let starting_counter = rand::random::<u128>();

        Self {
            conn_id,
            peer_id,

            req_id_counter: Arc::new(Mutex::new(starting_counter)),
            timeout,

            ws_reader,
            outbound: Arc::new(Mutex::new(ws_writer)),
            pending,
            inbound_writer,
            inbound_reader: Arc::new(Mutex::new(inbound_rx)),
        }
    }

    pub async fn run(&self) -> Result<(), RunError> {
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
                    // FIXME reconnect instead ^^
                    break;
                }
                Err(e) => Err(e)?,
            }
        }

        Ok(())
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Connection for WebSocket<T> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn connection_id(&self) -> ConnectionId {
        self.conn_id
    }

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    async fn next_request_id(&self) -> RequestId {
        let mut counter = self.req_id_counter.lock().await;
        *counter = counter.wrapping_add(1);
        tracing::debug!("generated message id {:?}", *counter);
        RequestId {
            requestor: self.peer_id,
            nonce: *counter,
        }
    }

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: Message) -> Result<(), Self::SendError> {
        self.outbound
            .lock()
            .await
            .send(tungstenite::Message::Binary(
                bincode::serde::encode_to_vec(&message, bincode::config::standard())?.into(),
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn recv(&self) -> Result<Message, Self::RecvError> {
        tracing::debug!("waiting for inbound message");
        let mut chan = self.inbound_reader.lock().await;
        let msg = chan.next().await.ok_or(RecvError::ReadFromClosed)?;
        tracing::info!("received inbound message id {:?}", msg.request_id());
        Ok(msg)
    }

    #[tracing::instrument(skip(self, req), fields(req_id = ?req.req_id))]
    async fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
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
}

#[derive(Debug, Clone, Copy)]
struct TimedOut;

async fn timeout<F: Future<Output = T> + Unpin, T>(dur: Duration, fut: F) -> Result<T, TimedOut> {
    match future::select(fut, Delay::new(dur)).await {
        future::Either::Left((val, _delay)) => Ok(val),
        future::Either::Right(_) => Err(TimedOut),
    }
}
