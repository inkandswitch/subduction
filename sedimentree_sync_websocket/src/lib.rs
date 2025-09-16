//! # Sedimentree Sync WebSocket

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(
    clippy::dbg_macro,
    clippy::expect_used,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::todo,
    clippy::unwrap_used,
    future_incompatible,
    let_underscore,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    rust_2021_compatibility
)]
#![deny(
    clippy::all,
    clippy::cargo,
    clippy::pedantic,
    rust_2018_idioms,
    unreachable_pub,
    unused_extern_crates
)]
#![forbid(unsafe_code)]

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use sedimentree_sync_core::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionId, Message, Reconnection,
        RequestId,
    },
    peer::id::PeerId,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex, RwLock},
    time::timeout,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// A WebSocket implementation for [`Connection`].
#[derive(Debug, Clone)]
pub struct WebSocket {
    conn_id: ConnectionId,
    peer_id: PeerId,

    req_id_counter: Arc<Mutex<u128>>,
    timeout: Duration,

    ws_reader: Arc<Mutex<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    outbound:
        Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, tungstenite::Message>>>,

    pending: Arc<RwLock<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    inbound_writer: mpsc::UnboundedSender<Message>,
    inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
}

impl WebSocket {
    /// Create a new WebSocket connection.
    pub fn new(
        ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Self {
        let (ws_writer, ws_reader_owned) = ws.split();
        let pending = Arc::new(RwLock::new(HashMap::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
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
}

impl Connection for WebSocket {
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
        tracing::info!("generated message id {:?}", *counter);
        RequestId {
            requestor: self.peer_id,
            nonce: *counter,
        }
    }

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: Message) -> Result<(), SendError> {
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
        let msg = chan.recv().await.ok_or(RecvError::ReadFromClosed)?;
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
        self.pending.write().await.insert(req_id, tx);

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
                Err(CallError::ChanError(e))
            }
            Err(elapsed) => {
                tracing::error!("request {:?} timed out", req_id);
                Err(CallError::Timeout(elapsed))
            }
        }
    }
}

impl Reconnection for WebSocket {
    type Address = String;
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    async fn connect(
        addr: Self::Address,
        timeout: Duration,
        peer_id: PeerId,
        conn_id: ConnectionId,
    ) -> Result<Box<Self>, Self::ConnectError> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(addr).await?;
        Ok(Box::new(Self::new(ws_stream, timeout, peer_id, conn_id)))
    }

    async fn run(&self) -> Result<(), RunError> {
        let pending = self.pending.clone();
        while let Some(msg) = self.ws_reader.lock().await.next().await {
            tracing::debug!("received ws message");
            match msg {
                Ok(tungstenite::Message::Binary(bytes)) => {
                    let (msg, _size): (Message, usize) =
                        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;

                    match msg {
                        Message::BatchSyncResponse(resp) => {
                            let req_id = resp.req_id;
                            tracing::info!("dispatching to waiter {:?}", req_id);
                            if let Some(waiting) = pending.write().await.remove(&req_id) {
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
                                self.inbound_writer.send(Message::BatchSyncResponse(resp))?;
                            }
                        }
                        other => {
                            self.inbound_writer.send(other)?;
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
                    std::mem::take(&mut *pending.write().await);
                    break;
                }
                Err(e) => Err(e)?,
            }
        }

        Ok(())
    }
}

/// Problem while attempting to send a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Serialization error.
    #[error("Bincode error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(bincode::error::EncodeError),

    /// Problem receiving on the internal channel.
    #[error("Channel receive error: {0}")]
    ChanError(#[from] oneshot::error::RecvError),

    /// Timed out waiting for response.
    #[error("Timed out waiting for response: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
}

/// Problem while attempting to receive a message.
#[derive(Debug, Error)]
pub enum RecvError {
    /// Problem receiving on the internal channel.
    #[error("Channel receive error: {0}")]
    ChanError(#[from] oneshot::error::RecvError),

    /// Attempted to read from a closed channel.
    #[error("Attempted to read from closed channel")]
    ReadFromClosed,
}

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("Disconnected")]
pub struct DisconnectionError;

/// Errors while running the connection loop.
#[derive(Debug, Error)]
pub enum RunError {
    /// Internal MPSC channel error.
    #[error("Channel send error: {0}")]
    ChanSend(#[from] tokio::sync::mpsc::error::SendError<Message>),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Deserialization error.
    #[error("Bincode deserialize error: {0}")]
    Deserialize(#[from] bincode::error::DecodeError),
}
