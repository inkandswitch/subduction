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
#![allow(clippy::multiple_crate_versions)]

use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use futures::{
    channel::{mpsc, oneshot},
    future,
    lock::Mutex,
};
use futures_timer::Delay;
use futures_util::{AsyncRead, AsyncWrite, SinkExt, StreamExt};
use sedimentree_sync_core::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionId, Message, Reconnection,
        RequestId,
    },
    peer::id::PeerId,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;

/// A WebSocket implementation for [`Connection`].
#[derive(Debug)]
pub struct WebSocket<TcpStream: AsyncRead + AsyncWrite + Unpin> {
    conn_id: ConnectionId,
    peer_id: PeerId,

    req_id_counter: Arc<Mutex<u128>>,
    timeout: Duration,

    ws_reader: Arc<Mutex<WebSocketReceiver<TcpStream>>>,
    outbound: Arc<Mutex<WebSocketSender<TcpStream>>>,

    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    inbound_writer: mpsc::UnboundedSender<Message>,
    inbound_reader: Arc<Mutex<mpsc::UnboundedReceiver<Message>>>,
}

impl<TcpStream: AsyncRead + AsyncWrite + Unpin> WebSocket<TcpStream> {
    /// Create a new WebSocket connection.
    pub fn new(
        ws: WebSocketStream<TcpStream>,
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
}

impl<TcpStream: AsyncRead + AsyncWrite + Unpin> Connection for WebSocket<TcpStream> {
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

impl<TcpStream: AsyncRead + AsyncWrite + Unpin> Reconnection for WebSocket<TcpStream> {
    type Address = String;
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn address(&self) -> &Self::Address {
        // This is a bit of a hack, but we don't store the address in the struct.
        // Instead, we assume that the peer ID can be converted to a string address.
        // In a real implementation, you would store the address separately.
        todo!("Store and return the actual address");
    }

    async fn reconnect(&mut self) -> Result<Self, Self::ConnectError> {
        let (ws_stream, _) = async_tungstenite::async_std::connect_async(self.address())
            .await
            .expect("FIXME");
        todo!("FIXME") // self.
    }

    async fn run(&self) -> Result<(), RunError> {
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
    #[error("Channel canceled: {0}")]
    ChanCanceled(#[from] oneshot::Canceled),

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    Timeout,
}

/// Problem while attempting to receive a message.
#[derive(Debug, Error)]
pub enum RecvError {
    /// Problem receiving on the internal channel.
    #[error("Channel receive error: {0}")]
    ChanCanceled(#[from] oneshot::Canceled),

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
    ChanSend(#[from] futures::channel::mpsc::SendError),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Deserialization error.
    #[error("Bincode deserialize error: {0}")]
    Deserialize(#[from] bincode::error::DecodeError),
}
