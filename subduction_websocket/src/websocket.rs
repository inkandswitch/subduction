//! # Generic WebSocket transport for Subduction

use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt::Debug,
    future::{Future, IntoFuture},
    marker::PhantomData,
};

use async_lock::Mutex;
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::future::BoxFuture;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use subduction_core::{peer::id::PeerId, transport::Transport};

use crate::error::{DisconnectionError, RecvError, RunError, SendError};

/// Channel capacity for outbound messages.
///
/// This is sized to allow many concurrent sends without blocking while still
/// providing backpressure if the sender task can't keep up.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// A background task that receives incoming WebSocket messages and dispatches them.
///
/// Must be spawned (e.g., via `tokio::spawn`) for the connection to receive messages.
pub struct ListenerTask<'a>(BoxFuture<'a, Result<(), RunError>>);

impl core::fmt::Debug for ListenerTask<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ListenerTask").finish_non_exhaustive()
    }
}

impl<'a> ListenerTask<'a> {
    pub(crate) fn new(fut: BoxFuture<'a, Result<(), RunError>>) -> Self {
        Self(fut)
    }
}

impl<'a> IntoFuture for ListenerTask<'a> {
    type Output = Result<(), RunError>;
    type IntoFuture = BoxFuture<'a, Result<(), RunError>>;

    fn into_future(self) -> Self::IntoFuture {
        self.0
    }
}

/// A background task that drains outbound messages to the WebSocket.
///
/// Must be spawned (e.g., via `tokio::spawn`) for the connection to send messages.
pub struct SenderTask<'a>(BoxFuture<'a, Result<(), RunError>>);

impl core::fmt::Debug for SenderTask<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SenderTask").finish_non_exhaustive()
    }
}

impl<'a> SenderTask<'a> {
    pub(crate) fn new(fut: BoxFuture<'a, Result<(), RunError>>) -> Self {
        Self(fut)
    }
}

impl<'a> IntoFuture for SenderTask<'a> {
    type Output = Result<(), RunError>;
    type IntoFuture = BoxFuture<'a, Result<(), RunError>>;

    fn into_future(self) -> Self::IntoFuture {
        self.0
    }
}

/// A WebSocket implementation for [`Transport`].
///
/// Parameterized over:
/// - `T`: the underlying async I/O stream (e.g., `TcpStream`, `ConnectStream`)
/// - `K`: the async future form (`Local` or `Sendable`)
#[derive(Debug)]
pub struct WebSocket<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm> {
    chan_id: u64,
    peer_id: PeerId,

    ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,

    /// Channel for outbound messages. A dedicated sender task drains this to the WebSocket.
    /// This eliminates mutex contention when many tasks send concurrently.
    outbound_tx: async_channel::Sender<tungstenite::Message>,

    /// The actual WebSocket sender, used only by the sender task.
    ws_sender: Arc<Mutex<WebSocketSender<T>>>,

    inbound_writer: async_channel::Sender<Vec<u8>>,
    inbound_reader: async_channel::Receiver<Vec<u8>>,

    _phantom: PhantomData<K>,
}

#[future_form(
    Sendable where
        T: AsyncRead + AsyncWrite + Unpin + Send,
    Local where
        T: AsyncRead + AsyncWrite + Unpin + Send
)]
impl<T, K: FutureForm> Transport<K> for WebSocket<T, K> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.peer_id, "WebSocket::disconnect");
        K::from_future(async { Ok(()) })
    }

    fn send_bytes(&self, bytes: &[u8]) -> K::Future<'_, Result<(), Self::SendError>> {
        let msg = tungstenite::Message::Binary(bytes.to_vec().into());
        let tx = self.outbound_tx.clone();
        K::from_future(async move {
            tx.send(msg).await.map_err(|_| SendError)?;
            Ok(())
        })
    }

    fn recv_bytes(&self) -> K::Future<'_, Result<Vec<u8>, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        tracing::debug!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);

        K::from_future(async move {
            let bytes = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel closed unexpectedly");
                RecvError
            })?;

            tracing::debug!("recv: inbound {} bytes", bytes.len());
            Ok(bytes)
        })
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm> WebSocket<T, K> {
    /// Create a new WebSocket transport.
    ///
    /// Returns the transport and a sender task future. The sender task drains
    /// the outbound message channel to the WebSocket write half and should be
    /// spawned as a background task.
    ///
    /// The sender task captures only the channel receiver and write half — it
    /// does _not_ retain a clone of the full `WebSocket` (and therefore does
    /// not keep the outbound channel sender alive). When the sender task exits
    /// (e.g., due to a WebSocket write error), the channel closes and
    /// subsequent `send_bytes()`/`call()` attempts fail fast.
    pub fn new(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
    ) -> (Self, impl Future<Output = Result<(), RunError>> + use<T, K>) {
        tracing::info!("new WebSocket connection for peer {:?}", peer_id);
        let (ws_writer, ws_reader) = ws.split();
        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = rand::random::<u64>();

        let ws_sender = Arc::new(Mutex::new(ws_writer));

        let sender_task = {
            let ws_sender = ws_sender.clone();
            async move {
                tracing::info!("starting WebSocket sender task for peer {:?}", peer_id);

                let mut ws_sender = ws_sender.lock().await;

                while let Ok(msg) = outbound_rx.recv().await {
                    tracing::debug!("sender task: sending message to WebSocket");
                    ws_sender.send(msg).await?;
                }

                tracing::info!("sender task: outbound channel closed, shutting down");
                Ok(())
            }
        };

        let ws = Self {
            chan_id,
            peer_id,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound_tx,
            ws_sender,
            inbound_writer,
            inbound_reader,

            _phantom: PhantomData,
        };

        (ws, sender_task)
    }

    /// Get the [`PeerId`] associated with this transport.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Listen for incoming messages and forward them to the inbound channel.
    ///
    /// Raw bytes from the WebSocket are forwarded to the inbound channel
    /// without decoding. Response routing is handled by
    /// [`Subduction::listen`](subduction_core::subduction::Subduction::listen).
    ///
    /// # Errors
    ///
    /// If there is an error reading from the WebSocket or processing messages.
    #[allow(clippy::too_many_lines)]
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
                    self.inbound_writer
                        .send(bytes.to_vec())
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                "failed to send inbound message to channel {}",
                                self.chan_id,
                            );
                            RunError::ChanSend(Box::new(e))
                        })?;

                    tracing::debug!("forwarded inbound message to channel {}", self.chan_id);
                }
                Ok(tungstenite::Message::Text(text)) => {
                    tracing::warn!("unexpected text message: {}", text);
                }
                Ok(tungstenite::Message::Ping(p)) => {
                    tracing::debug!(size = p.len(), "received ping");
                    self.outbound_tx
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
                    // Distinguish between expected disconnects and real errors
                    if is_expected_disconnect(&e) {
                        tracing::debug!("connection closed: {}", e);
                    } else {
                        tracing::error!("error reading from websocket: {}", e);
                    }
                    Err(e)?;
                }
            }
        }

        Ok(())
    }
}

/// Check if a WebSocket error is an expected disconnect (not a real error).
///
/// These are normal occurrences when the remote end closes without a proper
/// WebSocket close handshake (e.g., browser tab closed, network disconnect).
const fn is_expected_disconnect(e: &tungstenite::Error) -> bool {
    use tungstenite::Error;
    matches!(
        e,
        Error::ConnectionClosed
            | Error::AlreadyClosed
            | Error::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)
    )
}

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm> Clone for WebSocket<T, K> {
    fn clone(&self) -> Self {
        Self {
            chan_id: self.chan_id,
            peer_id: self.peer_id,
            ws_reader: self.ws_reader.clone(),
            outbound_tx: self.outbound_tx.clone(),
            ws_sender: self.ws_sender.clone(),
            inbound_writer: self.inbound_writer.clone(),
            inbound_reader: self.inbound_reader.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm> PartialEq for WebSocket<T, K> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
            && self.outbound_tx.same_channel(&other.outbound_tx)
            && self.inbound_writer.same_channel(&other.inbound_writer)
            && self.inbound_reader.same_channel(&other.inbound_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;

    async fn create_mock_websocket_stream() -> WebSocketStream<Cursor<Vec<u8>>> {
        use async_tungstenite::WebSocketStream;
        use futures::io::Cursor;

        let buffer = Cursor::new(Vec::new());
        WebSocketStream::from_raw_socket(buffer, tungstenite::protocol::Role::Client, None).await
    }

    #[tokio::test]
    async fn test_peer_id_preserved() {
        let ws = create_mock_websocket_stream().await;
        let peer_id = PeerId::new([99u8; 32]);

        let (websocket, _rx): (WebSocket<_, Sendable>, _) = WebSocket::new(ws, peer_id);

        assert_eq!(websocket.peer_id(), peer_id);
    }

    #[tokio::test]
    async fn test_clone_shares_peer_id() {
        let ws = create_mock_websocket_stream().await;
        let peer_id = PeerId::new([1u8; 32]);

        let (websocket, _rx): (WebSocket<_, Sendable>, _) = WebSocket::new(ws, peer_id);

        let cloned = websocket.clone();
        assert_eq!(websocket.peer_id(), cloned.peer_id());
        assert_eq!(websocket, cloned);
    }
}
