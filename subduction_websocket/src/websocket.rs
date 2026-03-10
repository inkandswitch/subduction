//! # Generic WebSocket connection for Subduction

use alloc::{boxed::Box, sync::Arc};
use core::{
    fmt::Debug,
    future::{Future, IntoFuture},
    marker::PhantomData,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use future_form::{FutureForm, Local, Sendable};
use futures::{
    FutureExt,
    channel::oneshot,
    future::{BoxFuture, LocalBoxFuture},
};
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    collections::Map,
};
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    peer::id::PeerId,
};

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    timeout::{TimedOut, Timeout},
};

/// Channel capacity for outbound messages.
///
/// This is sized to allow many concurrent sends without blocking while still
/// providing backpressure if the sender task can't keep up.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Trait for message types that can carry [`SyncMessage`] over internal
/// channels.
///
/// Implemented for [`SyncMessage`] (identity) and, with the `ephemeral`
/// feature, for [`WireMessage`](subduction_ephemeral::wire::WireMessage).
pub trait ChannelMessage: Clone + Debug + Send + Sync + Encode + Decode + 'static {
    /// Wrap a [`SyncMessage`] into this channel type.
    fn wrap_sync(msg: SyncMessage) -> Self;

    /// Try to extract a [`BatchSyncResponse`] without consuming the message.
    ///
    /// Returns `Some` if this message is (or wraps) a [`BatchSyncResponse`].
    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse>;

    /// Try to unwrap into a [`SyncMessage`].
    ///
    /// Returns `None` if the message is not a sync message (e.g., ephemeral).
    fn into_sync(self) -> Option<SyncMessage>;
}

impl ChannelMessage for SyncMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        msg
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            SyncMessage::BatchSyncResponse(resp) => Some(resp),
            SyncMessage::LooseCommit { .. }
            | SyncMessage::Fragment { .. }
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::BatchSyncRequest(_)
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::DataRequestRejected(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        Some(self)
    }
}

#[cfg(feature = "ephemeral")]
impl ChannelMessage for subduction_ephemeral::wire::WireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(alloc::boxed::Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(sync_msg) => match sync_msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::LooseCommit { .. }
                | SyncMessage::Fragment { .. }
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::BatchSyncRequest(_)
                | SyncMessage::RemoveSubscriptions(_)
                | SyncMessage::DataRequestRejected(_) => None,
            },
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }
}

/// A background task that receives incoming WebSocket messages and dispatches them.
///
/// Must be spawned (e.g., via `tokio::spawn`) for the connection to receive messages.
pub struct ListenerTask<'a, M: core::fmt::Debug>(BoxFuture<'a, Result<(), RunError<M>>>);

impl<M: core::fmt::Debug> core::fmt::Debug for ListenerTask<'_, M> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ListenerTask").finish_non_exhaustive()
    }
}

impl<'a, M: core::fmt::Debug> ListenerTask<'a, M> {
    pub(crate) fn new(fut: BoxFuture<'a, Result<(), RunError<M>>>) -> Self {
        Self(fut)
    }
}

impl<'a, M: core::fmt::Debug> IntoFuture for ListenerTask<'a, M> {
    type Output = Result<(), RunError<M>>;
    type IntoFuture = BoxFuture<'a, Result<(), RunError<M>>>;

    fn into_future(self) -> Self::IntoFuture {
        self.0
    }
}

/// A background task that drains outbound messages to the WebSocket.
///
/// Must be spawned (e.g., via `tokio::spawn`) for the connection to send messages.
pub struct SenderTask<'a, M: core::fmt::Debug>(BoxFuture<'a, Result<(), RunError<M>>>);

impl<M: core::fmt::Debug> core::fmt::Debug for SenderTask<'_, M> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SenderTask").finish_non_exhaustive()
    }
}

impl<'a, M: core::fmt::Debug> SenderTask<'a, M> {
    pub(crate) fn new(fut: BoxFuture<'a, Result<(), RunError<M>>>) -> Self {
        Self(fut)
    }
}

impl<'a, M: core::fmt::Debug> IntoFuture for SenderTask<'a, M> {
    type Output = Result<(), RunError<M>>;
    type IntoFuture = BoxFuture<'a, Result<(), RunError<M>>>;

    fn into_future(self) -> Self::IntoFuture {
        self.0
    }
}

/// A WebSocket implementation for [`Connection`].
///
/// Parameterized over:
/// - `T`: the underlying async I/O stream (e.g., `TcpStream`, `ConnectStream`)
/// - `K`: the async future form (`Local` or `Sendable`)
/// - `O`: the timeout strategy
/// - `M`: the channel message type
#[derive(Debug)]
pub struct WebSocket<
    T: AsyncRead + AsyncWrite + Unpin,
    K: FutureForm,
    O: Timeout<K>,
    M: ChannelMessage,
> {
    chan_id: u64,
    peer_id: PeerId,
    req_id_counter: Arc<AtomicU64>,

    timeout_strategy: O,
    default_time_limit: Duration,

    ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,

    /// Channel for outbound messages. A dedicated sender task drains this to the WebSocket.
    /// This eliminates mutex contention when many tasks send concurrently.
    outbound_tx: async_channel::Sender<tungstenite::Message>,

    /// The actual WebSocket sender, used only by the sender task.
    ws_sender: Arc<Mutex<WebSocketSender<T>>>,

    pending: Arc<Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>>,

    inbound_writer: async_channel::Sender<M>,
    inbound_reader: async_channel::Receiver<M>,

    _phantom: PhantomData<K>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Local>, M: ChannelMessage>
    Connection<Local, SyncMessage> for WebSocket<T, Local, O, M>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.peer_id, "WebSocket::disconnect");
        async { Ok(()) }.boxed_local()
    }

    fn send(&self, message: &SyncMessage) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "ws: sending outbound with message id {:?} to peer_id {}",
            message.request_id(),
            self.peer_id
        );

        let msg_bytes = M::wrap_sync(message.clone()).encode();

        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;

            Ok(())
        }
        .boxed_local()
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        tracing::debug!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);

        async move {
            loop {
                let msg = chan.recv().await.map_err(|_| {
                    tracing::error!("inbound channel closed unexpectedly");
                    RecvError
                })?;

                if let Some(sync_msg) = msg.into_sync() {
                    tracing::debug!("recv: inbound message {sync_msg:?}");
                    return Ok(sync_msg);
                }

                tracing::trace!("recv<SyncMessage>: skipping non-sync channel message");
            }
        }
        .boxed_local()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Local>, M: ChannelMessage>
    Roundtrip<Local, BatchSyncRequest, BatchSyncResponse> for WebSocket<T, Local, O, M>
{
    type CallError = CallError;

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

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.outbound_tx.clone();
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(req_id, tx);

            let msg_bytes = M::wrap_sync(SyncMessage::BatchSyncRequest(req)).encode();

            outbound_tx
                .send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| CallError::SenderTaskStopped)?;

            tracing::debug!(
                chan_id = self.chan_id,
                "sent WebSocket request {:?}",
                req_id
            );

            let req_timeout = override_timeout.unwrap_or(self.default_time_limit);

            match self
                .timeout_strategy
                .timeout(req_timeout, Box::pin(rx))
                .await
            {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    Err(CallError::ResponseDropped(e))
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

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm, O: Timeout<K>, M: ChannelMessage>
    WebSocket<T, K, O, M>
{
    /// Create a new WebSocket connection.
    ///
    /// Returns the connection and a sender task future. The sender task drains
    /// the outbound message channel to the WebSocket write half and should be
    /// spawned as a background task.
    ///
    /// The sender task captures only the channel receiver and write half — it
    /// does _not_ retain a clone of the full `WebSocket` (and therefore does
    /// not keep the outbound channel sender alive). When the sender task exits
    /// (e.g., due to a WebSocket write error), the channel closes and
    /// subsequent `send()`/`call()` attempts fail fast.
    pub fn new(
        ws: WebSocketStream<T>,
        timeout_strategy: O,
        default_time_limit: Duration,
        peer_id: PeerId,
    ) -> (
        Self,
        impl Future<Output = Result<(), RunError<M>>> + use<T, K, O, M>,
    ) {
        tracing::info!("new WebSocket connection for peer {:?}", peer_id);
        let (ws_writer, ws_reader) = ws.split();
        let pending = Arc::new(Mutex::new(Map::<
            RequestId,
            oneshot::Sender<BatchSyncResponse>,
        >::new()));
        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let starting_counter = rand::random::<u64>();
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
            peer_id,
            chan_id,

            req_id_counter: Arc::new(starting_counter.into()),
            timeout_strategy,
            default_time_limit,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound_tx,
            ws_sender,
            pending,
            inbound_writer,
            inbound_reader,

            _phantom: PhantomData,
        };

        (ws, sender_task)
    }

    /// The timeout strategy used for requests.
    #[must_use]
    pub const fn timeout_strategy(&self) -> &O {
        &self.timeout_strategy
    }

    /// The timeout for requests.
    #[must_use]
    pub const fn default_time_limit(&self) -> Duration {
        self.default_time_limit
    }

    /// Get the [`PeerId`] associated with this connection.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Listen for incoming messages and dispatch them appropriately.
    ///
    /// # Errors
    ///
    /// If there is an error reading from the WebSocket or processing messages.
    #[allow(clippy::too_many_lines)] // 101/100 allowed lines
    pub async fn listen(&self) -> Result<(), RunError<M>> {
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
                    let msg = M::try_decode(&bytes).map_err(|e| {
                        tracing::error!(
                            "failed to deserialize inbound message from peer {:?}: {}",
                            self.peer_id,
                            e
                        );
                        RunError::Deserialize(e)
                    })?;

                    tracing::debug!(
                        "decoded inbound message from peer {:?}: {:?}",
                        self.peer_id,
                        &msg
                    );

                    if let Some(resp) = msg.as_batch_sync_response() {
                        let req_id = resp.req_id;
                        if let Some(waiting) = self.pending.lock().await.remove(&req_id) {
                            tracing::info!("received BatchSyncResponse for req_id {:?}", req_id);
                            tracing::info!("dispatching to waiter {:?}", req_id);
                            let result = waiting.send(resp.clone());
                            if result.is_err() {
                                tracing::error!(
                                    "oneshot channel closed before sending response for req_id {:?}",
                                    req_id
                                );
                            }
                            continue;
                        }
                    }

                    self.inbound_writer.send(msg).await.map_err(|e| {
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

impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Sendable> + Sync, M: ChannelMessage>
    Connection<Sendable, SyncMessage> for WebSocket<T, Sendable, O, M>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.peer_id, "WebSocket::disconnect");
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: &SyncMessage) -> BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "sending outbound message id {:?} / {message:?}",
            message.request_id()
        );

        let msg_bytes = M::wrap_sync(message.clone()).encode();

        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;

            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        tracing::debug!(chan_id = self.chan_id, "waiting on recv {:?}", self.peer_id);

        async move {
            loop {
                let msg = chan.recv().await.map_err(|_| {
                    tracing::error!("inbound channel closed unexpectedly");
                    RecvError
                })?;

                if let Some(sync_msg) = msg.into_sync() {
                    tracing::debug!("recv: inbound message {sync_msg:?}");
                    return Ok(sync_msg);
                }

                tracing::trace!("recv<SyncMessage>: skipping non-sync channel message");
            }
        }
        .boxed()
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Sendable> + Sync, M: ChannelMessage>
    Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse> for WebSocket<T, Sendable, O, M>
{
    type CallError = CallError;

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

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.outbound_tx.clone();
        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            // Pre-register channel
            let (tx, rx) = oneshot::channel();
            self.pending.lock().await.insert(req_id, tx);

            let msg_bytes = M::wrap_sync(SyncMessage::BatchSyncRequest(req)).encode();

            outbound_tx
                .send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| CallError::SenderTaskStopped)?;

            tracing::info!(
                chan_id = self.chan_id,
                "sent WebSocket request {:?}",
                req_id
            );

            let req_timeout = override_timeout.unwrap_or(self.default_time_limit);

            match self
                .timeout_strategy
                .timeout(req_timeout, Box::pin(rx))
                .await
            {
                Ok(Ok(resp)) => {
                    tracing::info!("request {:?} completed", req_id);
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {:?} failed to receive response: {}", req_id, e);
                    Err(CallError::ResponseDropped(e))
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

// --- Feature-gated Connection impls for WireMessage and EphemeralMessage ---

#[cfg(feature = "ephemeral")]
impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Local>>
    Connection<Local, subduction_ephemeral::wire::WireMessage>
    for WebSocket<T, Local, O, subduction_ephemeral::wire::WireMessage>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(
        &self,
        message: &subduction_ephemeral::wire::WireMessage,
    ) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let msg_bytes = message.encode();
        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(
        &self,
    ) -> LocalBoxFuture<'_, Result<subduction_ephemeral::wire::WireMessage, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        async move {
            let msg = chan.recv().await.map_err(|_| RecvError)?;
            Ok(msg)
        }
        .boxed_local()
    }
}

#[cfg(feature = "ephemeral")]
impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Sendable> + Sync>
    Connection<Sendable, subduction_ephemeral::wire::WireMessage>
    for WebSocket<T, Sendable, O, subduction_ephemeral::wire::WireMessage>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(
        &self,
        message: &subduction_ephemeral::wire::WireMessage,
    ) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let msg_bytes = message.encode();
        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;
            Ok(())
        }
        .boxed()
    }

    fn recv(
        &self,
    ) -> BoxFuture<'_, Result<subduction_ephemeral::wire::WireMessage, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        async move {
            let msg = chan.recv().await.map_err(|_| RecvError)?;
            Ok(msg)
        }
        .boxed()
    }
}

#[cfg(feature = "ephemeral")]
impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Local>>
    Connection<Local, subduction_ephemeral::message::EphemeralMessage>
    for WebSocket<T, Local, O, subduction_ephemeral::wire::WireMessage>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(
        &self,
        message: &subduction_ephemeral::message::EphemeralMessage,
    ) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let wire = subduction_ephemeral::wire::WireMessage::Ephemeral(message.clone());
        let msg_bytes = wire.encode();
        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;
            Ok(())
        }
        .boxed_local()
    }

    fn recv(
        &self,
    ) -> LocalBoxFuture<'_, Result<subduction_ephemeral::message::EphemeralMessage, Self::RecvError>>
    {
        let chan = self.inbound_reader.clone();
        async move {
            loop {
                let wire = chan.recv().await.map_err(|_| RecvError)?;
                if let subduction_ephemeral::wire::WireMessage::Ephemeral(msg) = wire {
                    return Ok(msg);
                }
                tracing::trace!("recv<EphemeralMessage>: skipping non-ephemeral message");
            }
        }
        .boxed_local()
    }
}

#[cfg(feature = "ephemeral")]
impl<T: AsyncRead + AsyncWrite + Unpin + Send, O: Timeout<Sendable> + Sync>
    Connection<Sendable, subduction_ephemeral::message::EphemeralMessage>
    for WebSocket<T, Sendable, O, subduction_ephemeral::wire::WireMessage>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(
        &self,
        message: &subduction_ephemeral::message::EphemeralMessage,
    ) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let wire = subduction_ephemeral::wire::WireMessage::Ephemeral(message.clone());
        let msg_bytes = wire.encode();
        let tx = self.outbound_tx.clone();
        async move {
            tx.send(tungstenite::Message::Binary(msg_bytes.into()))
                .await
                .map_err(|_| SendError)?;
            Ok(())
        }
        .boxed()
    }

    fn recv(
        &self,
    ) -> BoxFuture<'_, Result<subduction_ephemeral::message::EphemeralMessage, Self::RecvError>>
    {
        let chan = self.inbound_reader.clone();
        async move {
            loop {
                let wire = chan.recv().await.map_err(|_| RecvError)?;
                if let subduction_ephemeral::wire::WireMessage::Ephemeral(msg) = wire {
                    return Ok(msg);
                }
                tracing::trace!("recv<EphemeralMessage>: skipping non-ephemeral message");
            }
        }
        .boxed()
    }
}

// --- Clone, PartialEq ---

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm, O: Timeout<K>, M: ChannelMessage> Clone
    for WebSocket<T, K, O, M>
{
    fn clone(&self) -> Self {
        Self {
            chan_id: self.chan_id,
            peer_id: self.peer_id,
            req_id_counter: self.req_id_counter.clone(),
            timeout_strategy: self.timeout_strategy.clone(),
            default_time_limit: self.default_time_limit,
            ws_reader: self.ws_reader.clone(),
            outbound_tx: self.outbound_tx.clone(),
            ws_sender: self.ws_sender.clone(),
            pending: self.pending.clone(),
            inbound_writer: self.inbound_writer.clone(),
            inbound_reader: self.inbound_reader.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, K: FutureForm, O: Timeout<K>, M: ChannelMessage> PartialEq
    for WebSocket<T, K, O, M>
{
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
            && self.outbound_tx.same_channel(&other.outbound_tx)
            && Arc::ptr_eq(&self.pending, &other.pending)
            && self.inbound_writer.same_channel(&other.inbound_writer)
            && self.inbound_reader.same_channel(&other.inbound_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    use futures::io::Cursor;
    use testresult::TestResult;

    // Mock timeout strategy for testing
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct MockTimeout;

    impl Timeout<Local> for MockTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            fut: LocalBoxFuture<'a, T>,
        ) -> LocalBoxFuture<'a, Result<T, crate::timeout::TimedOut>> {
            async move { Ok(fut.await) }.boxed_local()
        }
    }

    impl Timeout<Sendable> for MockTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, crate::timeout::TimedOut>> {
            async move { Ok(fut.await) }.boxed()
        }
    }

    async fn create_mock_websocket_stream() -> WebSocketStream<Cursor<Vec<u8>>> {
        use async_tungstenite::WebSocketStream;
        use futures::io::Cursor;

        // Create a mock stream
        let buffer = Cursor::new(Vec::new());
        WebSocketStream::from_raw_socket(buffer, tungstenite::protocol::Role::Client, None).await
    }

    mod request_ids {
        use super::*;

        #[tokio::test]
        async fn test_next_request_id_includes_peer_id() {
            let ws = create_mock_websocket_stream().await;
            let peer_id = PeerId::new([99u8; 32]);
            let timeout = MockTimeout;
            let duration = Duration::from_secs(30);

            let (websocket, _rx): (WebSocket<_, Sendable, _, SyncMessage>, _) =
                WebSocket::new(ws, timeout, duration, peer_id);

            let req_id = websocket.next_request_id().await;
            assert_eq!(req_id.requestor, peer_id);
        }

        #[tokio::test]
        async fn test_next_request_id_increments_nonce() {
            let ws = create_mock_websocket_stream().await;
            let peer_id = PeerId::new([1u8; 32]);
            let timeout = MockTimeout;
            let duration = Duration::from_secs(30);

            let (websocket, _rx): (WebSocket<_, Sendable, _, SyncMessage>, _) =
                WebSocket::new(ws, timeout, duration, peer_id);

            let req_id1 = websocket.next_request_id().await;
            let req_id2 = websocket.next_request_id().await;
            let req_id3 = websocket.next_request_id().await;

            assert_eq!(req_id2.nonce, req_id1.nonce + 1);
            assert_eq!(req_id3.nonce, req_id2.nonce + 1);
        }

        #[tokio::test]
        async fn test_concurrent_request_ids_are_unique() -> TestResult {
            let ws = create_mock_websocket_stream().await;
            let peer_id = PeerId::new([1u8; 32]);
            let timeout = MockTimeout;
            let duration = Duration::from_secs(30);

            let (websocket, _rx): (WebSocket<_, Sendable, _, SyncMessage>, _) =
                WebSocket::new(ws, timeout, duration, peer_id);

            let ws1 = websocket.clone();
            let ws2 = websocket.clone();
            let ws3 = websocket.clone();

            let handle1 = tokio::spawn(async move { ws1.next_request_id().await });
            let handle2 = tokio::spawn(async move { ws2.next_request_id().await });
            let handle3 = tokio::spawn(async move { ws3.next_request_id().await });

            let req_id1 = handle1.await?;
            let req_id2 = handle2.await?;
            let req_id3 = handle3.await?;

            // All IDs should be unique (have different nonces)
            assert_ne!(req_id1.nonce, req_id2.nonce);
            assert_ne!(req_id2.nonce, req_id3.nonce);
            assert_ne!(req_id1.nonce, req_id3.nonce);

            Ok(())
        }
    }
}
