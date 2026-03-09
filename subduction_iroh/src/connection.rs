//! Iroh (QUIC) connection implementing [`Connection<Sendable>`].
//!
//! Uses a single QUIC bi-directional stream for framed message exchange.
//! The connection follows the same internal architecture as the WebSocket
//! and HTTP long-poll transports:
//!
//! ```text
//! send()  --> outbound_tx --> [sender task] --> QUIC SendStream
//! QUIC RecvStream --> [listener task] --> inbound_writer --> recv()
//! ```
//!
//! The `call()` method uses the pending-map + oneshot pattern for
//! request-response correlation, identical to other transports.
//!
//! The connection is parameterized over a channel message type `M`.
//! Use `SyncMessage` for plain sync traffic, or `WireMessage` (with the
//! `ephemeral` feature) to multiplex sync and ephemeral traffic on a single
//! connection.

use alloc::sync::Arc;
use core::{
    fmt::Debug,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use future_form::{FutureForm, Sendable};
use futures::{FutureExt, channel::oneshot};
use rand::RngCore;
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    collections::Map,
};
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
        timeout::{TimedOut, Timeout},
    },
    peer::id::PeerId,
};

use crate::error::{CallError, DisconnectionError, RecvError, SendError};

/// Channel capacity for outbound messages.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for inbound messages.
const INBOUND_CHANNEL_CAPACITY: usize = 128;

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
            Self::Ephemeral(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) => None,
        }
    }
}

/// Shared interior state for an Iroh connection.
#[derive(Debug)]
struct Inner<O, M> {
    peer_id: PeerId,
    chan_id: u64,
    req_id_counter: AtomicU64,
    default_time_limit: Duration,
    timeout: O,

    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,

    /// Messages from `send()` / `call()` -> drained by the sender task.
    outbound_tx: async_channel::Sender<M>,

    /// Messages from the listener task -> picked up by `recv()`.
    inbound_writer: async_channel::Sender<M>,
    inbound_reader: async_channel::Receiver<M>,

    /// The underlying iroh QUIC connection (for closing).
    quic_conn: iroh::endpoint::Connection,
}

/// An Iroh (QUIC) connection that implements [`Connection<Sendable>`].
///
/// Created by [`connect`](crate::client::connect) or the server accept loop.
/// Uses a single QUIC bi-directional stream under the hood for framed message
/// passing.
///
/// The `O` parameter is the timeout strategy (e.g., `TimeoutTokio`).
///
/// The `M` parameter is the channel message type. Use [`SyncMessage`] for
/// plain sync traffic, or [`WireMessage`](subduction_ephemeral::wire::WireMessage)
/// (with the `ephemeral` feature) for multiplexed sync + ephemeral traffic.
#[derive(Debug, Clone)]
pub struct IrohConnection<O, M> {
    inner: Arc<Inner<O, M>>,
}

impl<O, M: ChannelMessage> IrohConnection<O, M> {
    /// Create a new Iroh connection from a QUIC connection.
    ///
    /// The caller is responsible for spawning the listener and sender tasks
    /// returned by [`listener_task`] and [`sender_task`].
    ///
    /// [`listener_task`]: crate::tasks::listener_task
    /// [`sender_task`]: crate::tasks::sender_task
    #[must_use]
    pub fn new(
        peer_id: PeerId,
        quic_conn: iroh::endpoint::Connection,
        default_time_limit: Duration,
        timeout: O,
    ) -> (Self, async_channel::Receiver<M>) {
        let (inbound_writer, inbound_reader) = async_channel::bounded(INBOUND_CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let starting_counter = rand::rngs::OsRng.next_u64();
        let chan_id = rand::rngs::OsRng.next_u64();

        let conn = Self {
            inner: Arc::new(Inner {
                peer_id,
                chan_id,
                req_id_counter: AtomicU64::new(starting_counter),
                default_time_limit,
                timeout,
                pending: Mutex::new(Map::new()),
                outbound_tx,
                inbound_writer,
                inbound_reader,
                quic_conn,
            }),
        };

        (conn, outbound_rx)
    }

    /// Push a decoded message into the inbound channel.
    ///
    /// `BatchSyncResponse` messages are routed to waiting `call()` oneshots.
    /// All other messages go to the inbound channel for `recv()`.
    pub(crate) async fn push_inbound(&self, msg: M) -> Result<(), async_channel::SendError<M>> {
        // Route BatchSyncResponse to pending waiters (for call/roundtrip).
        if let Some(resp) = msg.as_batch_sync_response() {
            let req_id = resp.req_id;
            if let Some(waiting) = self.inner.pending.lock().await.remove(&req_id) {
                if waiting.send(resp.clone()).is_err() {
                    tracing::error!("oneshot closed before sending response for req_id {req_id:?}");
                }
                return Ok(());
            }
        }

        // All other messages go to the inbound channel.
        self.inner.inbound_writer.send(msg).await
    }

    /// Close the connection's channels and the underlying QUIC connection.
    pub fn close(&self) {
        self.inner.inbound_writer.close();
        self.inner.outbound_tx.close();
        self.inner.inbound_reader.close();
        self.inner.quic_conn.close(0u32.into(), b"subduction close");
    }

    /// Access the underlying iroh QUIC connection.
    #[must_use]
    pub fn quic_connection(&self) -> &iroh::endpoint::Connection {
        &self.inner.quic_conn
    }
}

impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage> Connection<Sendable, SyncMessage>
    for IrohConnection<O, M>
{
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id
    }

    fn disconnect(&self) -> futures::future::BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.inner.peer_id, "IrohConnection::disconnect");
        let conn = self.clone();
        async move {
            conn.close();
            Ok(())
        }
        .boxed()
    }

    fn send(
        &self,
        message: &SyncMessage,
    ) -> futures::future::BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "iroh: sending outbound message id {:?} to peer {}",
            message.request_id(),
            self.inner.peer_id
        );

        let msg = M::wrap_sync(message.clone());
        let tx = self.inner.outbound_tx.clone();
        async move {
            tx.send(msg).await.map_err(|_| SendError)?;
            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> futures::future::BoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        let chan = self.inner.inbound_reader.clone();
        tracing::debug!(
            chan_id = self.inner.chan_id,
            "waiting on recv {:?}",
            self.inner.peer_id
        );

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

                // Skip non-sync messages (e.g., ephemeral when M = WireMessage)
                tracing::trace!("recv<SyncMessage>: skipping non-sync channel message");
            }
        }
        .boxed()
    }
}

impl<O: Timeout<Sendable> + Send + Sync, M: ChannelMessage>
    Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse> for IrohConnection<O, M>
{
    type CallError = CallError;

    fn next_request_id(&self) -> futures::future::BoxFuture<'_, RequestId> {
        async {
            let counter = self.inner.req_id_counter.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("generated request id {counter:?}");
            RequestId {
                requestor: self.inner.peer_id,
                nonce: counter,
            }
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> futures::future::BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.inner.outbound_tx.clone();
        let timeout = self.inner.timeout.clone();
        let default_time_limit = self.inner.default_time_limit;
        let inner = self.inner.clone();

        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            let (tx, rx) = oneshot::channel();
            inner.pending.lock().await.insert(req_id, tx);

            let msg = M::wrap_sync(SyncMessage::BatchSyncRequest(req));
            outbound_tx
                .send(msg)
                .await
                .map_err(|_| CallError::ChannelClosed)?;

            tracing::debug!(chan_id = inner.chan_id, "sent iroh request {req_id:?}");

            let req_timeout = override_timeout.unwrap_or(default_time_limit);
            let rx_fut = Sendable::from_future(rx);

            match timeout.timeout(req_timeout, rx_fut).await {
                Ok(Ok(resp)) => {
                    tracing::info!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(e)) => {
                    tracing::error!("request {req_id:?} failed: {e}");
                    Err(CallError::ResponseDropped(e))
                }
                Err(TimedOut) => {
                    tracing::error!("request {req_id:?} timed out");
                    inner.pending.lock().await.remove(&req_id);
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

// ── Ephemeral Connection impls ──────────────────────────────────────────

#[cfg(feature = "ephemeral")]
mod ephemeral_impls {
    use future_form::Sendable;
    use futures::FutureExt;
    use subduction_core::{
        connection::{Connection, timeout::Timeout},
        peer::id::PeerId,
    };
    use subduction_ephemeral::{message::EphemeralMessage, wire::WireMessage};

    use super::{DisconnectionError, IrohConnection, RecvError, SendError};

    impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable, WireMessage>
        for IrohConnection<O, WireMessage>
    {
        type SendError = SendError;
        type RecvError = RecvError;
        type DisconnectionError = DisconnectionError;

        fn peer_id(&self) -> PeerId {
            self.inner.peer_id
        }

        fn disconnect(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<(), Self::DisconnectionError>> {
            tracing::info!(peer_id = %self.inner.peer_id, "IrohConnection<WireMessage>::disconnect");
            let conn = self.clone();
            async move {
                conn.close();
                Ok(())
            }
            .boxed()
        }

        fn send(
            &self,
            message: &WireMessage,
        ) -> futures::future::BoxFuture<'_, Result<(), Self::SendError>> {
            let msg = message.clone();
            let tx = self.inner.outbound_tx.clone();
            async move {
                tx.send(msg).await.map_err(|_| SendError)?;
                Ok(())
            }
            .boxed()
        }

        fn recv(&self) -> futures::future::BoxFuture<'_, Result<WireMessage, Self::RecvError>> {
            let chan = self.inner.inbound_reader.clone();
            async move {
                chan.recv().await.map_err(|_| {
                    tracing::error!("inbound channel closed unexpectedly");
                    RecvError
                })
            }
            .boxed()
        }
    }

    impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable, EphemeralMessage>
        for IrohConnection<O, WireMessage>
    {
        type SendError = SendError;
        type RecvError = RecvError;
        type DisconnectionError = DisconnectionError;

        fn peer_id(&self) -> PeerId {
            self.inner.peer_id
        }

        fn disconnect(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<(), Self::DisconnectionError>> {
            tracing::info!(
                peer_id = %self.inner.peer_id,
                "IrohConnection<EphemeralMessage>::disconnect"
            );
            let conn = self.clone();
            async move {
                conn.close();
                Ok(())
            }
            .boxed()
        }

        fn send(
            &self,
            message: &EphemeralMessage,
        ) -> futures::future::BoxFuture<'_, Result<(), Self::SendError>> {
            let msg = WireMessage::Ephemeral(message.clone());
            let tx = self.inner.outbound_tx.clone();
            async move {
                tx.send(msg).await.map_err(|_| SendError)?;
                Ok(())
            }
            .boxed()
        }

        fn recv(
            &self,
        ) -> futures::future::BoxFuture<'_, Result<EphemeralMessage, Self::RecvError>> {
            let chan = self.inner.inbound_reader.clone();
            async move {
                loop {
                    let wire = chan.recv().await.map_err(|_| {
                        tracing::error!("inbound channel closed unexpectedly");
                        RecvError
                    })?;

                    if let WireMessage::Ephemeral(msg) = wire {
                        return Ok(msg);
                    }

                    // Skip non-ephemeral messages
                    tracing::trace!(
                        "recv<EphemeralMessage>: skipping non-ephemeral channel message"
                    );
                }
            }
            .boxed()
        }
    }
}

impl<O, M> PartialEq for IrohConnection<O, M> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
