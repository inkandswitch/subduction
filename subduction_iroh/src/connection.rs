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

use alloc::sync::Arc;
use core::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use future_form::{FutureForm, Sendable};
use futures::{FutureExt, channel::oneshot};
use rand::RngCore;
use sedimentree_core::collections::Map;
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        timeout::{TimedOut, Timeout},
    },
    peer::id::PeerId,
};

use crate::error::{CallError, DisconnectionError, RecvError, SendError};

/// Channel capacity for outbound messages.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for inbound messages.
const INBOUND_CHANNEL_CAPACITY: usize = 128;

/// Shared interior state for an Iroh connection.
#[derive(Debug)]
struct Inner<O> {
    peer_id: PeerId,
    chan_id: u64,
    req_id_counter: AtomicU64,
    default_time_limit: Duration,
    timeout: O,

    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,

    /// Messages from `send()` / `call()` -> drained by the sender task.
    outbound_tx: async_channel::Sender<Message>,

    /// Messages from the listener task -> picked up by `recv()`.
    inbound_writer: async_channel::Sender<Message>,
    inbound_reader: async_channel::Receiver<Message>,

    /// The underlying iroh QUIC connection (for closing).
    quic_conn: iroh::endpoint::Connection,
}

/// An Iroh (QUIC) connection that implements [`Connection<Sendable>`].
///
/// Created by [`IrohClient::connect`](crate::client::IrohClient::connect) or
/// the server accept loop. Uses a single QUIC bi-directional stream under the
/// hood for framed message passing.
///
/// The `O` parameter is the timeout strategy (e.g., `TimeoutTokio`).
#[derive(Debug, Clone)]
pub struct IrohConnection<O> {
    inner: Arc<Inner<O>>,
}

impl<O> IrohConnection<O> {
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
    ) -> (Self, async_channel::Receiver<Message>) {
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
    pub(crate) async fn push_inbound(
        &self,
        msg: Message,
    ) -> Result<(), async_channel::SendError<Message>> {
        match msg {
            Message::BatchSyncResponse(resp) => {
                let req_id = resp.req_id;
                if let Some(waiting) = self.inner.pending.lock().await.remove(&req_id) {
                    if waiting.send(resp).is_err() {
                        tracing::error!(
                            "oneshot closed before sending response for req_id {req_id:?}"
                        );
                    }
                    Ok(())
                } else {
                    self.inner
                        .inbound_writer
                        .send(Message::BatchSyncResponse(resp))
                        .await
                }
            }
            other @ (Message::LooseCommit { .. }
            | Message::Fragment { .. }
            | Message::BlobsRequest { .. }
            | Message::BlobsResponse { .. }
            | Message::BatchSyncRequest(_)
            | Message::RemoveSubscriptions(_)
            | Message::DataRequestRejected(_)) => self.inner.inbound_writer.send(other).await,
        }
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

impl<O: Timeout<Sendable> + Send + Sync> Connection<Sendable> for IrohConnection<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id
    }

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
        message: &Message,
    ) -> futures::future::BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "iroh: sending outbound message id {:?} to peer {}",
            message.request_id(),
            self.inner.peer_id
        );

        let msg = message.clone();
        let tx = self.inner.outbound_tx.clone();
        async move {
            tx.send(msg).await.map_err(|_| SendError)?;
            Ok(())
        }
        .boxed()
    }

    fn recv(&self) -> futures::future::BoxFuture<'_, Result<Message, Self::RecvError>> {
        let chan = self.inner.inbound_reader.clone();
        tracing::debug!(
            chan_id = self.inner.chan_id,
            "waiting on recv {:?}",
            self.inner.peer_id
        );

        async move {
            let msg = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel closed unexpectedly");
                RecvError
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

            let msg = Message::BatchSyncRequest(req);
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

impl<O> PartialEq for IrohConnection<O> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
