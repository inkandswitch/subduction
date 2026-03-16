//! Iroh (QUIC) connection implementing [`Transport<Sendable>`].
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
//! The `call()` method delegates request-response correlation to the
//! shared [`Multiplexer`], identical to other transports.

use alloc::{sync::Arc, vec::Vec};
use core::{fmt::Debug, time::Duration};

use future_form::{FutureForm, Sendable};
use futures::{FutureExt, future::BoxFuture};
use rand::RngCore;
use subduction_core::{
    connection::{
        Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
    },
    multiplexer::Multiplexer,
    peer::id::PeerId,
    timeout::{TimedOut, Timeout},
    transport::Transport,
};

use crate::error::{CallError, DisconnectionError, RecvError, SendError};

/// Channel capacity for outbound messages.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for inbound messages.
const INBOUND_CHANNEL_CAPACITY: usize = 128;

/// Shared interior state for an Iroh connection.
#[derive(Debug)]
struct Inner<O> {
    chan_id: u64,
    multiplexer: Multiplexer<O>,

    /// Raw bytes from `send_bytes()` / `call()` -> drained by the sender task.
    outbound_tx: async_channel::Sender<Vec<u8>>,

    /// Raw bytes from the listener task -> picked up by `recv_bytes()`.
    inbound_writer: async_channel::Sender<Vec<u8>>,
    inbound_reader: async_channel::Receiver<Vec<u8>>,

    /// The underlying iroh QUIC connection (for closing).
    quic_conn: iroh::endpoint::Connection,
}

/// An Iroh (QUIC) connection that implements [`Transport<Sendable>`].
///
/// Created by [`connect`](crate::client::connect) or the server accept loop.
/// Uses a single QUIC bi-directional stream under the hood for framed message
/// passing.
///
/// The `O` parameter is the timeout strategy (e.g., `TimeoutTokio`).
#[derive(Debug, Clone)]
pub struct IrohTransport<O> {
    inner: Arc<Inner<O>>,
}

impl<O> IrohTransport<O> {
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
    ) -> (Self, async_channel::Receiver<Vec<u8>>) {
        let (inbound_writer, inbound_reader) = async_channel::bounded(INBOUND_CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = rand::rngs::OsRng.next_u64();

        let conn = Self {
            inner: Arc::new(Inner {
                chan_id,
                multiplexer: Multiplexer::new(peer_id, timeout, default_time_limit),
                outbound_tx,
                inbound_writer,
                inbound_reader,
                quic_conn,
            }),
        };

        (conn, outbound_rx)
    }

    /// Push raw inbound bytes into the connection's channels.
    ///
    /// Attempts to decode as [`SyncMessage`] to check for [`BatchSyncResponse`];
    /// if found, routes to the pending `call()` oneshot. All other bytes go to
    /// the inbound channel for `recv_bytes()`.
    pub(crate) async fn push_inbound(
        &self,
        bytes: Vec<u8>,
    ) -> Result<(), async_channel::SendError<Vec<u8>>> {
        // Try to decode as SyncMessage to route BatchSyncResponse to pending waiters.
        if let Ok(SyncMessage::BatchSyncResponse(ref resp)) = SyncMessage::try_decode(&bytes)
            && self.inner.multiplexer.resolve_pending(resp).await
        {
            return Ok(());
        }

        // All other bytes go to the inbound channel.
        self.inner.inbound_writer.send(bytes).await
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

impl<O: Timeout<Sendable> + Send + Sync> Transport<Sendable> for IrohTransport<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "iroh: sending {} outbound bytes to peer {}",
            bytes.len(),
            self.inner.multiplexer.peer_id()
        );

        let data = bytes.to_vec();
        let tx = self.inner.outbound_tx.clone();
        async move {
            tx.send(data).await.map_err(|_| SendError)?;
            Ok(())
        }
        .boxed()
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        let chan = self.inner.inbound_reader.clone();
        tracing::debug!(
            chan_id = self.inner.chan_id,
            "waiting on recv {:?}",
            self.inner.multiplexer.peer_id()
        );

        async move {
            let bytes = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel closed unexpectedly");
                RecvError
            })?;

            tracing::debug!("recv: inbound {} bytes", bytes.len());
            Ok(bytes)
        }
        .boxed()
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.inner.multiplexer.peer_id(), "IrohTransport::disconnect");
        let conn = self.clone();
        async move {
            conn.close();
            Ok(())
        }
        .boxed()
    }
}

impl<O: Timeout<Sendable> + Send + Sync> Roundtrip<Sendable, BatchSyncRequest, BatchSyncResponse>
    for IrohTransport<O>
{
    type CallError = CallError;

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async {
            let req_id = self.inner.multiplexer.next_request_id();
            tracing::debug!("generated request id {req_id:?}");
            req_id
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.inner.outbound_tx.clone();
        let inner = self.inner.clone();

        async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            let rx = inner.multiplexer.register_pending(req_id).await;

            let msg_bytes = Multiplexer::<O>::encode_request(&req);
            outbound_tx
                .send(msg_bytes)
                .await
                .map_err(|_| CallError::ChannelClosed)?;

            tracing::debug!(chan_id = inner.chan_id, "sent iroh request {req_id:?}");

            let req_timeout = override_timeout.unwrap_or(inner.multiplexer.default_time_limit());
            let rx_fut = Sendable::from_future(rx);

            match inner
                .multiplexer
                .timeout()
                .timeout(req_timeout, rx_fut)
                .await
            {
                Ok(Ok(resp)) => {
                    tracing::info!("request {req_id:?} completed");
                    Ok(resp)
                }
                Ok(Err(_)) => {
                    tracing::error!("request {req_id:?} failed: response dropped");
                    Err(CallError::ResponseDropped)
                }
                Err(TimedOut) => {
                    tracing::error!("request {req_id:?} timed out");
                    inner.multiplexer.cancel_pending(&req_id).await;
                    Err(CallError::Timeout)
                }
            }
        }
        .boxed()
    }
}

impl<O> PartialEq for IrohTransport<O> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
