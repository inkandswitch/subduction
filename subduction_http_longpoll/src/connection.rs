//! HTTP long-poll connection implementing [`Transport<K>`].
//!
//! Unlike the WebSocket transport, there are no background listener/sender
//! tasks. Instead, the HTTP server's request handlers directly push to and
//! pull from the internal channels:
//!
//! ```text
//! POST /lp/send   ──► inbound_writer  ──► inbound_reader  ──► recv_bytes()
//! send_bytes()    ──► outbound_tx     ──► outbound_rx      ──► POST /lp/recv
//! ```
//!
//! The `call()` method delegates request-response correlation to the shared
//! [`Multiplexer`], identical to other transports.

use alloc::{sync::Arc, vec::Vec};
use core::time::Duration;

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use rand::{RngCore, rngs::OsRng};
use subduction_core::{
    connection::{
        Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage},
        multiplexer::Multiplexer,
        timeout::{TimedOut, Timeout},
    },
    peer::id::PeerId,
    transport::Transport,
};

use crate::error::{CallError, DisconnectionError, RecvError, SendError};

/// Channel capacity for outbound messages (server → client via `/lp/recv`).
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Channel capacity for inbound messages (client → server via `/lp/send`).
const INBOUND_CHANNEL_CAPACITY: usize = 128;

/// Shared interior state for an HTTP long-poll connection.
///
/// This struct is wrapped in an `Arc` so that clones share the same channels
/// and pending-request map — exactly like the WebSocket transport.
#[derive(Debug)]
struct Inner<O> {
    chan_id: u64,
    multiplexer: Multiplexer<O>,

    /// Raw bytes from `send_bytes()` / `call()` → picked up by `/lp/recv` handler.
    outbound_tx: async_channel::Sender<Vec<u8>>,

    /// Raw bytes from `/lp/send` handler → picked up by `recv_bytes()`.
    inbound_writer: async_channel::Sender<Vec<u8>>,
    inbound_reader: async_channel::Receiver<Vec<u8>>,

    /// Keeps background poll/send tasks alive. Dropping this closes the cancel
    /// channel, which signals the tasks to exit. Set via
    /// [`HttpLongPollConnection::set_cancel_guard`] after construction.
    cancel_guard: Mutex<Option<async_channel::Sender<()>>>,
}

/// An HTTP long-poll connection that implements [`Transport<K>`].
///
/// Created during handshake and stored in the [`SessionStore`](crate::session::SessionStore).
/// The server's HTTP handlers interact with this connection's channels to
/// bridge HTTP request-response pairs to Subduction's bidirectional protocol.
///
/// The `O` parameter is the timeout strategy (e.g., `TimeoutTokio` for native,
/// or a browser-based timeout for Wasm).
#[derive(Debug, Clone)]
pub struct HttpLongPollConnection<O> {
    inner: Arc<Inner<O>>,
    /// Server-facing receiver: the `/lp/recv` handler drains this.
    outbound_rx: async_channel::Receiver<Vec<u8>>,
}

impl<O> HttpLongPollConnection<O> {
    /// Create a new HTTP long-poll connection for the given peer.
    #[must_use]
    pub fn new(peer_id: PeerId, default_time_limit: Duration, timeout: O) -> Self {
        let (inbound_writer, inbound_reader) = async_channel::bounded(INBOUND_CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = OsRng.next_u64();

        Self {
            inner: Arc::new(Inner {
                chan_id,
                multiplexer: Multiplexer::new(peer_id, timeout, default_time_limit),
                outbound_tx,
                inbound_writer,
                inbound_reader,
                cancel_guard: Mutex::new(None),
            }),
            outbound_rx,
        }
    }

    /// Store the cancel-channel sender so that background poll/send tasks stay
    /// alive for as long as this connection (and its clones) exist.
    ///
    /// When the connection is closed via [`Self::close`] or all clones are
    /// dropped, the guard is released and the tasks exit.
    pub async fn set_cancel_guard(&self, guard: async_channel::Sender<()>) {
        *self.inner.cancel_guard.lock().await = Some(guard);
    }

    /// Push raw inbound bytes into the connection's channels.
    ///
    /// Called by HTTP handlers (server's `POST /lp/send`) or the client poll
    /// loop when a message arrives from the remote peer.
    ///
    /// Attempts to decode as [`SyncMessage`] to check for [`BatchSyncResponse`];
    /// if found, routes to the pending `call()` oneshot. All other bytes go to
    /// the inbound channel for `recv_bytes()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the inbound channel is full or closed.
    pub async fn push_inbound(
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

    /// Pull the next outbound bytes destined for the client (via `POST /lp/recv`).
    ///
    /// Blocks until bytes are available or the channel closes.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound channel is closed.
    pub async fn pull_outbound(&self) -> Result<Vec<u8>, async_channel::RecvError> {
        self.outbound_rx.recv().await
    }

    /// Close the connection's channels and cancel background tasks.
    pub fn close(&self) {
        self.inner.inbound_writer.close();
        self.inner.outbound_tx.close();
        self.outbound_rx.close();
        self.inner.inbound_reader.close();
        // Clear the cancel guard synchronously (try_lock avoids blocking close).
        if let Some(mut guard) = self.inner.cancel_guard.try_lock() {
            *guard = None;
        }
    }
}

#[future_form(Sendable where O: Send + Sync, Local)]
impl<K: FutureForm, O: Timeout<K>> Transport<K> for HttpLongPollConnection<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.inner.multiplexer.peer_id(), "HttpLongPoll::disconnect");
        let conn = self.clone();
        K::from_future(async move {
            conn.close();
            Ok(())
        })
    }

    fn send_bytes(&self, bytes: &[u8]) -> K::Future<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "http-lp: sending {} outbound bytes to peer {}",
            bytes.len(),
            self.inner.multiplexer.peer_id()
        );

        let data = bytes.to_vec();
        let tx = self.inner.outbound_tx.clone();
        K::from_future(async move {
            tx.send(data).await.map_err(|_| SendError)?;
            Ok(())
        })
    }

    fn recv_bytes(&self) -> K::Future<'_, Result<Vec<u8>, Self::RecvError>> {
        let chan = self.inner.inbound_reader.clone();
        tracing::debug!(
            chan_id = self.inner.chan_id,
            "waiting on recv {:?}",
            self.inner.multiplexer.peer_id()
        );

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

#[future_form(Sendable where O: Send + Sync, Local)]
impl<K: FutureForm, O: Timeout<K>> Roundtrip<K, BatchSyncRequest, BatchSyncResponse>
    for HttpLongPollConnection<O>
{
    type CallError = CallError;

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        K::from_future(async {
            let req_id = self.inner.multiplexer.next_request_id();
            tracing::debug!("generated request id {req_id:?}");
            req_id
        })
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.inner.outbound_tx.clone();
        let inner = self.inner.clone();

        K::from_future(async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            let rx = inner.multiplexer.register_pending(req_id).await;

            let msg_bytes = Multiplexer::<O>::encode_request(&req);
            outbound_tx
                .send(msg_bytes)
                .await
                .map_err(|_| CallError::ChannelClosed)?;

            tracing::debug!(
                chan_id = inner.chan_id,
                "sent HTTP long-poll request {req_id:?}"
            );

            let req_timeout = override_timeout.unwrap_or(inner.multiplexer.default_time_limit());
            let rx_fut = K::from_future(rx);

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
        })
    }
}

impl<O> PartialEq for HttpLongPollConnection<O> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use future_form::Sendable;
    use subduction_core::peer::id::PeerId;

    /// A simple timer-based timeout for tests using `futures_timer::Delay`.
    #[derive(Debug, Clone, Copy)]
    struct TestTimeout;

    impl Timeout<Sendable> for TestTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            dur: Duration,
            fut: futures::future::BoxFuture<'a, T>,
        ) -> futures::future::BoxFuture<'a, Result<T, subduction_core::connection::timeout::TimedOut>>
        {
            use futures::{
                FutureExt,
                future::{Either, select},
            };
            async move {
                match select(fut, futures_timer::Delay::new(dur)).await {
                    Either::Left((val, _)) => Ok(val),
                    Either::Right(_) => Err(subduction_core::connection::timeout::TimedOut),
                }
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn next_request_id_increments() {
        let peer_id = PeerId::new([1u8; 32]);
        let conn: HttpLongPollConnection<TestTimeout> =
            HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let id1 =
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(&conn)
                .await;
        let id2 =
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(&conn)
                .await;
        let id3 =
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(&conn)
                .await;

        assert_eq!(id1.requestor, peer_id);
        assert_eq!(id2.nonce, id1.nonce + 1);
        assert_eq!(id3.nonce, id2.nonce + 1);
    }

    #[tokio::test]
    async fn push_inbound_and_recv() {
        use sedimentree_core::id::SedimentreeId;
        use subduction_core::connection::{
            Connection, message::RemoveSubscriptions, transport::MessageTransport,
        };

        let peer_id = PeerId::new([2u8; 32]);
        let conn: HttpLongPollConnection<TestTimeout> =
            HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let msg = SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
            ids: alloc::vec![SedimentreeId::from_bytes([0u8; 32])],
        });

        conn.push_inbound(msg.encode()).await.expect("push ok");
        let mt = MessageTransport::new(conn);
        let received = Connection::<Sendable, SyncMessage>::recv(&mt)
            .await
            .expect("recv ok");

        assert!(matches!(received, SyncMessage::RemoveSubscriptions(_)));
    }

    #[tokio::test]
    async fn send_and_pull_outbound() {
        use sedimentree_core::id::SedimentreeId;
        use subduction_core::connection::{
            Connection, message::RemoveSubscriptions, transport::MessageTransport,
        };

        let peer_id = PeerId::new([3u8; 32]);
        let conn: HttpLongPollConnection<TestTimeout> =
            HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let msg = SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
            ids: alloc::vec![SedimentreeId::from_bytes([0u8; 32])],
        });

        let mt = MessageTransport::new(conn.clone());
        Connection::<Sendable, SyncMessage>::send(&mt, &msg)
            .await
            .expect("send ok");
        let pulled = conn.pull_outbound().await.expect("pull ok");
        let decoded = SyncMessage::try_decode(&pulled).expect("decode ok");

        assert!(matches!(decoded, SyncMessage::RemoveSubscriptions(_)));
    }
}
