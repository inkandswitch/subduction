//! HTTP long-poll connection implementing [`Connection<K>`].
//!
//! Unlike the WebSocket transport, there are no background listener/sender
//! tasks. Instead, the HTTP server's request handlers directly push to and
//! pull from the internal channels:
//!
//! ```text
//! POST /lp/send   ──► inbound_writer  ──► inbound_reader  ──► recv()
//! send()          ──► outbound_tx     ──► outbound_rx      ──► POST /lp/recv
//! ```
//!
//! The `call()` method uses the same pending-map + oneshot pattern as WebSocket:
//! the caller pre-registers a oneshot channel keyed by [`RequestId`], sends the
//! request, and waits on the oneshot receiver with a timeout.

use alloc::sync::Arc;
use core::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable};
use futures::channel::oneshot;
use rand::{RngCore, rngs::OsRng};
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
    peer_id: PeerId,
    chan_id: u64,
    req_id_counter: AtomicU64,
    default_time_limit: Duration,
    timeout: O,

    pending: Mutex<Map<RequestId, oneshot::Sender<BatchSyncResponse>>>,

    /// Messages from `send()` / `call()` → picked up by `/lp/recv` handler.
    outbound_tx: async_channel::Sender<Message>,

    /// Messages from `/lp/send` handler → picked up by `recv()`.
    inbound_writer: async_channel::Sender<Message>,
    inbound_reader: async_channel::Receiver<Message>,

    /// Keeps background poll/send tasks alive. Dropping this closes the cancel
    /// channel, which signals the tasks to exit. Set via
    /// [`HttpLongPollConnection::set_cancel_guard`] after construction.
    cancel_guard: Mutex<Option<async_channel::Sender<()>>>,
}

/// An HTTP long-poll connection that implements [`Connection<K>`].
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
    outbound_rx: async_channel::Receiver<Message>,
}

impl<O> HttpLongPollConnection<O> {
    /// Create a new HTTP long-poll connection for the given peer.
    #[must_use]
    pub fn new(peer_id: PeerId, default_time_limit: Duration, timeout: O) -> Self {
        let (inbound_writer, inbound_reader) = async_channel::bounded(INBOUND_CHANNEL_CAPACITY);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let starting_counter = OsRng.next_u64();
        let chan_id = OsRng.next_u64();

        Self {
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

    /// Push a message from the client (via `POST /lp/send`) into the inbound channel.
    ///
    /// The server handler calls this when it receives a message from the client.
    /// `BatchSyncResponse` messages are routed to waiting `call()` oneshots.
    ///
    /// # Errors
    ///
    /// Returns an error if the inbound channel is full or closed.
    pub async fn push_inbound(
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

    /// Pull the next outbound message destined for the client (via `POST /lp/recv`).
    ///
    /// Blocks until a message is available or the channel closes.
    ///
    /// # Errors
    ///
    /// Returns an error if the outbound channel is closed.
    pub async fn pull_outbound(&self) -> Result<Message, async_channel::RecvError> {
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

#[future_form::future_form(Sendable where O: Send + Sync, Local)]
impl<K: FutureForm, O: Timeout<K>> Connection<K> for HttpLongPollConnection<O> {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id
    }

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        K::from_future(async {
            let counter = self.inner.req_id_counter.fetch_add(1, Ordering::Relaxed);
            tracing::debug!("generated request id {counter:?}");
            RequestId {
                requestor: self.inner.peer_id,
                nonce: counter,
            }
        })
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.inner.peer_id, "HttpLongPoll::disconnect");
        let conn = self.clone();
        K::from_future(async move {
            conn.close();
            Ok(())
        })
    }

    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>> {
        tracing::debug!(
            "http-lp: sending outbound message id {:?} to peer {}",
            message.request_id(),
            self.inner.peer_id
        );

        let msg = message.clone();
        let tx = self.inner.outbound_tx.clone();
        K::from_future(async move {
            tx.send(msg).await.map_err(|_| SendError)?;
            Ok(())
        })
    }

    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>> {
        let chan = self.inner.inbound_reader.clone();
        tracing::debug!(
            chan_id = self.inner.chan_id,
            "waiting on recv {:?}",
            self.inner.peer_id
        );

        K::from_future(async move {
            let msg = chan.recv().await.map_err(|_| {
                tracing::error!("inbound channel closed unexpectedly");
                RecvError
            })?;

            tracing::debug!("recv: inbound message {msg:?}");
            Ok(msg)
        })
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        let outbound_tx = self.inner.outbound_tx.clone();
        let timeout = self.inner.timeout.clone();
        let default_time_limit = self.inner.default_time_limit;
        let inner = self.inner.clone();

        K::from_future(async move {
            tracing::debug!("making call with request id {:?}", req.req_id);
            let req_id = req.req_id;

            let (tx, rx) = oneshot::channel();
            inner.pending.lock().await.insert(req_id, tx);

            let msg = Message::BatchSyncRequest(req);
            outbound_tx
                .send(msg)
                .await
                .map_err(|_| CallError::ChannelClosed)?;

            tracing::debug!(
                chan_id = inner.chan_id,
                "sent HTTP long-poll request {req_id:?}"
            );

            let req_timeout = override_timeout.unwrap_or(default_time_limit);
            let rx_fut = K::from_future(async { rx.await });

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
        })
    }
}

impl<O> PartialEq for HttpLongPollConnection<O> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
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
        let conn = HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let id1 = Connection::<Sendable>::next_request_id(&conn).await;
        let id2 = Connection::<Sendable>::next_request_id(&conn).await;
        let id3 = Connection::<Sendable>::next_request_id(&conn).await;

        assert_eq!(id1.requestor, peer_id);
        assert_eq!(id2.nonce, id1.nonce + 1);
        assert_eq!(id3.nonce, id2.nonce + 1);
    }

    #[tokio::test]
    async fn push_inbound_and_recv() {
        use sedimentree_core::id::SedimentreeId;
        use subduction_core::connection::message::RemoveSubscriptions;

        let peer_id = PeerId::new([2u8; 32]);
        let conn = HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let msg = Message::RemoveSubscriptions(RemoveSubscriptions {
            ids: alloc::vec![SedimentreeId::from_bytes([0u8; 32])],
        });

        conn.push_inbound(msg.clone()).await.expect("push ok");
        let received = Connection::<Sendable>::recv(&conn).await.expect("recv ok");

        assert!(matches!(received, Message::RemoveSubscriptions(_)));
    }

    #[tokio::test]
    async fn send_and_pull_outbound() {
        use sedimentree_core::id::SedimentreeId;
        use subduction_core::connection::message::RemoveSubscriptions;

        let peer_id = PeerId::new([3u8; 32]);
        let conn = HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);

        let msg = Message::RemoveSubscriptions(RemoveSubscriptions {
            ids: alloc::vec![SedimentreeId::from_bytes([0u8; 32])],
        });

        Connection::<Sendable>::send(&conn, &msg)
            .await
            .expect("send ok");
        let pulled = conn.pull_outbound().await.expect("pull ok");

        assert!(matches!(pulled, Message::RemoveSubscriptions(_)));
    }

    #[tokio::test]
    async fn clone_shares_channels() {
        let peer_id = PeerId::new([4u8; 32]);
        let conn1 = HttpLongPollConnection::new(peer_id, Duration::from_secs(30), TestTimeout);
        let conn2 = conn1.clone();

        assert_eq!(conn1, conn2);
    }
}
