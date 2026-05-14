//! # Generic WebSocket transport for Subduction

use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt::Debug,
    future::{Future, IntoFuture},
    marker::PhantomData,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
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

/// Configuration for WebSocket keepalive (Ping/Pong-based liveness detection).
///
/// When supplied to [`WebSocket::new_with_keepalive`], a background task
/// periodically sends [`tungstenite::Message::Ping`] frames. If the peer
/// fails to respond with a Pong within [`pong_timeout`] for
/// [`missed_pong_threshold`] consecutive cycles, the connection's outbound
/// and inbound channels are closed (sending a Close frame first), which
/// causes the listener and sender tasks to exit and propagates the failure
/// up to [`Subduction`].
///
/// The total dead-link detection latency is approximately
/// `ping_interval * missed_pong_threshold + pong_timeout` (the final cycle
/// fires at the pong-deadline rather than at the next ping).
///
/// [`pong_timeout`]: KeepAlive::pong_timeout
/// [`missed_pong_threshold`]: KeepAlive::missed_pong_threshold
/// [`Subduction`]: subduction_core::subduction::Subduction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeepAlive {
    /// Interval between consecutive outbound Ping frames. The first Ping
    /// fires `ping_interval` after the connection is established (not
    /// immediately).
    pub ping_interval: Duration,

    /// Maximum wait for a Pong after each Ping is sent. If the deadline
    /// passes without a Pong, this cycle counts as one miss.
    pub pong_timeout: Duration,

    /// Number of consecutive missed pongs before the connection is
    /// declared dead and torn down. Single transient misses (e.g., GC
    /// pauses, brief network jitter) are forgiven by setting this above 1.
    pub missed_pong_threshold: u32,
}

impl KeepAlive {
    /// Balanced defaults: 30 s ping interval, 10 s pong timeout, 2
    /// consecutive misses before close. Dead-link detection latency is
    /// roughly `ping_interval + ping_interval + pong_timeout` = 70 s.
    ///
    /// Picked to stay comfortably under typical 60 s load-balancer /
    /// NAT idle drops (the first ping at 30 s prevents the idle drop)
    /// while tolerating a single transient slow cycle.
    #[must_use]
    pub const fn balanced() -> Self {
        Self {
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(10),
            missed_pong_threshold: 2,
        }
    }
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self::balanced()
    }
}

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

/// Outcome of the keepalive task when it exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeepAliveOutcome {
    /// The keepalive task exited because the outbound channel was closed
    /// externally (e.g., the connection was shut down for an unrelated
    /// reason). This is a normal lifecycle event.
    #[error("keepalive task exited: connection shut down")]
    ConnectionClosed,

    /// The peer failed to respond to [`KeepAlive::missed_pong_threshold`]
    /// consecutive pings, so the keepalive task forcibly closed the
    /// inbound and outbound channels.
    #[error("keepalive task exited: peer failed to respond after {missed} consecutive pings")]
    Timeout {
        /// Number of consecutive missed pongs at the moment of close.
        missed: u32,
    },
}

/// A background task that drives WebSocket keepalive via Ping/Pong.
///
/// Returned from [`WebSocket::new_with_keepalive`] when a [`KeepAlive`]
/// config is supplied. Must be spawned (e.g., via `tokio::spawn`) for
/// keepalive to take effect.
pub struct KeepAliveTask(BoxFuture<'static, KeepAliveOutcome>);

impl core::fmt::Debug for KeepAliveTask {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeepAliveTask").finish_non_exhaustive()
    }
}

impl KeepAliveTask {
    pub(crate) fn new(fut: BoxFuture<'static, KeepAliveOutcome>) -> Self {
        Self(fut)
    }
}

impl IntoFuture for KeepAliveTask {
    type Output = KeepAliveOutcome;
    type IntoFuture = BoxFuture<'static, KeepAliveOutcome>;

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

    /// Set to `true` by [`Self::listen`] every time a [`tungstenite::Message::Pong`]
    /// arrives. Read and cleared by the optional keepalive task to detect a
    /// dead peer. When keepalive is disabled this flag is harmless overhead
    /// (one atomic store per Pong, which is rare).
    pong_received: Arc<AtomicBool>,

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
    /// Create a new WebSocket transport _without_ keepalive.
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
    ///
    /// For Ping/Pong-based liveness detection, see [`Self::new_with_keepalive`].
    pub fn new(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
    ) -> (Self, impl Future<Output = Result<(), RunError>> + use<T, K>) {
        let (this, sender, no_keepalive) = Self::new_with_keepalive(ws, peer_id, None);
        debug_assert!(no_keepalive.is_none(), "keepalive is None => no task");
        (this, sender)
    }

    /// Create a new WebSocket transport with optional keepalive.
    ///
    /// Returns the transport, a sender task future, and (when keepalive is
    /// configured) a keepalive task future. All three should be spawned as
    /// background tasks. The sender and keepalive tasks are independent;
    /// neither holds the other's `JoinHandle`. The keepalive task exits
    /// (with [`KeepAliveOutcome::Timeout`]) if the peer misses
    /// [`KeepAlive::missed_pong_threshold`] consecutive pongs, closing both
    /// the inbound and outbound channels so the rest of the stack observes
    /// the disconnect via the normal channel-closed paths.
    ///
    /// When `keepalive` is `None`, no keepalive task is spawned and the
    /// returned `Option<KeepAliveTask>` is `None`.
    pub fn new_with_keepalive(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
        keepalive: Option<KeepAlive>,
    ) -> (
        Self,
        impl Future<Output = Result<(), RunError>> + use<T, K>,
        Option<KeepAliveTask>,
    ) {
        tracing::info!(
            "new WebSocket connection for peer {peer_id:?} (keepalive: {})",
            keepalive.is_some()
        );
        let (ws_writer, ws_reader) = ws.split();
        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = rand::random::<u64>();
        // Initialise `true`: the first keepalive cycle wouldn't count this
        // as a "miss" because no Ping has been sent yet. The first
        // ping_interval-delayed cycle clears it before sending the Ping,
        // restoring the correct check semantics from cycle two onward.
        let pong_received = Arc::new(AtomicBool::new(true));

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

        let keepalive_task = keepalive.map(|config| {
            let outbound_tx = outbound_tx.clone();
            let inbound_writer = inbound_writer.clone();
            let pong_received = pong_received.clone();
            KeepAliveTask::new(Box::pin(keepalive_loop(
                config,
                peer_id,
                outbound_tx,
                inbound_writer,
                pong_received,
            )))
        });

        let ws = Self {
            chan_id,
            peer_id,

            ws_reader: Arc::new(Mutex::new(ws_reader)),
            outbound_tx,
            ws_sender,
            inbound_writer,
            inbound_reader,
            pong_received,

            _phantom: PhantomData,
        };

        (ws, sender_task, keepalive_task)
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
                    tracing::trace!(size = p.len(), peer_id = ?self.peer_id, "received pong");
                    self.pong_received.store(true, Ordering::SeqCst);
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

/// Body of the keepalive task.
///
/// Cycle: sleep `ping_interval` → clear `pong_received` → send a Ping →
/// sleep `pong_timeout` → check `pong_received`. A cleared flag at the end
/// of a cycle counts as one miss; `missed_pong_threshold` consecutive
/// misses triggers a forced disconnect (Close frame + channel close).
///
/// The task exits with [`KeepAliveOutcome::ConnectionClosed`] when the
/// outbound channel is already closed (so we can't even send a ping)
/// or with [`KeepAliveOutcome::Timeout`] when too many pongs were missed.
async fn keepalive_loop(
    config: KeepAlive,
    peer_id: PeerId,
    outbound_tx: async_channel::Sender<tungstenite::Message>,
    inbound_writer: async_channel::Sender<Vec<u8>>,
    pong_received: Arc<AtomicBool>,
) -> KeepAliveOutcome {
    use tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

    let mut consecutive_misses: u32 = 0;

    loop {
        // Sleep until the next ping moment.
        futures_timer::Delay::new(config.ping_interval).await;

        // Clear the flag BEFORE sending so a stale Pong from an earlier
        // cycle can't satisfy this one.
        pong_received.store(false, Ordering::SeqCst);

        let payload: [u8; 8] = rand::random();
        let ping = tungstenite::Message::Ping(payload.to_vec().into());
        if outbound_tx.send(ping).await.is_err() {
            tracing::debug!(
                ?peer_id,
                "keepalive: outbound channel already closed; exiting"
            );
            return KeepAliveOutcome::ConnectionClosed;
        }
        tracing::trace!(?peer_id, "keepalive: sent ping");

        // Wait for the pong deadline.
        futures_timer::Delay::new(config.pong_timeout).await;

        if pong_received.load(Ordering::SeqCst) {
            if consecutive_misses > 0 {
                tracing::debug!(?peer_id, "keepalive: pong recovered after misses");
            }
            consecutive_misses = 0;
            continue;
        }

        consecutive_misses += 1;
        tracing::warn!(
            ?peer_id,
            misses = consecutive_misses,
            threshold = config.missed_pong_threshold,
            "keepalive: pong missed"
        );

        if consecutive_misses >= config.missed_pong_threshold {
            tracing::warn!(
                ?peer_id,
                misses = consecutive_misses,
                "keepalive: pong missed past threshold; closing connection"
            );
            // Best-effort: nudge a Close frame onto the outbound queue
            // so the peer sees a clean WebSocket close instead of a TCP
            // reset. Ignore failures — the sender task may already be
            // exiting.
            let close_frame = tungstenite::Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "keepalive timeout".into(),
            }));
            drop(outbound_tx.send(close_frame).await);

            outbound_tx.close();
            inbound_writer.close();
            return KeepAliveOutcome::Timeout {
                missed: consecutive_misses,
            };
        }
    }
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
            pong_received: self.pong_received.clone(),
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
    use testresult::TestResult;

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

    /// Sanity: balanced defaults match the documented intent.
    #[test]
    fn keepalive_balanced_defaults_are_documented_values() {
        let kp = KeepAlive::balanced();
        assert_eq!(kp.ping_interval, Duration::from_secs(30));
        assert_eq!(kp.pong_timeout, Duration::from_secs(10));
        assert_eq!(kp.missed_pong_threshold, 2);
    }

    /// Sanity: `new_with_keepalive(.., None)` returns no keepalive task.
    #[tokio::test]
    async fn new_with_keepalive_none_returns_no_task() {
        let ws = create_mock_websocket_stream().await;
        let peer_id = PeerId::new([7u8; 32]);

        let (_socket, _sender, keepalive_task): (WebSocket<_, Sendable>, _, _) =
            WebSocket::new_with_keepalive(ws, peer_id, None);

        assert!(keepalive_task.is_none());
    }

    /// Sanity: `new_with_keepalive(.., Some(_))` returns a keepalive task.
    #[tokio::test]
    async fn new_with_keepalive_some_returns_task() {
        let ws = create_mock_websocket_stream().await;
        let peer_id = PeerId::new([7u8; 32]);

        let (_socket, _sender, keepalive_task): (WebSocket<_, Sendable>, _, _) =
            WebSocket::new_with_keepalive(ws, peer_id, Some(KeepAlive::balanced()));

        assert!(keepalive_task.is_some());
    }

    /// Drive `keepalive_loop` directly with a tight config and an
    /// always-quiet pong flag. After `missed_pong_threshold` cycles, the
    /// loop must close both channels and return [`KeepAliveOutcome::Timeout`].
    #[tokio::test]
    async fn keepalive_loop_times_out_on_silent_peer() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

        // Drain the outbound channel so the keepalive task's Ping sends
        // don't block on a full channel — but never set `pong_received`.
        let outbound_drain = tokio::spawn(async move {
            let mut pings = 0u32;
            while outbound_rx.recv().await.is_ok() {
                pings += 1;
            }
            pings
        });

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: 2,
        };
        let outcome = keepalive_loop(
            config,
            PeerId::new([42u8; 32]),
            outbound_tx,
            inbound_writer.clone(),
            pong_received,
        )
        .await;

        // Allow the drain task to observe the channel close.
        let pings = outbound_drain.await?;

        let KeepAliveOutcome::Timeout { missed } = outcome else {
            return Err(format!("expected Timeout outcome, got {outcome:?}").into());
        };
        assert!(
            missed >= 2,
            "should have missed at least the threshold count: got {missed}"
        );

        assert!(inbound_writer.is_closed(), "inbound_writer should be closed");
        // At least 2 pings (one per missed cycle) + 1 close frame; in practice 2 or 3.
        assert!(pings >= 2, "expected >=2 outbound messages, got {pings}");
        // Inbound is closed, so a recv attempt returns Err immediately.
        assert!(inbound_reader.recv().await.is_err());
        Ok(())
    }

    /// If `pong_received` is set to `true` between cycles, the loop must
    /// not declare a timeout — it should keep pinging indefinitely.
    /// We drive a few cycles, flipping the flag on, and confirm no
    /// timeout occurs within the deadline.
    #[tokio::test]
    async fn keepalive_loop_does_not_time_out_with_responsive_peer() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

        // Simulate a responsive peer: every time we see a Ping on the
        // outbound channel, immediately set `pong_received = true`.
        let responsive_peer = {
            let pong_received = pong_received.clone();
            tokio::spawn(async move {
                while let Ok(msg) = outbound_rx.recv().await {
                    if matches!(msg, tungstenite::Message::Ping(_)) {
                        pong_received.store(true, Ordering::SeqCst);
                    }
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(20),
            pong_timeout: Duration::from_millis(10),
            missed_pong_threshold: 2,
        };
        // Run the loop for long enough to exceed the natural timeout
        // window if the responsive peer weren't working. With 20 ms
        // pings + 2 misses, a silent peer would close in ~40+10 = 50 ms.
        // 250 ms is well past that.
        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(250),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                pong_received,
            ),
        )
        .await;

        // We expect the loop to STILL be running (no Timeout outcome
        // before our enclosing timeout fired).
        if let Ok(unexpected) = outcome_or_timeout {
            return Err(format!(
                "keepalive_loop should still be running with responsive peer, got {unexpected:?}"
            )
            .into());
        }

        // Cleanup: closing the channel exits the loop.
        outbound_tx.close();
        responsive_peer.await?;
        Ok(())
    }
}
