//! # Generic WebSocket transport for Subduction

use alloc::{boxed::Box, string::String, sync::Arc, vec::Vec};
use core::{
    fmt::Debug,
    future::{Future, IntoFuture},
    marker::PhantomData,
    num::NonZeroU32,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{FutureExt, future::BoxFuture};
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use subduction_core::{peer::id::PeerId, transport::Transport};
use tungstenite::{
    Error, Message,
    error::{CapacityError, ProtocolError},
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use crate::{
    error::{DisconnectionError, RecvError, RunError, SendError},
    sleep::Sleeper,
};

/// Channel capacity for outbound messages.
///
/// This is sized to allow many concurrent sends without blocking while still
/// providing backpressure if the sender task can't keep up.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Hard upper bound on consecutive *delivered-but-unanswered* pings before
/// the connection is reaped, regardless of write progress.
///
/// The keepalive's progress gate forgives missing pongs while
/// data writes are completing (a saturated-but-draining link). Without a
/// ceiling, a peer whose transport drains our frames but whose protocol
/// endpoint never answers pings — a frame-swallowing middlebox, or a
/// proxy in front of a dead backend — would evade the reaper indefinitely.
///
/// Only delivered pings count: an undelivered ping reflects our own
/// congestion, not the peer, so it accrues no evidence against them.
/// Delivery is guaranteed under any drainage (see
/// [`PING_DELIVERY_ATTEMPTS`]); a zero-drainage window goes undelivered
/// and is caught by the progress-gated miss path instead. Pongs are RFC
/// 6455 auto-replies from the peer's protocol stack, independent of its
/// application load, so this many consecutive unanswered pings over TCP
/// means the remote endpoint has stopped servicing the protocol.
///
/// With [`KeepAlive::balanced`] (40 s cycles), worst-case detection for
/// this class is `8 × 40 s ≈ 5.3 min`. This also effectively caps
/// [`KeepAlive::missed_pong_threshold`] values above it.
///
/// If this ever becomes a `KeepAlive` field, make it `NonZeroU8` (zero
/// would mean reap-every-cycle — the same footgun `missed_pong_threshold`
/// guards against with `NonZeroU32`).
pub const MAX_UNANSWERED_PINGS: u8 = 8;

/// Queue-delivery attempts for each cycle's keepalive ping.
///
/// The ping is attempted at the start of the pong window and retried at
/// each sub-interval boundary until it enqueues; the sub-sleeps sum to
/// exactly `pong_timeout`, so cycle timing is unchanged. Retries alone
/// are best-effort (parked producers woken FIFO usually win freed slots),
/// so a failed attempt also raises `KeepAliveSignals::ping_requested` for
/// sender-task injection. Between the two paths, delivery is guaranteed
/// whenever the connection drains at least one frame during the window.
pub const PING_DELIVERY_ATTEMPTS: u32 = 4;

/// An outbound message, plus its enqueue instant under the `metrics` feature so
/// the sender task can record queue dwell. Without the feature the timestamp
/// field is gone — a zero-overhead newtype, and no `std::time` on wasm.
struct Outbound {
    msg: tungstenite::Message,
    #[cfg(feature = "metrics")]
    enqueued: std::time::Instant,
}

impl Outbound {
    // Const-eligible only without `metrics` (`Instant::now()` isn't const).
    #[allow(clippy::missing_const_for_fn)]
    fn new(msg: tungstenite::Message) -> Self {
        Self {
            msg,
            #[cfg(feature = "metrics")]
            enqueued: std::time::Instant::now(),
        }
    }
}

/// Configuration for WebSocket Ping/Pong keepalive.
///
/// A peer that misses [`missed_pong_threshold`] consecutive pongs is
/// declared dead; total detection latency is
/// `missed_pong_threshold × (ping_interval + pong_timeout)` for a peer
/// making no write progress. A peer whose connection keeps completing
/// data writes (saturated but draining) is forgiven fast-path misses, but
/// delivered pings that go unanswered accumulate toward
/// [`MAX_UNANSWERED_PINGS`], so worst-case detection for a
/// write-accepting-but-silent peer is
/// `MAX_UNANSWERED_PINGS × (ping_interval + pong_timeout)`
/// (~5.3 min with [`balanced`](KeepAlive::balanced)). That ceiling also
/// effectively caps `missed_pong_threshold` values above it whenever
/// pings are deliverable.
///
/// [`missed_pong_threshold`]: KeepAlive::missed_pong_threshold
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeepAlive {
    /// Interval between Pings. First Ping fires after `ping_interval`,
    /// not immediately.
    pub ping_interval: Duration,

    /// How long to wait for the Pong reply before counting a miss.
    pub pong_timeout: Duration,

    /// Consecutive missed pongs before close. `NonZeroU32` because
    /// `0` is equivalent to `1` and accepting it silently was a footgun.
    pub missed_pong_threshold: NonZeroU32,
}

impl KeepAlive {
    /// 30 s ping / 10 s pong / 2 misses → ~80 s detection latency.
    ///
    /// Tuned for the common 60 s LB / NAT idle drop: the first ping at
    /// 30 s keeps the connection alive, and a single slow cycle is
    /// forgiven.
    ///
    /// # Panics
    ///
    /// Never. `NonZeroU32::new(2)` is statically `Some`; the `expect`
    /// is only there because this is a `const fn` and `match` would
    /// be noisier.
    #[must_use]
    pub const fn balanced() -> Self {
        #[allow(clippy::expect_used, reason = "2 is statically nonzero")]
        let two = NonZeroU32::new(2).expect("2 should be a valid nonzero u32");
        Self {
            ping_interval: Duration::from_secs(30),
            pong_timeout: Duration::from_secs(10),
            missed_pong_threshold: two,
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
    // Only called from `tokio::client`; silences the dead-code warning
    // when building without the `tokio_*` features.
    #[cfg_attr(not(feature = "tokio_client_any"), allow(dead_code))]
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
    // Only called from `tokio::client`; silences the dead-code warning
    // when building without the `tokio_*` features.
    #[cfg_attr(not(feature = "tokio_client_any"), allow(dead_code))]
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

/// Why the keepalive task exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum KeepAliveOutcome {
    /// Outbound channel closed externally — normal lifecycle event.
    #[error("keepalive task exited: connection shut down")]
    ConnectionClosed,

    /// Peer missed `missed` consecutive pong replies; the keepalive
    /// task closed the channels.
    #[error("keepalive task exited: peer missed {missed} consecutive pong replies")]
    Timeout {
        /// Consecutive missed pongs at the moment of close.
        missed: u32,
    },

    /// [`MAX_UNANSWERED_PINGS`] consecutive delivered pings went
    /// unanswered; the keepalive task closed the channels.
    ///
    /// Distinct from [`Timeout`](Self::Timeout) because it fires even for
    /// a connection making write progress: a peer whose transport drains
    /// our frames but whose remote endpoint never pongs (frame-swallowing
    /// middlebox, WS proxy in front of a dead backend) — a different
    /// operational signal than a silent socket.
    #[error("keepalive task exited: {unanswered} consecutive pings unanswered")]
    StaleNoPong {
        /// Consecutive delivered-but-unanswered pings at the moment of close.
        /// Never exceeds [`MAX_UNANSWERED_PINGS`], hence the width.
        unanswered: u8,
    },
}

/// Background task that pings the peer and tears down the connection
/// on timeout. Must be spawned for keepalive to take effect.
///
/// Parameterized by [`FutureForm`]: `KeepAliveTask<Sendable>` is
/// `Send`-spawnable on multi-threaded runtimes; `KeepAliveTask<Local>`
/// is for single-threaded runtimes (Wasm).
pub struct KeepAliveTask<Async: FutureForm>(Async::Future<'static, KeepAliveOutcome>);

impl<Async: FutureForm> core::fmt::Debug for KeepAliveTask<Async> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeepAliveTask").finish_non_exhaustive()
    }
}

impl<Async: FutureForm> KeepAliveTask<Async> {
    pub(crate) const fn new(fut: Async::Future<'static, KeepAliveOutcome>) -> Self {
        Self(fut)
    }
}

impl<Async: FutureForm> IntoFuture for KeepAliveTask<Async> {
    type Output = KeepAliveOutcome;
    type IntoFuture = Async::Future<'static, KeepAliveOutcome>;

    fn into_future(self) -> Self::IntoFuture {
        self.0
    }
}

/// A WebSocket implementation for [`Transport`].
///
/// Parameterized over:
/// - `T`: the underlying async I/O stream (e.g., `TcpStream`, `ConnectStream`)
/// - `Async`: the async future form (`Local` or `Sendable`)
#[derive(Debug)]
pub struct WebSocket<T: AsyncRead + AsyncWrite + Unpin, Async: FutureForm> {
    chan_id: u64,
    peer_id: PeerId,

    ws_reader: Arc<Mutex<WebSocketReceiver<T>>>,

    /// Channel for outbound messages. A dedicated sender task drains this to the WebSocket.
    /// This eliminates mutex contention when many tasks send concurrently.
    outbound_tx: async_channel::Sender<Outbound>,

    /// The actual WebSocket sender, used only by the sender task.
    ws_sender: Arc<Mutex<WebSocketSender<T>>>,

    inbound_writer: async_channel::Sender<Vec<u8>>,
    inbound_reader: async_channel::Receiver<Vec<u8>>,

    /// Shared liveness signals linking the listener, the sender task, and
    /// the keepalive task. Unused (but cheap) when keepalive is disabled.
    signals: KeepAliveSignals,

    _phantom: PhantomData<Async>,
}

/// Shared atomics linking the listener, the sender task, and the keepalive
/// task's liveness evaluation.
///
/// All fields use `Relaxed` ordering: each is an independent flag or a
/// monotonic counter compared only against its own prior value — no
/// cross-atomic ordering invariants exist.
#[derive(Clone, Debug)]
struct KeepAliveSignals {
    /// Set by the listener on incoming Pong; read and cleared by the
    /// keepalive task.
    pong_received: Arc<AtomicBool>,

    /// Incremented by the sender task after each completed *data*
    /// (Binary/Text) socket write. Control frames (Ping/Pong/Close) are
    /// deliberately excluded: if the keepalive's own ping writes moved this
    /// counter, an idle dead peer would look "alive" to the progress gate
    /// and never be reaped.
    ///
    /// The keepalive loop reads this to distinguish a *wedged* connection
    /// (full outbound queue, no writes completing) from a *saturated but
    /// draining* one, and only counts pong misses against the former.
    data_write_progress: Arc<AtomicU64>,

    /// Raised by the keepalive task when its `try_send` found the outbound
    /// queue full; consumed by the sender task, which then injects a Ping
    /// directly into the sink after finishing its in-flight frame. This
    /// gives ping delivery priority over parked producers competing for
    /// freed queue slots: the party that owns drainage owns delivery, so a
    /// ping lands whenever at least one frame completes during the window.
    ping_requested: Arc<AtomicBool>,

    /// Set by the sender task after a successful injected ping write; read
    /// and cleared by the keepalive task at window end, where it counts as
    /// a delivered ping.
    ping_injected: Arc<AtomicBool>,
}

impl KeepAliveSignals {
    fn new() -> Self {
        Self {
            // Initial values are irrelevant: the keepalive loop clears the
            // pong and injected flags at each window boundary, and the
            // progress counter is only compared against its own snapshots.
            pong_received: Arc::new(AtomicBool::new(false)),
            data_write_progress: Arc::new(AtomicU64::new(0)),
            ping_requested: Arc::new(AtomicBool::new(false)),
            ping_injected: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[future_form(
    Sendable where
        T: AsyncRead + AsyncWrite + Unpin + Send,
    Local where
        T: AsyncRead + AsyncWrite + Unpin + Send
)]
impl<T, Async: FutureForm> Transport<Async> for WebSocket<T, Async> {
    type SendError = SendError;
    type RecvError = RecvError;
    type DisconnectionError = DisconnectionError;

    fn disconnect(&self) -> Async::Future<'_, Result<(), Self::DisconnectionError>> {
        tracing::info!(peer_id = %self.peer_id, "WebSocket::disconnect");
        Async::from_future(async { Ok(()) })
    }

    fn send_bytes(&self, bytes: &[u8]) -> Async::Future<'_, Result<(), Self::SendError>> {
        let item = Outbound::new(tungstenite::Message::Binary(bytes.to_vec().into()));
        let tx = self.outbound_tx.clone();
        Async::from_future(async move {
            // Try the fast path first (metrics only) so a full channel — TCP
            // backpressure from a slow peer — is counted before await.
            #[cfg(feature = "metrics")]
            let item = match tx.try_send(item) {
                Ok(()) => return Ok(()),
                Err(async_channel::TrySendError::Closed(_)) => return Err(SendError),
                Err(async_channel::TrySendError::Full(item)) => {
                    subduction_core::metrics::outbound_send_blocked("websocket");
                    item
                }
            };
            tx.send(item).await.map_err(|_| SendError)?;
            Ok(())
        })
    }

    fn recv_bytes(&self) -> Async::Future<'_, Result<Vec<u8>, Self::RecvError>> {
        let chan = self.inbound_reader.clone();
        tracing::trace!(conn = %self.chan_id, peer = %self.peer_id, "waiting on recv");

        Async::from_future(async move {
            let bytes = chan.recv().await.map_err(|_| {
                // The inbound channel closes when the listener tears the
                // connection down (peer close, EOF, over-cap, fatal, or
                // keepalive timeout). This is the expected disconnect signal,
                // not an error.
                tracing::debug!("inbound channel closed; connection torn down");
                RecvError
            })?;

            tracing::trace!(bytes = bytes.len(), "recv: inbound");
            Ok(bytes)
        })
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, Async: FutureForm> WebSocket<T, Async> {
    /// Create a new WebSocket transport without keepalive.
    ///
    /// Returns the transport and a sender task to spawn. The sender
    /// task captures only the channel receiver and write half, so it
    /// doesn't keep the outbound channel alive past its own exit.
    ///
    /// For liveness detection see
    /// [`WebSocket::<T, Sendable>::new_with_keepalive`].
    pub fn new(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
    ) -> (
        Self,
        impl Future<Output = Result<(), RunError>> + use<T, Async>,
    ) {
        tracing::info!(peer = %peer_id, keepalive = false, "new WebSocket connection");
        Self::new_inner(ws, peer_id)
    }

    /// Shared body of [`Self::new`] and [`Self::new_with_keepalive`].
    fn new_inner(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
    ) -> (
        Self,
        impl Future<Output = Result<(), RunError>> + use<T, Async>,
    ) {
        let (ws_writer, ws_reader) = ws.split();
        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let (outbound_tx, outbound_rx) =
            async_channel::bounded::<Outbound>(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = rand::random::<u64>();
        let signals = KeepAliveSignals::new();

        let ws_sender = Arc::new(Mutex::new(ws_writer));

        let sender_task = {
            let ws_sender = ws_sender.clone();
            let signals = signals.clone();
            async move {
                tracing::debug!(peer = %peer_id, "starting WebSocket sender task");

                let mut ws_sender = ws_sender.lock().await;

                while let Ok(item) = outbound_rx.recv().await {
                    tracing::trace!("sender task: sending message to WebSocket");
                    #[cfg(feature = "metrics")]
                    {
                        subduction_core::metrics::outbound_queue_dwell(
                            "websocket",
                            item.enqueued.elapsed().as_secs_f64(),
                            outbound_rx.len(),
                        );
                        subduction_core::metrics::network_frame(
                            "websocket",
                            "sent",
                            item.msg.len(),
                        );
                    }
                    // Data frames feed the keepalive's progress gate; control
                    // frames must not (see `KeepAliveSignals` field docs).
                    let is_data = matches!(item.msg, Message::Binary(_) | Message::Text(_));
                    ws_sender.send(item.msg).await?;
                    if is_data {
                        signals.data_write_progress.fetch_add(1, Ordering::Relaxed);
                    }

                    // Keepalive ping injection — between frames, directly
                    // into the sink, bypassing the queue. See
                    // `KeepAliveSignals::ping_requested`.
                    if signals.ping_requested.swap(false, Ordering::Relaxed) {
                        let ping = tungstenite::Message::Ping(Vec::new().into());
                        #[cfg(feature = "metrics")]
                        subduction_core::metrics::network_frame("websocket", "sent", ping.len());
                        ws_sender.send(ping).await?;
                        signals.ping_injected.store(true, Ordering::Relaxed);
                        tracing::trace!(peer = %peer_id, "keepalive: ping injected by sender task");
                    }
                }

                tracing::debug!("sender task: outbound channel closed, shutting down");
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
            signals,

            _phantom: PhantomData,
        };

        (ws, sender_task)
    }

    /// Get the [`PeerId`] associated with this transport.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Close the outbound and inbound channels.
    ///
    /// Causes the sender and (if any) keepalive tasks to exit cleanly.
    /// The listener task is _not_ cancelled — it is blocked on a read
    /// from the underlying socket and exits only on EOF/error or when
    /// it next tries to write to the now-closed inbound channel.
    pub fn close_channels(&self) {
        self.outbound_tx.close();
        self.inbound_writer.close();
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
        tracing::debug!(peer = %self.peer_id, "starting WebSocket listener");

        // Outcome to return once the loop ends. Teardown (closing the inbound
        // channel so a parked `recv_bytes` is notified) happens uniformly after
        // the loop, regardless of *why* it ended — error, remote Close, or EOF.
        // This is what stops the connection from sitting half-open until
        // keepalive reaps it (~80 s with `KeepAlive::balanced`).
        let outcome: Result<(), RunError> = {
            let mut in_chan = self.ws_reader.lock().await;
            loop {
                let Some(ws_msg) = in_chan.next().await else {
                    // Stream ended (EOF). The write half is already gone, so
                    // there is nothing to gracefully close — just tear down.
                    tracing::debug!(peer = %self.peer_id, "websocket stream ended (EOF)");
                    break Ok(());
                };

                tracing::trace!(peer = %self.peer_id, conn = %self.chan_id, "received WebSocket message");

                match ws_msg {
                    Ok(tungstenite::Message::Binary(bytes)) => {
                        #[cfg(feature = "metrics")]
                        subduction_core::metrics::network_frame(
                            "websocket",
                            "received",
                            bytes.len(),
                        );
                        if let Err(e) = self.inbound_writer.send(bytes.to_vec()).await {
                            tracing::error!(
                                conn = %self.chan_id,
                                error = %e,
                                "failed to forward inbound message to channel"
                            );
                            break Err(RunError::ChanSend(Box::new(e)));
                        }
                    }
                    Ok(tungstenite::Message::Text(text)) => {
                        // Bound the peer-controlled text in the log.
                        let preview: String = text.chars().take(64).collect();
                        tracing::warn!(
                            peer = %self.peer_id,
                            len = text.len(),
                            preview = %preview,
                            "unexpected text message"
                        );
                    }
                    Ok(tungstenite::Message::Ping(p)) => {
                        tracing::trace!(size = p.len(), peer = %self.peer_id, "received ping");
                        // Non-blocking so a saturated outbound queue can't
                        // stall the listener. A dropped pong may cost one
                        // keepalive cycle on the remote side.
                        if let Err(e) = self
                            .outbound_tx
                            .try_send(Outbound::new(tungstenite::Message::Pong(p)))
                        {
                            tracing::warn!(
                                error = ?e,
                                peer = %self.peer_id,
                                "dropped pong reply (outbound full or closed)"
                            );
                        }
                    }
                    Ok(tungstenite::Message::Pong(p)) => {
                        tracing::trace!(size = p.len(), peer = %self.peer_id, "received pong");
                        self.signals.pong_received.store(true, Ordering::Relaxed);
                    }
                    Ok(tungstenite::Message::Frame(f)) => {
                        tracing::warn!(peer = %self.peer_id, frame = ?f, "unexpected frame");
                    }
                    Ok(tungstenite::Message::Close(_)) => {
                        // The peer initiated the close; `tungstenite` has
                        // already auto-echoed the Close reply, so we must NOT
                        // originate our own (it would be a redundant
                        // double-close). Just stop reading.
                        tracing::info!(
                            peer = %self.peer_id,
                            "received close message, shutting down listener"
                        );
                        break Ok(());
                    }
                    Err(e) => {
                        // Classify once; the category drives log severity and
                        // whether *we* originate a graceful Close frame.
                        let kind = ReadErrorKind::classify(&e);
                        match kind {
                            ReadErrorKind::ExpectedDisconnect => {
                                tracing::debug!(peer = %self.peer_id, error = %e, "connection closed");
                            }
                            ReadErrorKind::OverCapacity => {
                                // Peer-induced (they sent a message exceeding
                                // our cap). Not a server fault — warn, don't
                                // error.
                                tracing::warn!(
                                    peer = %self.peer_id,
                                    error = %e,
                                    "peer sent an over-capacity message; closing connection"
                                );
                            }
                            ReadErrorKind::Fatal => {
                                tracing::error!(
                                    peer = %self.peer_id,
                                    error = %e,
                                    "error reading from websocket"
                                );
                            }
                        }

                        // For errors where we are the party ending the
                        // connection (over-cap / fatal), send a best-effort
                        // graceful Close frame so the peer learns why. The
                        // sender task drains it before the channel-close below
                        // takes effect (`async-channel` allows buffered
                        // messages to be received after `close()`).
                        if let Some(close_frame) = kind.close_frame() {
                            drop(self.outbound_tx.try_send(Outbound::new(close_frame)));
                        }

                        break Err(RunError::from(e));
                    }
                }
            }
        };

        // Uniform teardown: the read half is dead however we got here, so close
        // both channels. This notifies a parked `recv_bytes` immediately and
        // exits the sender task (after it flushes any queued graceful Close).
        self.close_channels();

        outcome
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> WebSocket<T, Sendable> {
    /// Create a new WebSocket transport with Ping/Pong keepalive.
    ///
    /// Returns the transport, a sender task, and a keepalive task —
    /// all three should be spawned. The keepalive task closes both
    /// channels on [`KeepAliveOutcome::Timeout`] or
    /// [`KeepAliveOutcome::StaleNoPong`]; the rest of the stack observes
    /// the disconnect through the normal channel-closed paths.
    ///
    /// # Pong asymmetry
    ///
    /// Outbound *ping* delivery is guaranteed under drainage, but our
    /// *pong replies* remain single-shot `try_send`s from the listener
    /// (the read loop cannot sleep-retry without stalling inbound
    /// traffic). A symmetric peer running this same unanswered-ping
    /// ceiling could therefore reap a healthy connection whose queue
    /// toward it stays full for > `MAX_UNANSWERED_PINGS` cycles. Moot for
    /// browser peers (native auto-pong, no application pings); relevant
    /// for server↔server links. A priority slot for control frames would
    /// close it.
    ///
    /// `sleeper` provides the in-between waits — typically
    /// [`TokioSleeper`](crate::sleep::TokioSleeper) or
    /// [`FuturesTimerSleeper`](crate::sleep::FuturesTimerSleeper).
    ///
    /// This constructor is `Sendable`-specific. A `Local` (e.g. Wasm)
    /// counterpart can be added as an additive `impl WebSocket<T, Local>`
    /// block when needed.
    pub fn new_with_keepalive<S: Sleeper<Sendable> + Send>(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
        keepalive: KeepAlive,
        sleeper: S,
    ) -> (
        Self,
        impl Future<Output = Result<(), RunError>> + use<T, S>,
        KeepAliveTask<Sendable>,
    ) {
        tracing::info!(peer = %peer_id, keepalive = true, "new WebSocket connection");
        let (this, sender_task) = Self::new_inner(ws, peer_id);

        let body = keepalive_loop::<S, Sendable>(
            keepalive,
            peer_id,
            this.outbound_tx.clone(),
            this.inbound_writer.clone(),
            this.signals.clone(),
            sleeper,
        );
        let task = KeepAliveTask::new(body.boxed());

        (this, sender_task, task)
    }
}

/// How a read error from the WebSocket should be treated by the listener.
///
/// Parsing the raw [`tungstenite::Error`] into this category up front keeps the
/// listen loop's branching honest: the log severity and the teardown decision
/// both follow from the category rather than from ad-hoc `matches!` checks
/// scattered through the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadErrorKind {
    /// A benign disconnect: the remote end went away, with or without a clean
    /// close handshake (browser tab closed, network drop, TCP reset). Normal
    /// lifecycle, log quietly.
    ExpectedDisconnect,

    /// Peer-induced: the remote sent a message larger than our configured cap.
    /// This is the peer's doing, not a fault on our side, so it should not be
    /// logged at error severity. The connection cannot continue (the framing
    /// is desynchronized once an over-cap frame is seen), so it is still a
    /// teardown condition — just not an *alarm* condition.
    OverCapacity,

    /// A genuine transport/protocol fault we did not anticipate. Log loudly and
    /// tear the connection down.
    Fatal,
}

impl ReadErrorKind {
    /// Classify a [`tungstenite::Error`] surfaced while reading.
    #[allow(
        clippy::wildcard_enum_match_arm,
        reason = "anything we don't explicitly name is, by definition, an \
                  unanticipated fatal read error; a catch-all is the correct \
                  default and means new tungstenite variants fail safe."
    )]
    pub(crate) const fn classify(e: &Error) -> Self {
        match e {
            Error::ConnectionClosed
            | Error::AlreadyClosed
            | Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                Self::ExpectedDisconnect
            }

            // An over-cap inbound message is peer-induced, not our fault.
            Error::Capacity(CapacityError::MessageTooLong { .. }) => Self::OverCapacity,

            _ => Self::Fatal,
        }
    }

    /// The graceful RFC 6455 Close frame to send to the peer for this error
    /// kind, or `None` if we should not originate a Close.
    ///
    /// We only originate a Close when *we* are the party deciding to end the
    /// connection because of a problem:
    ///
    /// - [`Self::OverCapacity`] → `1009 Message Too Big`: tells the peer
    ///   precisely why (their message exceeded our cap) so it can react
    ///   (chunk, surface an error) instead of blindly reconnecting.
    /// - [`Self::Fatal`] → `1011 Internal Error`: an unanticipated server-side
    ///   condition.
    /// - [`Self::ExpectedDisconnect`] → `None`: the *remote* initiated the
    ///   close (or the socket is already gone). `tungstenite` auto-echoes the
    ///   peer's Close frame, so originating our own would be a redundant
    ///   double-close.
    ///
    /// This is a pure function of the category — unit-testable without a socket.
    fn close_frame(self) -> Option<Message> {
        let (code, reason) = match self {
            Self::OverCapacity => (CloseCode::Size, "message exceeds size limit"),
            Self::Fatal => (CloseCode::Error, "internal error"),
            Self::ExpectedDisconnect => return None,
        };

        Some(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
    }
}

/// Keepalive task body.
///
/// Cycle: snapshot write progress → `sleep(ping)` → clear flags → deliver
/// Ping (non-blocking `try_send`, retried across the window; on a full
/// queue the sender task injects it between frames — see
/// [`KeepAliveSignals::ping_requested`]) → window elapses → evaluate.
///
/// A cycle counts as alive if either a Pong arrived (end-to-end liveness)
/// or the sender completed at least one *data* write during the cycle
/// (`data_write_progress` advanced). The progress gate stops a saturated
/// but draining connection from being reaped just because its outbound
/// queue happened to be full at the ping instant; a wedged socket makes
/// no write progress, so it still accumulates misses and is reaped at the
/// threshold.
#[allow(
    clippy::too_many_lines,
    reason = "a deliberately linear liveness state machine; the extractable \
              units (window schedule, teardown) already live in PongWindow \
              and reap()"
)]
async fn keepalive_loop<S, Async>(
    config: KeepAlive,
    peer_id: PeerId,
    outbound_tx: async_channel::Sender<Outbound>,
    inbound_writer: async_channel::Sender<Vec<u8>>,
    signals: KeepAliveSignals,
    sleeper: S,
) -> KeepAliveOutcome
where
    S: Sleeper<Async>,
    Async: FutureForm + ?Sized,
{
    // One liveness detector, two patience levels over the same ping/pong
    // evidence stream:
    //
    //  - `consecutive_misses` (reset by pong OR data-write progress):
    //    fast path, `missed_pong_threshold` cycles (~80 s balanced).
    //    Catches wedged/dead connections where nothing moves.
    //
    //  - `unanswered_pings` (reset by pong ONLY; counts only pings that
    //    were actually delivered to the outbound queue): patient path,
    //    MAX_UNANSWERED_PINGS cycles (~5.3 min balanced). Catches
    //    transports that keep draining data while the remote protocol
    //    stack never answers — exactly the peers progress forgives.
    let mut consecutive_misses: u32 = 0;
    let mut unanswered_pings: u8 = 0;
    let threshold = config.missed_pong_threshold.get();

    loop {
        let progress_snapshot = signals.data_write_progress.load(Ordering::Relaxed);

        sleeper.sleep(config.ping_interval).await;

        // Clear before sending so a stale Pong from a previous cycle can't
        // satisfy this one — but a pong that arrived during the interval
        // still answers the *previous* ping (late, e.g. one delivered on
        // the window's last attempt). Credit it against the ceiling so a
        // slow-but-answering peer isn't misclassified as protocol-dead.
        // The miss counter is not credited: late pongs say nothing about
        // whether anything moved this cycle.
        if signals.pong_received.swap(false, Ordering::Relaxed) && unanswered_pings > 0 {
            tracing::debug!(
                peer = %peer_id,
                "keepalive: late pong observed; resetting unanswered-ping count"
            );
            unanswered_pings = 0;
        }
        signals.ping_injected.store(false, Ordering::Relaxed);

        // Deliver the ping: try_send at each sub-interval boundary, plus
        // sender-task injection when the queue is full (see
        // PING_DELIVERY_ATTEMPTS). Non-blocking throughout — a blocking
        // send would park this task on the very condition it exists to
        // detect. Empty payload: we don't match Pongs to specific Pings.
        let window = PongWindow::plan(config.pong_timeout);
        let mut ping_queued = false;
        for attempt in 0..window.attempts {
            if !ping_queued {
                let ping = tungstenite::Message::Ping(Vec::new().into());
                match outbound_tx.try_send(Outbound::new(ping)) {
                    Ok(()) => {
                        ping_queued = true;
                        // Cancel any standing injection request from an
                        // earlier failed attempt; one ping per cycle.
                        signals.ping_requested.store(false, Ordering::Relaxed);
                        tracing::trace!(peer = %peer_id, attempt, "keepalive: sent ping");
                    }
                    Err(async_channel::TrySendError::Closed(_)) => {
                        tracing::debug!(peer = %peer_id, "keepalive: outbound closed; exiting");
                        return KeepAliveOutcome::ConnectionClosed;
                    }
                    Err(async_channel::TrySendError::Full(_)) => {
                        signals.ping_requested.store(true, Ordering::Relaxed);
                        tracing::trace!(
                            peer = %peer_id,
                            attempt,
                            "keepalive: outbound full; requested sender-task injection"
                        );
                    }
                }
            }

            sleeper.sleep(window.sleep_after(attempt)).await;
        }

        let ping_sent = ping_queued || signals.ping_injected.swap(false, Ordering::Relaxed);

        if !ping_sent {
            // Cancel the standing injection request so a stale ping isn't
            // injected long after this window (the next cycle re-requests).
            signals.ping_requested.store(false, Ordering::Relaxed);
            // Undelivered pings accrue no evidence (see
            // MAX_UNANSWERED_PINGS); the miss path judges this cycle by
            // write progress alone.
            #[cfg(feature = "metrics")]
            subduction_core::metrics::keepalive_ping_undelivered();
            tracing::debug!(
                peer = %peer_id,
                "keepalive: zero drainage for the whole window; ping undelivered"
            );
        }

        // `swap`, not `load`: pong evidence is consumed exactly once. A
        // `load` would leave the flag set for the next cycle's late-pong
        // credit, double-counting one pong as two cycles of evidence.
        if signals.pong_received.swap(false, Ordering::Relaxed) {
            if consecutive_misses > 0 || unanswered_pings > 0 {
                tracing::debug!(peer = %peer_id, "keepalive: pong received; counters reset");
            }
            consecutive_misses = 0;
            unanswered_pings = 0;
            continue;
        }

        if ping_sent {
            unanswered_pings += 1;

            if unanswered_pings >= MAX_UNANSWERED_PINGS {
                // A delivered ping is answered by any compliant, reachable
                // WS stack regardless of application load, so this many in
                // a row means the remote protocol endpoint is gone — even
                // if the transport is still draining our data frames.
                tracing::warn!(
                    peer = %peer_id,
                    unanswered = unanswered_pings,
                    "keepalive: consecutive pings unanswered; closing connection"
                );
                reap(&outbound_tx, &inbound_writer, "keepalive timeout (no pong)");
                return KeepAliveOutcome::StaleNoPong {
                    unanswered: unanswered_pings,
                };
            }
        }

        // No pong — but if data writes completed this cycle the transport
        // is demonstrably moving (e.g. the queue was full for the whole
        // window on a link mid-bulk-transfer). Don't count a fast-path miss
        // against a connection that is making progress; a wedged socket
        // completes nothing and falls through to the miss path.
        if signals.data_write_progress.load(Ordering::Relaxed) != progress_snapshot {
            if consecutive_misses > 0 {
                tracing::debug!(
                    peer = %peer_id,
                    "keepalive: write progress observed; resetting misses"
                );
            }
            consecutive_misses = 0;
            continue;
        }

        consecutive_misses += 1;
        tracing::warn!(
            peer = %peer_id,
            misses = consecutive_misses,
            threshold,
            "keepalive: pong missed"
        );
        #[cfg(feature = "metrics")]
        subduction_core::metrics::keepalive_pong_missed();

        if consecutive_misses >= threshold {
            tracing::warn!(
                peer = %peer_id,
                misses = consecutive_misses,
                "keepalive: threshold reached; closing connection"
            );
            reap(&outbound_tx, &inbound_writer, "keepalive timeout");
            return KeepAliveOutcome::Timeout {
                missed: consecutive_misses,
            };
        }
    }
}

/// Sub-sleep schedule for one pong window (see [`PING_DELIVERY_ATTEMPTS`]).
///
/// The sub-sleeps always sum to exactly the configured `pong_timeout`, so
/// cycle timing is identical whether or not ping retries happen.
struct PongWindow {
    attempts: u32,
    sub_sleep: Duration,
    last_sleep: Duration,
}

impl PongWindow {
    /// Plan the window schedule.
    ///
    /// Sub-sleeps are whole milliseconds: timer wheels (tokio's included)
    /// round sub-millisecond sleeps *up*, which would stretch the window.
    /// When the window is too small to split cleanly, fall back to a
    /// single delivery attempt spanning the full window.
    fn plan(pong_timeout: Duration) -> Self {
        let sub_ms = u64::try_from(pong_timeout.as_millis()).unwrap_or(u64::MAX)
            / u64::from(PING_DELIVERY_ATTEMPTS);
        let (attempts, sub_sleep) = if sub_ms == 0 {
            (1, pong_timeout)
        } else {
            (PING_DELIVERY_ATTEMPTS, Duration::from_millis(sub_ms))
        };
        // The final sub-sleep absorbs division rounding so the window's
        // total is exactly `pong_timeout`. The fallback is unreachable
        // (3·⌊x/4⌋ ≤ x) and deliberately non-panicking: a panic here would
        // kill the keepalive task itself.
        let last_sleep = pong_timeout
            .checked_sub(sub_sleep * (attempts - 1))
            .unwrap_or(sub_sleep);

        Self {
            attempts,
            sub_sleep,
            last_sleep,
        }
    }

    /// The sleep duration following attempt number `attempt`.
    const fn sleep_after(&self, attempt: u32) -> Duration {
        if attempt == self.attempts - 1 {
            self.last_sleep
        } else {
            self.sub_sleep
        }
    }
}

/// Tear a connection down from the keepalive task: best-effort Close
/// frame (non-blocking — the channel may be full or already closing),
/// then close both channels so every parked producer and `recv_bytes`
/// waiter errors out and the connection unwinds.
fn reap(
    outbound_tx: &async_channel::Sender<Outbound>,
    inbound_writer: &async_channel::Sender<Vec<u8>>,
    reason: &'static str,
) {
    #[cfg(feature = "metrics")]
    subduction_core::metrics::keepalive_close();

    let close_frame = tungstenite::Message::Close(Some(CloseFrame {
        code: CloseCode::Away,
        reason: reason.into(),
    }));
    drop(outbound_tx.try_send(Outbound::new(close_frame)));

    outbound_tx.close();
    inbound_writer.close();
}

impl<T: AsyncRead + AsyncWrite + Unpin, Async: FutureForm> Clone for WebSocket<T, Async> {
    fn clone(&self) -> Self {
        Self {
            chan_id: self.chan_id,
            peer_id: self.peer_id,
            ws_reader: self.ws_reader.clone(),
            outbound_tx: self.outbound_tx.clone(),
            ws_sender: self.ws_sender.clone(),
            inbound_writer: self.inbound_writer.clone(),
            inbound_reader: self.inbound_reader.clone(),
            signals: self.signals.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin, Async: FutureForm> PartialEq for WebSocket<T, Async> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
            && Arc::ptr_eq(&self.ws_reader, &other.ws_reader)
            && self.outbound_tx.same_channel(&other.outbound_tx)
            && self.inbound_writer.same_channel(&other.inbound_writer)
            && self.inbound_reader.same_channel(&other.inbound_reader)
    }
}

#[cfg(all(test, feature = "tokio_base"))]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use testresult::TestResult;

    use crate::sleep::TokioSleeper;

    #[allow(clippy::expect_used, reason = "test-only helper")]
    const fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("non-zero")
    }

    /// An over-cap message is peer-induced: classified as `OverCapacity`, and
    /// we originate a graceful `Close(Size)` (1009 Message Too Big) so the peer
    /// learns precisely why.
    #[test]
    fn classify_over_cap_originates_close_size() {
        let err = Error::Capacity(CapacityError::MessageTooLong {
            size: 100,
            max_size: 10,
        });
        let kind = ReadErrorKind::classify(&err);
        assert_eq!(kind, ReadErrorKind::OverCapacity);

        let Some(Message::Close(Some(frame))) = kind.close_frame() else {
            unreachable!("over-cap must originate a Close(Some(_)) frame");
        };
        assert_eq!(frame.code, CloseCode::Size, "over-cap should send 1009");
        assert!(!frame.reason.is_empty(), "should carry a reason string");
    }

    /// Benign disconnects classify as `ExpectedDisconnect` and do NOT originate
    /// a Close frame: the remote already initiated the close (tungstenite
    /// auto-echoes), so we must not double-close.
    #[test]
    fn classify_expected_disconnects_send_no_close() {
        for err in [
            Error::ConnectionClosed,
            Error::AlreadyClosed,
            Error::Protocol(ProtocolError::ResetWithoutClosingHandshake),
        ] {
            let kind = ReadErrorKind::classify(&err);
            assert_eq!(
                kind,
                ReadErrorKind::ExpectedDisconnect,
                "{err:?} should be an expected disconnect"
            );
            assert!(
                kind.close_frame().is_none(),
                "expected disconnects must not originate a Close frame ({err:?})"
            );
        }
    }

    /// An unanticipated read error is `Fatal`: we originate a `Close(Error)`
    /// (1011 Internal Error).
    #[test]
    fn classify_unexpected_originates_close_error() {
        // A protocol error other than the benign reset is unexpected.
        let err = Error::Protocol(ProtocolError::UnmaskedFrameFromClient);
        let kind = ReadErrorKind::classify(&err);
        assert_eq!(kind, ReadErrorKind::Fatal);

        let Some(Message::Close(Some(frame))) = kind.close_frame() else {
            unreachable!("fatal must originate a Close(Some(_)) frame");
        };
        assert_eq!(frame.code, CloseCode::Error, "fatal should send 1011");
    }

    async fn create_mock_websocket_stream() -> WebSocketStream<Cursor<Vec<u8>>> {
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
        assert_eq!(kp.missed_pong_threshold, nz(2));
    }

    /// Silent peer → Timeout, channels closed, final message is
    /// `Close(Away, ...)`. Mock-time so we're CI-jitter-free.
    #[tokio::test(start_paused = true)]
    async fn keepalive_loop_times_out_on_silent_peer() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Vec<u8>>(16);

        let outbound_drain = tokio::spawn(async move {
            let mut msgs = Vec::new();
            while let Ok(item) = outbound_rx.recv().await {
                msgs.push(item.msg);
            }
            msgs
        });

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };
        let outcome = keepalive_loop(
            config,
            PeerId::new([42u8; 32]),
            outbound_tx,
            inbound_writer.clone(),
            KeepAliveSignals::new(),
            TokioSleeper,
        )
        .await;

        let outbound_msgs = outbound_drain.await?;

        let KeepAliveOutcome::Timeout { missed } = outcome else {
            return Err(format!("expected Timeout outcome, got {outcome:?}").into());
        };
        assert_eq!(missed, 2, "should close on exactly the threshold count");

        assert!(
            inbound_writer.is_closed(),
            "inbound_writer should be closed"
        );
        let ping_count = outbound_msgs
            .iter()
            .filter(|m| matches!(m, tungstenite::Message::Ping(_)))
            .count();
        assert_eq!(ping_count, 2, "expected 2 pings, got: {outbound_msgs:?}");
        assert!(
            matches!(
                outbound_msgs.last(),
                Some(tungstenite::Message::Close(Some(CloseFrame {
                    code: CloseCode::Away,
                    ..
                })))
            ),
            "expected final message to be Close(Away, ...), got: {:?}",
            outbound_msgs.last()
        );
        assert!(inbound_reader.recv().await.is_err());
        Ok(())
    }

    /// Responsive peer → loop runs indefinitely (no Timeout).
    #[tokio::test(start_paused = true)]
    async fn keepalive_loop_does_not_time_out_with_responsive_peer() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        let responsive_peer = {
            let pong_received = signals.pong_received.clone();
            tokio::spawn(async move {
                while let Ok(item) = outbound_rx.recv().await {
                    if matches!(item.msg, tungstenite::Message::Ping(_)) {
                        pong_received.store(true, Ordering::Relaxed);
                    }
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(20),
            pong_timeout: Duration::from_millis(10),
            missed_pong_threshold: nz(2),
        };
        // With virtual time, the timeout(250ms) bound is virtual-ms, well
        // past the silent-peer close window of (20+10) × 2 = 60 ms.
        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(250),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals,
                TokioSleeper,
            ),
        )
        .await;

        if let Ok(unexpected) = outcome_or_timeout {
            return Err(format!(
                "keepalive_loop should still be running with responsive peer, got {unexpected:?}"
            )
            .into());
        }

        outbound_tx.close();
        responsive_peer.await?;
        Ok(())
    }

    /// One miss + recovery resets the counter; threshold is never reached.
    #[tokio::test(start_paused = true)]
    async fn keepalive_loop_resets_misses_after_recovery() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        let pattern_runner = {
            let pong_received = signals.pong_received.clone();
            tokio::spawn(async move {
                let mut seen = 0u32;
                while let Ok(item) = outbound_rx.recv().await {
                    if matches!(item.msg, tungstenite::Message::Ping(_)) {
                        seen += 1;
                        // Respond on even-numbered pings only (miss-respond-miss-respond-…).
                        // With threshold = 2, a non-resetting loop would
                        // close after ping 3. A correct loop resets on
                        // ping 2 and never reaches the threshold.
                        if seen.is_multiple_of(2) {
                            pong_received.store(true, Ordering::Relaxed);
                        }
                    }
                }
                seen
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(20),
            pong_timeout: Duration::from_millis(10),
            missed_pong_threshold: nz(2),
        };
        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(180),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals,
                TokioSleeper,
            ),
        )
        .await;

        if let Ok(unexpected) = outcome_or_timeout {
            return Err(format!(
                "keepalive_loop should still be running (recovery should reset misses), got {unexpected:?}"
            )
            .into());
        }

        outbound_tx.close();
        let seen = pattern_runner.await?;
        assert!(seen >= 4, "expected at least 4 ping cycles, saw {seen}");
        Ok(())
    }

    /// External `outbound_tx.close()` → `ConnectionClosed` (not `Timeout`).
    /// Graceful path must not touch `inbound_writer`.
    #[tokio::test(start_paused = true)]
    async fn keepalive_loop_exits_when_outbound_closes_externally() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);

        // Externally close the channel after a short delay — well before
        // the silent-peer Timeout window (1000+500 = 1500ms virtual).
        let closer_tx = outbound_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            closer_tx.close();
        });

        // Also drain so the loop can send its initial ping.
        tokio::spawn(async move { while outbound_rx.recv().await.is_ok() {} });

        let config = KeepAlive {
            ping_interval: Duration::from_secs(1),
            pong_timeout: Duration::from_millis(500),
            missed_pong_threshold: nz(3),
        };
        let outcome = keepalive_loop(
            config,
            PeerId::new([42u8; 32]),
            outbound_tx,
            inbound_writer.clone(),
            KeepAliveSignals::new(),
            TokioSleeper,
        )
        .await;

        assert!(
            matches!(outcome, KeepAliveOutcome::ConnectionClosed),
            "expected ConnectionClosed, got {outcome:?}"
        );
        // The loop must NOT touch `inbound_writer` on the graceful path —
        // only the Timeout path closes it.
        assert!(
            !inbound_writer.is_closed(),
            "inbound_writer should not be force-closed on graceful exit"
        );
        Ok(())
    }

    /// Regression: pong reply uses `try_send`. Reverting to
    /// `send().await` would deadlock when the outbound channel is full.
    #[tokio::test]
    async fn pong_reply_via_try_send_does_not_block_when_outbound_full() -> TestResult {
        // Channel capacity = 2; saturate it.
        let (tx, rx) = async_channel::bounded::<tungstenite::Message>(2);
        tx.send(tungstenite::Message::Binary(vec![1].into()))
            .await?;
        tx.send(tungstenite::Message::Binary(vec![2].into()))
            .await?;
        assert_eq!(tx.len(), 2);

        // Mirror exactly what `WebSocket::listen()` does on Ping arrival.
        let pong = tungstenite::Message::Pong(vec![0xab; 8].into());
        let result = tx.try_send(pong);

        // A blocking `.send().await` here would deadlock until the channel
        // drained. `try_send` returns immediately with Err(Full).
        assert!(
            matches!(result, Err(async_channel::TrySendError::Full(_))),
            "expected Full error, got {result:?}"
        );
        // The original two messages are still in the channel — the failed
        // Pong did not displace anything.
        assert_eq!(rx.len(), 2);

        // If a drain frees a slot, a subsequent try_send succeeds.
        drop(rx.recv().await?);
        let pong2 = tungstenite::Message::Pong(vec![0xcd; 8].into());
        assert!(tx.try_send(pong2).is_ok());
        Ok(())
    }

    /// Regression: the keepalive *ping* uses `try_send`. Reverting to
    /// `send().await` would park the keepalive task forever when the
    /// outbound channel is full — the exact wedged-socket condition where
    /// the reaper is needed most.
    ///
    /// A full-and-never-drained channel must still produce `Timeout` at
    /// the threshold, with the inbound writer closed so the connection
    /// tears down.
    #[tokio::test(start_paused = true)]
    async fn keepalive_reaps_connection_when_outbound_full() -> TestResult {
        // Capacity 1, pre-filled, never drained: simulates a sender task
        // parked on a peer that stopped reading its socket.
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(1);
        outbound_tx
            .send(Outbound::new(tungstenite::Message::Binary(
                vec![0xEE].into(),
            )))
            .await?;

        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        // Bound the whole run in virtual time: with a blocking ping send
        // this would never complete (the channel is never drained), and
        // with paused time the timeout fires deterministically.
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                KeepAliveSignals::new(),
                TokioSleeper,
            ),
        )
        .await
        .map_err(|_| "keepalive_loop parked on a full outbound channel (ping send blocked)")?;

        let KeepAliveOutcome::Timeout { missed } = outcome else {
            return Err(format!("expected Timeout outcome, got {outcome:?}").into());
        };
        assert_eq!(missed, 2, "should close on exactly the threshold count");

        assert!(
            inbound_writer.is_closed(),
            "inbound_writer should be closed so the connection tears down"
        );
        assert!(outbound_tx.is_closed(), "outbound should be closed");

        // The wedged payload is still queued — dropped pings/Close never
        // displaced it (that would reorder the peer's stream).
        assert_eq!(outbound_rx.len(), 1, "pre-existing message untouched");
        Ok(())
    }

    /// A saturated-but-draining connection must NOT be reaped: the outbound
    /// queue is full at every ping instant (pings are dropped), but the
    /// sender keeps completing data writes, so the progress gate resets the
    /// miss counter each cycle.
    ///
    /// Reverting the progress gate would reap this connection at
    /// `threshold × (ping + pong)` — exactly what a bulk cold-sync to a
    /// slow-but-alive client looks like.
    #[tokio::test(start_paused = true)]
    async fn keepalive_does_not_reap_full_but_progressing_connection() -> TestResult {
        // Capacity 1, pre-filled, never drained: full at every ping instant.
        let (outbound_tx, _outbound_rx) = async_channel::bounded::<Outbound>(1);
        outbound_tx
            .send(Outbound::new(tungstenite::Message::Binary(
                vec![0xEE].into(),
            )))
            .await?;

        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        // Simulate a sender task that keeps completing data writes even
        // though the queue stays full (producers instantly refill it).
        // Note: no injection happens (there is no real sender task), so
        // pings are never delivered in this test.
        let writer = {
            let progress = signals.data_write_progress.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    progress.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        // Without the gate this reaps at 2 × (40+20) = 120ms of virtual
        // time. The ceiling never engages: zero drainage means no ping is
        // ever delivered, and undelivered pings accrue no evidence. The
        // window extends past 8 × 60 = 480ms to catch a mutant that counts
        // undelivered pings toward the ceiling.
        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(650),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals.clone(),
                TokioSleeper,
            ),
        )
        .await;

        writer.abort();
        if let Ok(unexpected) = outcome_or_timeout {
            return Err(format!(
                "keepalive must not reap a connection making write progress, got {unexpected:?}"
            )
            .into());
        }
        assert!(
            !inbound_writer.is_closed(),
            "progressing connection must not be torn down"
        );
        Ok(())
    }

    /// The progress gate forgives missing pongs only up to
    /// `MAX_UNANSWERED_PINGS`: a peer that keeps accepting data writes
    /// but never answers delivered pings (frame-swallowing middlebox,
    /// proxy fronting a dead backend) must still be reaped at the hard
    /// ceiling. Removing the ceiling would let such a peer evade the
    /// reaper indefinitely.
    #[tokio::test(start_paused = true)]
    async fn keepalive_reaps_progressing_peer_that_never_pongs() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        // Drain pings (so try_send succeeds) but never pong; keep write
        // progress advancing every cycle.
        let drain = tokio::spawn(async move { while outbound_rx.recv().await.is_ok() {} });
        let writer = {
            let progress = signals.data_write_progress.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    progress.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        let start = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals.clone(),
                TokioSleeper,
            ),
        )
        .await
        .map_err(|_| "progressing-but-silent peer must still be reaped at the hard ceiling")?;
        let elapsed = start.elapsed();
        writer.abort();

        let KeepAliveOutcome::StaleNoPong { unanswered } = outcome else {
            return Err(format!("expected StaleNoPong outcome, got {outcome:?}").into());
        };
        assert_eq!(unanswered, MAX_UNANSWERED_PINGS);
        // 8 cycles × (40 + 20)ms of virtual time — the retry sub-sleeps
        // sum to exactly pong_timeout, so cycle timing is unchanged.
        assert_eq!(
            elapsed,
            Duration::from_millis(u64::from(MAX_UNANSWERED_PINGS) * 60),
            "reap must land exactly at the ceiling"
        );
        assert!(
            inbound_writer.is_closed(),
            "stale connection must be torn down"
        );

        outbound_tx.close();
        drop(drain.await);
        Ok(())
    }

    /// A queue that is full at the ping instant but *draining* must not
    /// cost the peer liveness evidence: the in-window retry delivers the
    /// ping once a slot frees, the (healthy) peer pongs, and the cycle
    /// counts as answered. Without the retry, a busy peer whose inbound
    /// queue from us hovers at capacity would never be pinged at all and
    /// could be falsely reaped by the unanswered-ping ceiling.
    #[tokio::test(start_paused = true)]
    async fn ping_retry_delivers_on_draining_queue() -> TestResult {
        // Capacity 1, pre-filled: the first try_send (window start, t=40ms)
        // fails. Config: 40ms interval, 20ms window → retries at t=45/50/55.
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(1);
        outbound_tx
            .send(Outbound::new(tungstenite::Message::Binary(
                vec![0xEE].into(),
            )))
            .await?;

        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        // Scripted peer: at t=47ms drain the Binary (freeing a slot between
        // the t=45 and t=50 retries), then receive the retried ping at t=50
        // and answer it. Records that a Ping was actually observed.
        let saw_ping = Arc::new(AtomicBool::new(false));
        let peer = {
            let pong_received = signals.pong_received.clone();
            let saw_ping = saw_ping.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(47)).await;
                let first = outbound_rx.recv().await;
                assert!(
                    matches!(
                        first.as_ref().map(|o| &o.msg),
                        Ok(tungstenite::Message::Binary(_))
                    ),
                    "first drained frame should be the pre-filled Binary"
                );

                // Parks until the t=50 retry enqueues the ping.
                if let Ok(item) = outbound_rx.recv().await
                    && matches!(item.msg, tungstenite::Message::Ping(_))
                {
                    saw_ping.store(true, Ordering::Relaxed);
                    pong_received.store(true, Ordering::Relaxed);
                }

                // Keep draining so subsequent cycles behave normally.
                while let Ok(item) = outbound_rx.recv().await {
                    if matches!(item.msg, tungstenite::Message::Ping(_)) {
                        pong_received.store(true, Ordering::Relaxed);
                    }
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        // Run several cycles; a false reap would resolve the loop early.
        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(300),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals.clone(),
                TokioSleeper,
            ),
        )
        .await;

        if let Ok(unexpected) = outcome_or_timeout {
            return Err(format!(
                "healthy draining+ponging peer must not be reaped, got {unexpected:?}"
            )
            .into());
        }
        assert!(
            saw_ping.load(Ordering::Relaxed),
            "the retry must have delivered a ping after the initial Full"
        );
        assert!(!inbound_writer.is_closed());

        outbound_tx.close();
        peer.await?;
        Ok(())
    }

    /// A pong resets the unanswered-ping counter. The peer answers only
    /// every second ping: a resetting counter oscillates between 0 and 1
    /// and never reaps; a non-resetting counter accumulates one unanswered
    /// ping per two cycles and reaps `StaleNoPong` at cycle 15 (~900ms
    /// here). The intervening pongs keep the fast-path miss counter below
    /// its threshold, so only the ceiling's reset behavior is in play.
    #[tokio::test(start_paused = true)]
    async fn pong_resets_unanswered_ping_counter() -> TestResult {
        let (outbound_tx, outbound_rx) = async_channel::bounded::<Outbound>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        // Peer: answers even-numbered pings only.
        let peer = {
            let pong_received = signals.pong_received.clone();
            tokio::spawn(async move {
                let mut seen = 0u32;
                while let Ok(item) = outbound_rx.recv().await {
                    if matches!(item.msg, tungstenite::Message::Ping(_)) {
                        seen += 1;
                        if seen.is_multiple_of(2) {
                            pong_received.store(true, Ordering::Relaxed);
                        }
                    }
                }
                seen
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        let outcome_or_timeout = tokio::time::timeout(
            Duration::from_millis(1100),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals.clone(),
                TokioSleeper,
            ),
        )
        .await;

        if let Ok(unexpected) = outcome_or_timeout {
            return Err(
                format!("alternating-pong peer must not be reaped, got {unexpected:?}").into(),
            );
        }
        assert!(!inbound_writer.is_closed());

        outbound_tx.close();
        let seen = peer.await?;
        assert!(seen > 16, "expected many ping cycles, saw {seen}");
        Ok(())
    }

    /// An injected ping (delivered by the sender task when the queue was
    /// full) must count as delivered: with no pong and ongoing progress,
    /// injected-but-unanswered pings accrue toward the ceiling and reap at
    /// exactly `MAX_UNANSWERED_PINGS` cycles. A mutant that ignores
    /// `ping_injected` would classify these cycles as undelivered and let
    /// the peer live forever.
    #[tokio::test(start_paused = true)]
    async fn injected_ping_counts_as_delivered() -> TestResult {
        // Capacity 1, pre-filled, never drained: every try_send fails, so
        // delivery can only be observed via the injected flag.
        let (outbound_tx, _outbound_rx) = async_channel::bounded::<Outbound>(1);
        outbound_tx
            .send(Outbound::new(tungstenite::Message::Binary(
                vec![0xEE].into(),
            )))
            .await?;

        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let signals = KeepAliveSignals::new();

        // Simulated sender task: keeps progress moving and "injects" the
        // requested ping each cycle (sets ping_injected mid-window), but no
        // pong ever arrives.
        let simulator = {
            let signals = signals.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(15)).await;
                    signals.data_write_progress.fetch_add(1, Ordering::Relaxed);
                    if signals.ping_requested.swap(false, Ordering::Relaxed) {
                        signals.ping_injected.store(true, Ordering::Relaxed);
                    }
                }
            })
        };

        let config = KeepAlive {
            ping_interval: Duration::from_millis(40),
            pong_timeout: Duration::from_millis(20),
            missed_pong_threshold: nz(2),
        };

        let start = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            keepalive_loop(
                config,
                PeerId::new([42u8; 32]),
                outbound_tx.clone(),
                inbound_writer.clone(),
                signals.clone(),
                TokioSleeper,
            ),
        )
        .await
        .map_err(|_| "injected-but-unanswered pings must reap at the ceiling")?;
        let elapsed = start.elapsed();
        simulator.abort();

        let KeepAliveOutcome::StaleNoPong { unanswered } = outcome else {
            return Err(format!("expected StaleNoPong outcome, got {outcome:?}").into());
        };
        assert_eq!(unanswered, MAX_UNANSWERED_PINGS);
        assert_eq!(
            elapsed,
            Duration::from_millis(u64::from(MAX_UNANSWERED_PINGS) * 60),
            "injected pings must count from the first cycle"
        );
        Ok(())
    }

    /// End-to-end injection through the real sender task: when the
    /// keepalive raises `ping_requested`, the sender injects a Ping
    /// directly into the sink after its next completed frame, sets
    /// `ping_injected`, and consumes the request. Data progress counts
    /// only the data frames — the injected ping is a control frame.
    #[tokio::test]
    async fn sender_task_injects_ping_on_request() -> TestResult {
        let ws = create_mock_websocket_stream().await;
        let (websocket, sender_task): (WebSocket<_, Sendable>, _) =
            WebSocket::new(ws, PeerId::new([7u8; 32]));

        let signals = websocket.signals.clone();
        let tx = websocket.outbound_tx.clone();

        signals.ping_requested.store(true, Ordering::Relaxed);
        tx.send(Outbound::new(tungstenite::Message::Binary(
            vec![1, 2, 3].into(),
        )))
        .await?;
        tx.send(Outbound::new(tungstenite::Message::Binary(vec![4].into())))
            .await?;
        tx.close();

        tokio::spawn(sender_task).await??;

        assert!(
            signals.ping_injected.load(Ordering::Relaxed),
            "sender must inject the requested ping"
        );
        assert!(
            !signals.ping_requested.load(Ordering::Relaxed),
            "the injection request must be consumed"
        );
        assert_eq!(
            signals.data_write_progress.load(Ordering::Relaxed),
            2,
            "the injected ping is a control frame and must not count as progress"
        );
        Ok(())
    }

    /// The progress gate must only see *data* writes: the sender task
    /// counts Binary/Text frames and excludes control frames. If the
    /// keepalive's own Ping writes moved the counter, an idle dead peer
    /// would look alive forever and never be reaped.
    #[tokio::test]
    async fn sender_task_counts_only_data_writes() -> TestResult {
        let ws = create_mock_websocket_stream().await;
        let (websocket, sender_task): (WebSocket<_, Sendable>, _) =
            WebSocket::new(ws, PeerId::new([7u8; 32]));

        let progress = websocket.signals.data_write_progress.clone();
        let tx = websocket.outbound_tx.clone();

        let sender = tokio::spawn(sender_task);

        tx.send(Outbound::new(tungstenite::Message::Ping(Vec::new().into())))
            .await?;
        tx.send(Outbound::new(tungstenite::Message::Binary(
            vec![1, 2, 3].into(),
        )))
        .await?;
        tx.send(Outbound::new(tungstenite::Message::Pong(Vec::new().into())))
            .await?;
        tx.send(Outbound::new(tungstenite::Message::Text("hi".into())))
            .await?;

        // Close the channel so the sender task drains and exits.
        tx.close();
        sender.await??;

        assert_eq!(
            progress.load(Ordering::Relaxed),
            2,
            "only the Binary and Text frames may count as progress"
        );
        Ok(())
    }

    /// Property: silent peer closes at exactly
    /// `threshold × (ping + pong)` of virtual time.
    #[test]
    fn property_silent_peer_closes_at_predicted_virtual_time() {
        bolero::check!()
            .with_arbitrary::<(u16, u16, u8)>()
            .for_each(|input| {
                // Clamp to non-zero + reasonable bounds. The formula
                // overflows above (threshold × (ping + pong)) ~ u64::MAX,
                // and very small values just collapse to ~zero virtual
                // time which doesn't exercise anything.
                let interval_ms = u64::from(input.0).max(1);
                let timeout_ms = u64::from(input.1).max(1);
                let threshold_u32 = u32::from(input.2.clamp(1, 10));
                #[allow(clippy::expect_used, reason = "threshold_u32 was just clamped to >= 1")]
                let threshold = NonZeroU32::new(threshold_u32).expect("clamped to >=1");

                #[allow(
                    clippy::expect_used,
                    reason = "building a tokio current-thread runtime never fails in practice"
                )]
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .start_paused(true)
                    .build()
                    .expect("paused tokio runtime");

                rt.block_on(async move {
                    let (tx, rx) = async_channel::bounded::<Outbound>(64);
                    let (inbound_tx, _) = async_channel::bounded::<Vec<u8>>(16);

                    let drain = tokio::spawn(async move { while rx.recv().await.is_ok() {} });

                    let cfg = KeepAlive {
                        ping_interval: Duration::from_millis(interval_ms),
                        pong_timeout: Duration::from_millis(timeout_ms),
                        missed_pong_threshold: threshold,
                    };

                    let start = tokio::time::Instant::now();
                    let outcome = keepalive_loop(
                        cfg,
                        PeerId::new([0u8; 32]),
                        tx,
                        inbound_tx,
                        KeepAliveSignals::new(),
                        TokioSleeper,
                    )
                    .await;
                    let elapsed = start.elapsed();
                    drop(drain.await);

                    // A silent-but-draining peer receives every ping, so
                    // both liveness counters advance each cycle and the
                    // reap lands at whichever bound is lower: the
                    // configured miss threshold (fast path, `Timeout`) or
                    // the unanswered-ping ceiling (`StaleNoPong`). The
                    // ceiling check runs first within a cycle, so a
                    // threshold equal to the ceiling also yields
                    // `StaleNoPong`.
                    let expected_cycles = threshold_u32.min(u32::from(MAX_UNANSWERED_PINGS));
                    let expected = Duration::from_millis(
                        u64::from(expected_cycles) * (interval_ms + timeout_ms),
                    );
                    assert_eq!(
                        elapsed, expected,
                        "ping={interval_ms}ms pong={timeout_ms}ms threshold={threshold_u32}: \
                         expected close at {expected:?}, observed {elapsed:?}"
                    );
                    if threshold_u32 < u32::from(MAX_UNANSWERED_PINGS) {
                        assert!(
                            matches!(
                                outcome,
                                KeepAliveOutcome::Timeout { missed } if missed == threshold_u32
                            ),
                            "outcome was {outcome:?}, expected Timeout {{ missed: {threshold_u32} }}"
                        );
                    } else {
                        assert!(
                            matches!(
                                outcome,
                                KeepAliveOutcome::StaleNoPong { unanswered }
                                    if unanswered == MAX_UNANSWERED_PINGS
                            ),
                            "outcome was {outcome:?}, expected StaleNoPong {{ unanswered: {MAX_UNANSWERED_PINGS} }}"
                        );
                    }
                });
            });
    }
}
