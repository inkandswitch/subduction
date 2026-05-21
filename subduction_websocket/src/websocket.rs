//! # Generic WebSocket transport for Subduction

use alloc::{boxed::Box, sync::Arc, vec::Vec};
use core::{
    fmt::Debug,
    future::{Future, IntoFuture},
    marker::PhantomData,
    num::NonZeroU32,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_lock::Mutex;
use async_tungstenite::{WebSocketReceiver, WebSocketSender, WebSocketStream};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{FutureExt, future::BoxFuture};
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use subduction_core::{peer::id::PeerId, transport::Transport};

use crate::{
    error::{DisconnectionError, RecvError, RunError, SendError},
    sleep::Sleeper,
};

/// Channel capacity for outbound messages.
///
/// This is sized to allow many concurrent sends without blocking while still
/// providing backpressure if the sender task can't keep up.
const OUTBOUND_CHANNEL_CAPACITY: usize = 1024;

/// Configuration for WebSocket Ping/Pong keepalive.
///
/// A peer that misses [`missed_pong_threshold`] consecutive pongs is
/// declared dead; total detection latency is
/// `missed_pong_threshold × (ping_interval + pong_timeout)`.
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
    #[must_use]
    pub const fn balanced() -> Self {
        let Some(two) = NonZeroU32::new(2) else {
            unreachable!()
        };
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
    #[cfg_attr(not(feature = "tokio_base"), allow(dead_code))]
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
    #[cfg_attr(not(feature = "tokio_base"), allow(dead_code))]
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
}

/// Background task that pings the peer and tears down the connection
/// on timeout. Must be spawned for keepalive to take effect.
///
/// Parameterized by [`FutureForm`]: `KeepAliveTask<Sendable>` is
/// `Send`-spawnable on multi-threaded runtimes; `KeepAliveTask<Local>`
/// is for single-threaded runtimes (Wasm).
pub struct KeepAliveTask<K: FutureForm>(K::Future<'static, KeepAliveOutcome>);

impl<K: FutureForm> core::fmt::Debug for KeepAliveTask<K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeepAliveTask").finish_non_exhaustive()
    }
}

impl<K: FutureForm> KeepAliveTask<K> {
    pub(crate) const fn new(fut: K::Future<'static, KeepAliveOutcome>) -> Self {
        Self(fut)
    }
}

impl<K: FutureForm> IntoFuture for KeepAliveTask<K> {
    type Output = KeepAliveOutcome;
    type IntoFuture = K::Future<'static, KeepAliveOutcome>;

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

    /// Set by [`Self::listen`] on incoming Pong; read and cleared by the
    /// keepalive task. Unused (but cheap) when keepalive is disabled.
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
    ) -> (Self, impl Future<Output = Result<(), RunError>> + use<T, K>) {
        tracing::info!("new WebSocket connection for peer {peer_id:?} (keepalive: false)");
        Self::new_inner(ws, peer_id)
    }

    /// Shared body of [`Self::new`] and [`Self::new_with_keepalive`].
    fn new_inner(
        ws: WebSocketStream<T>,
        peer_id: PeerId,
    ) -> (Self, impl Future<Output = Result<(), RunError>> + use<T, K>) {
        let (ws_writer, ws_reader) = ws.split();
        let (inbound_writer, inbound_reader) = async_channel::bounded(128);
        let (outbound_tx, outbound_rx) = async_channel::bounded(OUTBOUND_CHANNEL_CAPACITY);
        let chan_id = rand::random::<u64>();
        // Initial value is irrelevant — the keepalive loop clears the
        // flag before each ping.
        let pong_received = Arc::new(AtomicBool::new(false));

        let ws_sender = Arc::new(Mutex::new(ws_writer));

        let sender_task = {
            let ws_sender = ws_sender.clone();
            async move {
                tracing::info!("starting WebSocket sender task for peer {peer_id:?}");

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
            pong_received,

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
                    tracing::trace!(size = p.len(), peer_id = ?self.peer_id, "received ping");
                    // Non-blocking so a saturated outbound queue can't
                    // stall the listener. A dropped pong may cost one
                    // keepalive cycle on the remote side.
                    if let Err(e) = self.outbound_tx.try_send(tungstenite::Message::Pong(p)) {
                        tracing::warn!(
                            error = ?e,
                            peer_id = ?self.peer_id,
                            "dropped pong reply (outbound full or closed)"
                        );
                    }
                }
                Ok(tungstenite::Message::Pong(p)) => {
                    tracing::trace!(size = p.len(), peer_id = ?self.peer_id, "received pong");
                    self.pong_received.store(true, Ordering::Relaxed);
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

impl<T: AsyncRead + AsyncWrite + Unpin> WebSocket<T, Sendable> {
    /// Create a new WebSocket transport with Ping/Pong keepalive.
    ///
    /// Returns the transport, a sender task, and a keepalive task —
    /// all three should be spawned. The keepalive task closes both
    /// channels on [`KeepAliveOutcome::Timeout`]; the rest of the
    /// stack observes the disconnect through the normal channel-closed
    /// paths.
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
        tracing::info!("new WebSocket connection for peer {peer_id:?} (keepalive: true)");
        let (this, sender_task) = Self::new_inner(ws, peer_id);

        let body = keepalive_loop::<S, Sendable>(
            keepalive,
            peer_id,
            this.outbound_tx.clone(),
            this.inbound_writer.clone(),
            this.pong_received.clone(),
            sleeper,
        );
        let task = KeepAliveTask::new(body.boxed());

        (this, sender_task, task)
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

/// Keepalive task body.
///
/// Cycle: `sleep(ping)` → clear flag → send Ping → `sleep(pong)` →
/// check flag. Threshold consecutive misses trigger disconnect.
async fn keepalive_loop<S, K>(
    config: KeepAlive,
    peer_id: PeerId,
    outbound_tx: async_channel::Sender<tungstenite::Message>,
    inbound_writer: async_channel::Sender<Vec<u8>>,
    pong_received: Arc<AtomicBool>,
    sleeper: S,
) -> KeepAliveOutcome
where
    S: Sleeper<K>,
    K: FutureForm + ?Sized,
{
    use tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

    let mut consecutive_misses: u32 = 0;
    let threshold = config.missed_pong_threshold.get();

    loop {
        sleeper.sleep(config.ping_interval).await;

        // Clear before sending so a stale Pong from a previous cycle
        // can't satisfy this one.
        pong_received.store(false, Ordering::Relaxed);

        // Empty payload: we don't verify the Pong reply matches a
        // specific Ping, so a unique payload would just be overhead.
        let ping = tungstenite::Message::Ping(Vec::new().into());
        if outbound_tx.send(ping).await.is_err() {
            tracing::debug!(?peer_id, "keepalive: outbound closed; exiting");
            return KeepAliveOutcome::ConnectionClosed;
        }
        tracing::trace!(?peer_id, "keepalive: sent ping");

        sleeper.sleep(config.pong_timeout).await;

        if pong_received.load(Ordering::Relaxed) {
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
            threshold,
            "keepalive: pong missed"
        );

        if consecutive_misses >= threshold {
            tracing::warn!(
                ?peer_id,
                misses = consecutive_misses,
                "keepalive: threshold reached; closing connection"
            );
            // Best-effort Close frame; non-blocking since we're closing
            // the channel right after.
            let close_frame = tungstenite::Message::Close(Some(CloseFrame {
                code: CloseCode::Away,
                reason: "keepalive timeout".into(),
            }));
            drop(outbound_tx.try_send(close_frame));

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

    use crate::sleep::TokioSleeper;

    #[allow(clippy::expect_used, reason = "test-only helper")]
    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("non-zero")
    }

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
        assert_eq!(kp.missed_pong_threshold, nz(2));
    }

    /// Silent peer → Timeout, channels closed, final message is
    /// `Close(Away, ...)`. Mock-time so we're CI-jitter-free.
    #[tokio::test(start_paused = true)]
    async fn keepalive_loop_times_out_on_silent_peer() -> TestResult {
        use tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

        let outbound_drain = tokio::spawn(async move {
            let mut msgs = Vec::new();
            while let Ok(msg) = outbound_rx.recv().await {
                msgs.push(msg);
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
            pong_received,
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
        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

        let responsive_peer = {
            let pong_received = pong_received.clone();
            tokio::spawn(async move {
                while let Ok(msg) = outbound_rx.recv().await {
                    if matches!(msg, tungstenite::Message::Ping(_)) {
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
                pong_received,
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
        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

        let pattern_runner = {
            let pong_received = pong_received.clone();
            tokio::spawn(async move {
                let mut seen = 0u32;
                while let Ok(msg) = outbound_rx.recv().await {
                    if matches!(msg, tungstenite::Message::Ping(_)) {
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
                pong_received,
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
        let (outbound_tx, outbound_rx) = async_channel::bounded::<tungstenite::Message>(16);
        let (inbound_writer, _inbound_reader) = async_channel::bounded::<Vec<u8>>(16);
        let pong_received = Arc::new(AtomicBool::new(false));

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
            pong_received,
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
                    let (tx, rx) = async_channel::bounded::<tungstenite::Message>(64);
                    let (inbound_tx, _) = async_channel::bounded::<Vec<u8>>(16);
                    let pong_received = Arc::new(AtomicBool::new(false));

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
                        pong_received,
                        TokioSleeper,
                    )
                    .await;
                    let elapsed = start.elapsed();
                    drop(drain.await);

                    let expected = Duration::from_millis(
                        u64::from(threshold_u32) * (interval_ms + timeout_ms),
                    );
                    assert_eq!(
                        elapsed, expected,
                        "ping={interval_ms}ms pong={timeout_ms}ms threshold={threshold_u32}: \
                         expected close at {expected:?}, observed {elapsed:?}"
                    );
                    assert!(
                        matches!(
                            outcome,
                            KeepAliveOutcome::Timeout { missed } if missed == threshold_u32
                        ),
                        "outcome was {outcome:?}, expected Timeout {{ missed: {threshold_u32} }}"
                    );
                });
            });
    }
}
