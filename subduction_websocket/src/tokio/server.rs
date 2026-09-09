//! # Subduction WebSocket server for Tokio

use subduction_core::timeout::Timeout;

use crate::{
    handshake::{WebSocketHandshake, WebSocketHandshakeError},
    sleep::TokioSleeper,
    tokio::unified::UnifiedWebSocket,
    websocket::{KeepAlive, WebSocket},
};

use alloc::sync::Arc;
use async_tungstenite::tokio::{accept_hdr_async_with_config, connect_async_with_config};
use core::{net::SocketAddr, time::Duration};
use future_form::Sendable;
use sedimentree_core::depth::DepthMetric;
use subduction_core::{
    authenticated::Authenticated,
    handler::sync::SyncHandler,
    handshake::{
        self, AuthenticateError,
        audience::{Audience, DiscoveryId},
    },
    nonce_cache::NonceCache,
    peer::{
        counter::{PeerCounter, wall_clock_seed},
        id::PeerId,
    },
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    subduction::{Subduction, builder::SubductionBuilder, error::AddConnectionError},
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};
use subduction_tokio::{node::TokioSubduction, spawn::TrackedTokioSpawn};
use tracing::Instrument;

use tokio::{net::TcpListener, task::JoinSet};
use tungstenite::{handshake::server::NoCallback, http::Uri, protocol::WebSocketConfig};

// NOTE: `O: Timeout<Sendable>` remains on the server type because
// `Subduction` / `SubductionBuilder` still require a timer parameter,
// even though WebSocket itself no longer stores it.

/// A Tokio-flavoured [`WebSocket`] server implementation.
///
/// Owns a [`TokioSubduction`] and adds an accept loop on top. All lifecycle
/// — supervision of the node's loops, task tracking, cancellation, ordered
/// teardown — is the node's; the server only contributes socket concerns.
/// Dropping the server stops the node.
#[derive(Debug)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable> + core::fmt::Debug,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    Sig: 'static + Send + Sync + Signer<Sendable>,
    M: 'static + Send + Sync + DepthMetric,
    O: 'static + Send + Sync + Timeout<Sendable> + core::fmt::Debug,
> where
    S::Error: 'static + Send + Sync,
    P::PutDisallowed: Send + 'static,
    P::FetchDisallowed: Send + 'static,
{
    node: TokioWebSocketNode<S, P, Sig, O, M>,
    address: SocketAddr,
    /// Tungstenite max message size used for outbound connections
    /// (`try_connect` / `try_connect_discover`). The incoming accept loop
    /// already receives this value as a local variable — storing it here
    /// lets the outbound methods use the configured value instead of
    /// falling back to the transport default.
    max_message_size: usize,
    /// Ping/Pong keepalive applied to every WebSocket connection
    /// (inbound and outbound) managed by this server. Defaults to
    /// [`KeepAlive::balanced`] (30 s ping / 10 s pong / 2 misses →
    /// dead-peer detection in ~80 s, idle-connection survival across
    /// the typical 60 s LB / NAT idle drop).
    keepalive: KeepAlive,
}

impl<
    S: 'static + Send + Sync + Storage<Sendable> + core::fmt::Debug,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    Sig: 'static + Send + Sync + Signer<Sendable> + Clone,
    M: 'static + Send + Sync + DepthMetric,
    O: 'static + Send + Sync + Timeout<Sendable> + core::fmt::Debug,
> TokioWebSocketServer<S, P, Sig, M, O>
where
    S::Error: 'static + Send + Sync,
    P::PutDisallowed: Send + 'static,
    P::FetchDisallowed: Send + 'static,
{
    /// Create a new [`TokioWebSocketServer`] serving the given node.
    ///
    /// The node's signer authenticates incoming connections during the
    /// handshake phase. The accept loop and every per-connection task are
    /// registered with the node's tracker and cancelled by its token, so
    /// [`stop_and_drain`](Self::stop_and_drain) — or dropping the server —
    /// tears everything down together. Defaults to [`KeepAlive::balanced`]
    /// on every accepted and dialed connection; use
    /// [`Self::new_with_keepalive`] to override or disable.
    ///
    /// # Arguments
    ///
    /// * `address` - The socket address to bind to
    /// * `handshake_max_drift` - Maximum acceptable clock drift during handshake
    /// * `max_message_size` - Maximum WebSocket message size in bytes
    /// * `node` - The running node to register connections with
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if there is a problem binding the socket.
    pub async fn new(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        node: TokioWebSocketNode<S, P, Sig, O, M>,
    ) -> Result<Self, tungstenite::Error> {
        Self::new_with_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            KeepAlive::balanced(),
            node,
        )
        .await
    }

    /// Like [`new`](Self::new) but with an explicit [`KeepAlive`]
    /// config (e.g. for tests that need aggressive timings).
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if binding the socket fails.
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_keepalive(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        keepalive: KeepAlive,
        node: TokioWebSocketNode<S, P, Sig, O, M>,
    ) -> Result<Self, tungstenite::Error> {
        let server_peer_id = node.peer_id();
        tracing::info!(
            "Starting WebSocket server on {} as {}",
            address,
            server_peer_id
        );
        let tcp_listener = TcpListener::bind(address).await?;
        let assigned_address = tcp_listener.local_addr()?;

        let child_cancellation_token = node.cancellation_token();
        let tasks = node.tracker();

        // Convert optional DiscoveryId to Audience for handshake
        let discovery_audience: Option<Audience> = node.discovery_id().map(Audience::discover_id);

        if discovery_audience.is_some() {
            tracing::info!("Discovery mode enabled");
        }

        let inner_subduction = Arc::clone(node.subduction());
        let accept_loop_tracker = tasks.clone();
        tasks.spawn(async move {
            let mut conns = JoinSet::new();
            loop {
                tokio::select! {
                    () = child_cancellation_token.cancelled() => {
                            tracing::info!("accept loop canceled");
                            break;
                        }
                    res = tcp_listener.accept() => {
                        match res {
                            Ok((tcp, addr)) => {
                                tracing::info!(client = %addr, "new TCP connection");
                                if let Err(e) = tcp.set_nodelay(true) {
                                    tracing::debug!(error = %e, "failed to set TCP_NODELAY");
                                }

                                let task_subduction = inner_subduction.clone();
                                let task_discovery_audience = discovery_audience;
                                // Clone for the spawned task; the outer loop keeps its own
                                // copy for the `cancelled()` arm above.
                                let task_cancel = child_cancellation_token.clone();
                                let task_tracker = accept_loop_tracker.clone();
                                let outer_cancel = task_cancel.clone();
                                let conn_span = tracing::info_span!("ws_connection", client = %addr);
                                conns.spawn(async move {
                                    // `accept_hdr_async_with_config` and `handshake::respond`
                                    // are not cancellation-aware; race against the token so
                                    // a stalled client can't block server teardown.
                                    let handshake_fut = async {
                                        let mut ws_config = WebSocketConfig::default();
                                        ws_config.max_message_size = Some(max_message_size);
                                        ws_config.max_frame_size = Some(max_message_size);

                                        // Step 1: WebSocket protocol upgrade
                                        let ws_stream = match accept_hdr_async_with_config(tcp, NoCallback, Some(ws_config)).await {
                                            Ok(ws) => ws,
                                            Err(e) => {
                                                tracing::error!(error = %e, "WebSocket upgrade error");
                                                return;
                                            }
                                        };

                                        tracing::debug!("WebSocket upgrade complete");

                                        // Step 2: Subduction handshake and connection setup
                                        // Accepts either Audience::Known(peer_id) or discovery audience
                                        let now = TimestampSeconds::now();
                                        let listen_cancel = task_cancel.clone();
                                        let sender_cancel = task_cancel.clone();
                                        let listen_tracker = task_tracker.clone();
                                        let sender_tracker = task_tracker.clone();
                                        let keepalive_tracker = task_tracker.clone();
                                        let keepalive_cancel = task_cancel.clone();
                                        let result = handshake::respond::<Sendable, _, _, _, _>(
                                            WebSocketHandshake::new(ws_stream),
                                            move |ws_handshake, peer_id| {
                                                // Create WebSocket wrapper with verified PeerId
                                                let (ws, sender_fut, keepalive_task) =
                                                    WebSocket::new_with_keepalive(
                                                        ws_handshake.into_inner(),
                                                        peer_id,
                                                        keepalive,
                                                        TokioSleeper,
                                                    );

                                                let listen_ws = ws.clone();
                                                listen_tracker.spawn(async move {
                                                    tokio::select! {
                                                        () = listen_cancel.cancelled() => {
                                                            tracing::debug!("WebSocket listener cancelled");
                                                        }
                                                        result = listen_ws.listen() => {
                                                            if let Err(e) = result {
                                                                tracing::info!(error = %e, "WebSocket listener disconnected");
                                                            }
                                                        }
                                                    }
                                                });

                                                sender_tracker.spawn(async move {
                                                    tokio::select! {
                                                        () = sender_cancel.cancelled() => {
                                                            tracing::debug!("WebSocket sender cancelled");
                                                        }
                                                        result = sender_fut => {
                                                            if let Err(e) = result {
                                                                tracing::info!(error = %e, "WebSocket sender disconnected");
                                                            }
                                                        }
                                                    }
                                                });

                                                let keepalive_fut = keepalive_task.into_future();
                                                keepalive_tracker.spawn(async move {
                                                    tokio::select! {
                                                        () = keepalive_cancel.cancelled() => {
                                                            tracing::debug!("WebSocket keepalive cancelled");
                                                        }
                                                        outcome = keepalive_fut => {
                                                            tracing::debug!(?outcome, "WebSocket keepalive exited");
                                                        }
                                                    }
                                                });

                                                (UnifiedWebSocket::Accepted(ws), ())
                                            },
                                            task_subduction.signer(),
                                            task_subduction.nonce_cache(),
                                            server_peer_id,
                                            task_discovery_audience,
                                            now,
                                            handshake_max_drift,
                                        ).await;

                                        let authenticated = match result {
                                            Ok((auth, ())) => {
                                                tracing::info!(peer = %auth.peer_id(), "handshake complete");
                                                auth
                                            }
                                            Err(e) => {
                                                tracing::warn!(error = %e, "handshake failed");
                                                return;
                                            }
                                        };

                                        // Step 3: Add connection to Subduction
                                        let auth_mt = authenticated.map(MessageTransport::new);
                                        if let Err(e) = task_subduction.add_connection(auth_mt).await {
                                            tracing::error!(error = %e, "failed to add connection");
                                        }
                                    };

                                    tokio::select! {
                                        () = outer_cancel.cancelled() => {
                                            tracing::debug!("per-connection handshake task cancelled (server shutdown)");
                                        }
                                        () = handshake_fut => {}
                                    }
                                }.instrument(conn_span));
                            }
                            Err(e) => tracing::error!(error = %e, "accept error"),
                        }
                    }
                }
            }

            while (conns.join_next().await).is_some() {}
        });

        Ok(Self {
            node,
            address: assigned_address,
            max_message_size,
            keepalive,
        })
    }

    /// Create a new [`TokioWebSocketServer`] with storage and policy.
    ///
    /// This is a convenience method that creates the Subduction instance
    /// and spawns the background tasks. Defaults to [`KeepAlive::balanced`]
    /// keepalive; use [`Self::setup_with_keepalive`] to customise or
    /// disable.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound.
    #[allow(clippy::too_many_arguments)]
    pub async fn setup(
        address: SocketAddr,
        timeout: O,
        handshake_max_drift: Duration,
        max_message_size: usize,
        signer: Sig,
        service_name: Option<&str>,
        storage: S,
        policy: P,
        nonce_cache: NonceCache,
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error>
    where
        M: Clone,
        S: core::fmt::Debug,
    {
        Self::setup_with_keepalive(
            address,
            timeout,
            handshake_max_drift,
            max_message_size,
            KeepAlive::balanced(),
            signer,
            service_name,
            storage,
            policy,
            nonce_cache,
            depth_metric,
        )
        .await
    }

    /// Like [`setup`](Self::setup) but with an explicit [`KeepAlive`]
    /// config.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound.
    #[allow(clippy::too_many_arguments)]
    pub async fn setup_with_keepalive(
        address: SocketAddr,
        timeout: O,
        handshake_max_drift: Duration,
        max_message_size: usize,
        keepalive: KeepAlive,
        signer: Sig,
        service_name: Option<&str>,
        storage: S,
        policy: P,
        nonce_cache: NonceCache,
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error>
    where
        M: Clone,
        S: core::fmt::Debug,
    {
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));

        // The node owns the tracker; its `TrackedTokioSpawn` is handed to
        // the builder so connection readers and dispatch land on the same
        // tracker as the accept loop, and it supervises its own loops.
        let node = TokioSubduction::start(move |spawner| {
            let mut builder = SubductionBuilder::new()
                .signer(signer)
                .storage(storage, Arc::new(policy))
                .spawner(spawner)
                .timer(timeout)
                .nonce_cache(nonce_cache)
                // Seeded so sequences resume above previous values across
                // restarts; see `peer::counter`.
                .send_counter(PeerCounter::with_seed(wall_clock_seed))
                .depth_metric(depth_metric);

            if let Some(id) = discovery_id {
                builder = builder.discovery_id(id);
            }

            let (subduction, _handler, listener_fut, manager_fut) =
                builder.build::<Sendable, MessageTransport<UnifiedWebSocket>>();
            (subduction, listener_fut, manager_fut)
        });

        Self::new_with_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            keepalive,
            node,
        )
        .await
    }

    /// Get the server's peer ID.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.node.peer_id()
    }

    /// Get the server's socket address.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Get a reference to the underlying [`Subduction`] instance.
    #[must_use]
    pub const fn subduction(&self) -> &TokioWebSocketSubduction<S, P, Sig, O, M> {
        self.node.subduction()
    }

    /// The owned node this server is serving.
    #[must_use]
    pub const fn node(&self) -> &TokioWebSocketNode<S, P, Sig, O, M> {
        &self.node
    }

    /// Add an authenticated WebSocket connection to the server.
    ///
    /// The connection must already have completed handshake verification via
    /// [`handshake::initiate`] or [`handshake::respond`].
    ///
    /// Returns `true` if this is a new peer, `false` if already connected.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is rejected by the policy.
    ///
    /// [`handshake::initiate`]: subduction_core::handshake::initiate
    /// [`handshake::respond`]: subduction_core::handshake::respond
    pub async fn add_connection(
        &self,
        authenticated: Authenticated<UnifiedWebSocket, Sendable>,
    ) -> Result<bool, AddConnectionError<P::ConnectionDisallowed>> {
        let auth_mt = authenticated.map(MessageTransport::new);
        self.node.add_connection(auth_mt).await
    }

    /// Connect to a peer and add the connection for bidirectional sync.
    ///
    /// Performs the handshake protocol to authenticate both sides. The client
    /// identity is derived from the signer stored in the Subduction instance.
    ///
    /// # Arguments
    ///
    /// * `uri` - The WebSocket URI to connect to
    /// * `expected_peer_id` - The expected peer ID of the server
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established,
    /// handshake fails, or adding the connection fails.
    pub async fn try_connect(
        &self,
        uri: Uri,
        expected_peer_id: PeerId,
    ) -> Result<PeerId, TryConnectError<P::ConnectionDisallowed>> {
        let uri_str = uri.to_string();
        tracing::info!(uri = %uri_str, "connecting to peer");

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(self.max_message_size);
        ws_config.max_frame_size = Some(self.max_message_size);
        let (ws_stream, _resp) = connect_async_with_config(uri, Some(ws_config))
            .await
            .map_err(TryConnectError::WebSocket)?;

        // Perform handshake
        let audience = Audience::known(expected_peer_id);
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let cancel_token = self.node.cancellation_token();
        let listen_tracker = self.node.tracker();
        let sender_tracker = self.node.tracker();
        let keepalive_tracker = self.node.tracker();
        let listen_uri_str = uri_str.clone();
        let sender_uri_str = uri_str.clone();
        let keepalive_uri_str = uri_str.clone();
        let keepalive = self.keepalive;

        let (authenticated, ()) = handshake::initiate::<Sendable, _, _, _, _>(
            WebSocketHandshake::new(ws_stream),
            move |ws_handshake, peer_id| {
                let (ws, sender_fut, keepalive_task) = WebSocket::new_with_keepalive(
                    ws_handshake.into_inner(),
                    peer_id,
                    keepalive,
                    TokioSleeper,
                );
                let ws_conn = UnifiedWebSocket::Dialed(ws.clone());

                let listen_ws = ws.clone();
                let listener_cancel = cancel_token.clone();
                listen_tracker.spawn(async move {
                    tokio::select! {
                        () = listener_cancel.cancelled() => {
                            tracing::debug!(uri = %listen_uri_str, "shutting down listener");
                        }
                        result = listen_ws.listen() => {
                            if let Err(e) = result {
                                tracing::info!(uri = %listen_uri_str, error = %e, "WebSocket listener disconnected");
                            }
                        }
                    }
                });

                let sender_cancel = cancel_token.clone();
                sender_tracker.spawn(async move {
                    tokio::select! {
                        () = sender_cancel.cancelled() => {
                            tracing::debug!(uri = %sender_uri_str, "shutting down sender");
                        }
                        result = sender_fut => {
                            if let Err(e) = result {
                                tracing::info!(uri = %sender_uri_str, error = %e, "WebSocket sender disconnected");
                            }
                        }
                    }
                });

                let keepalive_cancel = cancel_token;
                let keepalive_fut = keepalive_task.into_future();
                keepalive_tracker.spawn(async move {
                    tokio::select! {
                        () = keepalive_cancel.cancelled() => {
                            tracing::debug!(uri = %keepalive_uri_str, "shutting down keepalive");
                        }
                        outcome = keepalive_fut => {
                            tracing::debug!(uri = %keepalive_uri_str, ?outcome, "keepalive task exited");
                        }
                    }
                });

                (ws_conn, ())
            },
            self.node.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        let server_id = authenticated.peer_id();

        // Verify we connected to the expected peer
        if server_id != expected_peer_id {
            tracing::warn!(
                expected = %expected_peer_id,
                actual = %server_id,
                "server identity mismatch"
            );
            // Continue anyway - the caller specified the expected peer,
            // but the server proved a different identity. This could be
            // legitimate (e.g., load balancer routing to different server).
            // Policy can reject if needed.
        }

        tracing::info!(peer = %server_id, "handshake complete: connected");

        let auth_mt = authenticated.map(MessageTransport::new);
        self.node
            .add_connection(auth_mt)
            .await
            .map_err(TryConnectError::AddConnection)?;

        tracing::info!(uri = %uri_str, "connected to peer");
        Ok(server_id)
    }

    /// Connect to a peer using discovery mode (without knowing their peer ID).
    ///
    /// Uses the service name to authenticate via `Audience::Discover` instead
    /// of requiring the peer's ID upfront. The server's actual peer ID is
    /// returned on success.
    ///
    /// # Arguments
    ///
    /// * `uri` - The WebSocket URI to connect to
    /// * `service_name` - The service name for discovery (e.g., "sync.example.com")
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established,
    /// handshake fails, or adding the connection fails.
    pub async fn try_connect_discover(
        &self,
        uri: Uri,
        service_name: &str,
    ) -> Result<PeerId, TryConnectError<P::ConnectionDisallowed>> {
        let uri_str = uri.to_string();
        tracing::info!(uri = %uri_str, service = %service_name, "connecting to peer via discovery");

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(self.max_message_size);
        ws_config.max_frame_size = Some(self.max_message_size);
        let (ws_stream, _resp) = connect_async_with_config(uri, Some(ws_config))
            .await
            .map_err(TryConnectError::WebSocket)?;

        // Perform handshake with discovery audience
        let audience = Audience::discover(service_name.as_bytes());
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let cancel_token = self.node.cancellation_token();
        let listen_tracker = self.node.tracker();
        let sender_tracker = self.node.tracker();
        let keepalive_tracker = self.node.tracker();
        let listen_uri_str = uri_str.clone();
        let sender_uri_str = uri_str.clone();
        let keepalive_uri_str = uri_str.clone();
        let keepalive = self.keepalive;

        let (authenticated, ()) = handshake::initiate::<Sendable, _, _, _, _>(
            WebSocketHandshake::new(ws_stream),
            move |ws_handshake, peer_id| {
                let (ws, sender_fut, keepalive_task) = WebSocket::new_with_keepalive(
                    ws_handshake.into_inner(),
                    peer_id,
                    keepalive,
                    TokioSleeper,
                );
                let ws_conn = UnifiedWebSocket::Dialed(ws.clone());

                let listen_ws = ws.clone();
                let listener_cancel = cancel_token.clone();
                listen_tracker.spawn(async move {
                    tokio::select! {
                        () = listener_cancel.cancelled() => {
                            tracing::debug!(uri = %listen_uri_str, "shutting down listener");
                        }
                        result = listen_ws.listen() => {
                            if let Err(e) = result {
                                tracing::info!(uri = %listen_uri_str, error = %e, "WebSocket listener disconnected");
                            }
                        }
                    }
                });

                let sender_cancel = cancel_token.clone();
                sender_tracker.spawn(async move {
                    tokio::select! {
                        () = sender_cancel.cancelled() => {
                            tracing::debug!(uri = %sender_uri_str, "shutting down sender");
                        }
                        result = sender_fut => {
                            if let Err(e) = result {
                                tracing::info!(uri = %sender_uri_str, error = %e, "WebSocket sender disconnected");
                            }
                        }
                    }
                });

                let keepalive_cancel = cancel_token;
                let keepalive_fut = keepalive_task.into_future();
                keepalive_tracker.spawn(async move {
                    tokio::select! {
                        () = keepalive_cancel.cancelled() => {
                            tracing::debug!(uri = %keepalive_uri_str, "shutting down keepalive");
                        }
                        outcome = keepalive_fut => {
                            tracing::debug!(uri = %keepalive_uri_str, ?outcome, "keepalive task exited");
                        }
                    }
                });

                (ws_conn, ())
            },
            self.node.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        let server_id = authenticated.peer_id();
        tracing::info!(peer = %server_id, "handshake complete: connected");

        let auth_mt = authenticated.map(MessageTransport::new);
        self.node
            .add_connection(auth_mt)
            .await
            .map_err(TryConnectError::AddConnection)?;

        tracing::info!(uri = %uri_str, "connected to peer");
        Ok(server_id)
    }

    /// Signal graceful shutdown without waiting.
    ///
    /// Delegates to [`TokioSubduction::request_stop`]: the node's loops are
    /// told to exit, the shared token is cancelled (so the accept loop and
    /// per-connection tasks exit via their `select!` arms), and the tracker
    /// is closed. For deterministic teardown that releases every
    /// `Arc<Subduction>` before returning, use
    /// [`stop_and_drain`](Self::stop_and_drain). Idempotent.
    pub fn stop(&mut self) {
        self.node.request_stop();
    }

    /// [`Self::stop`] plus `await` until every task the node owns has exited.
    pub async fn stop_and_drain(&mut self) {
        self.node.stop().await;
    }
}

/// The [`Subduction`] a [`TokioWebSocketServer`] serves.
pub type TokioWebSocketSubduction<S, P, Sig, O, M> = Arc<
    Subduction<
        'static,
        Sendable,
        S,
        MessageTransport<UnifiedWebSocket>,
        SyncHandler<Sendable, S, MessageTransport<UnifiedWebSocket>, P, M, TrackedTokioSpawn>,
        P,
        Sig,
        O,
        TrackedTokioSpawn,
        M,
    >,
>;

/// The owned node a [`TokioWebSocketServer`] serves. Build one with
/// [`TokioSubduction::start`] and a `SubductionBuilder` whose connection
/// type is `MessageTransport<UnifiedWebSocket>`, or let
/// [`TokioWebSocketServer::setup`] do it.
pub type TokioWebSocketNode<S, P, Sig, O, M> = TokioSubduction<
    S,
    MessageTransport<UnifiedWebSocket>,
    SyncHandler<Sendable, S, MessageTransport<UnifiedWebSocket>, P, M, TrackedTokioSpawn>,
    P,
    Sig,
    O,
    M,
>;

/// Error type for connecting to a peer.
#[derive(Debug, thiserror::Error)]
pub enum TryConnectError<E: core::error::Error> {
    /// WebSocket connection error.
    #[error("WebSocket connection error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Handshake failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] AuthenticateError<WebSocketHandshakeError>),

    /// Adding the connection failed.
    #[error("add connection error: {0}")]
    AddConnection(#[from] AddConnectionError<E>),
}
