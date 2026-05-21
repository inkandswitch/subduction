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
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    subduction::{Subduction, builder::SubductionBuilder, error::AddConnectionError},
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::{nonce::Nonce, signer::Signer};

use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tungstenite::{handshake::server::NoCallback, http::Uri, protocol::WebSocketConfig};

// NOTE: `O: Timeout<Sendable>` remains on the server type because
// `Subduction` / `SubductionBuilder` still require a timer parameter,
// even though WebSocket itself no longer stores it.

/// A Tokio-flavoured [`WebSocket`] server implementation.
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
    subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
    address: SocketAddr,
    cancellation_token: CancellationToken,
    /// Tracks the accept loop, the per-connection WebSocket
    /// listener/sender, and (in [`Self::setup`]) the [`Subduction`]
    /// listener/manager futures. [`Self::stop_and_drain`] awaits all
    /// of them.
    tasks: TaskTracker,
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

impl<S, P, Sig, M, O> Clone for TokioWebSocketServer<S, P, Sig, M, O>
where
    S: 'static + Send + Sync + Storage<Sendable> + core::fmt::Debug,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    P::PutDisallowed: Send + 'static,
    P::FetchDisallowed: Send + 'static,
    Sig: 'static + Send + Sync + Signer<Sendable>,
    M: 'static + Send + Sync + DepthMetric,
    O: 'static + Send + Sync + Timeout<Sendable> + core::fmt::Debug,
    S::Error: 'static + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            subduction: self.subduction.clone(),
            address: self.address,
            cancellation_token: self.cancellation_token.clone(),
            tasks: self.tasks.clone(),
            max_message_size: self.max_message_size,
            keepalive: self.keepalive,
        }
    }
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
    /// Create a new [`TokioWebSocketServer`] to manage connections to a [`Subduction`].
    ///
    /// The signer from the Subduction instance is used to authenticate incoming
    /// connections during the handshake phase. Defaults to
    /// [`KeepAlive::balanced`] keepalive on every accepted and dialed
    /// connection; use [`Self::new_with_keepalive`] to override or disable.
    ///
    /// # Arguments
    ///
    /// * `address` - The socket address to bind to
    /// * `handshake_max_drift` - Maximum acceptable clock drift during handshake
    /// * `max_message_size` - Maximum WebSocket message size in bytes
    /// * `subduction` - The Subduction instance to register connections with
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if there is a problem binding the socket.
    pub async fn new(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
    ) -> Result<Self, tungstenite::Error> {
        Self::new_with_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            KeepAlive::balanced(),
            subduction,
            TaskTracker::new(),
        )
        .await
    }

    /// Like [`new`](Self::new) but with an explicit [`KeepAlive`]
    /// config (e.g. for tests that need aggressive timings).
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if binding the socket fails.
    pub async fn new_with_keepalive(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        keepalive: KeepAlive,
        subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
        tasks: TaskTracker,
    ) -> Result<Self, tungstenite::Error> {
        Self::new_with_tracker_and_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            keepalive,
            subduction,
            tasks,
        )
        .await
    }

    /// Like [`new`](Self::new) but uses a caller-supplied [`TaskTracker`].
    /// Pass the same tracker to the [`Subduction`] builder (via
    /// [`TrackedTokioSpawn`][crate::tokio::TrackedTokioSpawn]) and a single
    /// [`Self::stop_and_drain`] will await every server- and Subduction-side
    /// task — including per-peer `connection_loop`s.
    ///
    /// Equivalent to [`Self::new_with_tracker_and_keepalive`] with
    /// `keepalive = Some(KeepAlive::balanced())`.
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if binding the socket fails.
    pub async fn new_with_tracker(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
        tasks: TaskTracker,
    ) -> Result<Self, tungstenite::Error> {
        Self::new_with_tracker_and_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            KeepAlive::balanced(),
            subduction,
            tasks,
        )
        .await
    }

    /// Like [`new_with_tracker`](Self::new_with_tracker) but with explicit
    /// keepalive control.
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if binding the socket fails.
    #[allow(clippy::too_many_lines)]
    pub async fn new_with_tracker_and_keepalive(
        address: SocketAddr,
        handshake_max_drift: Duration,
        max_message_size: usize,
        keepalive: KeepAlive,
        subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
        tasks: TaskTracker,
    ) -> Result<Self, tungstenite::Error> {
        let server_peer_id = subduction.peer_id();
        tracing::info!(
            "Starting WebSocket server on {} as {}",
            address,
            server_peer_id
        );
        let tcp_listener = TcpListener::bind(address).await?;
        let assigned_address = tcp_listener.local_addr()?;

        let cancellation_token = CancellationToken::new();
        let child_cancellation_token = cancellation_token.child_token();

        // Convert optional DiscoveryId to Audience for handshake
        let discovery_audience: Option<Audience> =
            subduction.discovery_id().map(Audience::discover_id);

        if discovery_audience.is_some() {
            tracing::info!("Discovery mode enabled");
        }

        let inner_subduction = subduction.clone();
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
                                tracing::info!("new TCP connection from {addr}");

                                let task_subduction = inner_subduction.clone();
                                let task_discovery_audience = discovery_audience;
                                // Clone for the spawned task; the outer loop keeps its own
                                // copy for the `cancelled()` arm above.
                                let task_cancel = child_cancellation_token.clone();
                                let task_tracker = accept_loop_tracker.clone();
                                let outer_cancel = task_cancel.clone();
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
                                                tracing::error!("WebSocket upgrade error from {addr}: {e}");
                                                return;
                                            }
                                        };

                                        tracing::debug!("WebSocket upgrade complete for {addr}");

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
                                                                tracing::info!("WebSocket listener disconnected: {e}");
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
                                                                tracing::info!("WebSocket sender disconnected: {e}");
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
                                                tracing::info!(
                                                    "Handshake complete: client {} from {addr}",
                                                    auth.peer_id()
                                                );
                                                auth
                                            }
                                            Err(e) => {
                                                tracing::warn!("Handshake failed from {addr}: {e}");
                                                return;
                                            }
                                        };

                                        // Step 3: Add connection to Subduction
                                        let auth_mt = authenticated.map(MessageTransport::new);
                                        if let Err(e) = task_subduction.add_connection(auth_mt).await {
                                            tracing::error!("Failed to add connection: {e}");
                                        }
                                    };

                                    tokio::select! {
                                        () = outer_cancel.cancelled() => {
                                            tracing::debug!("per-connection handshake task cancelled (server shutdown)");
                                        }
                                        () = handshake_fut => {}
                                    }
                                });
                            }
                            Err(e) => tracing::error!("Accept error: {e}"),
                        }
                    }
                }
            }

            while (conns.join_next().await).is_some() {}
        });

        Ok(Self {
            address: assigned_address,
            subduction,
            cancellation_token,
            tasks,
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

        // Shared tracker: Subduction's `connection_loop`s spawn via
        // `TrackedTokioSpawn`; the server's accept loop and per-WS tasks
        // spawn directly. `stop_and_drain` awaits all of them.
        let tasks = TaskTracker::new();
        let spawner = crate::tokio::TrackedTokioSpawn::new(tasks.clone());

        let mut builder = SubductionBuilder::new()
            .signer(signer)
            .storage(storage, Arc::new(policy))
            .spawner(spawner)
            .timer(timeout.clone())
            .nonce_cache(nonce_cache)
            .depth_metric(depth_metric);

        if let Some(id) = discovery_id {
            builder = builder.discovery_id(id);
        }

        let (subduction, _handler, listener_fut, manager_fut) =
            builder.build::<Sendable, MessageTransport<UnifiedWebSocket>>();

        let server = Self::new_with_tracker_and_keepalive(
            address,
            handshake_max_drift,
            max_message_size,
            keepalive,
            subduction,
            tasks,
        )
        .await?;

        // Spawned directly (no `select!`-against-token wrapper): the
        // token race always loses to `cancel()`, dropping `manager_fut`
        // before its `connection_loop`-abort cleanup can run.
        // `stop_and_drain` instead calls `subduction.shutdown()` first
        // so both futures exit via their channel-close paths.
        server.tasks.spawn(async move {
            let _ = manager_fut.await;
        });
        server.tasks.spawn(async move {
            let _ = listener_fut.await;
        });

        Ok(server)
    }

    /// Get the server's peer ID.
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        self.subduction.peer_id()
    }

    /// Get the server's socket address.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Get a reference to the underlying [`Subduction`] instance.
    #[must_use]
    pub const fn subduction(&self) -> &TokioWebSocketSubduction<S, P, Sig, O, M> {
        &self.subduction
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
        self.subduction.add_connection(auth_mt).await
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
        tracing::info!("Connecting to peer at {uri_str}");

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

        let cancel_token = self.cancellation_token.clone();
        let listen_tracker = self.tasks.clone();
        let sender_tracker = self.tasks.clone();
        let keepalive_tracker = self.tasks.clone();
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
                            tracing::debug!("Shutting down listener for peer {listen_uri_str}");
                        }
                        result = listen_ws.listen() => {
                            if let Err(e) = result {
                                tracing::info!("WebSocket listener disconnected for peer {listen_uri_str}: {e}");
                            }
                        }
                    }
                });

                let sender_cancel = cancel_token.clone();
                sender_tracker.spawn(async move {
                    tokio::select! {
                        () = sender_cancel.cancelled() => {
                            tracing::debug!("Shutting down sender for peer {sender_uri_str}");
                        }
                        result = sender_fut => {
                            if let Err(e) = result {
                                tracing::info!("WebSocket sender disconnected for peer {sender_uri_str}: {e}");
                            }
                        }
                    }
                });

                let keepalive_cancel = cancel_token;
                let keepalive_fut = keepalive_task.into_future();
                keepalive_tracker.spawn(async move {
                    tokio::select! {
                        () = keepalive_cancel.cancelled() => {
                            tracing::debug!("Shutting down keepalive for peer {keepalive_uri_str}");
                        }
                        outcome = keepalive_fut => {
                            tracing::debug!(?outcome, "keepalive task for peer {keepalive_uri_str} exited");
                        }
                    }
                });

                (ws_conn, ())
            },
            self.subduction.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        let server_id = authenticated.peer_id();

        // Verify we connected to the expected peer
        if server_id != expected_peer_id {
            tracing::warn!(
                "Server identity mismatch: expected {}, got {}",
                expected_peer_id,
                server_id
            );
            // Continue anyway - the caller specified the expected peer,
            // but the server proved a different identity. This could be
            // legitimate (e.g., load balancer routing to different server).
            // Policy can reject if needed.
        }

        tracing::info!("Handshake complete: connected to {server_id}");

        let auth_mt = authenticated.map(MessageTransport::new);
        self.subduction
            .add_connection(auth_mt)
            .await
            .map_err(TryConnectError::AddConnection)?;

        tracing::info!("Connected to peer at {uri_str}");
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
        tracing::info!("Connecting to peer at {uri_str} via discovery ({service_name})");

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

        let cancel_token = self.cancellation_token.clone();
        let listen_tracker = self.tasks.clone();
        let sender_tracker = self.tasks.clone();
        let keepalive_tracker = self.tasks.clone();
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
                            tracing::debug!("Shutting down listener for peer {listen_uri_str}");
                        }
                        result = listen_ws.listen() => {
                            if let Err(e) = result {
                                tracing::info!("WebSocket listener disconnected for peer {listen_uri_str}: {e}");
                            }
                        }
                    }
                });

                let sender_cancel = cancel_token.clone();
                sender_tracker.spawn(async move {
                    tokio::select! {
                        () = sender_cancel.cancelled() => {
                            tracing::debug!("Shutting down sender for peer {sender_uri_str}");
                        }
                        result = sender_fut => {
                            if let Err(e) = result {
                                tracing::info!("WebSocket sender disconnected for peer {sender_uri_str}: {e}");
                            }
                        }
                    }
                });

                let keepalive_cancel = cancel_token;
                let keepalive_fut = keepalive_task.into_future();
                keepalive_tracker.spawn(async move {
                    tokio::select! {
                        () = keepalive_cancel.cancelled() => {
                            tracing::debug!("Shutting down keepalive for peer {keepalive_uri_str}");
                        }
                        outcome = keepalive_fut => {
                            tracing::debug!(?outcome, "keepalive task for peer {keepalive_uri_str} exited");
                        }
                    }
                });

                (ws_conn, ())
            },
            self.subduction.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        let server_id = authenticated.peer_id();
        tracing::info!("Handshake complete: connected to {server_id}");

        let auth_mt = authenticated.map(MessageTransport::new);
        self.subduction
            .add_connection(auth_mt)
            .await
            .map_err(TryConnectError::AddConnection)?;

        tracing::info!("Connected to peer at {uri_str}");
        Ok(server_id)
    }

    /// Signal graceful shutdown without waiting. Closes the Subduction
    /// channels (so the listener/manager exit on their next poll),
    /// cancels the [`CancellationToken`] (so the accept loop and
    /// per-connection tasks exit via their `select!` arms), and closes
    /// the [`TaskTracker`] (so [`stop_and_drain`](Self::stop_and_drain)
    /// can `wait` afterwards). For deterministic teardown that releases
    /// every `Arc<Subduction>` before returning, use `stop_and_drain`.
    ///
    /// Order is load-bearing: `subduction.shutdown()` must run before
    /// `cancel()`, otherwise `manager_fut` is dropped mid-execution
    /// (before its `connection_loop`-abort cleanup runs) and any
    /// later `tracker.wait()` deadlocks on parked `connection_loop`s.
    ///
    /// Idempotent.
    pub fn stop(&mut self) {
        self.subduction.shutdown();
        self.cancellation_token.cancel();
        self.tasks.close();
    }

    /// [`Self::stop`] plus `await` every tracked task.
    pub async fn stop_and_drain(&mut self) {
        self.stop();
        self.tasks.wait().await;
    }
}

type TokioWebSocketSubduction<S, P, Sig, O, M> = Arc<
    Subduction<
        'static,
        Sendable,
        S,
        MessageTransport<UnifiedWebSocket>,
        SyncHandler<Sendable, S, MessageTransport<UnifiedWebSocket>, P, M>,
        P,
        Sig,
        O,
        M,
    >,
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
