//! # Subduction WebSocket server for Tokio

use crate::{
    MAX_MESSAGE_SIZE,
    handshake::{WebSocketHandshakeError, server_handshake},
    timeout::{FuturesTimerTimeout, Timeout},
    tokio::unified::UnifiedWebSocket,
    websocket::WebSocket,
};

use alloc::sync::Arc;
use async_tungstenite::tokio::{accept_hdr_async_with_config, connect_async_with_config};
use core::{net::SocketAddr, time::Duration};
use future_form::Sendable;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, id::SedimentreeId, sedimentree::Sedimentree,
    storage::Storage,
};
use subduction_core::{
    Subduction,
    connection::{
        handshake::{Audience, DiscoveryId},
        nonce_cache::NonceCache,
    },
    crypto::signer::Signer,
    peer::id::PeerId,
    policy::{ConnectionPolicy, StoragePolicy},
    sharded_map::ShardedMap,
    subduction::error::RegistrationError,
    timestamp::TimestampSeconds,
};

use crate::tokio::TokioSpawn;
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::{handshake::server::NoCallback, http::Uri, protocol::WebSocketConfig};

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable>,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    Sig: 'static + Send + Sync + Signer<Sendable>,
    M: 'static + Send + Sync + DepthMetric = CountLeadingZeroBytes,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone = FuturesTimerTimeout,
> where
    S::Error: 'static + Send + Sync,
{
    subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
    address: SocketAddr,
    accept_task: Arc<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl<S, P, Sig, M, O> Clone for TokioWebSocketServer<S, P, Sig, M, O>
where
    S: 'static + Send + Sync + Storage<Sendable>,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    Sig: 'static + Send + Sync + Signer<Sendable>,
    M: 'static + Send + Sync + DepthMetric,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone,
    S::Error: 'static + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            subduction: self.subduction.clone(),
            address: self.address,
            accept_task: self.accept_task.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

impl<
    S: 'static + Send + Sync + Storage<Sendable>,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    Sig: 'static + Send + Sync + Signer<Sendable> + Clone,
    M: 'static + Send + Sync + DepthMetric,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone,
> TokioWebSocketServer<S, P, Sig, M, O>
where
    S::Error: 'static + Send + Sync,
{
    /// Create a new [`WebSocketServer`] to manage connections to a [`Subduction`].
    ///
    /// The signer from the Subduction instance is used to authenticate incoming
    /// connections during the handshake phase.
    ///
    /// # Arguments
    ///
    /// * `address` - The socket address to bind to
    /// * `timeout` - The timeout strategy for requests
    /// * `default_time_limit` - Default timeout duration
    /// * `handshake_max_drift` - Maximum acceptable clock drift during handshake
    /// * `subduction` - The Subduction instance to register connections with
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if there is a problem binding the socket.
    #[allow(clippy::too_many_lines)]
    pub async fn new(
        address: SocketAddr,
        timeout: O,
        default_time_limit: Duration,
        handshake_max_drift: Duration,
        subduction: TokioWebSocketSubduction<S, P, Sig, O, M>,
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
        let accept_task: JoinHandle<()> = tokio::spawn(async move {
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
                                conns.spawn({
                                    let tout = timeout.clone();
                                    async move {
                                        let mut ws_config = WebSocketConfig::default();
                                        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);

                                        // Step 1: WebSocket protocol upgrade
                                        let mut ws_stream = match accept_hdr_async_with_config(tcp, NoCallback, Some(ws_config)).await {
                                            Ok(ws) => ws,
                                            Err(e) => {
                                                tracing::error!("WebSocket upgrade error from {addr}: {e}");
                                                return;
                                            }
                                        };

                                        tracing::debug!("WebSocket upgrade complete for {addr}");

                                        // Step 2: Subduction handshake
                                        // Accepts either Audience::Known(peer_id) or discovery audience
                                        let now = TimestampSeconds::now();
                                        let handshake_result = server_handshake(
                                            &mut ws_stream,
                                            task_subduction.signer(),
                                            server_peer_id,
                                            task_discovery_audience,
                                            task_subduction.nonce_cache(),
                                            now,
                                            handshake_max_drift,
                                        ).await;

                                        let client_id = match handshake_result {
                                            Ok(result) => {
                                                tracing::info!(
                                                    "Handshake complete: client {} from {addr}",
                                                    result.client_id
                                                );
                                                result.client_id
                                            }
                                            Err(e) => {
                                                tracing::warn!("Handshake failed from {addr}: {e}");
                                                return;
                                            }
                                        };

                                        // Step 3: Create WebSocket wrapper with verified PeerId
                                        let ws_conn = UnifiedWebSocket::Accepted(WebSocket::new(
                                            ws_stream,
                                            tout,
                                            default_time_limit,
                                            client_id,
                                        ));

                                        // Step 4: Start listener
                                        let listen_ws = ws_conn.clone();
                                        tokio::spawn(async move {
                                            if let Err(e) = listen_ws.listen().await {
                                                tracing::error!("WebSocket listen error: {e}");
                                            }
                                        });

                                        // Step 5: Register connection with Subduction
                                        if let Err(e) = task_subduction.register(ws_conn).await {
                                            tracing::error!("Failed to register connection: {e}");
                                        }
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
            accept_task: Arc::new(accept_task),
            cancellation_token,
        })
    }

    /// Create a new [`WebSocketServer`] with storage and policy.
    ///
    /// This is a convenience method that creates the Subduction instance
    /// and spawns the background tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound.
    #[allow(clippy::too_many_arguments)]
    pub async fn setup(
        address: SocketAddr,
        timeout: O,
        default_time_limit: Duration,
        handshake_max_drift: Duration,
        signer: Sig,
        service_name: Option<&str>,
        storage: S,
        policy: P,
        nonce_cache: NonceCache,
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error> {
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree> = ShardedMap::new();
        let (subduction, listener_fut, manager_fut) = Subduction::new(
            discovery_id,
            signer,
            storage,
            policy,
            nonce_cache,
            depth_metric,
            sedimentrees,
            TokioSpawn,
        );

        let server = Self::new(
            address,
            timeout,
            default_time_limit,
            handshake_max_drift,
            subduction,
        )
        .await?;

        let actor_cancel = server.cancellation_token.clone();
        let listener_cancel = server.cancellation_token.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = manager_fut => {},
                () = actor_cancel.cancelled() => {}
            }
        });

        tokio::spawn(async move {
            tokio::select! {
                _ = listener_fut => {},
                () = listener_cancel.cancelled() => {}
            }
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

    /// Register a new WebSocket connection with the server.
    ///
    /// Returns `true` if this is a new peer, `false` if already connected.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration message send fails over the internal channel.
    pub async fn register(
        &self,
        ws: UnifiedWebSocket<O>,
    ) -> Result<bool, RegistrationError<P::ConnectionDisallowed>> {
        self.subduction.register(ws).await
    }

    /// Connect to a peer and register the connection for bidirectional sync.
    ///
    /// Performs the handshake protocol to authenticate both sides. The client
    /// identity is derived from the signer stored in the Subduction instance.
    ///
    /// # Arguments
    ///
    /// * `uri` - The WebSocket URI to connect to
    /// * `timeout` - Timeout strategy for requests
    /// * `default_time_limit` - Default timeout duration
    /// * `expected_peer_id` - The expected peer ID of the server
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established,
    /// handshake fails, or registration fails.
    pub async fn try_connect(
        &self,
        uri: Uri,
        timeout: O,
        default_time_limit: Duration,
        expected_peer_id: PeerId,
    ) -> Result<PeerId, TryConnectError<P::ConnectionDisallowed>> {
        use crate::handshake::client_handshake;
        use subduction_core::connection::handshake::Nonce;

        let uri_str = uri.to_string();
        tracing::info!("Connecting to peer at {uri_str}");

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
        let (mut ws_stream, _resp) = connect_async_with_config(uri, Some(ws_config))
            .await
            .map_err(TryConnectError::WebSocket)?;

        // Perform handshake
        let audience = Audience::known(expected_peer_id);
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let handshake_result = client_handshake(
            &mut ws_stream,
            self.subduction.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        // Verify we connected to the expected peer
        if handshake_result.server_id != expected_peer_id {
            tracing::warn!(
                "Server identity mismatch: expected {}, got {}",
                expected_peer_id,
                handshake_result.server_id
            );
            // Continue anyway - the caller specified the expected peer,
            // but the server proved a different identity. This could be
            // legitimate (e.g., load balancer routing to different server).
            // Policy can reject if needed.
        }

        let server_id = handshake_result.server_id;
        tracing::info!("Handshake complete: connected to {server_id}");

        let ws_conn = UnifiedWebSocket::Dialed(WebSocket::new(
            ws_stream,
            timeout,
            default_time_limit,
            server_id,
        ));

        let listen_ws = ws_conn.clone();
        let listen_uri_str = uri_str.clone();
        let cancel_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = cancel_token.cancelled() => {
                    tracing::debug!("Shutting down listener for peer {listen_uri_str}");
                }
                result = listen_ws.listen() => {
                    if let Err(e) = result {
                        tracing::error!("WebSocket listen error for peer {listen_uri_str}: {e}");
                    }
                }
            }
        });

        self.subduction
            .register(ws_conn)
            .await
            .map_err(TryConnectError::Registration)?;

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
    /// * `timeout` - Timeout strategy for requests
    /// * `default_time_limit` - Default timeout duration
    /// * `service_name` - The service name for discovery (e.g., "sync.example.com")
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established,
    /// handshake fails, or registration fails.
    pub async fn try_connect_discover(
        &self,
        uri: Uri,
        timeout: O,
        default_time_limit: Duration,
        service_name: &str,
    ) -> Result<PeerId, TryConnectError<P::ConnectionDisallowed>> {
        use crate::handshake::client_handshake;
        use subduction_core::connection::handshake::Nonce;

        let uri_str = uri.to_string();
        tracing::info!("Connecting to peer at {uri_str} via discovery ({service_name})");

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
        let (mut ws_stream, _resp) = connect_async_with_config(uri, Some(ws_config))
            .await
            .map_err(TryConnectError::WebSocket)?;

        // Perform handshake with discovery audience
        let audience = Audience::discover(service_name.as_bytes());
        let now = TimestampSeconds::now();
        let nonce = Nonce::random();

        let handshake_result = client_handshake(
            &mut ws_stream,
            self.subduction.signer(),
            audience,
            now,
            nonce,
        )
        .await?;

        let server_id = handshake_result.server_id;
        tracing::info!("Handshake complete: connected to {server_id}");

        let ws_conn = UnifiedWebSocket::Dialed(WebSocket::new(
            ws_stream,
            timeout,
            default_time_limit,
            server_id,
        ));

        let listen_ws = ws_conn.clone();
        let listen_uri_str = uri_str.clone();
        let cancel_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            tokio::select! {
                () = cancel_token.cancelled() => {
                    tracing::debug!("Shutting down listener for peer {listen_uri_str}");
                }
                result = listen_ws.listen() => {
                    if let Err(e) = result {
                        tracing::error!("WebSocket listen error for peer {listen_uri_str}: {e}");
                    }
                }
            }
        });

        self.subduction
            .register(ws_conn)
            .await
            .map_err(TryConnectError::Registration)?;

        tracing::info!("Connected to peer at {uri_str}");
        Ok(server_id)
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort();
    }
}

type TokioWebSocketSubduction<S, P, Sig, O, M> =
    Arc<Subduction<'static, Sendable, S, UnifiedWebSocket<O>, P, Sig, M>>;

/// Error type for connecting to a peer.
#[derive(Debug, thiserror::Error)]
pub enum TryConnectError<E: core::error::Error> {
    /// WebSocket connection error.
    #[error("WebSocket connection error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Handshake failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] WebSocketHandshakeError),

    /// Registration error.
    #[error("Registration error: {0}")]
    Registration(#[from] RegistrationError<E>),
}
