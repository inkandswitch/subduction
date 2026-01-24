//! # Subduction WebSocket server for Tokio

use crate::{
    timeout::{FuturesTimerTimeout, Timeout},
    tokio::unified::UnifiedWebSocket,
    websocket::WebSocket,
    MAX_MESSAGE_SIZE,
};

use alloc::{string::ToString, sync::Arc};
use async_tungstenite::tokio::{accept_hdr_async_with_config, connect_async_with_config};
use core::{net::SocketAddr, time::Duration};
use futures_kind::Sendable;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, id::SedimentreeId, sedimentree::Sedimentree,
    storage::Storage,
};
use subduction_core::{
    connection::id::ConnectionId,
    peer::id::PeerId,
    policy::{ConnectionPolicy, StoragePolicy},
    sharded_map::ShardedMap,
    subduction::error::RegistrationError,
    Subduction,
};

use crate::tokio::TokioSpawn;
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::{handshake::server::NoCallback, http::Uri, protocol::WebSocketConfig};

/// Error type for connecting to a peer.
#[derive(Debug, thiserror::Error)]
pub enum ConnectToPeerError<E: core::error::Error> {
    /// WebSocket connection error.
    #[error("WebSocket connection error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Registration error.
    #[error("Registration error: {0}")]
    Registration(#[from] RegistrationError<E>),
}

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable>,
    P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
    M: 'static + Send + Sync + DepthMetric = CountLeadingZeroBytes,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone = FuturesTimerTimeout,
> where
    S::Error: 'static + Send + Sync,
{
    subduction: TokioWebSocketSubduction<S, P, O, M>,
    server_peer_id: PeerId,
    address: SocketAddr,
    accept_task: Arc<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl<
        S: 'static + Send + Sync + Storage<Sendable>,
        P: 'static + Send + Sync + ConnectionPolicy<Sendable> + StoragePolicy<Sendable>,
        M: 'static + Send + Sync + DepthMetric,
        O: 'static + Send + Sync + Timeout<Sendable> + Clone,
    > TokioWebSocketServer<S, P, M, O>
where
    S::Error: 'static + Send + Sync,
{
    /// Create a new [`WebSocketServer`] to manage connections to a [`Subduction`].
    ///
    /// # Errors
    ///
    /// Returns [`tungstenite::Error`] if there is a problem directly with the WebSocket system.
    pub async fn new(
        address: SocketAddr,
        timeout: O,
        default_time_limit: Duration,
        server_peer_id: PeerId,
        subduction: TokioWebSocketSubduction<S, P, O, M>,
    ) -> Result<Self, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {}", address);
        let tcp_listener = TcpListener::bind(address).await?;
        let assigned_address = tcp_listener.local_addr()?;

        let cancellation_token = CancellationToken::new();
        let child_cancellation_token = cancellation_token.child_token();

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

                                // FIXME HACK: this will be replaced with a pubkey
                                let client_digest = {
                                    let mut hasher = blake3::Hasher::new();
                                    hasher.update(addr.ip().to_string().as_bytes());
                                    hasher.update(addr.port().to_le_bytes().as_ref());
                                    *hasher.finalize().as_bytes()
                                };
                                let client_id = PeerId::new(client_digest);

                                let task_subduction = inner_subduction.clone();
                                conns.spawn({
                                    let tout = timeout.clone();
                                    async move {
                                        let mut ws_config = WebSocketConfig::default();
                                        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
                                        match accept_hdr_async_with_config(tcp, NoCallback, Some(ws_config)).await {
                                            Ok(hs) => {
                                                let ws_conn = UnifiedWebSocket::Accepted(WebSocket::new(
                                                    hs,
                                                    tout,
                                                    default_time_limit,
                                                    client_id,
                                                ));

                                                tracing::info!("WebSocket handshake upgraded {addr}");

                                                let listen_ws = ws_conn.clone();
                                                tokio::spawn(async move {
                                                    if let Err(e) = listen_ws.listen().await {
                                                        tracing::error!("WebSocket listen error: {}", e);
                                                    }
                                                });

                                                if let Err(e) = task_subduction.register(ws_conn).await {
                                                    tracing::error!("failed to register new connection: {}", e);
                                                }
                                            },
                                            Err(e) => {
                                                tracing::error!("WebSocket handshake error from {addr}: {}", e);
                                            },
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
            server_peer_id,
            address: assigned_address,
            subduction,
            accept_task: Arc::new(accept_task),
            cancellation_token,
        })
    }

    /// Create a new [`WebSocketServer`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound,
    /// or if the connection could not be established.
    pub async fn setup(
        address: SocketAddr,
        timeout: O,
        default_time_limit: Duration,
        server_peer_id: PeerId,
        storage: S,
        policy: P,
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error> {
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree> = ShardedMap::new();
        let (subduction, listener_fut, manager_fut) =
            Subduction::new(storage, policy, depth_metric, sedimentrees, TokioSpawn);

        let server = Self::new(
            address,
            timeout,
            default_time_limit,
            server_peer_id,
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
    pub const fn peer_id(&self) -> PeerId {
        self.server_peer_id
    }

    /// Get the server's socket address.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }

    /// Register a new WebSocket connection with the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration message send fails over the internal channel.
    pub async fn register(
        &self,
        ws: UnifiedWebSocket<O>,
    ) -> Result<(bool, ConnectionId), RegistrationError<P::ConnectionDisallowed>> {
        self.subduction.register(ws).await
    }

    /// Connect to a peer and register the connection for bidirectional sync.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection could not be established or registered.
    pub async fn connect_to_peer(
        &self,
        uri: Uri,
        timeout: O,
        default_time_limit: Duration,
        peer_id: PeerId,
    ) -> Result<ConnectionId, ConnectToPeerError<P::ConnectionDisallowed>> {
        let uri_str = uri.to_string();
        tracing::info!("Connecting to peer at {uri_str}");

        let mut ws_config = WebSocketConfig::default();
        ws_config.max_message_size = Some(MAX_MESSAGE_SIZE);
        let (ws_stream, _resp) = connect_async_with_config(uri, Some(ws_config))
            .await
            .map_err(ConnectToPeerError::WebSocket)?;

        let ws_conn = UnifiedWebSocket::Dialed(WebSocket::new(
            ws_stream,
            timeout,
            default_time_limit,
            peer_id,
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

        let (_is_new, conn_id) = self
            .subduction
            .register(ws_conn)
            .await
            .map_err(ConnectToPeerError::Registration)?;

        tracing::info!("Connected to peer at {uri_str} with connection ID {conn_id:?}");
        Ok(conn_id)
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort();
    }
}

type TokioWebSocketSubduction<S, P, O, M> =
    Arc<Subduction<'static, Sendable, S, UnifiedWebSocket<O>, P, M>>;
