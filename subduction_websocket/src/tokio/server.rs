//! # Subduction WebSocket server for Tokio

use crate::{
    timeout::{FuturesTimerTimeout, Timeout},
    tokio::unified::UnifiedWebSocket,
    websocket::WebSocket,
};

use alloc::{string::ToString, sync::Arc};
use async_tungstenite::tokio::{accept_hdr_async, connect_async};
use core::{net::SocketAddr, time::Duration};
use futures_kind::Sendable;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, id::SedimentreeId, sedimentree::Sedimentree,
    storage::Storage,
};
use subduction_core::{
    connection::id::ConnectionId, peer::id::PeerId, sharded_map::ShardedMap,
    subduction::error::RegistrationError, Subduction,
};
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::{handshake::server::NoCallback, http::Uri};

/// Error type for connecting to a peer.
#[derive(Debug, thiserror::Error)]
pub enum ConnectToPeerError {
    /// WebSocket connection error.
    #[error("WebSocket connection error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Registration error.
    #[error("Registration error: {0}")]
    Registration(#[from] RegistrationError),
}

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable>,
    M: 'static + Send + Sync + DepthMetric = CountLeadingZeroBytes,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone = FuturesTimerTimeout,
> where
    S::Error: 'static + Send + Sync,
{
    subduction: TokioWebSocketSubduction<S, O, M>,
    server_peer_id: PeerId,
    address: SocketAddr,
    accept_task: Arc<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl<
        S: 'static + Send + Sync + Storage<Sendable>,
        M: 'static + Send + Sync + DepthMetric,
        O: 'static + Send + Sync + Timeout<Sendable> + Clone,
    > TokioWebSocketServer<S, M, O>
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
        subduction: TokioWebSocketSubduction<S, O, M>,
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
                                        match accept_hdr_async(tcp, NoCallback).await {
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
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error> {
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree> = ShardedMap::new();
        let (subduction, listener_fut, actor_fut) =
            Subduction::new(storage, depth_metric, sedimentrees);

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
                _ = actor_fut => {},
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
    ) -> Result<(bool, ConnectionId), RegistrationError> {
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
    ) -> Result<ConnectionId, ConnectToPeerError> {
        tracing::info!("Connecting to peer at {uri}");

        let (ws_stream, _resp) = connect_async(uri.clone())
            .await
            .map_err(ConnectToPeerError::WebSocket)?;

        let ws_conn = UnifiedWebSocket::Dialed(WebSocket::new(
            ws_stream,
            timeout,
            default_time_limit,
            peer_id,
        ));

        // Start the listener for this connection
        let listen_ws = ws_conn.clone();
        let listen_uri = uri.clone();
        tokio::spawn(async move {
            if let Err(e) = listen_ws.listen().await {
                tracing::error!("WebSocket listen error for peer {}: {}", listen_uri, e);
            }
        });

        // Register with Subduction for sync
        let (_is_new, conn_id) = self
            .subduction
            .register(ws_conn)
            .await
            .map_err(ConnectToPeerError::Registration)?;

        tracing::info!("Connected to peer at {uri} with connection ID {conn_id:?}",);
        Ok(conn_id)
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort();
    }
}

type TokioWebSocketSubduction<S, O, M> =
    Arc<Subduction<'static, Sendable, S, UnifiedWebSocket<O>, M>>;
