//! # Subduction WebSocket server for Tokio

use crate::{
    timeout::{FuturesTimerTimeout, Timeout},
    websocket::WebSocket,
};

use alloc::{string::ToString, sync::Arc};
use async_tungstenite::tokio::{accept_hdr_async, TokioAdapter};
use core::{net::SocketAddr, time::Duration};
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, future::Sendable, storage::Storage,
};
use subduction_core::{
    connection::id::ConnectionId, peer::id::PeerId, subduction::error::RegistrationError,
    Subduction,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::handshake::server::NoCallback;

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable>,
    M: 'static + Send + Sync + DepthMetric = CountLeadingZeroBytes,
    O: 'static + Send + Sync + Timeout<Sendable> + Clone = FuturesTimerTimeout,
> where
    S::Error: 'static + Send + Sync,
{
    server_peer_id: PeerId,
    address: SocketAddr,
    subduction:
        Arc<Subduction<'static, Sendable, S, WebSocket<TokioAdapter<TcpStream>, Sendable, O>, M>>,
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
    pub async fn new(
        address: SocketAddr,
        timeout: O,
        default_time_limit: Duration,
        server_peer_id: PeerId,
        subduction: Arc<
            Subduction<'static, Sendable, S, WebSocket<TokioAdapter<TcpStream>, Sendable, O>, M>,
        >,
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
                                                let ws_conn = WebSocket::<TokioAdapter<TcpStream>, Sendable, O>::new(
                                                    hs,
                                                    tout,
                                                    default_time_limit,
                                                    client_id,
                                                );

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
        let (subduction, listener_fut, actor_fut) = Subduction::new(storage, depth_metric);

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
                _ = actor_cancel.cancelled() => {}
            }
        });

        tokio::spawn(async move {
            tokio::select! {
                _ = listener_fut => {},
                _ = listener_cancel.cancelled() => {}
            }
        });

        Ok(server)
    }

    /// Get the server's peer ID.
    pub fn peer_id(&self) -> PeerId {
        self.server_peer_id
    }

    /// Get the server's socket address.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Register a new WebSocket connection with the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration message send fails over the internal channel.
    pub async fn register(
        &self,
        ws: WebSocket<TokioAdapter<TcpStream>, Sendable, O>,
    ) -> Result<(bool, ConnectionId), RegistrationError> {
        self.subduction.register(ws).await
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort();
    }
}
