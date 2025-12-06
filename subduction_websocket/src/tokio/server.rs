//! # Subduction WebSocket server for Tokio

use crate::websocket::WebSocket;
use async_tungstenite::tokio::{accept_hdr_async, TokioAdapter};
use futures::future::Aborted;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, future::Sendable, storage::Storage,
};
use std::{marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};
use subduction_core::{peer::id::PeerId, Subduction};
use tokio::{
    net::{TcpListener, TcpStream},
    task::{coop, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::handshake::server::NoCallback;

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug)]
pub struct TokioWebSocketServer<
    S: 'static + Send + Sync + Storage<Sendable>,
    M: 'static + Send + Sync + DepthMetric = CountLeadingZeroBytes,
> where
    S::Error: 'static + Send + Sync,
{
    server_peer_id: PeerId,
    address: SocketAddr,
    subduction_actor_handle: SubductionActorHandle<S, M>,
    accept_task: Arc<JoinHandle<()>>,
    cancellation_token: CancellationToken,
}

impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric>
    TokioWebSocketServer<S, M>
where
    S::Error: 'static + Send + Sync,
{
    /// Create a new [`WebSocketServer`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound,
    /// or if the connection could not be established.
    pub async fn setup(
        address: SocketAddr,
        timeout: Duration,
        server_peer_id: PeerId,
        storage: S,
        depth_metric: M,
    ) -> Result<Self, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {}", address);
        let tcp_listener = TcpListener::bind(address).await?;

        let subduction_actor_handle = SubductionActorHandle::new(storage, depth_metric);
        let task_subduction_actor_handle = subduction_actor_handle.clone();

        let cancellation_token = CancellationToken::new();
        let child_cancellation_token = cancellation_token.child_token();

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

                                conns.spawn({
                                    let sd = task_subduction_actor_handle.clone();
                                    async move {
                                        match accept_hdr_async(tcp, NoCallback).await {
                                            Ok(hs) => {
                                                let ws_conn = WebSocket::<TokioAdapter<TcpStream>>::new(
                                                    hs,
                                                    timeout,
                                                    client_id,
                                                );

                                                tracing::info!("WebSocket handshake upgraded {addr}");

                                                if let Err(e) = sd.tx.send(Cmd::Register { ws: ws_conn }).await {
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
            address,
            subduction_actor_handle,
            accept_task: Arc::new(accept_task),
            cancellation_token,
        })
    }

    /// Register a new WebSocket connection with the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration message send fails over the internal channel.
    pub async fn register(
        &self,
        ws: WebSocket<TokioAdapter<TcpStream>>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Cmd>> {
        self.subduction_actor_handle
            .tx
            .send(Cmd::Register { ws })
            .await
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort();
    }
}

impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric> Clone
    for TokioWebSocketServer<S, M>
where
    S::Error: 'static + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            server_peer_id: self.server_peer_id,
            address: self.address,
            subduction_actor_handle: self.subduction_actor_handle.clone(),
            accept_task: self.accept_task.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}

#[derive(Debug)]
struct SubductionActorHandle<S, M> {
    tx: tokio::sync::mpsc::Sender<Cmd>,
    cancel_token: CancellationToken,
    subduction_actor_join_handle: Arc<JoinHandle<()>>,
    _phantom: PhantomData<(S, M)>,
}

impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric>
    SubductionActorHandle<S, M>
where
    S::Error: 'static + Send + Sync,
{
    fn new(storage: S, depth_metric: M) -> Self {
        let cancel_token = CancellationToken::new();
        let child_cancel_token = cancel_token.child_token();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Cmd>(1024);
        let subduction_actor_join_handle = tokio::spawn({
            let task_child_cancel_token = child_cancel_token.clone();
            async move {
                let (subduction, listener_fut, actor_fut) = Subduction::new(storage, depth_metric);

                let t1 = task_child_cancel_token.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        result = coop::cooperative(actor_fut) => {
                            if let Err(Aborted) = result {
                                tracing::debug!("Subduction actor aborted");
                            }
                        }
                        () = t1.cancelled() => {}
                    }
                });

                let t2 = task_child_cancel_token.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        result = coop::cooperative(listener_fut) => {
                            if let Err(Aborted) = result {
                                tracing::error!("Subduction listener aborted");
                            }
                        }
                        () = t2.cancelled() => {}
                    }
                });

                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        Cmd::Register { ws } => {
                            tracing::info!("registering new WebSocket connection");

                            let thread_ws = ws.clone();
                            tokio::spawn(async move {
                                if let Err(e) = coop::cooperative(thread_ws.listen()).await {
                                    tracing::error!("WebSocket listen error: {}", e);
                                }
                            });

                            if let Err(e) = subduction.register(ws).await {
                                tracing::error!("failed to register connection: {}", e);
                            }
                        }
                    }
                }
            }
        });

        Self {
            tx,
            cancel_token,
            subduction_actor_join_handle: Arc::new(subduction_actor_join_handle),
            _phantom: PhantomData,
        }
    }
}

impl<S, M> Drop for SubductionActorHandle<S, M> {
    fn drop(&mut self) {
        if Arc::strong_count(&self.subduction_actor_join_handle) == 1 {
            self.cancel_token.cancel();
            self.subduction_actor_join_handle.abort();
        }
    }
}

impl<S, M> Clone for SubductionActorHandle<S, M> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            cancel_token: self.cancel_token.clone(),
            subduction_actor_join_handle: self.subduction_actor_join_handle.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Commands for the Subduction actor.
#[derive(Debug, Clone, PartialEq)]
pub enum Cmd {
    /// Register a new peer.
    Register {
        /// The WebSocket connection to register.
        ws: WebSocket<TokioAdapter<TcpStream>>,
    },
}
