//! # Subduction WebSocket server for Tokio

use super::start::{Start, Unstarted};
use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::{
    tokio::{accept_hdr_async, TokioAdapter},
    WebSocketStream,
};
use dashmap::DashMap;
use futures_util::StreamExt;
use sedimentree_core::{
    commit::CountLeadingZeroBytes, depth::DepthMetric, future::Sendable, storage::Storage,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
    Subduction,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::{JoinError, JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tungstenite::handshake::client::{Request, Response};

#[derive(Debug, Clone)]
pub struct SubductionActor {
    tx: tokio::sync::mpsc::Sender<Cmd>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cmd {
    Start,
    Register {
        ws: WebSocket<TokioAdapter<TcpStream>>,
    },
}

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
    subduction_actor: SubductionActor,
    accept_task: JoinHandle<()>,
    cancellation_token: CancellationToken,
    _phantom_storage: std::marker::PhantomData<(S, M)>,
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
    ) -> Result<Unstarted<Self>, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {}", address);
        let listener = TcpListener::bind(address).await.expect("FIXME");
        // .map_err(|e| WsError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        let (subduction_actor, _sd_task) = start_subduction_actor::<S, M>(storage, depth_metric);

        let cancellation_token = CancellationToken::new();
        let child_cancellation_token = cancellation_token.child_token();
        let conns = Arc::new(Mutex::new(JoinSet::new()));

        let subd_actor = subduction_actor.clone();
        let conns_for_task = conns.clone();

        let accept_task: JoinHandle<()> = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = child_cancellation_token.cancelled() => {
                            tracing::info!("accept loop canceled");
                            break;
                        }
                    res = listener.accept() => {
                        match res {
                            Ok((tcp, addr)) => {
                                tracing::info!("New TCP connection from {addr}");

                                // HACK FIXME include client ID in welcome message instead of this hack?
                                let client_digest = {
                                    let mut hasher = blake3::Hasher::new();
                                    hasher.update(addr.ip().to_string().as_bytes());
                                    hasher.update(addr.port().to_le_bytes().as_ref());
                                    hasher.finalize().as_bytes().clone()
                                };
                                let client_id = PeerId::new(client_digest);
                                let inner_timeout = timeout; // Copy

                                let mut set = conns_for_task.lock().await;
                                set.spawn({
                                    let sd = subd_actor.clone();
                                    async move {
                                        // let hs = accept_hdr_async(tcp, |req, resp| {
                                        //     // resp.headers_mut().append("x-server", "subduction".parse().unwrap());
                                        //     Ok(resp)
                                        // }).await.expect("FIXME");

                                        let hs = accept_hdr_async(tcp, tungstenite::handshake::server::NoCallback)
                                            .await
                                            .expect("FIXME");

                                        let ws_conn = WebSocket::<TokioAdapter<TcpStream>>::new(
                                            hs,
                                            inner_timeout,
                                            client_id,
                                        );
                                        tracing::info!("WebSocket handshake UPGRADED with {addr}");

                                        if let Err(e) = sd.tx.send(Cmd::Register { ws: ws_conn }).await {
                                            tracing::error!("Failed to register new connection: {}", e);
                                        }
                                    }
                                });
                            }
                            Err(e) => tracing::error!("Accept error: {e}"),
                        }
                    }
                }
            }

            let mut set = conns_for_task.lock().await;
            while let Some(_) = set.join_next().await {}
        });

        let server = Self {
            server_peer_id,
            address,
            subduction_actor,
            accept_task,
            cancellation_token,
            _phantom_storage: PhantomData,
        };

        Ok(Unstarted(server))
    }

    pub async fn start(&self) -> Result<(), tokio::sync::mpsc::error::SendError<Cmd>> {
        tracing::info!("Starting Subduction actor");
        self.subduction_actor.tx.send(Cmd::Start).await
    }

    pub async fn register(
        &self,
        ws: WebSocket<TokioAdapter<TcpStream>>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Cmd>> {
        self.subduction_actor.tx.send(Cmd::Register { ws }).await
    }

    /// Graceful shutdown: cancel and await tasks.
    pub fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort()
    }
}

impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric> Drop
    for TokioWebSocketServer<S, M>
where
    S::Error: 'static + Send + Sync,
{
    fn drop(&mut self) {
        tracing::info!("Shutting down WebSocket server at {}", self.address);
        self.stop()
    }
}

#[tracing::instrument(skip_all)]
fn start_subduction_actor<
    S: 'static + Send + Sync + Storage<Sendable>,
    M: 'static + Send + Sync + DepthMetric,
>(
    storage: S,
    depth_metric: M,
) -> (SubductionActor, JoinHandle<()>)
where
    S::Error: 'static + Send + Sync,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Cmd>(1024);
    let handle = SubductionActor { tx };

    let join_handle = tokio::spawn(async move {
        let arc_subduction = Arc::new(Subduction::new(
            Default::default(),
            storage,
            Default::default(),
            depth_metric,
        ));

        while let Some(cmd) = rx.recv().await {
            tracing::info!("Subduction actor received command: {:?}", cmd); // FIXME
            match cmd {
                Cmd::Start => {
                    tokio::spawn({
                        tracing::debug!("Spawning Subduction run task");
                        let inner = arc_subduction.clone();
                        async move {
                            if let Err(e) = inner.run().await {
                                tracing::error!("Subduction run error: {}", e);
                            }
                        }
                    });
                }
                Cmd::Register { ws } => {
                    tracing::info!("Registering new WebSocket connection");
                    // FIXME here for debuggng
                    let foo = ws.clone();
                    tokio::spawn(async move {
                        if let Err(e) = foo.listen().await {
                            tracing::error!("WebSocket listen error: {}", e);
                        }
                    });
                    // FIXME
                    // let _ = ws.listen().await;
                    if let Err(e) = arc_subduction.register(ws).await {
                        tracing::error!("Failed to register connection: {}", e);
                    }
                }
            }
        }
    });

    (handle, join_handle)
}
