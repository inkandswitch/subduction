//! # Subduction WebSocket server for Tokio

use super::start::{Start, Unstarted};
use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::{
    tokio::{accept_async, TokioAdapter},
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
                                        match accept_async(tcp).await {
                                            Ok(ws_stream) => {
                                                tracing::info!("WebSocket handshake complete with {addr}");
                                                let ws_conn = WebSocket::<TokioAdapter<TcpStream>>::new(
                                                    ws_stream,
                                                    inner_timeout,
                                                    client_id,
                                                );

                                                let cmd = Cmd::Register { ws: ws_conn };
                                                sd.tx.send(cmd).await.expect("FIXME");
                                            }
                                            Err(e) => tracing::warn!("WS handshake failed from {addr}: {e}"),
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
        self.subduction_actor.tx.send(Cmd::Start).await
    }

    pub async fn register(
        &self,
        ws: WebSocket<TokioAdapter<TcpStream>>,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<Cmd>> {
        self.subduction_actor.tx.send(Cmd::Register { ws }).await
    }

    /// Graceful shutdown: cancel and await tasks.
    pub async fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.accept_task.abort()
    }

    // /// Start listening for incoming messages.
    // ///
    // /// # Errors
    // ///
    // /// Returns an error if:
    // /// * the connection drops unexpectedly
    // /// * a message could not be sent or received
    // /// * a message could not be parsed
    // pub async fn listen(&self) -> Result<(), RunError> {
    //     self.socket.listen().await
    // }
}

impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric> Drop
    for TokioWebSocketServer<S, M>
where
    S::Error: 'static + Send + Sync,
{
    fn drop(&mut self) {
        tracing::info!("Shutting down WebSocket server at {}", self.address);
        self.stop();
    }
}

// impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric> Start
//     for TokioWebSocketServer<S, M>
// {
//     fn start(&self) -> JoinHandle<Result<(), RunError>> {
//         let inner = self.clone();
//         tokio::spawn(async move { inner.subduction.run().await })
//     }
// }

// impl Connection<Sendable> for TokioWebSocketServer {
//     type SendError = SendError;
//     type RecvError = RecvError;
//     type CallError = CallError;
//     type DisconnectionError = DisconnectionError;
//
//     fn peer_id(&self) -> PeerId {
//         self.server_peer_id
//     }
//
//     fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
//         async { Connection::<Sendable>::next_request_id(&self.socket).await }.boxed()
//     }
//
//     fn disconnect(&mut self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
//         async { Ok(()) }.boxed()
//     }
//
//     fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
//         async {
//             tracing::debug!("Server sending message: {:?}", message);
//             Connection::<Sendable>::send(&self.socket, message).await
//         }
//         .boxed()
//     }
//
//     fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
//         async {
//             tracing::debug!("Server waiting to receive message");
//             Connection::<Sendable>::recv(&self.socket).await
//         }
//         .boxed()
//     }
//
//     fn call(
//         &self,
//         req: BatchSyncRequest,
//         override_timeout: Option<Duration>,
//     ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
//         async move {
//             tracing::debug!("Server making call with request: {:?}", req);
//             Connection::<Sendable>::call(&self.socket, req, override_timeout).await
//         }
//         .boxed()
//     }
// }

// impl Reconnect<Sendable> for TokioWebSocketServer {
//     type ConnectError = tungstenite::Error;
//     type RunError = RunError;
//
//     fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ConnectError>> {
//         async {
//             *self =
//                 TokioWebSocketServer::setup(self.address, self.socket.timeout, self.socket.peer_id)
//                     .await?
//                     .start();
//
//             Ok(())
//         }
//         .boxed()
//     }
//
//     fn run(&mut self) -> BoxFuture<'_, Result<(), Self::RunError>> {
//         async {
//             loop {
//                 self.socket.listen().await?;
//                 self.reconnect().await?;
//             }
//         }
//         .boxed()
//     }
// }

// impl<S: 'static + Send + Sync + Storage<Sendable>, M: 'static + Send + Sync + DepthMetric> PartialEq
//     for TokioWebSocketServer<S, M>
// {
//     fn eq(&self, other: &Self) -> bool {
//         if self.address != other.address {
//             return false;
//         }
//
//         if self.server_peer_id != other.server_peer_id {
//             return false;
//         }
//
//         if self.connection_listener_cancellation != other.connection_listener_cancellation {
//             return false;
//         }
//
//         true
//     }
// }
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
        let subduction = Arc::new(Subduction::new(
            Default::default(),
            storage,
            Default::default(),
            depth_metric,
        ));

        while let Some(cmd) = rx.recv().await {
            match cmd {
                Cmd::Start => {
                    tokio::spawn({
                        let inner = subduction.clone();
                        async move {
                            if let Err(e) = inner.run().await {
                                tracing::error!("Subduction run error: {}", e);
                            }
                        }
                    });
                    subduction.run().await;
                }
                Cmd::Register { ws } => {
                    if let Err(e) = subduction.register(ws).await {
                        tracing::error!("Failed to register connection: {}", e);
                    }
                }
            }
        }
    });

    (handle, join_handle)
}
