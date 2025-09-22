//! # Subduction WebSocket server for Tokio

use crate::{
    error::{CallError, DisconnectionError, RecvError, RunError, SendError},
    websocket::WebSocket,
};
use async_tungstenite::{
    tokio::{accept_async, TokioAdapter},
    WebSocketStream,
};
use core::net::SocketAddr;
use futures::{future::BoxFuture, FutureExt};
use sedimentree_core::future::Sendable;
use std::time::Duration;
use subduction_core::{
    connection::{
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
        Connection, Reconnect,
    },
    peer::id::PeerId,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use super::start::{Start, Unstarted};

/// A Tokio-flavoured [`WebSocket`] server implementation.
#[derive(Debug, Clone)]
pub struct TokioWebSocketServer {
    address: SocketAddr,
    socket: WebSocket<TokioAdapter<TcpStream>>,
}

impl TokioWebSocketServer {
    /// Create a new [`WebSocketServer`] connection from an accepted TCP stream.
    pub fn new(
        address: SocketAddr,
        timeout: Duration,
        peer_id: PeerId,
        ws_stream: WebSocketStream<TokioAdapter<TcpStream>>,
    ) -> Unstarted<Self> {
        let socket = WebSocket::<_>::new(ws_stream, timeout, peer_id);
        tracing::info!("Accepting WebSocket connections at {address}");
        Unstarted(TokioWebSocketServer { address, socket })
    }

    /// Create a new [`WebSocketServer`] connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket could not be bound,
    /// or if the connection could not be established.
    pub async fn setup(
        address: SocketAddr,
        timeout: Duration,
        peer_id: PeerId,
    ) -> Result<Unstarted<Self>, tungstenite::Error> {
        tracing::info!("Starting WebSocket server on {address}");
        let listener = TcpListener::bind(address).await?;
        let (tcp, _peer) = listener.accept().await?;
        let ws_stream = accept_async(tcp).await?;
        Ok(Self::new(address, timeout, peer_id, ws_stream))
    }

    /// Start listening for incoming messages.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * the connection drops unexpectedly
    /// * a message could not be sent or received
    /// * a message could not be parsed
    pub async fn listen(&self) -> Result<(), RunError> {
        self.socket.listen().await
    }
}

impl Start for TokioWebSocketServer {
    fn start(&self) -> JoinHandle<Result<(), RunError>> {
        let inner = self.clone();
        tokio::spawn(async move { inner.socket.listen().await })
    }
}

impl Connection<Sendable> for TokioWebSocketServer {
    type SendError = SendError;
    type RecvError = RecvError;
    type CallError = CallError;
    type DisconnectionError = DisconnectionError;

    fn peer_id(&self) -> PeerId {
        Connection::<Sendable>::peer_id(&self.socket)
    }

    fn next_request_id(&self) -> BoxFuture<'_, RequestId> {
        async { Connection::<Sendable>::next_request_id(&self.socket).await }.boxed()
    }

    fn disconnect(&mut self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed()
    }

    fn send(&self, message: Message) -> BoxFuture<'_, Result<(), Self::SendError>> {
        async {
            tracing::debug!("Server sending message: {:?}", message);
            Connection::<Sendable>::send(&self.socket, message).await
        }
        .boxed()
    }

    fn recv(&self) -> BoxFuture<'_, Result<Message, Self::RecvError>> {
        async {
            tracing::debug!("Server waiting to receive message");
            Connection::<Sendable>::recv(&self.socket).await
        }
        .boxed()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        override_timeout: Option<Duration>,
    ) -> BoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        async move {
            tracing::debug!("Server making call with request: {:?}", req);
            Connection::<Sendable>::call(&self.socket, req, override_timeout).await
        }
        .boxed()
    }
}

impl Reconnect<Sendable> for TokioWebSocketServer {
    type ConnectError = tungstenite::Error;
    type RunError = RunError;

    fn reconnect(&mut self) -> BoxFuture<'_, Result<(), Self::ConnectError>> {
        async {
            *self =
                TokioWebSocketServer::setup(self.address, self.socket.timeout, self.socket.peer_id)
                    .await?
                    .start();

            Ok(())
        }
        .boxed()
    }

    fn run(&mut self) -> BoxFuture<'_, Result<(), Self::RunError>> {
        async {
            loop {
                self.socket.listen().await?;
                self.reconnect().await?;
            }
        }
        .boxed()
    }
}

impl PartialEq for TokioWebSocketServer {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address && self.socket.peer_id == other.socket.peer_id
    }
}
