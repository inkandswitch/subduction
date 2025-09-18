//! Manage connections to peers in the network.

pub mod id;
pub mod message;

use std::{sync::Arc, time::Duration};

use self::{
    id::ConnectionId,
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
};
use crate::peer::id::PeerId;
use futures::{executor::block_on, lock::Mutex, Future};
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection {
    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

    /// A problem when sending a message.
    type SendError: core::error::Error;

    /// A problem when receiving a message.
    type RecvError: core::error::Error;

    /// A problem with a roundtrip call.
    type CallError: core::error::Error;

    /// A unique identifier for this connection.
    ///
    /// This number should be a counter or random number.
    /// We assume that the same ID is never reused for different connections.
    /// For this reason, it is not recommended to use or derive from the peer ID on its own.
    fn connection_id(&self) -> ConnectionId;

    /// The peer ID of the remote peer.
    fn peer_id(&self) -> PeerId;

    /// Disconnect from the peer gracefully.
    fn disconnect(&mut self) -> impl Future<Output = Result<(), Self::DisconnectionError>>;

    /// Send a message.
    fn send(&self, message: Message) -> impl Future<Output = Result<(), Self::SendError>>;

    /// Receive a message.
    fn recv(&self) -> impl Future<Output = Result<Message, Self::RecvError>>;

    /// Get the next request ID e.g. for a [`call`].
    fn next_request_id(&self) -> impl Future<Output = RequestId>;

    /// Make a synchronous call to the peer, expecting a response.
    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> impl Future<Output = Result<BatchSyncResponse, Self::CallError>>;
}

/// A trait for connections that can be re-established if they drop.
pub trait Reconnection: Connection + Sized {
    /// A problem when creating the connection.
    type ConnectError: core::error::Error;

    /// A problem when running the connection.
    type RunError: core::error::Error;

    /// Setup the connection, but don't run it.
    fn reconnect(&mut self) -> impl Future<Output = Result<(), Self::ConnectError>>;

    /// Run the connection send/receive loop.
    fn run(&mut self) -> impl Future<Output = Result<(), Self::RunError>>;
}

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy {
    /// Check if a connection from the given peer is allowed.
    ///
    /// # Errors
    ///
    /// * Returns [`ConnectionDisallowed`] if the connection is not allowed.
    fn allowed_to_connect(&self, peer: &PeerId) -> Result<(), ConnectionDisallowed>;
}

/// An error indicating that a connection is disallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Hash)]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;

impl<T: Connection> Connection for Arc<Mutex<T>> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type CallError = T::CallError;

    fn connection_id(&self) -> ConnectionId {
        block_on(self.lock()).connection_id()
    }

    fn peer_id(&self) -> PeerId {
        block_on(self.lock()).peer_id()
    }

    async fn disconnect(&mut self) -> Result<(), Self::DisconnectionError> {
        let conn = self.clone();
        conn.lock().await.disconnect().await
    }

    async fn send(&self, message: Message) -> Result<(), Self::SendError> {
        let conn = self.clone();
        conn.lock().await.send(message).await
    }

    async fn recv(&self) -> Result<Message, Self::RecvError> {
        let conn = self.clone();
        conn.lock().await.recv().await
    }

    async fn next_request_id(&self) -> RequestId {
        let conn = self.clone();
        conn.lock().await.next_request_id().await
    }

    async fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> Result<BatchSyncResponse, Self::CallError> {
        let conn = self.clone();
        conn.lock().await.call(req, timeout).await
    }
}

impl<T: Reconnection> Reconnection for Arc<Mutex<T>> {
    type ConnectError = T::ConnectError;
    type RunError = T::RunError;

    async fn reconnect(&mut self) -> Result<(), Self::ConnectError> {
        let conn = self.clone();
        conn.lock().await.reconnect().await
    }

    async fn run(&mut self) -> Result<(), Self::RunError> {
        let conn = self.clone();
        conn.lock().await.run().await
    }
}
