//! Manage connections to peers in the network.

pub mod id;
pub mod message;

use std::time::Duration;

use self::message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId};
use crate::peer::id::PeerId;
use futures::Future;
use sedimentree_core::future::FutureKind;
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection<K: FutureKind>: Clone {
    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

    /// A problem when sending a message.
    type SendError: core::error::Error;

    /// A problem when receiving a message.
    type RecvError: core::error::Error;

    /// A problem with a roundtrip call.
    type CallError: core::error::Error;

    /// The peer ID of the remote peer.
    fn peer_id(&self) -> PeerId;

    /// Disconnect from the peer gracefully.
    fn disconnect(&mut self) -> K::Future<'_, Result<(), Self::DisconnectionError>>;

    /// Send a message.
    fn send(&self, message: Message) -> K::Future<'_, Result<(), Self::SendError>>;

    /// Receive a message.
    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>>;

    /// Get the next request ID e.g. for a [`call`].
    fn next_request_id(&self) -> K::Future<'_, RequestId>;

    /// Make a synchronous call to the peer, expecting a response.
    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>>;
}

/// A trait for connections that can be re-established if they drop.
pub trait Reconnect<K: FutureKind>: Connection<K> {
    /// A problem when creating the connection.
    type ConnectError: core::error::Error;

    /// A problem when running the connection.
    type RunError: core::error::Error;

    /// Setup the connection, but don't run it.
    fn reconnect(&mut self) -> K::Future<'_, Result<(), Self::ConnectError>>;

    /// Run the connection send/receive loop.
    fn run(&mut self) -> K::Future<'_, Result<(), Self::RunError>>;
}

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy {
    /// Check if a connection from the given peer is allowed.
    ///
    /// # Errors
    ///
    /// * Returns [`ConnectionDisallowed`] if the connection is not allowed.
    fn allowed_to_connect(
        &self,
        peer: &PeerId,
    ) -> impl Future<Output = Result<(), ConnectionDisallowed>>;
}

/// An error indicating that a connection is disallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Hash)]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;
