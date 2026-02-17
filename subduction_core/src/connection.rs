//! Manage connections to peers in the network.

pub mod authenticated;
pub mod backoff;
pub mod handshake;
pub mod id;
pub mod manager;
pub mod message;
pub mod nonce_cache;
pub mod stats;

#[cfg(feature = "test_utils")]
pub mod test_utils;

use alloc::sync::Arc;
use core::time::Duration;

use self::message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId};
use crate::peer::id::PeerId;
use future_form::FutureForm;
use thiserror::Error;

/// A trait representing a connection to a peer in the network.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection<K: FutureForm + ?Sized>: Clone + PartialEq {
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
    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>>;

    /// Send a message.
    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>>;

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

impl<T: Connection<K>, K: FutureForm> Connection<K> for Arc<T> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type CallError = T::CallError;

    fn peer_id(&self) -> PeerId {
        T::peer_id(self)
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        T::disconnect(self)
    }

    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>> {
        T::send(self, message)
    }

    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>> {
        T::recv(self)
    }

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        T::next_request_id(self)
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        T::call(self, req, timeout)
    }
}

/// A trait for connections that can be re-established if they drop.
///
/// Connections implementing this trait can be automatically reconnected
/// by the connection manager when they drop unexpectedly.
pub trait Reconnect<K: FutureForm>: Connection<K> {
    /// A problem when reconnecting.
    type ReconnectionError: core::error::Error + Send + 'static;

    /// Attempt to reconnect to the same peer.
    ///
    /// This should:
    /// 1. Close any existing connection resources
    /// 2. Establish a fresh connection
    /// 3. Complete the handshake
    fn reconnect(&mut self) -> K::Future<'_, Result<(), Self::ReconnectionError>>;

    /// Classify whether an error is retryable.
    ///
    /// Return `false` for fatal errors that should not be retried (e.g.,
    /// authentication rejection, policy violation). Return `true` for
    /// transient errors like network timeouts.
    ///
    /// The default implementation considers all errors retryable.
    fn should_retry(&self, _error: &Self::ReconnectionError) -> bool {
        true
    }
}

/// An error indicating that a connection is disallowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[error("Connection disallowed")]
pub struct ConnectionDisallowed;
