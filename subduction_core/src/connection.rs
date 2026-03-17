//! Manage connections to peers in the network.
//!
//! [`Connection<K, M>`] models fire-and-forget cast + async recv (mailbox).
//! Transports implement this trait via [`MessageTransport`](crate::transport::message::MessageTransport).

pub mod backoff;
pub mod id;
pub mod managed;
pub mod manager;
pub mod message;
pub mod stats;

#[cfg(feature = "test_utils")]
pub mod test_utils;

use alloc::sync::Arc;

use future_form::FutureForm;
use sedimentree_core::codec::{decode::Decode, encode::Encode};
use thiserror::Error;

/// Fire-and-forget messaging (cast) and async receive (mailbox).
///
/// The message type `M` must know how to encode/decode itself to bytes.
/// Transports call `msg.encode()` in send and `M::try_decode(bytes)` in
/// recv — they never need to know what `M` _is_.
///
/// It is assumed that a [`Connection`] is authenticated to a particular peer.
/// Encrypting this channel is also strongly recommended.
pub trait Connection<K: FutureForm + ?Sized, M: Encode + Decode>: Clone + PartialEq {
    /// A problem when gracefully disconnecting.
    type DisconnectionError: core::error::Error;

    /// A problem when sending a message.
    type SendError: core::error::Error;

    /// A problem when receiving a message.
    type RecvError: core::error::Error;

    /// Disconnect from the peer gracefully.
    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>>;

    /// Send a message (fire-and-forget).
    fn send(&self, message: &M) -> K::Future<'_, Result<(), Self::SendError>>;

    /// Receive the next message from the peer.
    fn recv(&self) -> K::Future<'_, Result<M, Self::RecvError>>;
}

// ── Blanket impls for Arc<T> ────────────────────────────────────────────

impl<T, K, M> Connection<K, M> for Arc<T>
where
    T: Connection<K, M>,
    K: FutureForm,
    M: Encode + Decode,
{
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = T::RecvError;

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        T::disconnect(self)
    }

    fn send(&self, message: &M) -> K::Future<'_, Result<(), Self::SendError>> {
        T::send(self, message)
    }

    fn recv(&self) -> K::Future<'_, Result<M, Self::RecvError>> {
        T::recv(self)
    }
}

// ── Reconnect ───────────────────────────────────────────────────────────

/// A trait for connections that can be re-established if they drop.
///
/// Connections implementing this trait can be automatically reconnected
/// by the connection manager when they drop unexpectedly.
pub trait Reconnect<K: FutureForm, M: Encode + Decode>: Connection<K, M> {
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
