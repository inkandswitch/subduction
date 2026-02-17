//! Authenticated connection wrapper.
//!
//! Provides [`Authenticated<C, K>`], a witness type proving that a connection
//! has completed cryptographic handshake verification.

use core::{marker::PhantomData, time::Duration};

use future_form::FutureForm;

use super::{
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    Connection, Reconnect,
};
use crate::peer::id::PeerId;

/// A connection that has completed handshake verification.
///
/// This is a witness type proving the connection was authenticated.
/// The inner connection has been verified via cryptographic handshake,
/// and the `peer_id` matches the signing key used in verification.
///
/// # Type Parameters
///
/// * `C` — The connection type, which must implement [`Connection<K>`]
/// * `K` — The [`FutureForm`] (e.g., [`Sendable`] or [`Local`])
///
/// [`Sendable`]: future_form::Sendable
/// [`Local`]: future_form::Local
///
/// # Construction
///
/// Use [`handshake::initiate`] or [`handshake::respond`] to perform
/// handshake verification and obtain an `Authenticated` connection.
///
/// [`handshake::initiate`]: super::handshake::initiate
/// [`handshake::respond`]: super::handshake::respond
///
/// # Example
///
/// ```ignore
/// use subduction_core::connection::handshake;
///
/// // Initiator side - performs handshake and returns Authenticated<MyConnection, Sendable>
/// let authenticated = handshake::initiate(
///     &mut transport,
///     |_, peer_id| MyConnection::new(stream, peer_id),
///     &signer,
///     audience,
///     now,
///     nonce,
/// ).await?;
///
/// // Now register() accepts the authenticated connection
/// subduction.register(authenticated).await?;
/// ```
pub struct Authenticated<C: Connection<K>, K: FutureForm> {
    inner: C,
    _marker: PhantomData<fn() -> K>,
}

impl<C: Connection<K>, K: FutureForm> Clone for Authenticated<C, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<C: Connection<K>, K: FutureForm> PartialEq for Authenticated<C, K> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<C: Connection<K> + core::fmt::Debug, K: FutureForm> core::fmt::Debug for Authenticated<C, K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Authenticated")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<C: Connection<K>, K: FutureForm> Authenticated<C, K> {
    /// Construct from a successful handshake.
    ///
    /// This is only accessible within the `connection` module.
    /// Use [`handshake::initiate`] or [`handshake::respond`] to obtain
    /// an `Authenticated` connection.
    ///
    /// [`handshake::initiate`]: super::handshake::initiate
    /// [`handshake::respond`]: super::handshake::respond
    pub(super) fn from_handshake(inner: C) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Construct for testing purposes only.
    ///
    /// This bypasses handshake verification and should only be used in tests.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn new_for_test(inner: C) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// The verified peer identity.
    pub fn peer_id(&self) -> PeerId {
        self.inner.peer_id()
    }

    /// Access the inner connection.
    pub const fn inner(&self) -> &C {
        &self.inner
    }

    /// Consume and return the inner connection.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

impl<C: Connection<K>, K: FutureForm> Connection<K> for Authenticated<C, K> {
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;
    type CallError = C::CallError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id()
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(&self, message: &Message) -> K::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> K::Future<'_, Result<Message, Self::RecvError>> {
        self.inner.recv()
    }

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

impl<C: Reconnect<K>, K: FutureForm> Reconnect<K> for Authenticated<C, K> {
    type ReconnectionError = C::ReconnectionError;

    fn reconnect(&mut self) -> K::Future<'_, Result<(), Self::ReconnectionError>> {
        self.inner.reconnect()
    }

    fn should_retry(&self, error: &Self::ReconnectionError) -> bool {
        self.inner.should_retry(error)
    }
}
