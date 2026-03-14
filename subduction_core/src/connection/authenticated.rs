//! Authenticated connection wrapper.
//!
//! Provides [`Authenticated<C, K>`], a witness type proving that a connection
//! has completed cryptographic handshake verification.

use core::{marker::PhantomData, time::Duration};

use future_form::FutureForm;

use super::{
    Connection, Reconnect,
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
};
use crate::peer::id::PeerId;

/// A connection that has completed handshake verification.
///
/// This is a witness type proving the connection was authenticated.
/// The inner connection has been verified via cryptographic handshake,
/// and the `peer_id` is the verified remote peer's identity.
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
/// // Initiator side - transport is consumed, returned to build_connection
/// let (authenticated, ()) = handshake::initiate(
///     transport,  // consumed
///     |transport, peer_id| (MyConnection::new(transport, peer_id), ()),
///     &signer,
///     audience,
///     now,
///     nonce,
/// ).await?;
///
/// // Now add_connection() accepts the authenticated connection
/// subduction.add_connection(authenticated).await?;
/// ```
pub struct Authenticated<C: Connection<K>, K: FutureForm> {
    inner: C,
    peer_id: PeerId,
    _marker: PhantomData<fn() -> K>,
}

impl<C: Connection<K>, K: FutureForm> Clone for Authenticated<C, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            peer_id: self.peer_id,
            _marker: PhantomData,
        }
    }
}

impl<C: Connection<K>, K: FutureForm> PartialEq for Authenticated<C, K> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.inner == other.inner
    }
}

impl<C: Connection<K> + core::fmt::Debug, K: FutureForm> core::fmt::Debug for Authenticated<C, K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Authenticated")
            .field("inner", &self.inner)
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

impl<C: Connection<K>, K: FutureForm> Authenticated<C, K> {
    /// Construct from a successful handshake.
    ///
    /// The `peer_id` is the verified remote peer identity from the handshake.
    ///
    /// This is only accessible within the `connection` module.
    /// Use [`handshake::initiate`] or [`handshake::respond`] to obtain
    /// an `Authenticated` connection.
    ///
    /// [`handshake::initiate`]: super::handshake::initiate
    /// [`handshake::respond`]: super::handshake::respond
    pub(super) fn from_handshake(inner: C, peer_id: PeerId) -> Self {
        Self {
            inner,
            peer_id,
            _marker: PhantomData,
        }
    }

    /// Construct for testing purposes only.
    ///
    /// This bypasses handshake verification and should only be used in tests.
    #[cfg(any(test, feature = "test_utils"))]
    pub fn new_for_test(inner: C, peer_id: PeerId) -> Self {
        Self {
            inner,
            peer_id,
            _marker: PhantomData,
        }
    }

    /// Transform the inner connection while preserving the authentication proof.
    ///
    /// The verified peer identity is carried through to the new `Authenticated`.
    ///
    /// # Type Safety
    ///
    /// The new connection type `D` must wrap or delegate to the original
    /// authenticated connection `C`. The caller is responsible for ensuring
    /// this invariant holds. Typical use is when `D` is a newtype or wrapper
    /// around `C` that adds functionality (reconnection, logging, etc.)
    /// without changing the underlying authenticated channel.
    #[must_use]
    pub fn map<D: Connection<K>>(self, f: impl FnOnce(C) -> D) -> Authenticated<D, K> {
        Authenticated {
            inner: f(self.inner),
            peer_id: self.peer_id,
            _marker: PhantomData,
        }
    }

    /// The verified remote peer identity, established during the handshake.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
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
