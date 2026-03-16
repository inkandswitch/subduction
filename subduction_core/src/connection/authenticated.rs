//! Authenticated connection wrapper.
//!
//! Provides [`Authenticated<C, K>`], a witness type proving that a connection
//! has completed cryptographic handshake verification.

use core::{marker::PhantomData, time::Duration};

use future_form::FutureForm;
use sedimentree_core::codec::{decode::Decode, encode::Encode};

use super::{Connection, Reconnect, Roundtrip, message::RequestId};
use crate::peer::id::PeerId;

/// A connection that has completed handshake verification.
///
/// This is a witness type proving the connection was authenticated.
/// The inner connection has been verified via cryptographic handshake,
/// and the `peer_id` is the verified identity from the handshake.
///
/// # Type Parameters
///
/// * `C` — The connection type (struct bound relaxed to `Clone`)
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
pub struct Authenticated<C: Clone, K: FutureForm> {
    inner: C,
    peer_id: PeerId,
    _marker: PhantomData<fn() -> K>,
}

impl<C: Clone, K: FutureForm> Clone for Authenticated<C, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            peer_id: self.peer_id,
            _marker: PhantomData,
        }
    }
}

impl<C: Clone + PartialEq, K: FutureForm> PartialEq for Authenticated<C, K> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<C: Clone + core::fmt::Debug, K: FutureForm> core::fmt::Debug for Authenticated<C, K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Authenticated")
            .field("peer_id", &self.peer_id)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<C: Clone, K: FutureForm> Authenticated<C, K> {
    /// Construct from a successful handshake.
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

    /// The verified peer identity, established during the handshake.
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Transform the inner connection while preserving the authentication proof.
    ///
    /// This is useful when wrapping an authenticated connection in a higher-level
    /// type (e.g., `WebSocket` → `UnifiedWebSocket`) while preserving the
    /// proof that the underlying connection completed handshake verification.
    #[must_use]
    pub fn map<D: Clone>(self, f: impl FnOnce(C) -> D) -> Authenticated<D, K> {
        Authenticated {
            inner: f(self.inner),
            peer_id: self.peer_id,
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

    /// Access the inner connection.
    pub const fn inner(&self) -> &C {
        &self.inner
    }

    /// Consume and return the inner connection.
    pub fn into_inner(self) -> C {
        self.inner
    }
}

// ── Delegation impls ────────────────────────────────────────────────────

impl<C: Connection<K, M>, K: FutureForm, M: Encode + Decode> Connection<K, M>
    for Authenticated<C, K>
{
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(&self, message: &M) -> K::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> K::Future<'_, Result<M, Self::RecvError>> {
        self.inner.recv()
    }
}

impl<C, K, Req, Resp> Roundtrip<K, Req, Resp> for Authenticated<C, K>
where
    C: Roundtrip<K, Req, Resp>,
    K: FutureForm,
{
    type CallError = C::CallError;

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: Req,
        timeout: Option<Duration>,
    ) -> K::Future<'_, Result<Resp, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

impl<C: Reconnect<K, M>, K: FutureForm, M: Encode + Decode> Reconnect<K, M>
    for Authenticated<C, K>
{
    type ReconnectionError = C::ReconnectionError;

    fn reconnect(&mut self) -> K::Future<'_, Result<(), Self::ReconnectionError>> {
        self.inner.reconnect()
    }

    fn should_retry(&self, error: &Self::ReconnectionError) -> bool {
        self.inner.should_retry(error)
    }
}
