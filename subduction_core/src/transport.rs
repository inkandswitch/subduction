//! Byte-oriented transport abstraction.
//!
//! [`Transport`] provides a single trait that transports implement once.
//! Typed message send/recv is provided via [`MessageTransport<T>`], a
//! zero-cost wrapper that encodes on send and decodes on recv for any
//! message type `M: Encode + Decode`.
//!
//! # Architecture
//!
//! ```text
//! Transport (one impl per backend)
//!   └── MessageTransport<T>   — wraps Transport, implements Connection<K, M> for any M
//! ```
//!
//! This eliminates duplicate impls: each backend (WebSocket, iroh, HTTP
//! long-poll, `MessagePort`) implements `Transport` once instead of
//! implementing both `Handshake` and `Connection` separately.

use alloc::vec::Vec;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::codec::{decode::Decode, encode::Encode, error::DecodeError};

use super::Connection;

/// A bidirectional transport.
///
/// Implement this for transport backends (WebSocket, QUIC, HTTP long-poll,
/// `MessagePort`, etc). Wrap in [`MessageTransport`] to get a typed
/// [`Connection<K, M>`] for any message type.
///
/// # Contract
///
/// - `send_bytes` must deliver the entire byte slice atomically (no partial sends).
/// - `recv_bytes` must return a complete message frame (no fragmentation).
pub trait Transport<K: FutureForm + ?Sized>: Clone + PartialEq {
    /// A problem when sending bytes.
    type SendError: core::error::Error;

    /// A problem when receiving bytes.
    type RecvError: core::error::Error;

    /// A problem when disconnecting.
    type DisconnectionError: core::error::Error;

    /// Send raw bytes over the transport.
    fn send_bytes(&self, bytes: &[u8]) -> K::Future<'_, Result<(), Self::SendError>>;

    /// Receive the next complete message frame as raw bytes.
    fn recv_bytes(&self) -> K::Future<'_, Result<Vec<u8>, Self::RecvError>>;

    /// Disconnect from the peer gracefully.
    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>>;
}

// ── MessageTransport wrapper ────────────────────────────────────────────

/// Typed message layer over a [`Transport`].
///
/// Wraps a transport and provides [`Connection<K, M>`] for any
/// `M: Encode + Decode`. The `M` parameter is phantom — no runtime
/// overhead.
///
/// # Example
///
/// ```ignore
/// let ws: WebSocket = ...;
/// let conn: MessageTransport<WebSocket> = MessageTransport::new(ws);
/// // conn now implements Connection<K, SyncMessage>, Connection<K, WireMessage>, etc.
/// ```
#[derive(Clone, PartialEq)]
pub struct MessageTransport<T> {
    inner: T,
}

impl<T: core::fmt::Debug> core::fmt::Debug for MessageTransport<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MessageTransport")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T> MessageTransport<T> {
    /// Wrap a transport.
    pub const fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Access the inner transport.
    pub const fn inner(&self) -> &T {
        &self.inner
    }

    /// Consume and return the inner transport.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

/// Error from decoding received bytes into a typed message.
#[derive(Debug, thiserror::Error)]
pub enum RecvDecodeError<R: core::error::Error> {
    /// The transport's recv operation failed.
    #[error("recv error: {0}")]
    Recv(R),

    /// The received bytes could not be decoded.
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),
}

#[future_form(
    Sendable where
        T: Transport<Sendable> + Send + Sync,
        T::SendError: Send + 'static,
        T::RecvError: Send + 'static,
        T::DisconnectionError: Send + 'static,
        M: Encode + Decode + Send + 'static,
    Local where
        T: Transport<Local>,
        M: Encode + Decode + 'static
)]
impl<K: FutureForm, T, M> Connection<K, M> for MessageTransport<T> {
    type DisconnectionError = T::DisconnectionError;
    type SendError = T::SendError;
    type RecvError = RecvDecodeError<T::RecvError>;

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(&self, message: &M) -> K::Future<'_, Result<(), Self::SendError>> {
        let bytes = message.encode();
        K::from_future(async move { self.inner.send_bytes(&bytes).await })
    }

    fn recv(&self) -> K::Future<'_, Result<M, Self::RecvError>> {
        K::from_future(async move {
            let bytes = self
                .inner
                .recv_bytes()
                .await
                .map_err(RecvDecodeError::Recv)?;
            M::try_decode(&bytes).map_err(RecvDecodeError::from)
        })
    }
}

// ── Roundtrip delegation ─────────────────────────────────────────────────

impl<T, K, Req, Resp> super::Roundtrip<K, Req, Resp> for MessageTransport<T>
where
    T: super::Roundtrip<K, Req, Resp>,
    K: FutureForm,
{
    type CallError = T::CallError;

    fn next_request_id(&self) -> K::Future<'_, super::message::RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: Req,
        timeout: Option<core::time::Duration>,
    ) -> K::Future<'_, Result<Resp, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

// ── Arc impl ────────────────────────────────────────────────────────────

impl<T, K> Transport<K> for alloc::sync::Arc<T>
where
    T: Transport<K>,
    K: FutureForm,
{
    type SendError = T::SendError;
    type RecvError = T::RecvError;
    type DisconnectionError = T::DisconnectionError;

    fn send_bytes(&self, bytes: &[u8]) -> K::Future<'_, Result<(), Self::SendError>> {
        T::send_bytes(self, bytes)
    }

    fn recv_bytes(&self) -> K::Future<'_, Result<Vec<u8>, Self::RecvError>> {
        T::recv_bytes(self)
    }

    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectionError>> {
        T::disconnect(self)
    }
}
