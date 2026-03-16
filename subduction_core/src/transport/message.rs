//! Typed message layer over a [`Transport`](super::Transport).
//!
//! [`MessageTransport<T>`] wraps any transport and provides
//! [`Connection<K, M>`](crate::connection::Connection) for any
//! `M: Encode + Decode`. The `M` parameter is phantom — no runtime
//! overhead.

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::codec::{decode::Decode, encode::Encode, error::DecodeError};

use super::Transport;
use crate::connection::{Connection, Roundtrip, message::RequestId};

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

impl<T, K, Req, Resp> Roundtrip<K, Req, Resp> for MessageTransport<T>
where
    T: Roundtrip<K, Req, Resp>,
    K: FutureForm,
{
    type CallError = T::CallError;

    fn next_request_id(&self) -> K::Future<'_, RequestId> {
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
