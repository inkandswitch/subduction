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
//!   └── MessageTransport<T>   — adds Connection<K, M> for any M
//! ```
//!
//! Each backend (WebSocket, iroh, HTTP long-poll, `MessagePort`)
//! implements `Transport` once instead of implementing both `Handshake`
//! and `Connection` separately.

pub mod message;
use alloc::{sync::Arc, vec::Vec};

use future_form::FutureForm;

/// A bidirectional transport.
///
/// Implement this for transport backends (WebSocket, QUIC, HTTP long-poll,
/// `MessagePort`, etc). Wrap in [`MessageTransport`] to get a typed
/// [`Connection<K, M>`](crate::connection::Connection) for any message type.
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

// ── Arc impl ────────────────────────────────────────────────────────────

impl<T, K> Transport<K> for Arc<T>
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
