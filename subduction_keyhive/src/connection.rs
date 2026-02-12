//! Connection abstraction for the keyhive protocol.

use futures_kind::FutureKind;

use crate::{peer_id::KeyhivePeerId, signed_message::SignedMessage};

/// A trait representing a connection to a peer for keyhive protocol messages.
///
/// This trait abstracts over the underlying transport mechanism, allowing
/// the keyhive protocol to work with different connection backends such as
/// WebSocket, in-memory channels, or other transports.
pub trait KeyhiveConnection<K: FutureKind + ?Sized>: Clone {
    /// The error type returned when sending a message fails.
    type SendError: core::error::Error;

    /// The error type returned when receiving a message fails.
    type RecvError: core::error::Error;

    /// The error type returned when disconnecting fails.
    type DisconnectError: core::error::Error;

    /// Get the peer ID of the remote peer.
    fn peer_id(&self) -> KeyhivePeerId;

    /// Send a signed message to the peer.
    fn send(&self, message: SignedMessage) -> K::Future<'_, Result<(), Self::SendError>>;

    /// Receive a signed message from the peer.
    fn recv(&self) -> K::Future<'_, Result<SignedMessage, Self::RecvError>>;

    /// Disconnect from the peer gracefully.
    fn disconnect(&self) -> K::Future<'_, Result<(), Self::DisconnectError>>;
}
