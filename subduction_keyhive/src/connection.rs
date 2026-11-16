//! Connection abstraction for the keyhive protocol.

use future_form::FutureForm;

use crate::{peer_id::KeyhivePeerId, signed_message::SignedMessage};

/// A connection to a peer for keyhive protocol messages.
///
/// The protocol only sends through it. Messages from the peer reach the
/// protocol through whatever owns the transport, such as `KeyhiveHandler`,
/// so there is no way to receive or disconnect here.
pub trait KeyhiveConnection<Async: FutureForm>: Clone {
    /// The error type returned when sending a message fails.
    type SendError: core::error::Error;

    /// Get the peer ID of the remote peer.
    fn peer_id(&self) -> KeyhivePeerId;

    /// Send a signed message to the peer.
    fn send(&self, message: SignedMessage) -> Async::Future<'_, Result<(), Self::SendError>>;
}
