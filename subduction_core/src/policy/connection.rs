//! Connection policy for controlling which peers can connect.

use core::error::Error;

use future_form::FutureForm;

use crate::peer::id::PeerId;

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy<K: FutureForm> {
    /// Error type returned when a connection is disallowed.
    type ConnectionDisallowed: Error;

    /// Authorize a connection from the given peer.
    ///
    /// Returns `Ok(())` if the connection is allowed, or an error if disallowed.
    fn authorize_connect(
        &self,
        peer: PeerId,
    ) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>>;
}
