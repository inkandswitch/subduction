use futures_kind::FutureKind;
use sedimentree_core::id::SedimentreeId;

use crate::peer::id::PeerId;

/// A policy for allowing or disallowing connections from peers.
pub trait Policy<K: FutureKind> {
    /// Check if a connection from the given peer is allowed.
    ///
    /// # Errors
    ///
    /// * Returns [`ConnectionDisallowed`] if the connection is not allowed.
    fn is_connect_allowed(&self, peer: &PeerId) -> K::Future<'_, bool>;

    fn is_fetch_allowed(&self, peer: PeerId, sedimentree_id: SedimentreeId) -> K::Future<'_, bool>;

    fn is_put_allowed(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, bool>;
}
