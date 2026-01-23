//! Policies for controlling access in Subduction.

use futures_kind::FutureKind;
use sedimentree_core::id::SedimentreeId;

use crate::peer::id::PeerId;

/// A policy for allowing or disallowing connections from peers.
pub trait ConnectionPolicy<K: FutureKind> {
    /// Check if a connection from the given peer is allowed.
    fn is_connect_allowed(&self, peer: PeerId) -> K::Future<'_, bool>;
}

/// A policy for allowing or disallowing storage operations.
pub trait StoragePolicy<K: FutureKind> {
    /// Check if fetching data for the given sedimentree is allowed.
    fn is_fetch_allowed(&self, peer: PeerId, sedimentree_id: SedimentreeId) -> K::Future<'_, bool>;

    /// Check if putting data for the given sedimentree is allowed.
    fn is_put_allowed(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, bool>;
}
