//! Storage policy for controlling read/write access to sedimentrees.

use alloc::vec::Vec;
use core::error::Error;

use futures_kind::FutureKind;
use sedimentree_core::id::SedimentreeId;

use crate::peer::id::PeerId;

/// A policy for allowing or disallowing storage operations.
///
/// This trait performs authorization checks. To get a capability that bundles
/// authorization with storage access, use [`Subduction::authorize_fetch`] or
/// [`Subduction::authorize_put`].
pub trait StoragePolicy<K: FutureKind> {
    /// Error type returned when fetch is disallowed.
    type FetchDisallowed: Error;

    /// Error type returned when put is disallowed.
    type PutDisallowed: Error;

    /// Authorize fetching data for the given sedimentree.
    ///
    /// Returns `Ok(())` if the fetch is allowed, or an error if disallowed.
    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::FetchDisallowed>>;

    /// Authorize putting data for the given sedimentree.
    ///
    /// Returns `Ok(())` if the put is allowed, or an error if disallowed.
    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PutDisallowed>>;

    /// Filter a list of sedimentree IDs to only those the peer is authorized to fetch.
    ///
    /// This is a batch authorization check, useful for filtering subscriptions
    /// when forwarding updates to peers.
    ///
    /// The default implementation checks each ID individually, but implementations
    /// may provide more efficient batch checks.
    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> K::Future<'_, Vec<SedimentreeId>>;
}
