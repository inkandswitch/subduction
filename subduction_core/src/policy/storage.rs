//! Storage policy for controlling read/write access to sedimentrees.

use alloc::vec::Vec;
use core::error::Error;

use future_form::FutureForm;
use sedimentree_core::id::SedimentreeId;

use subduction_crypto::verified_author::VerifiedAuthor;

use crate::{peer::id::PeerId, sync_session::SyncPolicyRejectionKind};

/// A policy for allowing or disallowing storage operations.
///
/// This trait performs authorization checks. To get a capability that bundles
/// authorization with storage access, use [`Subduction::authorize_fetch`] or
/// [`Subduction::authorize_put`].
pub trait StoragePolicy<Async: FutureForm> {
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
    ) -> Async::Future<'_, Result<(), Self::FetchDisallowed>>;

    /// Authorize putting data for the given sedimentree.
    ///
    /// The `author` is a [`VerifiedAuthor`] — the compiler guarantees
    /// the author's signing key has been cryptographically verified
    /// before this method is called.
    ///
    /// Returns `Ok(())` if the put is allowed, or an error if disallowed.
    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::PutDisallowed>>;

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
    ) -> Async::Future<'_, Vec<SedimentreeId>>;

    /// Classify a put rejection for sync-session diagnostics.
    ///
    /// Implementations may override this to preserve stable error categories;
    /// the default keeps custom policies source-compatible and reports an
    /// implementation-specific rejection.
    fn classify_put_rejection(&self, _error: &Self::PutDisallowed) -> SyncPolicyRejectionKind {
        SyncPolicyRejectionKind::Other
    }
}
