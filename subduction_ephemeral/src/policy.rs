//! Authorization policy for ephemeral messaging.
//!
//! Ephemeral authorization is a distinct concern from storage authorization.
//! [`StoragePolicy`] answers "can this peer read/write persistent data?" —
//! the wrong question for ephemeral messages, which are never stored.
//! [`EphemeralPolicy`] answers "can this peer participate in ephemeral
//! messaging for this topic?"
//!
//! # Policy Hierarchy
//!
//! ```text
//! ConnectionPolicy   — "can this peer connect?"
//! StoragePolicy      — "can this peer read/write persistent data?"
//! EphemeralPolicy    — "can this peer subscribe/publish ephemeral messages?"
//! ```
//!
//! Each is independent. `ConnectionPolicy` and `StoragePolicy` live in
//! `subduction_core`; `EphemeralPolicy` lives here.
//!
//! # Use Cases
//!
//! | Scenario               | `authorize_subscribe` | `authorize_publish` |
//! |------------------------|-----------------------|---------------------|
//! | Full participant       | `Ok(())`              | `Ok(())`            |
//! | Read-only spectator    | `Ok(())`              | `Err(...)`          |
//! | Write-only broadcaster | `Err(...)`            | `Ok(())`            |
//! | No ephemeral access    | `Err(...)`            | `Err(...)`          |
//!
//! [`StoragePolicy`]: subduction_core::policy::storage::StoragePolicy

use alloc::vec::Vec;
use core::convert::Infallible;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::id::SedimentreeId;
use subduction_core::peer::id::PeerId;

/// Policy for ephemeral message authorization.
///
/// Separate from `StoragePolicy` — a peer may have storage access
/// without ephemeral access, or vice versa.
///
/// `authorize_subscribe` is checked at subscribe time (courtesy) and
/// re-checked at forward time (security invariant — handles revocation).
pub trait EphemeralPolicy<K: FutureForm + ?Sized> {
    /// Error returned when a subscribe is disallowed.
    type SubscribeDisallowed: core::error::Error;

    /// Error returned when a publish is disallowed.
    type PublishDisallowed: core::error::Error;

    /// Check whether `peer` may subscribe to ephemeral messages for `id`.
    fn authorize_subscribe(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::SubscribeDisallowed>>;

    /// Check whether `peer` may publish ephemeral messages to `id`.
    fn authorize_publish(
        &self,
        peer: PeerId,
        id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PublishDisallowed>>;

    /// Batch-filter subscribers to only those currently authorized.
    ///
    /// Called before fan-out to re-check authorization (handles revocation).
    fn filter_authorized_subscribers(
        &self,
        id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> K::Future<'_, Vec<PeerId>>;
}

/// An open policy that allows all ephemeral operations.
///
// ── OpenPolicy blanket impl ─────────────────────────────────────────

/// Implements [`EphemeralPolicy`] for [`OpenPolicy`] (allow all).
///
/// This enables `OpenPolicy` to be used as a single `P` type satisfying
/// `ConnectionPolicy + StoragePolicy + EphemeralPolicy`.
#[future_form(Sendable, Local)]
impl<K: FutureForm> EphemeralPolicy<K> for subduction_core::policy::open::OpenPolicy {
    type SubscribeDisallowed = Infallible;
    type PublishDisallowed = Infallible;

    fn authorize_subscribe(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::SubscribeDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn authorize_publish(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PublishDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn filter_authorized_subscribers(
        &self,
        _id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> K::Future<'_, Vec<PeerId>> {
        K::from_future(async { peers })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use alloc::vec;

    use subduction_core::policy::open::OpenPolicy;

    use super::*;

    fn test_peer_id() -> PeerId {
        PeerId::new([0xAB; 32])
    }

    fn test_sedimentree_id() -> SedimentreeId {
        SedimentreeId::new([0xCC; 32])
    }

    #[tokio::test]
    async fn open_policy_allows_subscribe() {
        let result = EphemeralPolicy::<Sendable>::authorize_subscribe(
            &OpenPolicy,
            test_peer_id(),
            test_sedimentree_id(),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn open_policy_allows_publish() {
        let result = EphemeralPolicy::<Sendable>::authorize_publish(
            &OpenPolicy,
            test_peer_id(),
            test_sedimentree_id(),
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn open_policy_passes_through_subscribers() {
        let peers = vec![
            PeerId::new([0x01; 32]),
            PeerId::new([0x02; 32]),
            PeerId::new([0x03; 32]),
        ];
        let result = EphemeralPolicy::<Sendable>::filter_authorized_subscribers(
            &OpenPolicy,
            test_sedimentree_id(),
            peers.clone(),
        )
        .await;
        assert_eq!(result, peers);
    }
}

// ── OpenEphemeralPolicy ─────────────────────────────────────────────

/// Standalone open ephemeral policy. Useful for development and testing.
/// In production, use a policy backed by your authorization system
/// (e.g., Keyhive).
///
/// See also: [`OpenPolicy`](subduction_core::policy::open::OpenPolicy)
/// which now also implements `EphemeralPolicy`.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenEphemeralPolicy;

#[future_form(Sendable, Local)]
impl<K: FutureForm> EphemeralPolicy<K> for OpenEphemeralPolicy {
    type SubscribeDisallowed = Infallible;
    type PublishDisallowed = Infallible;

    fn authorize_subscribe(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::SubscribeDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn authorize_publish(
        &self,
        _peer: PeerId,
        _id: SedimentreeId,
    ) -> K::Future<'_, Result<(), Self::PublishDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn filter_authorized_subscribers(
        &self,
        _id: SedimentreeId,
        peers: Vec<PeerId>,
    ) -> K::Future<'_, Vec<PeerId>> {
        K::from_future(async { peers })
    }
}
