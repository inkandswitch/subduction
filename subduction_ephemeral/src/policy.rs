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
use subduction_core::peer::id::PeerId;

use crate::topic::Topic;

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
        id: Topic,
    ) -> K::Future<'_, Result<(), Self::SubscribeDisallowed>>;

    /// Check whether `peer` may publish ephemeral messages to `id`.
    fn authorize_publish(
        &self,
        peer: PeerId,
        id: Topic,
    ) -> K::Future<'_, Result<(), Self::PublishDisallowed>>;

    /// Batch-filter subscribers to only those currently authorized.
    ///
    /// Called before fan-out to re-check authorization (handles revocation).
    fn filter_authorized_subscribers(
        &self,
        id: Topic,
        peers: Vec<PeerId>,
    ) -> K::Future<'_, Vec<PeerId>>;
}

/// An open policy that allows all ephemeral operations.
///
/// Useful for development and testing. In production, use a policy
/// backed by your authorization system (e.g., Keyhive).
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenEphemeralPolicy;

#[future_form(Sendable, Local)]
impl<K: FutureForm> EphemeralPolicy<K> for OpenEphemeralPolicy {
    type SubscribeDisallowed = Infallible;
    type PublishDisallowed = Infallible;

    fn authorize_subscribe(
        &self,
        _peer: PeerId,
        _id: Topic,
    ) -> K::Future<'_, Result<(), Self::SubscribeDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn authorize_publish(
        &self,
        _peer: PeerId,
        _id: Topic,
    ) -> K::Future<'_, Result<(), Self::PublishDisallowed>> {
        K::from_future(async { Ok(()) })
    }

    fn filter_authorized_subscribers(
        &self,
        _id: Topic,
        peers: Vec<PeerId>,
    ) -> K::Future<'_, Vec<PeerId>> {
        K::from_future(async { peers })
    }
}
