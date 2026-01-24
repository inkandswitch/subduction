//! Nonce tracking for handshake replay protection.
//!
//! Tracks `(PeerId, Nonce)` pairs from successful handshakes to prevent
//! replay attacks. Failed handshake attempts should *not* call [`NonceTracker::try_claim`]
//! to avoid denial-of-service via cache filling.
//!
//! # Implementation Notes
//!
//! - TTL should match [`MAX_PLAUSIBLE_DRIFT`](super::handshake::MAX_PLAUSIBLE_DRIFT) (10 minutes)
//! - No false positives: a fresh nonce must never be rejected
//! - Pruning is caller-driven via [`NonceTracker::prune`]

pub mod generational;

use core::time::Duration;

use futures_kind::FutureKind;

use crate::{peer::id::PeerId, timestamp::TimestampSeconds};

pub use crate::crypto::nonce::Nonce;

/// Error returned when a nonce has already been used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonceReused;

impl core::fmt::Display for NonceReused {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "nonce has already been used")
    }
}

impl core::error::Error for NonceReused {}

/// Tracks seen nonces for replay protection.
///
/// Only successful handshakes should call [`try_claim`](Self::try_claim) — failed
/// attempts must not fill the cache (DoS vector).
pub trait NonceTracker<K: FutureKind> {
    /// Attempt to claim a nonce from a successful handshake.
    ///
    /// Returns `Ok(())` if the nonce is fresh and has been recorded.
    /// Returns `Err(NonceReused)` if this `(peer, nonce)` pair was already seen.
    fn try_claim(
        &self,
        peer: PeerId,
        nonce: Nonce,
        timestamp: TimestampSeconds,
    ) -> K::Future<'_, Result<(), NonceReused>>;

    /// Prune entries older than `max_age` before `now`.
    fn prune(&self, now: TimestampSeconds, max_age: Duration) -> K::Future<'_, ()>;
}
