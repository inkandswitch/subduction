//! Test utilities for `subduction_core`.
//!
//! This module provides deterministic peer-identity construction for tests and benchmarks.
//!
//! The signer-based helpers are gated behind the `test_utils` feature and rely on
//! [`subduction_crypto::test_utils::signer_from_seed`].
//!
//! Enable with the `test_utils` feature flag.

// Test utilities are allowed to panic for clearer test failures.
#![allow(clippy::expect_used, clippy::panic)]

use crate::peer::id::PeerId;

/// Deterministic [`PeerId`] derived from a u64 seed, **not** backed by a real signing key.
///
/// Suitable for tests that only need distinct peer identities and never need to sign anything.
/// Benches that need a peer to actually sign messages should use
/// [`peer_id_from_signer_seed`].
///
/// Uses BLAKE3 over the seed's little-endian bytes as a cheap, dep-free seeded PRNG
/// substitute.
#[must_use]
pub fn peer_id_from_seed(seed: u64) -> PeerId {
    let hash = blake3::hash(&seed.to_le_bytes());
    PeerId::new(*hash.as_bytes())
}

/// [`PeerId`] matching the verifying key of
/// [`subduction_crypto::test_utils::signer_from_seed(seed)`].
///
/// Use this when the bench needs both a signer and its corresponding `PeerId`.
///
/// Only available with the `test_utils` feature (which transitively enables
/// `subduction_crypto/test_utils`).
#[cfg(feature = "test_utils")]
#[must_use]
pub fn peer_id_from_signer_seed(seed: u64) -> PeerId {
    PeerId::from(subduction_crypto::test_utils::signer_from_seed(seed).verifying_key())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_id_from_seed_is_deterministic() {
        assert_eq!(peer_id_from_seed(42), peer_id_from_seed(42));
        assert_ne!(peer_id_from_seed(42), peer_id_from_seed(43));
    }

    #[cfg(feature = "test_utils")]
    #[test]
    fn peer_id_from_signer_seed_matches_signer_verifying_key() {
        let signer = subduction_crypto::test_utils::signer_from_seed(99);
        let peer = peer_id_from_signer_seed(99);
        assert_eq!(peer.as_bytes(), signer.verifying_key().as_bytes());
    }
}
