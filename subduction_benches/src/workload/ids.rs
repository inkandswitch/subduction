//! Deterministic identifier generators.
//!
//! All functions are seeded so the same seed produces the same ID across runs.

use rand::{rngs::SmallRng, Rng, SeedableRng};
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_core::peer::id::PeerId;

/// Generate a deterministic [`CommitId`] from a seed.
#[must_use]
pub fn commit_id_from_seed(seed: u64) -> CommitId {
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(seed);
    rng.fill(&mut bytes);
    CommitId::new(bytes)
}

/// Generate a [`CommitId`] with exactly `zeros` leading zero bytes.
///
/// The byte immediately after the zero prefix is guaranteed non-zero so the effective leading-zero
/// count matches `zeros` exactly — useful for depth-metric benches where the boundary distribution
/// matters.
///
/// # Panics
///
/// Panics in debug builds if `zeros > 32`. Release builds saturate.
#[must_use]
pub fn commit_id_with_leading_zeros(zeros: usize, seed: u64) -> CommitId {
    let zeros = zeros.min(32);
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(seed);

    if let Some(tail) = bytes.get_mut(zeros..) {
        rng.fill(tail);
    }

    // Ensure the first non-zero byte is actually non-zero for precise depth control.
    if let Some(slot) = bytes.get_mut(zeros) {
        if *slot == 0 {
            *slot = 1;
        }
    }

    CommitId::new(bytes)
}

/// Generate a deterministic [`SedimentreeId`] from a seed.
#[must_use]
pub fn sedimentree_id_from_seed(seed: u64) -> SedimentreeId {
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(seed);
    rng.fill(&mut bytes);
    SedimentreeId::new(bytes)
}

/// Generate a deterministic [`PeerId`] from a seed.
///
/// _Note:_ this is a synthetic `[u8; 32]` identity — it does _not_ correspond to a real Ed25519
/// verifying key. For benches that need a signable peer, use
/// [`signers::signer_from_seed`](super::signers::signer_from_seed) and derive the peer id from
/// its verifying key.
#[must_use]
pub fn peer_id_from_seed(seed: u64) -> PeerId {
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(seed);
    rng.fill(&mut bytes);
    PeerId::new(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_id_seed_is_deterministic() {
        assert_eq!(commit_id_from_seed(42), commit_id_from_seed(42));
        assert_ne!(commit_id_from_seed(42), commit_id_from_seed(43));
    }

    #[test]
    fn commit_id_leading_zeros_respected() {
        for zeros in 0..=6 {
            for seed in 0..8 {
                let id = commit_id_with_leading_zeros(zeros, seed);
                let bytes = id.as_bytes();

                for (idx, byte) in bytes.iter().enumerate().take(zeros) {
                    assert_eq!(*byte, 0, "byte {idx} should be zero for zeros={zeros}");
                }

                if zeros < 32 {
                    if let Some(first_non_zero) = bytes.get(zeros) {
                        assert_ne!(*first_non_zero, 0, "boundary byte should be non-zero");
                    }
                }
            }
        }
    }

    #[test]
    fn commit_id_zero_count_saturates_at_32() {
        // Asking for more zeros than bytes should not panic.
        let _id = commit_id_with_leading_zeros(100, 0);
    }
}
