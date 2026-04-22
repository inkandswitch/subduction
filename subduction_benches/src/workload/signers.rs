//! Deterministic signer construction for benchmark scenarios.

use subduction_core::peer::id::PeerId;
use subduction_crypto::signer::memory::MemorySigner;

/// Build a `MemorySigner` seeded with a 32-byte-repeated pattern derived from `seed`.
///
/// This matches the convention used in `subduction_websocket/benches/e2e.rs`
/// (`MemorySigner::from_bytes(&[seed; 32])`) but accepts a full `u64` seed via BLAKE3-free
/// pseudo-random expansion so benches can spawn more than 256 distinct signers when they need
/// many peers.
#[must_use]
pub fn signer_from_seed(seed: u64) -> MemorySigner {
    let mut bytes = [0u8; 32];
    // Interleave the seed across the 32-byte key so small-seed benches produce visually-distinct
    // signing keys. We don't need cryptographic quality here — just determinism.
    for (idx, slot) in bytes.iter_mut().enumerate() {
        let shift = (idx as u32 % 8) * 8;
        *slot = ((seed >> shift) & 0xff) as u8 ^ (idx as u8);
    }
    MemorySigner::from_bytes(&bytes)
}

/// The `PeerId` corresponding to [`signer_from_seed`] — i.e. `PeerId::from(signer.verifying_key())`.
#[must_use]
pub fn peer_id_from_signer_seed(seed: u64) -> PeerId {
    PeerId::from(signer_from_seed(seed).verifying_key())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signer_is_deterministic() {
        assert_eq!(
            signer_from_seed(42).verifying_key().to_bytes(),
            signer_from_seed(42).verifying_key().to_bytes(),
        );
    }

    #[test]
    fn different_seeds_different_keys() {
        assert_ne!(
            signer_from_seed(1).verifying_key().to_bytes(),
            signer_from_seed(2).verifying_key().to_bytes(),
        );
    }

    #[test]
    fn peer_id_matches_signer_key() {
        let signer = signer_from_seed(99);
        let peer = peer_id_from_signer_seed(99);
        assert_eq!(peer.as_bytes(), signer.verifying_key().as_bytes());
    }
}
