//! Test utilities for `subduction_crypto`.
//!
//! This module provides deterministic signer construction for tests and benchmarks.
//!
//! Enable with the `test_utils` feature flag.

// Test utilities are allowed to panic for clearer test failures.
#![allow(clippy::expect_used, clippy::panic)]

use crate::signer::memory::MemorySigner;

/// Build a [`MemorySigner`] from a u64 seed.
///
/// The seed is interleaved across the 32-byte signing key via a simple byte-shuffle scheme.
/// This is _not_ cryptographically meaningful — its only job is to produce distinct, stable
/// keys from distinct seeds. Two distinct u64 seeds always produce distinct signing keys
/// (they differ in the low 8 bytes).
///
/// # Example
///
/// ```
/// use subduction_crypto::test_utils::signer_from_seed;
///
/// let a = signer_from_seed(42);
/// let b = signer_from_seed(42);
/// assert_eq!(a.verifying_key().as_bytes(), b.verifying_key().as_bytes());
/// ```
#[must_use]
pub fn signer_from_seed(seed: u64) -> MemorySigner {
    let mut bytes = [0u8; 32];

    // Every iteration's `idx` is in 0..32 and every intermediate is explicitly masked to a
    // byte before narrowing, so the truncating casts below are lossless by construction.
    for (idx, slot) in bytes.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let shift: u32 = ((idx as u32) & 0x07) * 8;

        #[allow(clippy::cast_possible_truncation)]
        let seed_byte = ((seed >> shift) & 0xff) as u8;

        #[allow(clippy::cast_possible_truncation)]
        let idx_byte = (idx & 0xff) as u8;

        *slot = seed_byte ^ idx_byte;
    }

    MemorySigner::from_bytes(&bytes)
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
    fn distinct_seeds_produce_distinct_keys() {
        assert_ne!(
            signer_from_seed(1).verifying_key().to_bytes(),
            signer_from_seed(2).verifying_key().to_bytes(),
        );
    }
}
