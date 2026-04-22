//! Deterministic blob and blob-meta generators.

use rand::{rngs::SmallRng, Rng, SeedableRng};
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    crypto::digest::Digest,
};

/// Fill a [`Blob`] of exactly `size` bytes with deterministic pseudo-random data.
#[must_use]
pub fn blob_from_seed(seed: u64, size: usize) -> Blob {
    let mut data = vec![0u8; size];
    let mut rng = SmallRng::seed_from_u64(seed);
    rng.fill(data.as_mut_slice());
    Blob::new(data)
}

/// Synthetic `BlobMeta` with a random 32-byte digest.
///
/// This does _not_ hash a real blob — use [`blob_meta_from_blob`] when you need a digest that
/// corresponds to actual content. The synthetic form is useful when a bench needs `BlobMeta`
/// values but doesn't care about digest integrity (e.g., sedimentree structural benches).
#[must_use]
pub fn synthetic_blob_meta(seed: u64, size: u64) -> BlobMeta {
    let mut bytes = [0u8; 32];
    let mut rng = SmallRng::seed_from_u64(seed);
    rng.fill(&mut bytes);
    BlobMeta::from_digest_size(Digest::force_from_bytes(bytes), size)
}

/// `BlobMeta` computed from an actual blob (hashes the content via BLAKE3).
#[must_use]
pub fn blob_meta_from_blob(blob: &Blob) -> BlobMeta {
    BlobMeta::new(blob)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_size_matches_request() {
        for size in [0, 1, 64, 4096, 1_000_000] {
            let blob = blob_from_seed(0, size);
            assert_eq!(blob.as_slice().len(), size);
        }
    }

    #[test]
    fn same_seed_same_blob() {
        assert_eq!(
            blob_from_seed(42, 1024).as_slice(),
            blob_from_seed(42, 1024).as_slice(),
        );
    }

    #[test]
    fn different_seeds_different_blobs() {
        let a = blob_from_seed(1, 256);
        let b = blob_from_seed(2, 256);
        assert_ne!(a.as_slice(), b.as_slice());
    }
}
