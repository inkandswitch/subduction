//! Checkpoint type for fragment interior commits.
//!
//! A [`Checkpoint`] is a truncated commit digest marking a commit that falls
//! within a fragment's causal range. Checkpoints enable efficient coverage
//! checks (`supports_block`) without storing full 32-byte digests.

use core::fmt;

use crate::{
    crypto::{digest::Digest, truncated::Truncated},
    loose_commit::LooseCommit,
};

/// A truncated commit digest marking a commit within a fragment's range.
///
/// Checkpoints are stored as 12-byte truncations of the full 32-byte BLAKE3
/// commit digest. This provides:
/// - Compact storage (~62% savings vs full digest)
/// - Negligible random collision probability (~10⁻¹⁷ at 1M items)
/// - Efficient set membership checks
///
/// # Security Properties
///
/// - **Random collision**: ~N²/2⁹⁶ where N is set size
/// - **Adversarial collision**: ~2⁴⁸ work (birthday attack on 96 bits)
/// - **Preimage**: ~2⁹⁶ work
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode)]
#[cbor(transparent)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Checkpoint(#[n(0)] Truncated<Digest<LooseCommit>>);

impl Checkpoint {
    /// Create a checkpoint from a full commit digest.
    ///
    /// The digest is truncated to 12 bytes.
    #[must_use]
    pub fn new(digest: Digest<LooseCommit>) -> Self {
        Self(Truncated::new(digest))
    }

    /// Create a checkpoint from an already-truncated value.
    #[must_use]
    pub const fn from_truncated(truncated: Truncated<Digest<LooseCommit>>) -> Self {
        Self(truncated)
    }

    /// The underlying truncated digest.
    #[must_use]
    pub const fn as_truncated(&self) -> &Truncated<Digest<LooseCommit>> {
        &self.0
    }

    /// The raw bytes of the truncated checkpoint.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 12] {
        self.0.as_bytes()
    }
}

impl From<Digest<LooseCommit>> for Checkpoint {
    fn from(digest: Digest<LooseCommit>) -> Self {
        Self::new(digest)
    }
}

impl From<Truncated<Digest<LooseCommit>>> for Checkpoint {
    fn from(truncated: Truncated<Digest<LooseCommit>>) -> Self {
        Self::from_truncated(truncated)
    }
}

impl fmt::Debug for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Checkpoint(")?;
        for byte in &self.as_bytes()[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…)")
    }
}

impl fmt::Display for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.as_bytes() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Checkpoint {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let truncated: Truncated<Digest<LooseCommit>> = u.arbitrary()?;
        Ok(Self(truncated))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_from_digest() {
        let digest = Digest::<LooseCommit>::from_bytes([42u8; 32]);
        let checkpoint = Checkpoint::new(digest);
        assert_eq!(checkpoint.as_bytes(), &[42u8; 12]);
    }

    #[test]
    fn checkpoint_equality() {
        let digest = Digest::<LooseCommit>::from_bytes([1u8; 32]);
        let a = Checkpoint::new(digest);
        let b = Checkpoint::new(digest);
        assert_eq!(a, b);
    }

    #[test]
    fn different_suffixes_same_checkpoint() {
        let mut bytes_a = [0u8; 32];
        bytes_a[31] = 1;
        let mut bytes_b = [0u8; 32];
        bytes_b[31] = 2;

        let a = Checkpoint::new(Digest::<LooseCommit>::from_bytes(bytes_a));
        let b = Checkpoint::new(Digest::<LooseCommit>::from_bytes(bytes_b));
        assert_eq!(a, b); // Same first 12 bytes
    }
}
