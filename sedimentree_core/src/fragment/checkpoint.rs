//! Checkpoint type for fragment interior commits.
//!
//! A [`Checkpoint`] is a truncated commit identifier marking a commit that falls
//! within a fragment's causal range. Checkpoints enable efficient coverage
//! checks (`supports_block`) without storing full 32-byte identifiers.

use core::fmt;

use crate::loose_commit::id::CommitId;

/// A truncated commit identifier marking a commit within a fragment's range.
///
/// Checkpoints are stored as 12-byte truncations of the full 32-byte
/// commit identifier. This provides:
/// - Compact storage (~62% savings vs full identifier)
/// - Negligible random collision probability (~10⁻¹⁷ at 1M items)
/// - Efficient set membership checks
///
/// # Security Properties
///
/// - **Random collision**: ~N²/2⁹⁶ where N is set size
/// - **Adversarial collision**: ~2⁴⁸ work (birthday attack on 96 bits)
/// - **Preimage**: ~2⁹⁶ work
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Checkpoint([u8; 12]);

impl Checkpoint {
    /// Create a checkpoint from a full commit identifier.
    ///
    /// The identifier is truncated to 12 bytes.
    #[must_use]
    pub fn new(id: CommitId) -> Self {
        let mut bytes = [0u8; 12];
        bytes.copy_from_slice(&id.as_bytes()[..12]);
        Self(bytes)
    }

    /// Create a checkpoint from raw bytes.
    ///
    /// This is the inverse of [`as_bytes`](Self::as_bytes).
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 12]) -> Self {
        Self(bytes)
    }

    /// The raw bytes of the truncated checkpoint.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 12] {
        &self.0
    }
}

impl From<CommitId> for Checkpoint {
    fn from(id: CommitId) -> Self {
        Self::new(id)
    }
}

impl From<[u8; 12]> for Checkpoint {
    fn from(bytes: [u8; 12]) -> Self {
        Self::from_bytes(bytes)
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
        let bytes: [u8; 12] = u.arbitrary()?;
        Ok(Self(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_from_commit_id() {
        let id = CommitId::new([42u8; 32]);
        let checkpoint = Checkpoint::new(id);
        assert_eq!(checkpoint.as_bytes(), &[42u8; 12]);
    }

    #[test]
    fn different_suffixes_same_checkpoint() {
        let mut bytes_a = [0u8; 32];
        bytes_a[31] = 1;
        let mut bytes_b = [0u8; 32];
        bytes_b[31] = 2;

        let a = Checkpoint::new(CommitId::new(bytes_a));
        let b = Checkpoint::new(CommitId::new(bytes_b));
        assert_eq!(a, b); // Same first 12 bytes
    }
}
