//! Causal identity for fragments.

use crate::crypto::digest::Digest;
use crate::loose_commit::LooseCommit;

/// The causal identity of a fragment: its head commit digest.
///
/// Since the fragmentation algorithm is deterministic given a head commit
/// and the depth metric, the head uniquely identifies the fragment's
/// causal range. The boundary is deterministically derived from the head
/// and the DAG structure.
///
/// This newtype exists in parallel with
/// [`CommitId`](crate::loose_commit::id::CommitId) to keep fragment
/// fingerprints (`Fingerprint<FragmentId>`) type-distinct from commit
/// fingerprints (`Fingerprint<CommitId>`).
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentId(Digest<LooseCommit>);

impl FragmentId {
    /// Create a [`FragmentId`] from a fragment's head digest.
    #[must_use]
    pub const fn new(head: Digest<LooseCommit>) -> Self {
        Self(head)
    }

    /// The head digest.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.0
    }
}

impl core::fmt::Debug for FragmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "FragmentId(")?;
        for byte in &self.0.as_bytes()[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…)")
    }
}

impl core::fmt::Display for FragmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in self.0.as_bytes() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for FragmentId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self(Digest::arbitrary(u)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_head_produces_same_id() {
        let head = Digest::from_bytes([1u8; 32]);
        let id1 = FragmentId::new(head);
        let id2 = FragmentId::new(head);
        assert_eq!(id1, id2);
    }

    #[test]
    fn different_head_produces_different_id() {
        let id1 = FragmentId::new(Digest::from_bytes([1u8; 32]));
        let id2 = FragmentId::new(Digest::from_bytes([9u8; 32]));
        assert_ne!(id1, id2);
    }
}
