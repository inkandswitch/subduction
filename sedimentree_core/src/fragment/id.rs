//! Causal identity for fragments.

use alloc::collections::BTreeSet;

use crate::{crypto::digest::Digest, loose_commit::LooseCommit};

/// The causal identity of a fragment: a BLAKE3 hash of its head and boundary.
///
/// Computed from `BLAKE3(head || sorted(dedup(boundary)))`. The boundary is
/// sorted and deduplicated before hashing to ensure deterministic output
/// regardless of input ordering or duplicates.
///
/// Two fragments with the same head and boundary cover the same causal
/// range, regardless of blob content, size, or checkpoints.
///
/// This newtype exists in parallel with
/// [`CommitId`](crate::loose_commit::id::CommitId), which wraps a content
/// digest directly. For fragments, the causal identity is the range (head +
/// boundary), not the content hash.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentId([u8; 32]);

impl FragmentId {
    /// Compute the causal identity from head and boundary digests.
    ///
    /// The boundary must be a [`BTreeSet`], which guarantees sorted,
    /// deduplicated iteration order.
    #[must_use]
    pub fn new(head: Digest<LooseCommit>, boundary: &BTreeSet<Digest<LooseCommit>>) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(head.as_bytes());
        for b in boundary {
            hasher.update(b.as_bytes());
        }
        Self(*hasher.finalize().as_bytes())
    }

    /// The raw bytes of the identity hash.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl core::fmt::Debug for FragmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "FragmentId(")?;
        for byte in &self.0[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…)")
    }
}

impl core::fmt::Display for FragmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for FragmentId {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let bytes: [u8; 32] = u.arbitrary()?;
        Ok(Self(bytes))
    }
}
