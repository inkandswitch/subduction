//! Causal identity for loose commits.

use crate::{crypto::digest::Digest, loose_commit::LooseCommit};

/// The causal identity of a loose commit: its content digest.
///
/// For commits, the causal identity happens to be the content hash itself.
/// This newtype exists for symmetry with [`FragmentId`](crate::fragment::id::FragmentId),
/// where the causal identity (head + boundary range) is genuinely distinct
/// from the content hash.
///
/// Two commits with the same [`CommitId`] are the same item for
/// set reconciliation, regardless of parent or blob metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CommitId(Digest<LooseCommit>);

impl CommitId {
    /// Create from a commit digest.
    #[must_use]
    pub const fn new(digest: Digest<LooseCommit>) -> Self {
        Self(digest)
    }

    /// The underlying commit digest.
    #[must_use]
    pub const fn digest(&self) -> Digest<LooseCommit> {
        self.0
    }
}
