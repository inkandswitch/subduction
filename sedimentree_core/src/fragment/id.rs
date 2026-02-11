//! Causal identity for fragments.

use alloc::vec::Vec;

use crate::{crypto::digest::Digest, loose_commit::LooseCommit};

/// The causal identity of a fragment: the range it covers.
///
/// Two fragments with the same head and boundary cover the same causal
/// range, regardless of blob content, size, or checkpoints. This enables
/// future optimizations like re-compression and fragment covering without
/// changing the reconciliation protocol.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentId {
    #[n(0)]
    head: Digest<LooseCommit>,

    #[n(1)]
    boundary: Vec<Digest<LooseCommit>>,
}

impl FragmentId {
    /// Create from head and boundary digests.
    #[must_use]
    pub const fn new(head: Digest<LooseCommit>, boundary: Vec<Digest<LooseCommit>>) -> Self {
        Self { head, boundary }
    }

    /// The head commit digest.
    #[must_use]
    pub const fn head(&self) -> Digest<LooseCommit> {
        self.head
    }

    /// The boundary commit digests.
    #[must_use]
    pub const fn boundary(&self) -> &[Digest<LooseCommit>] {
        self.boundary.as_slice()
    }
}
