//! Remote heads: a peer's current tips for a sedimentree.
//!
//! The wire struct is copied verbatim from
//! `legacy/subduction_core/src/remote_heads.rs`. The legacy observer /
//! notifier machinery does not come along: staleness filtering becomes
//! machine state, and notifications become application-event effects.

use alloc::vec::Vec;

use sedimentree_core::loose_commit::id::CommitId;

/// A remote peer's heads for a sedimentree, with a monotonic counter
/// for ordering in the face of out-of-order delivery.
///
/// The counter is scoped per-peer and incremented each time the sender
/// sends a message carrying heads. Receivers should only accept updates
/// where `counter` is strictly greater than the last seen value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoteHeads {
    /// Monotonic per-peer counter — higher means newer.
    pub counter: u64,

    /// The heads (tip commits) of the sedimentree.
    pub heads: Vec<CommitId>,
}

impl RemoteHeads {
    /// Returns `true` if there are no heads.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }
}
