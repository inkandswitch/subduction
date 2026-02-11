//! Operational statistics for sync operations.
//!
//! These types track the result of sync operations (how many items were
//! sent/received). They are _not_ wire types — they are never serialized
//! or sent over the network.

/// Statistics from a sync operation.
///
/// Tracks how many commits and fragments were sent and received during a sync.
/// The "sent" counts reflect items that were _successfully_ sent over the wire,
/// not just items that were requested.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncStats {
    /// Number of commits received from the peer.
    pub commits_received: usize,

    /// Number of fragments received from the peer.
    pub fragments_received: usize,

    /// Number of commits successfully sent to the peer.
    pub commits_sent: usize,

    /// Number of fragments successfully sent to the peer.
    pub fragments_sent: usize,
}

impl SyncStats {
    /// Create stats with zero counts.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            commits_received: 0,
            fragments_received: 0,
            commits_sent: 0,
            fragments_sent: 0,
        }
    }

    /// Total items received (commits + fragments).
    #[must_use]
    pub const fn total_received(&self) -> usize {
        self.commits_received + self.fragments_received
    }

    /// Total items sent (commits + fragments).
    #[must_use]
    pub const fn total_sent(&self) -> usize {
        self.commits_sent + self.fragments_sent
    }

    /// Returns true if no data was exchanged.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.commits_received == 0
            && self.fragments_received == 0
            && self.commits_sent == 0
            && self.fragments_sent == 0
    }
}

/// Number of commits and fragments sent in a single
/// [`send_requested_data`](crate::subduction::Subduction::send_requested_data) call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendCount {
    /// Number of commits sent.
    pub commits: usize,

    /// Number of fragments sent.
    pub fragments: usize,
}
