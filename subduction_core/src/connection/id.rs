//! Types for identifying connections.

use std::sync::atomic::{AtomicU64, Ordering};

static LAST_CONNECTION_ID: AtomicU64 = AtomicU64::new(0);

/// A unique identifier for a particular connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Create a new [`ConnectionId`] from a `u64`.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        ConnectionId(id)
    }

    /// Get the next sequential [`ConnectionId`].
    #[must_use]
    pub fn generate() -> Self {
        let id = LAST_CONNECTION_ID.fetch_add(1, Ordering::SeqCst);
        ConnectionId(id)
    }

    /// Get the inner `u64` representation of the [`ConnectionId`].
    #[must_use]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for ConnectionId {
    fn from(value: u64) -> Self {
        ConnectionId(value)
    }
}

impl From<ConnectionId> for u64 {
    fn from(value: ConnectionId) -> Self {
        value.0
    }
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn-{}", self.as_u64())
    }
}
