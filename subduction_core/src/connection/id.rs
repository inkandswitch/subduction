//! Types for identifying connections.

/// A unique identifier for a particular connection.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ConnectionId(usize);

impl ConnectionId {
    /// Create a new [`ConnectionId`] from a `usize`.
    #[must_use]
    pub const fn new(id: usize) -> Self {
        ConnectionId(id)
    }

    /// Get the inner `usize` representation of the [`ConnectionId`].
    #[must_use]
    pub const fn as_usize(&self) -> usize {
        self.0
    }
}

impl From<usize> for ConnectionId {
    fn from(value: usize) -> Self {
        ConnectionId(value)
    }
}

impl From<ConnectionId> for usize {
    fn from(value: ConnectionId) -> Self {
        value.0
    }
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "conn-{}", self.as_usize())
    }
}
