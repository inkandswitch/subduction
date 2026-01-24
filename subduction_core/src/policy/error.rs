//! Error types for policy and capability operations.

/// A monotonic counter tracking policy/membership changes for revocation detection.
///
/// When a capability is issued, it captures the current generation. Before using
/// the capability, the current generation can be checked - if it differs, the
/// capability may have been revoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Generation(u64);

impl Generation {
    /// Create a new generation counter.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the underlying value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Generation {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Generation> for u64 {
    fn from(generation: Generation) -> Self {
        generation.0
    }
}

/// Error returned when a capability has been revoked due to permission changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("capability revoked: permission generation has changed")]
pub struct CapabilityRevoked {
    /// The generation when the capability was issued.
    pub issued_generation: Generation,

    /// The current generation.
    pub current_generation: Generation,
}
