//! Ephemeral pubsub topic identifier.
//!
//! [`Topic`] is an opaque 32-byte identifier for ephemeral messaging
//! channels. While topics often correspond to [`SedimentreeId`]s
//! (via `From<SedimentreeId>`), they are intentionally decoupled —
//! ephemeral messaging doesn't require a backing sedimentree.
//!
//! [`SedimentreeId`]: sedimentree_core::id::SedimentreeId

use core::fmt;

use sedimentree_core::id::SedimentreeId;

/// An opaque 32-byte topic identifier for ephemeral pubsub.
///
/// Use [`From<SedimentreeId>`] to scope ephemeral messages to a
/// specific sedimentree, or construct directly for non-sedimentree
/// topics.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Topic([u8; 32]);

impl Topic {
    /// Create a new topic from raw bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32-byte topic identifier.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Topic({:02x}{:02x}…)", self.0[0], self.0[1])
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0[..4] {
            write!(f, "{byte:02x}")?;
        }
        write!(f, "…")
    }
}

impl From<SedimentreeId> for Topic {
    fn from(id: SedimentreeId) -> Self {
        Self(*id.as_bytes())
    }
}

impl From<Topic> for SedimentreeId {
    fn from(topic: Topic) -> Self {
        SedimentreeId::new(topic.0)
    }
}
