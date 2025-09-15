//! A simple wrapper around a String to represent a Peer ID.

use serde::{Deserialize, Serialize};

/// A Peer ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Create a new [`PeerId`].
    pub fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// Get the byte array representation of the [`PeerId`].
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Get the slice representation of the [`PeerId`].
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b58 = base58::ToBase58::to_base58(self.as_slice());
        b58.fmt(f)
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b58 = base58::ToBase58::to_base58(self.as_slice());
        b58.fmt(f)
    }
}
