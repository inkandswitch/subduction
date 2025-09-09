//! A simple wrapper around a String to represent a Peer ID.

use serde::{Deserialize, Serialize};

/// A Peer ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PeerId([u8; 32]); // FIXME shoudl be ed25519 VK?

// impl PeerId {
//     /// Create a new [`PeerId`].
//     pub fn new(id: String) -> Self {
//         Self(id)
//     }
//
//     /// Get the string representation of the [`PeerId`].
//     pub fn as_str(&self) -> &str {
//         &self.0
//     }
// }
//
// impl From<String> for PeerId {
//     fn from(id: String) -> Self {
//         Self(id)
//     }
// }
//
// impl std::fmt::Display for PeerId {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }
