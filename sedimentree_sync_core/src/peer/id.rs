//! A simple wrapper around a String to represent a Peer ID.

use std::fmt::Write;

/// A Peer ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PeerId([u8; 32]);

impl PeerId {
    /// Create a new [`PeerId`].
    #[must_use]
    pub const fn new(id: [u8; 32]) -> Self {
        Self(id)
    }

    /// Get the byte array representation of the [`PeerId`].
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Get the slice representation of the [`PeerId`].
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        to_hex(self.as_slice()).fmt(f)
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        #[allow(clippy::expect_used)]
        write!(&mut s, "{b:02x}").expect("preallocated length should be sufficient");
    }
    s
}
