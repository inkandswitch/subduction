//! Session management types for HTTP long-polling.
//!
//! Sessions track authenticated connections between handshake and disconnect.

use alloc::string::String;
use core::fmt;

/// A 128-bit session identifier.
///
/// Encoded as base58 for use in HTTP headers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode)]
#[cbor(transparent)]
pub struct SessionId(#[cbor(with = "minicbor::bytes")] [u8; 16]);

impl SessionId {
    /// Create a new random session ID.
    ///
    /// # Panics
    ///
    /// Panics if the system's random number generator fails.
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn random() -> Self {
        let mut bytes = [0u8; 16];
        getrandom::getrandom(&mut bytes).expect("failed to generate random bytes");
        Self(bytes)
    }

    /// Create a session ID from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes of the session ID.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Encode as a base58 string for use in HTTP headers.
    #[must_use]
    pub fn to_base58(&self) -> String {
        base58::ToBase58::to_base58(&self.0[..])
    }

    /// Decode from a base58 string.
    ///
    /// # Errors
    ///
    /// Returns `None` if the string is not valid base58 or is not exactly 16 bytes.
    #[must_use]
    pub fn from_base58(s: &str) -> Option<Self> {
        let bytes = base58::FromBase58::from_base58(s).ok()?;
        let arr: [u8; 16] = bytes.try_into().ok()?;
        Some(Self(arr))
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionId({})", self.to_base58())
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base58())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_id_base58_roundtrip() {
        let id = SessionId::random();
        let encoded = id.to_base58();
        let decoded = SessionId::from_base58(&encoded).expect("decode");
        assert_eq!(id, decoded);
    }

    #[test]
    fn session_id_from_bytes_roundtrip() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let id = SessionId::from_bytes(bytes);
        assert_eq!(*id.as_bytes(), bytes);
    }

    #[test]
    fn session_id_cbor_roundtrip() {
        let id = SessionId::random();
        let bytes = minicbor::to_vec(&id).expect("encode");
        let decoded: SessionId = minicbor::decode(&bytes).expect("decode");
        assert_eq!(id, decoded);
    }

    #[test]
    fn session_id_invalid_base58_returns_none() {
        assert!(SessionId::from_base58("invalid!@#$").is_none());
    }

    #[test]
    fn session_id_wrong_length_returns_none() {
        assert!(SessionId::from_base58("abc").is_none());
    }

    #[test]
    fn session_id_debug_shows_base58() {
        let id = SessionId::from_bytes([0; 16]);
        let debug = format!("{id:?}");
        assert!(debug.starts_with("SessionId("));
        assert!(debug.ends_with(')'));
    }

    #[test]
    fn session_id_display_shows_base58() {
        let id = SessionId::from_bytes([0; 16]);
        let display = format!("{id}");
        assert!(!display.contains("SessionId"));
    }
}
