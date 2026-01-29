//! Peer identification for the keyhive protocol.

use alloc::string::{String, ToString};
use core::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::VerifyingKey;
use keyhive_core::principal::identifier::Identifier;

/// A peer identifier in the keyhive protocol.
///
/// The peer ID is derived from an Ed25519 verifying key with an optional suffix.
/// The suffix is used to distinguish between multiple connections or roles
/// for the same cryptographic identity.
///
/// # Format
///
/// When serialized as a string, the format is:
/// - Without suffix: `<base64(verifying_key)>`
/// - With suffix: `<base64(verifying_key)>-<suffix>`
///
/// For example: `"abc123...==-my-suffix"` or just `"abc123...=="`
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct KeyhivePeerId {
    /// The 32-byte Ed25519 verifying key.
    verifying_key: [u8; 32],

    /// Optional suffix to distinguish different connections/roles.
    suffix: Option<String>,
}

impl KeyhivePeerId {
    /// Create a new peer ID from raw bytes with no suffix.
    #[must_use]
    pub const fn from_bytes(verifying_key: [u8; 32]) -> Self {
        Self {
            verifying_key,
            suffix: None,
        }
    }

    /// Create a new peer ID from raw bytes with a suffix.
    #[must_use]
    pub const fn from_bytes_with_suffix(verifying_key: [u8; 32], suffix: String) -> Self {
        Self {
            verifying_key,
            suffix: Some(suffix),
        }
    }

    /// Create a peer ID from a keyhive Identifier.
    #[must_use]
    pub fn from_identifier(identifier: &Identifier) -> Self {
        Self {
            verifying_key: identifier.to_bytes(),
            suffix: None,
        }
    }

    /// Create a peer ID from a keyhive Identifier with a suffix.
    #[must_use]
    pub fn from_identifier_with_suffix(identifier: &Identifier, suffix: String) -> Self {
        Self {
            verifying_key: identifier.to_bytes(),
            suffix: Some(suffix),
        }
    }

    /// Get the verifying key bytes.
    #[must_use]
    pub const fn verifying_key(&self) -> &[u8; 32] {
        &self.verifying_key
    }

    /// Get the suffix, if any.
    #[must_use]
    pub fn suffix(&self) -> Option<&str> {
        self.suffix.as_deref()
    }

    /// Create a new peer ID with the same verifying key but no suffix.
    ///
    /// This is useful for comparing peer IDs regardless of their suffix.
    #[must_use]
    pub const fn without_suffix(&self) -> Self {
        Self {
            verifying_key: self.verifying_key,
            suffix: None,
        }
    }

    /// Check if this peer ID represents the same identity as another,
    /// ignoring suffixes.
    #[must_use]
    pub fn same_identity(&self, other: &Self) -> bool {
        self.verifying_key == other.verifying_key
    }

    /// Convert to a keyhive [`Identifier`].
    ///
    /// # Errors
    ///
    /// Returns an error if the verifying key bytes do not represent a valid
    /// Ed25519 public key.
    pub fn to_identifier(&self) -> Result<Identifier, ed25519_dalek::SignatureError> {
        let vk = VerifyingKey::from_bytes(&self.verifying_key)?;
        Ok(Identifier::from(vk))
    }

    /// Encode the verifying key as base64.
    #[must_use]
    pub fn verifying_key_base64(&self) -> String {
        STANDARD.encode(self.verifying_key)
    }

    /// Convert to a string representation.
    ///
    /// Format: `<base64(verifying_key)>` or `<base64(verifying_key)>-<suffix>`
    #[must_use]
    pub fn to_string_repr(&self) -> String {
        let base = self.verifying_key_base64();
        match &self.suffix {
            Some(s) => alloc::format!("{base}-{s}"),
            None => base,
        }
    }

    /// Parse from a string representation.
    ///
    /// Format: `<base64(verifying_key)>` or `<base64(verifying_key)>-<suffix>`
    ///
    /// # Errors
    ///
    /// Returns an error if the base64 decoding fails or the key length is wrong.
    pub fn from_string_repr(s: &str) -> Result<Self, ParsePeerIdError> {
        // Split on first hyphen to get base64 and optional suffix
        // Note: base64 can contain '+' and '/' but not '-', so this is safe
        let (base64_part, suffix) = match s.find('-') {
            Some(idx) => {
                let (base, rest) = s.split_at(idx);
                // Skip the '-' separator
                (base, Some(rest[1..].to_string()))
            }
            None => (s, None),
        };

        let decoded = STANDARD
            .decode(base64_part)
            .map_err(ParsePeerIdError::InvalidBase64)?;

        if decoded.len() != 32 {
            return Err(ParsePeerIdError::InvalidKeyLength(decoded.len()));
        }

        let mut verifying_key = [0u8; 32];
        verifying_key.copy_from_slice(&decoded);

        Ok(Self {
            verifying_key,
            suffix,
        })
    }
}

impl fmt::Debug for KeyhivePeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KeyhivePeerId({})", self.to_string_repr())
    }
}

impl fmt::Display for KeyhivePeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_repr())
    }
}

/// Errors that can occur when parsing a peer ID from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsePeerIdError {
    /// The base64 encoding is invalid.
    InvalidBase64(base64::DecodeError),

    /// The decoded key has the wrong length.
    InvalidKeyLength(usize),
}

impl fmt::Display for ParsePeerIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBase64(e) => write!(f, "invalid base64 encoding: {e}"),
            Self::InvalidKeyLength(len) => {
                write!(f, "invalid key length: expected 32 bytes, got {len}")
            }
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ParsePeerIdError {}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bytes() {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes(bytes);

        assert_eq!(peer_id.verifying_key(), &bytes);
        assert!(peer_id.suffix().is_none());
    }

    #[test]
    fn test_from_bytes_with_suffix() {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes_with_suffix(bytes, "test".to_string());

        assert_eq!(peer_id.verifying_key(), &bytes);
        assert_eq!(peer_id.suffix(), Some("test"));
    }

    #[test]
    fn test_without_suffix() {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes_with_suffix(bytes, "test".to_string());
        let without = peer_id.without_suffix();

        assert_eq!(without.verifying_key(), &bytes);
        assert!(without.suffix().is_none());
    }

    #[test]
    fn test_same_identity() {
        let bytes = [42u8; 32];
        let peer1 = KeyhivePeerId::from_bytes(bytes);
        let peer2 = KeyhivePeerId::from_bytes_with_suffix(bytes, "test".to_string());
        let peer3 = KeyhivePeerId::from_bytes([43u8; 32]);

        assert!(peer1.same_identity(&peer2));
        assert!(!peer1.same_identity(&peer3));
    }

    #[test]
    fn test_string_roundtrip_no_suffix() {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes(bytes);

        let s = peer_id.to_string_repr();
        let parsed = KeyhivePeerId::from_string_repr(&s).expect("should parse");

        assert_eq!(peer_id, parsed);
    }

    #[test]
    fn test_string_roundtrip_with_suffix() {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes_with_suffix(bytes, "my-suffix".to_string());

        let s = peer_id.to_string_repr();
        let parsed = KeyhivePeerId::from_string_repr(&s).expect("should parse");

        assert_eq!(peer_id, parsed);
    }

    #[test]
    fn test_equality() {
        let bytes = [42u8; 32];
        let peer1 = KeyhivePeerId::from_bytes(bytes);
        let peer2 = KeyhivePeerId::from_bytes(bytes);
        let peer3 = KeyhivePeerId::from_bytes_with_suffix(bytes, "test".to_string());

        assert_eq!(peer1, peer2);
        assert_ne!(peer1, peer3); // Different because of suffix
    }
}
