//! Peer identification for the keyhive protocol.

use alloc::{
    borrow::ToOwned,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};
use keyhive_crypto::identifier::{Identifier, InvalidIdentifier};
use sedimentree_core::codec::error::{DecodeError, InvalidEnumTag};

use crate::{
    codec::{self, Reader},
    error::WireDecodeError,
};

/// A peer identifier.
///
/// The peer ID is derived from an Ed25519 verifying key with an optional suffix.
/// The suffix is used to distinguish between multiple connections or roles
/// for the same cryptographic identity.
///
/// # Format
///
/// When serialized as a string, the format is:
/// * Without suffix: `<base64(verifying_key)>`
/// * With suffix: `<base64(verifying_key)>-<suffix>`
///
/// For example: `"abc123...==-my-suffix"` or just `"abc123...=="`
///
/// # Wire Format
///
/// ```text
/// ╔═══════════════╦══════╦══════════════════════════════╗
/// ║ Verifying Key ║ Tag  ║ Suffix (only if tag is 1)    ║
/// ║      32B      ║  1B  ║ bijou64 length + UTF-8 bytes ║
/// ╚═══════════════╩══════╩══════════════════════════════╝
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
    /// Returns an error if the verifying key bytes are not a canonical
    /// Ed25519 point of large order.
    pub fn to_identifier(&self) -> Result<Identifier, InvalidIdentifier> {
        Identifier::from_bytes(self.verifying_key)
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

    /// Append this peer ID's wire encoding to `buf`.
    pub(crate) fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.verifying_key);
        match &self.suffix {
            None => buf.push(NO_SUFFIX),
            Some(suffix) => {
                buf.push(SUFFIX);
                codec::put_bytes(suffix.as_bytes(), buf);
            }
        }
    }

    /// The size of [`Self::encode_into`]'s output.
    pub(crate) fn encoded_size(&self) -> usize {
        32 + 1
            + self
                .suffix
                .as_deref()
                .map_or(0, |s| codec::bytes_size(s.as_bytes()))
    }

    /// Read a peer ID written by [`Self::encode_into`].
    pub(crate) fn decode_from(reader: &mut Reader<'_>) -> Result<Self, WireDecodeError> {
        let verifying_key = reader.array::<32>()?;
        let suffix = match reader.u8()? {
            NO_SUFFIX => None,
            SUFFIX => Some(core::str::from_utf8(reader.bytes()?)?.to_owned()),
            tag => {
                return Err(DecodeError::from(InvalidEnumTag {
                    tag,
                    type_name: "KeyhivePeerId suffix",
                })
                .into());
            }
        };
        Ok(Self {
            verifying_key,
            suffix,
        })
    }
}

/// The wire tag of a peer ID without a suffix.
const NO_SUFFIX: u8 = 0;

/// The wire tag of a peer ID with a suffix.
const SUFFIX: u8 = 1;

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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParsePeerIdError {
    /// The base64 encoding is invalid.
    // Not a `#[source]`: without `std`, base64's error isn't a
    // `core::error::Error`.
    #[error("invalid base64 encoding: {0}")]
    InvalidBase64(base64::DecodeError),

    /// The decoded key has the wrong length.
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

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
    fn test_string_roundtrip_no_suffix() -> TestResult {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes(bytes);

        let s = peer_id.to_string_repr();
        let parsed = KeyhivePeerId::from_string_repr(&s)?;

        assert_eq!(peer_id, parsed);
        Ok(())
    }

    #[test]
    fn test_string_roundtrip_with_suffix() -> TestResult {
        let bytes = [42u8; 32];
        let peer_id = KeyhivePeerId::from_bytes_with_suffix(bytes, "my-suffix".to_string());

        let s = peer_id.to_string_repr();
        let parsed = KeyhivePeerId::from_string_repr(&s)?;

        assert_eq!(peer_id, parsed);
        Ok(())
    }

    fn wire_roundtrip(peer: &KeyhivePeerId) -> Result<KeyhivePeerId, WireDecodeError> {
        let mut buf = Vec::new();
        peer.encode_into(&mut buf);
        assert_eq!(buf.len(), peer.encoded_size());
        let mut reader = Reader::new(&buf);
        let decoded = KeyhivePeerId::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(decoded)
    }

    #[test]
    fn wire_roundtrip_keeps_the_suffix_or_its_absence() -> TestResult {
        for peer in [
            KeyhivePeerId::from_bytes([7; 32]),
            KeyhivePeerId::from_bytes_with_suffix([7; 32], String::new()),
            KeyhivePeerId::from_bytes_with_suffix([7; 32], "relay".to_string()),
        ] {
            assert_eq!(wire_roundtrip(&peer)?, peer);
        }
        Ok(())
    }

    #[test]
    fn a_suffix_that_is_not_utf8_is_rejected() {
        let mut buf = [7; 32].to_vec();
        buf.push(SUFFIX);
        codec::put_bytes(&[0xFF], &mut buf);
        assert!(matches!(
            KeyhivePeerId::decode_from(&mut Reader::new(&buf)),
            Err(WireDecodeError::Suffix(_))
        ));
    }

    #[test]
    fn test_equality() {
        let bytes = [42u8; 32];
        let peer1 = KeyhivePeerId::from_bytes(bytes);
        let peer2 = KeyhivePeerId::from_bytes(bytes);
        let peer3 = KeyhivePeerId::from_bytes_with_suffix(bytes, "test".to_string());

        assert_eq!(peer1, peer2);
        assert_ne!(peer1, peer3);
    }
}
