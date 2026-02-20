//! Peer identity types.

use alloc::string::String;
use core::fmt::Write;

/// A Peer ID.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct PeerId(
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    [u8; 32],
);

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

impl core::fmt::Display for PeerId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        to_hex(self.as_slice()).fmt(f)
    }
}

impl core::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(self, f)
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

impl From<ed25519_dalek::VerifyingKey> for PeerId {
    fn from(key: ed25519_dalek::VerifyingKey) -> Self {
        PeerId::new(key.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;

    mod hex_formatting {
        use super::*;

        #[test]
        fn test_hex_formatting_all_zeros() {
            let bytes = [0u8; 32];
            let peer_id = PeerId::new(bytes);
            let hex = format!("{peer_id}");

            assert_eq!(hex.len(), 64);
            assert_eq!(hex, "0".repeat(64));
        }

        #[test]
        fn test_hex_formatting_all_ones() {
            let bytes = [0xffu8; 32];
            let peer_id = PeerId::new(bytes);
            let hex = format!("{peer_id}");

            assert_eq!(hex.len(), 64);
            assert_eq!(hex, "f".repeat(64));
        }

        #[test]
        fn test_hex_formatting_known_values() {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xab;
            bytes[1] = 0xcd;
            bytes[2] = 0xef;
            bytes[31] = 0x12;

            let peer_id = PeerId::new(bytes);
            let hex = format!("{peer_id}");

            assert!(hex.starts_with("abcdef"));
            assert!(hex.ends_with("12"));
            assert_eq!(hex.len(), 64);
        }

        #[test]
        fn test_hex_lowercase() {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xAB; // uppercase in source, should be lowercase in output

            let peer_id = PeerId::new(bytes);
            let hex = format!("{peer_id}");

            assert!(hex.starts_with("ab")); // lowercase
            assert!(hex
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }

        #[test]
        fn test_debug_equals_display() {
            let bytes = [0x42u8; 32];
            let peer_id = PeerId::new(bytes);

            let display = format!("{peer_id}");
            let debug = format!("{peer_id:?}");

            assert_eq!(display, debug);
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_hex_format_always_64_chars() {
            bolero::check!().with_type::<PeerId>().for_each(|peer_id| {
                let hex = format!("{peer_id}");
                assert_eq!(hex.len(), 64);
            });
        }

        #[test]
        fn prop_hex_format_is_valid_lowercase_hex() {
            bolero::check!().with_type::<PeerId>().for_each(|peer_id| {
                let hex = format!("{peer_id}");
                assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
                assert!(hex.chars().all(|c| !c.is_ascii_uppercase()));
            });
        }
    }
}
