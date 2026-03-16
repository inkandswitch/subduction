//! Keyhive wire message encoding.
//!
//! # Wire Layout
//!
//! Every keyhive message on the wire begins with the `SUK\x00` schema
//! header, followed by a 4-byte big-endian total size and the
//! CBOR-encoded [`SignedMessage`] payload:
//!
//! ```text
//! +--------+-----------+-----------------------+
//! | Schema | TotalSize |    CBOR Payload       |
//! |   4B   |    4B     |      variable         |
//! +--------+-----------+-----------------------+
//! ```
//!
//! Unlike `SUM` and `SUE`, there is no tag byte — the payload is always
//! a single CBOR-encoded [`SignedMessage`].

use alloc::vec::Vec;

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema, SizeMismatch},
};

#[cfg(all(feature = "serde", feature = "std"))]
use crate::signed_message::SignedMessage;

/// Schema header for keyhive messages: **SU**bduction **K**eyhive v0.
pub const KEYHIVE_SCHEMA: [u8; 4] = *b"SUK\x00";

/// Minimum envelope size: `schema(4) + total_size(4)`.
const ENVELOPE_HEADER_SIZE: usize = 4 + 4;

/// A keyhive protocol message framed for the wire.
///
/// Wraps a CBOR-encoded [`SignedMessage`] in a `SUK\x00` binary frame.
/// The inner bytes are the raw CBOR payload — serialization and
/// deserialization of `SignedMessage` happen at a higher layer (requires
/// the `serde` feature on `SignedMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyhiveMessage {
    /// The raw CBOR-encoded payload (a serialized [`SignedMessage`]).
    payload: Vec<u8>,
}

impl KeyhiveMessage {
    /// Create a keyhive message from raw CBOR bytes.
    #[must_use]
    pub const fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Create a keyhive message by serializing a [`SignedMessage`] to CBOR.
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR serialization fails.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn from_signed(msg: &SignedMessage) -> Result<Self, crate::signed_message::CborError> {
        msg.to_cbor().map(|payload| Self { payload })
    }

    /// Deserialize the payload into a [`SignedMessage`].
    ///
    /// # Errors
    ///
    /// Returns an error if CBOR deserialization fails.
    #[cfg(all(feature = "serde", feature = "std"))]
    pub fn into_signed(self) -> Result<SignedMessage, crate::signed_message::CborError> {
        SignedMessage::from_cbor(&self.payload)
    }

    /// Get the raw CBOR payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consume and return the raw CBOR payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

impl Encode for KeyhiveMessage {
    fn encode(&self) -> Vec<u8> {
        let total_size = ENVELOPE_HEADER_SIZE + self.payload.len();
        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(&KEYHIVE_SCHEMA);

        #[allow(clippy::cast_possible_truncation)]
        buf.extend_from_slice(&(total_size as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);

        buf
    }

    fn encoded_size(&self) -> usize {
        ENVELOPE_HEADER_SIZE + self.payload.len()
    }
}

impl Decode for KeyhiveMessage {
    const MIN_SIZE: usize = ENVELOPE_HEADER_SIZE;

    #[allow(clippy::indexing_slicing)] // Length validated before access
    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < ENVELOPE_HEADER_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "KeyhiveMessage envelope",
                need: ENVELOPE_HEADER_SIZE,
                have: buf.len(),
            });
        }

        let schema: [u8; 4] =
            buf.get(0..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "KeyhiveMessage schema",
                    need: 4,
                    have: buf.len(),
                })?;

        if schema != KEYHIVE_SCHEMA {
            return Err(InvalidSchema {
                expected: KEYHIVE_SCHEMA,
                got: schema,
            }
            .into());
        }

        let total_size = u32::from_be_bytes(buf.get(4..8).and_then(|s| s.try_into().ok()).ok_or(
            DecodeError::MessageTooShort {
                type_name: "KeyhiveMessage total_size",
                need: 8,
                have: buf.len(),
            },
        )?) as usize;

        if buf.len() != total_size {
            return Err(SizeMismatch {
                declared: total_size,
                actual: buf.len(),
            }
            .into());
        }

        let payload = buf
            .get(ENVELOPE_HEADER_SIZE..)
            .ok_or(DecodeError::MessageTooShort {
                type_name: "KeyhiveMessage payload",
                need: ENVELOPE_HEADER_SIZE,
                have: buf.len(),
            })?
            .to_vec();

        Ok(Self { payload })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use alloc::vec;

    #[test]
    fn empty_payload_roundtrip() {
        let msg = KeyhiveMessage::new(vec![]);

        let encoded = msg.encode();
        let decoded = KeyhiveMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn nonempty_payload_roundtrip() {
        let msg = KeyhiveMessage::new(vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42]);

        let encoded = msg.encode();
        let decoded = KeyhiveMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn schema_bytes_are_correct() {
        assert_eq!(&KEYHIVE_SCHEMA, b"SUK\x00");
    }

    #[test]
    fn encoded_starts_with_schema() {
        let msg = KeyhiveMessage::new(vec![1, 2, 3]);
        let encoded = msg.encode();

        assert_eq!(&encoded[0..4], b"SUK\x00");
    }

    #[test]
    fn encoded_total_size_is_correct() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = KeyhiveMessage::new(payload.clone());
        let encoded = msg.encode();

        let total_size = u32::from_be_bytes(encoded[4..8].try_into().unwrap()) as usize;
        assert_eq!(total_size, encoded.len());
        assert_eq!(total_size, ENVELOPE_HEADER_SIZE + payload.len());
    }

    #[test]
    fn wrong_schema_rejected() {
        let msg = KeyhiveMessage::new(vec![1, 2, 3]);
        let mut encoded = msg.encode();
        encoded[2] = b'M'; // SUK -> SUM

        let err = KeyhiveMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }

    #[test]
    fn size_mismatch_rejected() {
        let msg = KeyhiveMessage::new(vec![1, 2, 3, 4, 5]);
        let mut encoded = msg.encode();
        encoded.truncate(encoded.len() - 2);

        let err = KeyhiveMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::SizeMismatch(_)),
            "expected SizeMismatch, got {err:?}"
        );
    }

    #[test]
    fn too_short_rejected() {
        let err = KeyhiveMessage::try_decode(&[0x00, 0x01, 0x02]).unwrap_err();
        assert!(
            matches!(err, DecodeError::MessageTooShort { .. }),
            "expected MessageTooShort, got {err:?}"
        );
    }

    #[test]
    fn payload_accessible() {
        let payload = vec![10, 20, 30];
        let msg = KeyhiveMessage::new(payload.clone());

        assert_eq!(msg.payload(), &payload[..]);
        assert_eq!(msg.into_payload(), payload);
    }

    #[cfg(all(feature = "serde", feature = "std"))]
    #[test]
    fn signed_message_roundtrip() {
        use crate::signed_message::SignedMessage;

        let signed = SignedMessage::with_contact_card(vec![1, 2, 3], vec![4, 5, 6]);
        let keyhive_msg = KeyhiveMessage::from_signed(&signed).expect("serialize");

        let encoded = keyhive_msg.encode();
        let decoded = KeyhiveMessage::try_decode(&encoded).expect("decode frame");
        let recovered = decoded.into_signed().expect("deserialize");

        assert_eq!(recovered, signed);
    }
}
