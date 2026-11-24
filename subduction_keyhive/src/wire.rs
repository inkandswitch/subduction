//! Keyhive wire message encoding.
//!
//! # Wire Layout
//!
//! Every keyhive message on the wire begins with the `SUK\x01` schema
//! header, followed by a 4-byte big-endian total size and an encoded
//! [`SignedMessage`]:
//!
//! ```text
//! +--------+-----------+-----------------------+
//! | Schema | TotalSize | SignedMessage         |
//! |   4B   |    4B     | variable              |
//! +--------+-----------+-----------------------+
//! ```
//!
//! Unlike `SUM` and `SUE`, there is no tag byte: the payload is always one
//! [`SignedMessage`], in the layout its docs give.
//!
//! `SUK\x00` carried the same envelope around CBOR. It is rejected as an
//! unknown schema rather than misread.

use alloc::vec::Vec;

use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidSchema, SizeMismatch},
    },
    id::SedimentreeId,
};
use subduction_core::connection::message::{
    BatchSyncResponse, TryAsBatchSyncResponse, TryAsSubscribeRequest,
};

use crate::{error::WireDecodeError, signed_message::SignedMessage};

/// Schema header for keyhive messages: **SU**bduction **K**eyhive v1.
pub const KEYHIVE_SCHEMA: [u8; 4] = *b"SUK\x01";

/// Minimum envelope size: `schema(4) + total_size(4)`.
const ENVELOPE_HEADER_SIZE: usize = 4 + 4;

/// A keyhive protocol message framed for the wire.
///
/// Wraps an encoded [`SignedMessage`] in a `SUK\x01` binary frame. The
/// frame doesn't check its payload: [`Self::into_signed`] decodes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyhiveMessage {
    /// The payload: an encoded [`SignedMessage`].
    payload: Vec<u8>,
}

impl KeyhiveMessage {
    /// Create a keyhive message from a raw payload.
    #[must_use]
    pub const fn new(payload: Vec<u8>) -> Self {
        Self { payload }
    }

    /// Create a keyhive message carrying `msg`.
    #[must_use]
    pub fn from_signed(msg: &SignedMessage) -> Self {
        Self {
            payload: msg.encode(),
        }
    }

    /// Decode the payload as a [`SignedMessage`].
    ///
    /// # Errors
    ///
    /// Returns [`WireDecodeError`] if the payload is not one encoded signed
    /// message.
    pub fn into_signed(self) -> Result<SignedMessage, WireDecodeError> {
        SignedMessage::try_decode(&self.payload)
    }

    /// Get the raw payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consume and return the raw payload bytes.
    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// Keyhive messages are never `BatchSyncResponse`s; this always
/// returns `None`.
impl TryAsBatchSyncResponse for KeyhiveMessage {
    fn try_as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        None
    }
}

/// Keyhive messages are never subscription requests; this always
/// returns `None`.
impl TryAsSubscribeRequest for KeyhiveMessage {
    fn try_as_subscribe_request(&self) -> Option<SedimentreeId> {
        None
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
    fn encoded_starts_with_schema() {
        let msg = KeyhiveMessage::new(vec![1, 2, 3]);
        let encoded = msg.encode();

        assert_eq!(&encoded[0..4], b"SUK\x01");
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
    fn extra_trailing_bytes_rejected() {
        let msg = KeyhiveMessage::new(vec![1, 2, 3]);
        let mut encoded = msg.encode();
        encoded.push(0xFF); // extra byte

        let err = KeyhiveMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::SizeMismatch(_)),
            "expected SizeMismatch, got {err:?}"
        );
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_roundtrip() {
        bolero::check!()
            .with_type::<alloc::vec::Vec<u8>>()
            .for_each(|payload| {
                let msg = KeyhiveMessage::new(payload.clone());
                let encoded = msg.encode();
                let decoded = KeyhiveMessage::try_decode(&encoded).expect("roundtrip decode");
                assert_eq!(decoded, msg);
            });
    }

    #[cfg(feature = "bolero")]
    #[test]
    fn prop_encoded_size_matches_encode_len() {
        bolero::check!()
            .with_type::<alloc::vec::Vec<u8>>()
            .for_each(|payload| {
                let msg = KeyhiveMessage::new(payload.clone());
                assert_eq!(msg.encoded_size(), msg.encode().len());
            });
    }

    #[test]
    fn signed_message_roundtrip() {
        // The SUK frame around a SignedMessage. The signed message's own
        // encoding is covered in `signed_message::tests`.
        let signed = SignedMessage::new(vec![1, 2, 3]);

        let encoded = KeyhiveMessage::from_signed(&signed).encode();
        let decoded = KeyhiveMessage::try_decode(&encoded).expect("decode frame");
        let recovered = decoded.into_signed().expect("decode signed message");

        assert_eq!(recovered, signed);
    }

    #[test]
    fn a_v0_frame_is_rejected_as_an_unknown_schema() {
        let mut encoded = KeyhiveMessage::new(vec![1, 2, 3]).encode();
        encoded[3] = 0;

        let err = KeyhiveMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }
}
