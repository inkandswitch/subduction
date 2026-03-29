//! Ephemeral message types and wire encoding.
//!
//! # Wire Layout
//!
//! Every ephemeral message on the wire begins with the `SUE\x00` schema
//! header, followed by a 1-byte tag and the variant-specific payload:
//!
//! ```text
//! ╔════════╦═════╦═════════════════════════════╗
//! ║ Schema ║ Tag ║         Payload             ║
//! ║   4B   ║ 1B  ║         variable            ║
//! ╚════════╩═════╩═════════════════════════════╝
//! ```
//!
//! ## `Ephemeral` variant (tag `0x00`)
//!
//! The tag payload is a schema-stripped [`Signed<EphemeralPayload>`].
//! The inner `SUE\x00` schema prefix is elided on the wire (the envelope
//! schema already identifies the protocol); it is reconstructed on decode
//! before passing to [`Signed::try_decode`].
//!
//! ```text
//! On the wire (schema elided):
//! ╔════════╦════════╦════════╦═══════╦═══════════╦════════════╦═════════╦═══════════╗
//! ║ Issuer ║   ID   ║ Nonce  ║  Time ║ PayloadLen║ Payload   ║ Signature           ║
//! ║  32B   ║  32B   ║   8B   ║   8B  ║  bijou64  ║  var      ║   64B               ║
//! ╚════════╩════════╩════════╩═══════╩═══════════╩═════════════╩═══════════════════╝
//!
//! Signed region (reconstructed for verification):
//! SUE\x00 || issuer(32) || id(32) || nonce(8) || timestamp(8) || payload_len || payload
//! ```
//!
//! [`Signed<EphemeralPayload>`]: subduction_crypto::signed::Signed
//! [`Signed::try_decode`]: subduction_crypto::signed::Signed::try_decode

use alloc::vec::Vec;

use nonempty::NonEmpty;
use sedimentree_core::codec::{
    decode::{self, Decode, DecodeFields},
    encode::{self, Encode, EncodeFields},
    error::{DecodeError, InvalidEnumTag, InvalidSchema},
    schema::{self, Schema},
};
use subduction_core::timestamp::TimestampSeconds;
use subduction_crypto::signed::Signed;

use crate::topic::Topic;

/// Schema header for [`EphemeralMessage`] envelope: **SU**bduction **E**phemeral v0.
pub const EPHEMERAL_SCHEMA: [u8; 4] = *b"SUE\x00";

/// Header size for unsigned control messages: `schema(4) + tag(1)`.
const CONTROL_HEADER_SIZE: usize = 4 + 1;

mod tags {
    use super::EphemeralPayload;

    pub(super) const EPHEMERAL: u8 = EphemeralPayload::TAG;
    pub(super) const SUBSCRIBE: u8 = 0x01;
    pub(super) const UNSUBSCRIBE: u8 = 0x02;
    pub(super) const SUBSCRIBE_REJECTED: u8 = 0x03;
}

/// Minimum payload sizes _after_ the tag byte (for control messages only).
/// The `Ephemeral` variant validates size via [`Signed::try_decode`].
mod min_sizes {
    // count(2) + topic(32) — at least one topic required (NonEmpty)
    pub(super) const SUBSCRIBE: usize = 2 + 32;
    // count(2) + topic(32)
    pub(super) const UNSUBSCRIBE: usize = 2 + 32;
    // count(2) + topic(32)
    pub(super) const SUBSCRIBE_REJECTED: usize = 2 + 32;
}

// ── EphemeralPayload ────────────────────────────────────────────────────

/// Size of `EphemeralPayload` fields: `id(32) + nonce(8) + timestamp(8) + payload_len(bijou64 min=1)`.
const EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE: usize = 32 + 8 + 8 + 1;

/// Minimum size of a `Signed<EphemeralPayload>`:
/// schema(4) + discriminant(1) + issuer(32) + fields + signature(64).
const EPHEMERAL_PAYLOAD_MIN_SIGNED_SIZE: usize =
    4 + 1 + 32 + EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE + 64;

/// The signable payload of an ephemeral message.
///
/// This type implements [`Schema`], [`EncodeFields`], and [`DecodeFields`]
/// so it can be used with [`Signed<EphemeralPayload>`] for signing,
/// verification, and wire encoding.
///
/// The issuer (sender) is stored in the [`Signed`] wrapper, not here.
///
/// [`Signed<EphemeralPayload>`]: subduction_crypto::signed::Signed
/// [`Signed`]: subduction_crypto::signed::Signed
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralPayload {
    /// The topic this message is published to.
    pub id: Topic,
    /// Random nonce for deduplication.
    pub nonce: u64,
    /// UTC time at message creation (seconds since epoch).
    pub timestamp: TimestampSeconds,
    /// Opaque application payload.
    pub payload: Vec<u8>,
}

impl EphemeralPayload {
    /// Variant tag within the `SUE\x00` ephemeral protocol.
    pub const TAG: u8 = 0x00;
}

impl Schema for EphemeralPayload {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'E';
    const VERSION: u8 = 0;
    const DISCRIMINANT: Option<u8> = Some(EphemeralPayload::TAG);
    // SCHEMA = b"SUE\x00"
}

impl EncodeFields for EphemeralPayload {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        encode::array(self.id.as_bytes(), buf);
        encode::u64(self.nonce, buf);
        encode::u64(self.timestamp.as_secs(), buf);
        #[allow(clippy::cast_possible_truncation)]
        bijou64::encode(self.payload.len() as u64, buf);
        buf.extend_from_slice(&self.payload);
    }

    fn fields_size(&self) -> usize {
        32 + 8 + 8 + bijou64::encoded_len(self.payload.len() as u64) + self.payload.len()
    }
}

impl DecodeFields for EphemeralPayload {
    const MIN_SIGNED_SIZE: usize = EPHEMERAL_PAYLOAD_MIN_SIGNED_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "EphemeralPayload",
                need: EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        let id_bytes: [u8; 32] = decode::array(buf, offset)?;
        offset += 32;
        let id = Topic::new(id_bytes);

        let nonce = decode::u64(buf, offset)?;
        offset += 8;

        let timestamp_secs = decode::u64(buf, offset)?;
        offset += 8;
        let timestamp = TimestampSeconds::new(timestamp_secs);

        let remaining = buf.get(offset..).ok_or(DecodeError::MessageTooShort {
            type_name: "EphemeralPayload payload_len",
            need: offset + 1,
            have: buf.len(),
        })?;
        let (payload_len, consumed) = bijou64::decode(remaining)
            .map_err(|kind| sedimentree_core::codec::error::Bijou64Error { offset, kind })?;
        offset += consumed;

        #[allow(clippy::cast_possible_truncation)]
        let payload_len = payload_len as usize;
        let payload = buf
            .get(offset..offset + payload_len)
            .ok_or(DecodeError::MessageTooShort {
                type_name: "EphemeralPayload payload data",
                need: offset + payload_len,
                have: buf.len(),
            })?
            .to_vec();
        offset += payload_len;

        Ok((
            Self {
                id,
                nonce,
                timestamp,
                payload,
            },
            offset,
        ))
    }
}

// ── EphemeralMessage ────────────────────────────────────────────────────

/// Wire message types for the ephemeral protocol.
///
/// These messages are scoped to the `SUE\x00` schema and are fully
/// independent of [`SyncMessage`]. Each variant maps to a tag byte
/// local to this schema.
///
/// [`SyncMessage`]: subduction_core::connection::message::SyncMessage
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EphemeralMessage {
    /// A signed ephemeral payload for a specific topic.
    ///
    /// Fire-and-forget delivery to all subscribers of the topic
    /// (minus the sender). The originator signs the message; relays
    /// forward it as-is.
    Ephemeral(Box<Signed<EphemeralPayload>>),

    /// Subscribe to ephemeral messages for the given topics.
    Subscribe {
        /// The topics to subscribe to.
        topics: NonEmpty<Topic>,
    },

    /// Unsubscribe from ephemeral messages for the given topics.
    Unsubscribe {
        /// The topics to unsubscribe from.
        topics: NonEmpty<Topic>,
    },

    /// Notification that some subscribe requests were rejected.
    ///
    /// Contains only the rejected topics. Accepted topics are implied
    /// by omission.
    SubscribeRejected {
        /// The topics that were rejected.
        topics: NonEmpty<Topic>,
    },
}

// ── Encode ──────────────────────────────────────────────────────────────

impl Encode for EphemeralMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            // The Signed<T> bytes ARE the wire message — schema + discriminant
            // + issuer + fields + signature. No outer envelope needed.
            Self::Ephemeral(signed) => signed.as_bytes().to_vec(),

            // Control messages: manual SUE\x00 + tag + payload.
            Self::Subscribe { topics } => {
                let mut buf = Vec::with_capacity(CONTROL_HEADER_SIZE + 2 + topics.len() * 32);
                buf.extend_from_slice(&EPHEMERAL_SCHEMA);
                buf.push(tags::SUBSCRIBE);
                encode_topic_list(&mut buf, topics);
                buf
            }
            Self::Unsubscribe { topics } => {
                let mut buf = Vec::with_capacity(CONTROL_HEADER_SIZE + 2 + topics.len() * 32);
                buf.extend_from_slice(&EPHEMERAL_SCHEMA);
                buf.push(tags::UNSUBSCRIBE);
                encode_topic_list(&mut buf, topics);
                buf
            }
            Self::SubscribeRejected { topics } => {
                let mut buf = Vec::with_capacity(CONTROL_HEADER_SIZE + 2 + topics.len() * 32);
                buf.extend_from_slice(&EPHEMERAL_SCHEMA);
                buf.push(tags::SUBSCRIBE_REJECTED);
                encode_topic_list(&mut buf, topics);
                buf
            }
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Ephemeral(signed) => signed.as_bytes().len(),
            Self::Subscribe { topics }
            | Self::Unsubscribe { topics }
            | Self::SubscribeRejected { topics } => CONTROL_HEADER_SIZE + 2 + topics.len() * 32,
        }
    }
}

fn encode_topic_list(buf: &mut Vec<u8>, topics: &NonEmpty<Topic>) {
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(topics.len() as u16).to_be_bytes());
    for topic in topics {
        buf.extend_from_slice(topic.as_bytes());
    }
}

// ── Decode ──────────────────────────────────────────────────────────────

impl Decode for EphemeralMessage {
    const MIN_SIZE: usize = CONTROL_HEADER_SIZE;

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        decode_message(buf)
    }
}

fn decode_message(bytes: &[u8]) -> Result<EphemeralMessage, DecodeError> {
    if bytes.len() < CONTROL_HEADER_SIZE {
        return Err(DecodeError::MessageTooShort {
            type_name: "EphemeralMessage",
            need: CONTROL_HEADER_SIZE,
            have: bytes.len(),
        });
    }

    let schema: [u8; 4] =
        bytes
            .get(0..4)
            .and_then(|s| s.try_into().ok())
            .ok_or(DecodeError::MessageTooShort {
                type_name: "EphemeralMessage schema",
                need: 4,
                have: bytes.len(),
            })?;
    if schema != EPHEMERAL_SCHEMA {
        return Err(InvalidSchema {
            expected: EPHEMERAL_SCHEMA,
            got: schema,
        }
        .into());
    }

    // Byte 4 is either:
    // - A discriminant (0x00) for Signed<EphemeralPayload>
    // - A control message tag (0x01-0x03)
    let tag = *bytes.get(4).ok_or(DecodeError::MessageTooShort {
        type_name: "EphemeralMessage tag",
        need: CONTROL_HEADER_SIZE,
        have: bytes.len(),
    })?;

    match tag {
        tags::EPHEMERAL => {
            // The full bytes are a Signed<EphemeralPayload>.
            // The discriminant at byte 4 is validated by Signed::try_decode.
            let signed = Signed::<EphemeralPayload>::try_decode(bytes.to_vec())?;
            Ok(EphemeralMessage::Ephemeral(Box::new(signed)))
        }
        tags::SUBSCRIBE | tags::UNSUBSCRIBE | tags::SUBSCRIBE_REJECTED => {
            let payload = bytes
                .get(CONTROL_HEADER_SIZE..)
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "EphemeralMessage control payload",
                    need: CONTROL_HEADER_SIZE + 1,
                    have: bytes.len(),
                })?;

            let (min_size, type_name) = match tag {
                tags::SUBSCRIBE => (min_sizes::SUBSCRIBE, "EphemeralSubscribe"),
                tags::UNSUBSCRIBE => (min_sizes::UNSUBSCRIBE, "EphemeralUnsubscribe"),
                tags::SUBSCRIBE_REJECTED => {
                    (min_sizes::SUBSCRIBE_REJECTED, "EphemeralSubscribeRejected")
                }
                _ => unreachable!(),
            };

            if payload.len() < min_size {
                return Err(DecodeError::MessageTooShort {
                    type_name,
                    need: CONTROL_HEADER_SIZE + min_size,
                    have: bytes.len(),
                });
            }

            let topics = decode_topic_list(payload)?;
            match tag {
                tags::SUBSCRIBE => Ok(EphemeralMessage::Subscribe { topics }),
                tags::UNSUBSCRIBE => Ok(EphemeralMessage::Unsubscribe { topics }),
                tags::SUBSCRIBE_REJECTED => Ok(EphemeralMessage::SubscribeRejected { topics }),
                _ => unreachable!(),
            }
        }
        _ => Err(InvalidEnumTag {
            tag,
            type_name: "EphemeralMessage",
        }
        .into()),
    }
}

fn decode_topic_list(payload: &[u8]) -> Result<NonEmpty<Topic>, DecodeError> {
    let mut offset = 0;

    let count = read_u16(payload, &mut offset)? as usize;
    if count == 0 {
        return Err(DecodeError::MessageTooShort {
            type_name: "EphemeralTopicList",
            need: 1,
            have: 0,
        });
    }

    let first = Topic::new(read_array::<32>(payload, &mut offset)?);
    let mut rest = Vec::with_capacity(count - 1);
    for _ in 1..count {
        rest.push(Topic::new(read_array::<32>(payload, &mut offset)?));
    }

    Ok(NonEmpty {
        head: first,
        tail: rest,
    })
}

// ── Decode helpers ──────────────────────────────────────────────────────

fn read_u16(buf: &[u8], offset: &mut usize) -> Result<u16, DecodeError> {
    let bytes: [u8; 2] = buf
        .get(*offset..*offset + 2)
        .and_then(|s| s.try_into().ok())
        .ok_or(DecodeError::MessageTooShort {
            type_name: "u16",
            need: *offset + 2,
            have: buf.len(),
        })?;
    *offset += 2;
    Ok(u16::from_be_bytes(bytes))
}

fn read_array<const N: usize>(buf: &[u8], offset: &mut usize) -> Result<[u8; N], DecodeError> {
    let bytes: [u8; N] = buf
        .get(*offset..*offset + N)
        .and_then(|s| s.try_into().ok())
        .ok_or(DecodeError::MessageTooShort {
            type_name: "array",
            need: *offset + N,
            have: buf.len(),
        })?;
    *offset += N;
    Ok(bytes)
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use subduction_core::peer::id::PeerId;

    /// Helper: create a signed ephemeral for roundtrip tests.
    /// Uses `MemorySigner` for a real signature.
    async fn make_signed_ephemeral(payload: Vec<u8>) -> EphemeralMessage {
        use subduction_crypto::signer::memory::MemorySigner;
        let signer = MemorySigner::from_bytes(&[0xAA; 32]);

        let ep = EphemeralPayload {
            id: Topic::new([0xBB; 32]),
            nonce: 0x1234_5678_9ABC_DEF0,
            timestamp: TimestampSeconds::new(1_700_000_000),
            payload,
        };

        let verified = Signed::seal::<future_form::Sendable, _>(&signer, ep).await;
        EphemeralMessage::Ephemeral(Box::new(verified.into_signed()))
    }

    #[tokio::test]
    async fn ephemeral_roundtrip() {
        let msg = make_signed_ephemeral(vec![1, 2, 3, 4, 5]).await;

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn empty_payload_roundtrip() {
        let msg = make_signed_ephemeral(vec![]).await;

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_roundtrip() {
        let mut topics = NonEmpty::new(Topic::new([0x01; 32]));
        topics.push(Topic::new([0x02; 32]));
        let msg = EphemeralMessage::Subscribe { topics };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let topics = NonEmpty::new(Topic::new([0xFF; 32]));
        let msg = EphemeralMessage::Unsubscribe { topics };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_rejected_roundtrip() {
        let topics = NonEmpty::new(Topic::new([0x42; 32]));
        let msg = EphemeralMessage::SubscribeRejected { topics };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn empty_topic_list_rejected() {
        // Manually construct: SUE\x00 || Subscribe tag || count=0
        let mut buf = Vec::new();
        buf.extend_from_slice(&EPHEMERAL_SCHEMA);
        buf.push(tags::SUBSCRIBE);
        buf.extend_from_slice(&0_u16.to_be_bytes());

        let err = EphemeralMessage::try_decode(&buf).unwrap_err();
        assert!(
            matches!(err, DecodeError::MessageTooShort { .. }),
            "expected MessageTooShort for empty topic list, got {err:?}"
        );
    }

    #[test]
    fn wrong_schema_rejected() {
        let topics = NonEmpty::new(Topic::new([0x01; 32]));
        let msg = EphemeralMessage::Subscribe { topics };
        let mut encoded = msg.encode();
        encoded[2] = b'M';

        let err = EphemeralMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }

    #[test]
    fn invalid_tag_rejected() {
        let topics = NonEmpty::new(Topic::new([0x01; 32]));
        let msg = EphemeralMessage::Subscribe { topics };
        let mut encoded = msg.encode();
        encoded[4] = 0xFF; // tag byte is at position 4 (after schema)

        let err = EphemeralMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidEnumTag(_)),
            "expected InvalidEnumTag, got {err:?}"
        );
    }

    #[test]
    fn schema_matches_ephemeral_payload() {
        assert_eq!(EphemeralPayload::SCHEMA, EPHEMERAL_SCHEMA);
    }

    #[tokio::test]
    async fn signed_ephemeral_verifies() {
        let msg = make_signed_ephemeral(vec![0xDE, 0xAD]).await;

        let EphemeralMessage::Ephemeral(ref signed) = msg else {
            panic!("expected Ephemeral");
        };

        let verified = signed.try_verify();
        assert!(verified.is_ok(), "signature should verify");

        let payload = verified.expect("verified").into_payload();
        assert_eq!(payload.id, Topic::new([0xBB; 32]));
        assert_eq!(payload.nonce, 0x1234_5678_9ABC_DEF0);
        assert_eq!(payload.payload, vec![0xDE, 0xAD]);
    }

    #[tokio::test]
    async fn tampered_signature_fails_verify() {
        let msg = make_signed_ephemeral(vec![1, 2, 3]).await;

        let EphemeralMessage::Ephemeral(signed) = msg else {
            panic!("expected Ephemeral");
        };

        // Tamper with the wire bytes to invalidate the signature.
        let mut bytes = signed.into_bytes();
        // Flip a bit in the signature (last 64 bytes).
        let sig_start = bytes.len() - 64;
        bytes[sig_start] ^= 0xFF;

        let tampered = Signed::<EphemeralPayload>::try_decode(bytes);
        if let Ok(s) = tampered {
            assert!(
                s.try_verify().is_err(),
                "tampered signature should fail verification"
            );
        }
        // Decode failure is also acceptable — the bytes are corrupt.
    }

    #[tokio::test]
    async fn issuer_is_sender() {
        use subduction_crypto::signer::memory::MemorySigner;
        let signer = MemorySigner::from_bytes(&[0x42; 32]);
        let expected_peer = PeerId::from(signer.verifying_key());

        let ep = EphemeralPayload {
            id: Topic::new([0x01; 32]),
            nonce: 1,
            timestamp: TimestampSeconds::new(100),
            payload: vec![],
        };

        let verified = Signed::seal::<future_form::Sendable, _>(&signer, ep).await;
        let sealed = verified.into_signed();

        assert_eq!(PeerId::from(sealed.issuer()), expected_peer);
    }
}
