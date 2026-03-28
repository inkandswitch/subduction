//! Ephemeral message types and wire encoding.
//!
//! # Wire Layout
//!
//! Every ephemeral message on the wire begins with the `SUE\x00` schema
//! header, followed by a 4-byte big-endian total size, a 1-byte tag,
//! and the variant-specific payload:
//!
//! ```text
//! ╔════════╦═══════════╦═════╦═════════════════════════════╗
//! ║ Schema ║ TotalSize ║ Tag ║         Payload             ║
//! ║   4B   ║    4B     ║ 1B  ║         variable            ║
//! ╚════════╩═══════════╩═════╩═════════════════════════════╝
//! ```
//!
//! Tags are local to the `SUE` schema — no coordination with `SUM`'s
//! tag space is needed.
//!
//! ## `Ephemeral` variant (tag `0x00`)
//!
//! Signed by the originator. Relays forward the message as-is,
//! preserving the original sender and signature.
//!
//! ```text
//! ╔════════╦════════╦═══════╦════════════╦═════════╦═══════════╗
//! ║ Sender ║   ID   ║ Nonce ║ PayloadLen ║ Payload ║ Signature ║
//! ║  32B   ║  32B   ║  8B   ║  bijou64   ║  var    ║   64B     ║
//! ╚════════╩════════╩═══════╩════════════╩═════════╩═══════════╝
//!  ↑────────── signed region ──────────────────────↑
//! ```

use alloc::vec::Vec;

use ed25519_dalek::{Signature, VerifyingKey};
use nonempty::NonEmpty;
use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{Bijou64Error, DecodeError, InvalidEnumTag, InvalidSchema, SizeMismatch},
};

use subduction_core::{peer::id::PeerId, timestamp::TimestampSeconds};

use crate::topic::Topic;

/// Schema header for [`EphemeralMessage`] envelope: **SU**bduction **E**phemeral v0.
pub const EPHEMERAL_SCHEMA: [u8; 4] = *b"SUE\x00";

/// Minimum envelope size: `schema(4) + total_size(4) + tag(1)`.
const ENVELOPE_HEADER_SIZE: usize = 4 + 4 + 1;

mod tags {
    pub(super) const EPHEMERAL: u8 = 0x00;
    pub(super) const SUBSCRIBE: u8 = 0x01;
    pub(super) const UNSUBSCRIBE: u8 = 0x02;
    pub(super) const SUBSCRIBE_REJECTED: u8 = 0x03;
}

mod min_sizes {
    // sender(32) + topic(32) + nonce(8) + timestamp_ms(8) + payload_len(bijou64 min=1) + signature(64)
    pub(super) const EPHEMERAL: usize = 32 + 32 + 8 + 8 + 1 + 64;
    // count(2)
    pub(super) const SUBSCRIBE: usize = 2;
    // count(2)
    pub(super) const UNSUBSCRIBE: usize = 2;
    // count(2)
    pub(super) const SUBSCRIBE_REJECTED: usize = 2;
}

/// Wire message types for the ephemeral protocol.
///
/// These messages are scoped to the `SUE\x00` schema and are fully
/// independent of [`SyncMessage`]. Each variant maps to a tag byte
/// local to this schema.
///
/// [`SyncMessage`]: subduction_core::connection::message::SyncMessage
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EphemeralMessage {
    /// A signed ephemeral payload for a specific sedimentree topic.
    ///
    /// Fire-and-forget delivery to all subscribers of `id` (minus sender).
    /// The originator signs the message; relays forward it as-is.
    Ephemeral {
        /// The originator's peer identity (verifying key bytes).
        sender: PeerId,
        /// The sedimentree topic.
        id: Topic,
        /// Random nonce for deduplication.
        nonce: u64,
        /// UTC time at message creation (seconds since epoch).
        ///
        /// Inside the signed region — the originator commits to when
        /// the message was created. Receivers reject messages whose
        /// timestamp is too far from their own wall clock.
        timestamp: TimestampSeconds,
        /// Opaque application payload.
        payload: Vec<u8>,
        /// Ed25519 signature over
        /// `sender || id || nonce || timestamp_ms || payload_len || payload`.
        signature: Signature,
    },

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

impl EphemeralMessage {
    /// Build the byte sequence covered by the signature.
    ///
    /// Layout: `sender(32) || id(32) || nonce(8) || timestamp(8) || payload_len(bijou64) || payload`.
    ///
    /// Returns `None` for non-`Ephemeral` variants.
    #[must_use]
    pub fn signed_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::Ephemeral {
                sender,
                id,
                nonce,
                timestamp,
                payload,
                ..
            } => {
                let mut buf = Vec::with_capacity(
                    32 + 32 + 8 + 8 + bijou64::encoded_len(payload.len() as u64) + payload.len(),
                );
                buf.extend_from_slice(sender.as_bytes());
                buf.extend_from_slice(id.as_bytes());
                buf.extend_from_slice(&nonce.to_be_bytes());
                buf.extend_from_slice(&timestamp.as_secs().to_be_bytes());
                #[allow(clippy::cast_possible_truncation)]
                bijou64::encode(payload.len() as u64, &mut buf);
                buf.extend_from_slice(payload);
                Some(buf)
            }
            Self::Subscribe { .. } | Self::Unsubscribe { .. } | Self::SubscribeRejected { .. } => {
                None
            }
        }
    }

    /// Verify the signature on an `Ephemeral` message.
    ///
    /// Uses `ed25519-dalek` strict verification (rejects small-order points).
    ///
    /// # Errors
    ///
    /// Returns [`SignatureError`] if the variant is not `Ephemeral`,
    /// the sender bytes are not a valid verifying key, or the
    /// signature does not match the signed region.
    pub fn verify_signature(&self) -> Result<(), SignatureError> {
        let Self::Ephemeral {
            sender, signature, ..
        } = self
        else {
            return Err(SignatureError::NotEphemeral);
        };

        let Some(signed_bytes) = self.signed_bytes() else {
            return Err(SignatureError::NotEphemeral);
        };

        let verifying_key = VerifyingKey::from_bytes(sender.as_bytes())
            .map_err(|_| SignatureError::InvalidSenderKey)?;

        verifying_key
            .verify_strict(&signed_bytes, signature)
            .map_err(|_| SignatureError::InvalidSignature)
    }

    /// Construct a signed `Ephemeral` message.
    ///
    /// The caller provides the nonce and the claimed epoch timestamp.
    /// The signer produces the Ed25519 signature over the signed region.
    pub async fn new_signed<K: future_form::FutureForm, S: subduction_crypto::signer::Signer<K>>(
        signer: &S,
        id: Topic,
        nonce: u64,
        timestamp: TimestampSeconds,
        payload: Vec<u8>,
    ) -> Self {
        let sender = PeerId::from(signer.verifying_key());

        // Build the signed region.
        let mut signed_buf = Vec::with_capacity(
            32 + 32 + 8 + 8 + bijou64::encoded_len(payload.len() as u64) + payload.len(),
        );
        signed_buf.extend_from_slice(sender.as_bytes());
        signed_buf.extend_from_slice(id.as_bytes());
        signed_buf.extend_from_slice(&nonce.to_be_bytes());
        signed_buf.extend_from_slice(&timestamp.as_secs().to_be_bytes());
        #[allow(clippy::cast_possible_truncation)]
        bijou64::encode(payload.len() as u64, &mut signed_buf);
        signed_buf.extend_from_slice(&payload);

        let signature = signer.sign(&signed_buf).await;

        Self::Ephemeral {
            sender,
            id,
            nonce,
            timestamp,
            payload,
            signature,
        }
    }

    /// Payload byte count (after the tag byte, before the envelope header).
    const fn payload_size(&self) -> usize {
        match self {
            Self::Ephemeral { payload, .. } => {
                // sender(32) + id(32) + nonce(8) + timestamp_ms(8) + payload_len(bijou64) + payload + signature(64)
                32 + 32
                    + 8
                    + 8
                    + bijou64::encoded_len(payload.len() as u64)
                    + payload.len()
                    + Signature::BYTE_SIZE
            }
            Self::Subscribe { topics }
            | Self::Unsubscribe { topics }
            | Self::SubscribeRejected { topics } => 2 + topics.len() * 32,
        }
    }
}

/// Errors from ephemeral signature verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SignatureError {
    /// Attempted to verify a non-`Ephemeral` variant.
    #[error("not an Ephemeral message")]
    NotEphemeral,

    /// The `sender` bytes are not a valid Ed25519 verifying key.
    #[error("invalid sender verifying key")]
    InvalidSenderKey,

    /// The signature does not match the signed region.
    #[error("invalid signature")]
    InvalidSignature,
}

// ── Encode ──────────────────────────────────────────────────────────────

impl Encode for EphemeralMessage {
    fn encode(&self) -> Vec<u8> {
        encode_message(self)
    }

    fn encoded_size(&self) -> usize {
        ENVELOPE_HEADER_SIZE + self.payload_size()
    }
}

fn encode_message(msg: &EphemeralMessage) -> Vec<u8> {
    let payload_size = msg.payload_size();
    let total_size = ENVELOPE_HEADER_SIZE + payload_size;

    let mut buf = Vec::with_capacity(total_size);

    buf.extend_from_slice(&EPHEMERAL_SCHEMA);

    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(total_size as u32).to_be_bytes());

    match msg {
        EphemeralMessage::Ephemeral {
            sender,
            id,
            nonce,
            timestamp,
            payload,
            signature,
        } => {
            buf.push(tags::EPHEMERAL);
            encode_ephemeral(&mut buf, sender, id, *nonce, *timestamp, payload, signature);
        }
        EphemeralMessage::Subscribe { topics } => {
            buf.push(tags::SUBSCRIBE);
            encode_topic_list(&mut buf, topics);
        }
        EphemeralMessage::Unsubscribe { topics } => {
            buf.push(tags::UNSUBSCRIBE);
            encode_topic_list(&mut buf, topics);
        }
        EphemeralMessage::SubscribeRejected { topics } => {
            buf.push(tags::SUBSCRIBE_REJECTED);
            encode_topic_list(&mut buf, topics);
        }
    }

    buf
}

fn encode_ephemeral(
    buf: &mut Vec<u8>,
    sender: &PeerId,
    id: &Topic,
    nonce: u64,
    timestamp: TimestampSeconds,
    payload: &[u8],
    signature: &Signature,
) {
    buf.extend_from_slice(sender.as_bytes());
    buf.extend_from_slice(id.as_bytes());
    buf.extend_from_slice(&nonce.to_be_bytes());
    buf.extend_from_slice(&timestamp.as_secs().to_be_bytes());
    #[allow(clippy::cast_possible_truncation)]
    bijou64::encode(payload.len() as u64, buf);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&signature.to_bytes());
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
    const MIN_SIZE: usize = ENVELOPE_HEADER_SIZE;

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        decode_message(buf)
    }
}

#[allow(clippy::indexing_slicing)] // Length validated before access
fn decode_message(bytes: &[u8]) -> Result<EphemeralMessage, DecodeError> {
    if bytes.len() < ENVELOPE_HEADER_SIZE {
        return Err(DecodeError::MessageTooShort {
            type_name: "EphemeralMessage envelope",
            need: ENVELOPE_HEADER_SIZE,
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

    let total_size = u32::from_be_bytes(bytes.get(4..8).and_then(|s| s.try_into().ok()).ok_or(
        DecodeError::MessageTooShort {
            type_name: "EphemeralMessage total_size",
            need: 8,
            have: bytes.len(),
        },
    )?) as usize;
    if bytes.len() != total_size {
        return Err(SizeMismatch {
            declared: total_size,
            actual: bytes.len(),
        }
        .into());
    }

    let tag = *bytes.get(8).ok_or(DecodeError::MessageTooShort {
        type_name: "EphemeralMessage tag",
        need: 9,
        have: bytes.len(),
    })?;
    let payload = bytes
        .get(ENVELOPE_HEADER_SIZE..)
        .ok_or(DecodeError::MessageTooShort {
            type_name: "EphemeralMessage payload",
            need: ENVELOPE_HEADER_SIZE,
            have: bytes.len(),
        })?;

    let (min_payload_size, type_name) = match tag {
        tags::EPHEMERAL => (min_sizes::EPHEMERAL, "Ephemeral"),
        tags::SUBSCRIBE => (min_sizes::SUBSCRIBE, "EphemeralSubscribe"),
        tags::UNSUBSCRIBE => (min_sizes::UNSUBSCRIBE, "EphemeralUnsubscribe"),
        tags::SUBSCRIBE_REJECTED => (min_sizes::SUBSCRIBE_REJECTED, "EphemeralSubscribeRejected"),
        _ => {
            return Err(InvalidEnumTag {
                tag,
                type_name: "EphemeralMessage",
            }
            .into());
        }
    };

    if payload.len() < min_payload_size {
        return Err(DecodeError::MessageTooShort {
            type_name,
            need: ENVELOPE_HEADER_SIZE + min_payload_size,
            have: bytes.len(),
        });
    }

    match tag {
        tags::EPHEMERAL => decode_ephemeral(payload),
        tags::SUBSCRIBE => {
            decode_topic_list(payload).map(|topics| EphemeralMessage::Subscribe { topics })
        }
        tags::UNSUBSCRIBE => {
            decode_topic_list(payload).map(|topics| EphemeralMessage::Unsubscribe { topics })
        }
        tags::SUBSCRIBE_REJECTED => {
            decode_topic_list(payload).map(|topics| EphemeralMessage::SubscribeRejected { topics })
        }
        _ => unreachable!("tag validated above"),
    }
}

fn decode_ephemeral(payload: &[u8]) -> Result<EphemeralMessage, DecodeError> {
    let mut offset = 0;

    let sender = PeerId::new(read_array::<32>(payload, &mut offset)?);
    let id = Topic::new(read_array::<32>(payload, &mut offset)?);
    let nonce = u64::from_be_bytes(read_array::<8>(payload, &mut offset)?);
    let timestamp =
        TimestampSeconds::new(u64::from_be_bytes(read_array::<8>(payload, &mut offset)?));
    let payload_len = read_bijou64_as_usize(payload, &mut offset)?;

    let data = payload
        .get(offset..offset + payload_len)
        .ok_or(DecodeError::MessageTooShort {
            type_name: "Ephemeral payload data",
            need: offset + payload_len,
            have: payload.len(),
        })?
        .to_vec();
    offset += payload_len;

    let signature_bytes: [u8; 64] = read_array::<64>(payload, &mut offset)?;
    let signature = Signature::from_bytes(&signature_bytes);

    Ok(EphemeralMessage::Ephemeral {
        sender,
        id,
        nonce,
        timestamp,
        payload: data,
        signature,
    })
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

fn read_bijou64_as_usize(buf: &[u8], offset: &mut usize) -> Result<usize, DecodeError> {
    let remaining = buf.get(*offset..).ok_or(DecodeError::MessageTooShort {
        type_name: "bijou64",
        need: *offset + 1,
        have: buf.len(),
    })?;
    let (value, consumed) = bijou64::decode(remaining).map_err(|kind| Bijou64Error {
        offset: *offset,
        kind,
    })?;
    *offset += consumed;
    #[allow(clippy::cast_possible_truncation)]
    Ok(value as usize)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    /// Helper: create a dummy signed ephemeral for wire roundtrip tests.
    /// Uses a fixed "signature" (not cryptographically valid).
    fn dummy_ephemeral(payload: Vec<u8>) -> EphemeralMessage {
        EphemeralMessage::Ephemeral {
            sender: PeerId::new([0xAA; 32]),
            id: Topic::new([0xBB; 32]),
            nonce: 0x1234_5678_9ABC_DEF0,
            timestamp: TimestampSeconds::new(1_700_000_000),
            payload,
            signature: Signature::from_bytes(&[0xCC; 64]),
        }
    }

    #[test]
    fn ephemeral_roundtrip() {
        let msg = dummy_ephemeral(vec![1, 2, 3, 4, 5]);

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn empty_payload_roundtrip() {
        let msg = dummy_ephemeral(vec![]);

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_roundtrip() {
        let ids = vec![Topic::new([0x01; 32]), Topic::new([0x02; 32])];
        let msg = EphemeralMessage::Subscribe { ids: ids.clone() };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let ids = vec![Topic::new([0xFF; 32])];
        let msg = EphemeralMessage::Unsubscribe { ids };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_rejected_roundtrip() {
        let ids = vec![Topic::new([0x42; 32])];
        let msg = EphemeralMessage::SubscribeRejected { ids };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn empty_subscribe_roundtrip() {
        let msg = EphemeralMessage::Subscribe { ids: vec![] };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn wrong_schema_rejected() {
        let msg = EphemeralMessage::Subscribe { ids: vec![] };
        let mut encoded = msg.encode();
        // Tamper the schema: SUE -> SUM
        encoded[2] = b'M';

        let err = EphemeralMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }

    #[test]
    fn invalid_tag_rejected() {
        let msg = EphemeralMessage::Subscribe { ids: vec![] };
        let mut encoded = msg.encode();
        // Tamper the tag byte (offset 8)
        encoded[8] = 0xFF;

        let err = EphemeralMessage::try_decode(&encoded).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidEnumTag(_)),
            "expected InvalidEnumTag, got {err:?}"
        );
    }

    #[test]
    fn schema_bytes_are_correct() {
        assert_eq!(&EPHEMERAL_SCHEMA, b"SUE\x00");
    }

    #[test]
    fn signed_bytes_contains_all_fields() {
        let msg = dummy_ephemeral(vec![0xDE, 0xAD]);
        let signed = msg.signed_bytes().expect("Ephemeral has signed_bytes");

        // sender(32) + id(32) + nonce(8) + timestamp_ms(8) + payload_len(1 for len=2) + payload(2)
        assert_eq!(signed.len(), 32 + 32 + 8 + 8 + 1 + 2);
        assert_eq!(&signed[0..32], &[0xAA; 32]); // sender
        assert_eq!(&signed[32..64], &[0xBB; 32]); // id
        assert_eq!(&signed[64..72], &0x1234_5678_9ABC_DEF0_u64.to_be_bytes()); // nonce
        assert_eq!(&signed[72..80], &1_700_000_000_u64.to_be_bytes()); // timestamp
    }

    #[test]
    fn non_ephemeral_has_no_signed_bytes() {
        let msg = EphemeralMessage::Subscribe { ids: vec![] };
        assert!(msg.signed_bytes().is_none());
    }

    #[test]
    fn tampered_signature_fails_verify() {
        let mut msg = dummy_ephemeral(vec![1, 2, 3]);
        // The dummy has a fake signature — verification should fail
        // because [0xCC; 64] is not a valid Ed25519 signature for any message.
        assert!(msg.verify_signature().is_err());

        // Even if we tamper further, it should still fail
        if let EphemeralMessage::Ephemeral {
            ref mut signature, ..
        } = msg
        {
            let mut bytes = signature.to_bytes();
            bytes[0] ^= 0xFF;
            *signature = Signature::from_bytes(&bytes);
        }
        assert!(msg.verify_signature().is_err());
    }
}
