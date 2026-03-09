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

use alloc::vec::Vec;

use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{Bijou64Error, DecodeError, InvalidEnumTag, InvalidSchema, SizeMismatch},
    },
    id::SedimentreeId,
};

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
    // sed_id(32) + payload_len(bijou64 min=1)
    pub(super) const EPHEMERAL: usize = 32 + 1;
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
    /// An ephemeral payload for a specific sedimentree topic.
    ///
    /// Fire-and-forget delivery to all subscribers of `id` (minus sender).
    Ephemeral {
        /// The sedimentree topic.
        id: SedimentreeId,
        /// Opaque application payload.
        payload: Vec<u8>,
    },

    /// Subscribe to ephemeral messages for the given sedimentree IDs.
    Subscribe {
        /// The sedimentree IDs to subscribe to.
        ids: Vec<SedimentreeId>,
    },

    /// Unsubscribe from ephemeral messages for the given sedimentree IDs.
    Unsubscribe {
        /// The sedimentree IDs to unsubscribe from.
        ids: Vec<SedimentreeId>,
    },

    /// Notification that some subscribe requests were rejected.
    ///
    /// Contains only the rejected IDs. Accepted IDs are implied by omission.
    SubscribeRejected {
        /// The sedimentree IDs that were rejected.
        ids: Vec<SedimentreeId>,
    },
}

impl EphemeralMessage {
    /// Get the sedimentree ID associated with this message, if unique.
    ///
    /// Returns `None` for multi-ID messages (subscribe/unsubscribe/rejected).
    #[must_use]
    pub const fn sedimentree_id(&self) -> Option<SedimentreeId> {
        match self {
            Self::Ephemeral { id, .. } => Some(*id),
            Self::Subscribe { .. } | Self::Unsubscribe { .. } | Self::SubscribeRejected { .. } => {
                None
            }
        }
    }

    /// Payload byte count (after the tag byte, before the envelope header).
    const fn payload_size(&self) -> usize {
        match self {
            Self::Ephemeral { payload, .. } => {
                32 + bijou64::encoded_len(payload.len() as u64) + payload.len()
            }
            Self::Subscribe { ids }
            | Self::Unsubscribe { ids }
            | Self::SubscribeRejected { ids } => 2 + ids.len() * 32,
        }
    }
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
        EphemeralMessage::Ephemeral { id, payload } => {
            buf.push(tags::EPHEMERAL);
            encode_ephemeral(&mut buf, id, payload);
        }
        EphemeralMessage::Subscribe { ids } => {
            buf.push(tags::SUBSCRIBE);
            encode_id_list(&mut buf, ids);
        }
        EphemeralMessage::Unsubscribe { ids } => {
            buf.push(tags::UNSUBSCRIBE);
            encode_id_list(&mut buf, ids);
        }
        EphemeralMessage::SubscribeRejected { ids } => {
            buf.push(tags::SUBSCRIBE_REJECTED);
            encode_id_list(&mut buf, ids);
        }
    }

    buf
}

fn encode_ephemeral(buf: &mut Vec<u8>, id: &SedimentreeId, payload: &[u8]) {
    buf.extend_from_slice(id.as_bytes());
    #[allow(clippy::cast_possible_truncation)]
    bijou64::encode(payload.len() as u64, buf);
    buf.extend_from_slice(payload);
}

fn encode_id_list(buf: &mut Vec<u8>, ids: &[SedimentreeId]) {
    #[allow(clippy::cast_possible_truncation)]
    buf.extend_from_slice(&(ids.len() as u16).to_be_bytes());
    for id in ids {
        buf.extend_from_slice(id.as_bytes());
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
        tags::SUBSCRIBE => decode_id_list(payload).map(|ids| EphemeralMessage::Subscribe { ids }),
        tags::UNSUBSCRIBE => {
            decode_id_list(payload).map(|ids| EphemeralMessage::Unsubscribe { ids })
        }
        tags::SUBSCRIBE_REJECTED => {
            decode_id_list(payload).map(|ids| EphemeralMessage::SubscribeRejected { ids })
        }
        _ => unreachable!("tag validated above"),
    }
}

fn decode_ephemeral(payload: &[u8]) -> Result<EphemeralMessage, DecodeError> {
    let mut offset = 0;

    let id = SedimentreeId::new(read_array::<32>(payload, &mut offset)?);
    let payload_len = read_bijou64_as_usize(payload, &mut offset)?;

    let data = payload
        .get(offset..offset + payload_len)
        .ok_or(DecodeError::MessageTooShort {
            type_name: "Ephemeral payload data",
            need: offset + payload_len,
            have: payload.len(),
        })?
        .to_vec();

    Ok(EphemeralMessage::Ephemeral { id, payload: data })
}

fn decode_id_list(payload: &[u8]) -> Result<Vec<SedimentreeId>, DecodeError> {
    let mut offset = 0;

    let count = read_u16(payload, &mut offset)? as usize;

    let mut ids = Vec::with_capacity(count);
    for _ in 0..count {
        ids.push(SedimentreeId::new(read_array::<32>(payload, &mut offset)?));
    }

    Ok(ids)
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

    #[test]
    fn ephemeral_roundtrip() {
        let id = SedimentreeId::new([0xAB; 32]);
        let payload = vec![1, 2, 3, 4, 5];
        let msg = EphemeralMessage::Ephemeral {
            id,
            payload: payload.clone(),
        };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_roundtrip() {
        let ids = vec![
            SedimentreeId::new([0x01; 32]),
            SedimentreeId::new([0x02; 32]),
        ];
        let msg = EphemeralMessage::Subscribe { ids: ids.clone() };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn unsubscribe_roundtrip() {
        let ids = vec![SedimentreeId::new([0xFF; 32])];
        let msg = EphemeralMessage::Unsubscribe { ids };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn subscribe_rejected_roundtrip() {
        let ids = vec![SedimentreeId::new([0x42; 32])];
        let msg = EphemeralMessage::SubscribeRejected { ids };

        let encoded = msg.encode();
        let decoded = EphemeralMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, msg);
    }

    #[test]
    fn empty_payload_roundtrip() {
        let msg = EphemeralMessage::Ephemeral {
            id: SedimentreeId::new([0x00; 32]),
            payload: vec![],
        };

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
}
