//! Composed wire message carrying either sync or ephemeral traffic.
//!
//! A single physical connection carries both protocols via [`WireMessage`].
//! The encoded bytes are identical to encoding the inner message directly —
//! there is no extra framing. Dispatch happens at decode time by reading the
//! 4-byte schema header:
//!
//! - `SUM\x00` → [`SyncMessage`]
//! - `SUE\x00` → [`EphemeralMessage`]
//!
//! [`SyncMessage`]: subduction_core::connection::message::SyncMessage

use alloc::{boxed::Box, vec::Vec};

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema, SizeMismatch},
};
use subduction_core::connection::message::{MESSAGE_SCHEMA, SyncMessage};

use crate::message::{EPHEMERAL_SCHEMA, EphemeralMessage};

/// Schema header for keyhive messages: **SU**bduction **K**eyhive v0.
///
/// Duplicated here to avoid a circular dependency on `subduction_keyhive`.
/// Must be kept in sync with `subduction_keyhive::wire::KEYHIVE_SCHEMA`.
pub const KEYHIVE_SCHEMA: [u8; 4] = *b"SUK\x00";

/// Composed wire message carrying sync, ephemeral, or keyhive traffic.
///
/// Encode delegates to the inner variant (schema headers are already
/// distinct: `SUM\x00` vs `SUE\x00` vs `SUK\x00`). Decode reads the
/// 4-byte schema header and dispatches to the appropriate decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    /// A sync-protocol message.
    Sync(Box<SyncMessage>),
    /// An ephemeral-protocol message.
    Ephemeral(EphemeralMessage),
    /// A keyhive-protocol message (raw framed bytes).
    ///
    /// Carries the complete `SUK\x00`-framed wire bytes. The handler
    /// is responsible for decoding the inner payload via
    /// `subduction_keyhive::KeyhiveMessage::try_decode`.
    Keyhive(Vec<u8>),
}

impl From<SyncMessage> for WireMessage {
    fn from(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }
}

impl From<EphemeralMessage> for WireMessage {
    fn from(msg: EphemeralMessage) -> Self {
        Self::Ephemeral(msg)
    }
}

impl Encode for WireMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync(msg) => Encode::encode(msg.as_ref()),
            Self::Ephemeral(msg) => msg.encode(),
            Self::Keyhive(raw) => raw.clone(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Ephemeral(msg) => msg.encoded_size(),
            Self::Keyhive(raw) => raw.len(),
        }
    }
}

impl Decode for WireMessage {
    /// Minimum size is the smallest envelope header: `SUK` has no tag (8 bytes).
    const MIN_SIZE: usize = 8; // schema(4) + total_size(4)

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < 4 {
            return Err(DecodeError::MessageTooShort {
                type_name: "WireMessage schema",
                need: 4,
                have: buf.len(),
            });
        }

        let schema: [u8; 4] =
            buf.get(0..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "WireMessage schema",
                    need: 4,
                    have: buf.len(),
                })?;

        match schema {
            MESSAGE_SCHEMA => SyncMessage::try_decode(buf).map(|m| WireMessage::Sync(Box::new(m))),
            EPHEMERAL_SCHEMA => EphemeralMessage::try_decode(buf).map(WireMessage::Ephemeral),
            KEYHIVE_SCHEMA => decode_keyhive_frame(buf).map(WireMessage::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA, // Use sync as the "expected" — all three are valid
                got: schema,
            }
            .into()),
        }
    }
}

/// Validate the `SUK\x00` frame envelope and return the complete framed bytes.
///
/// We only validate the schema + total_size here. The CBOR payload
/// inside is decoded later by the keyhive handler.
fn decode_keyhive_frame(buf: &[u8]) -> Result<Vec<u8>, DecodeError> {
    if buf.len() < 8 {
        return Err(DecodeError::MessageTooShort {
            type_name: "KeyhiveMessage envelope",
            need: 8,
            have: buf.len(),
        });
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

    Ok(buf.to_vec())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use sedimentree_core::id::SedimentreeId;

    use super::*;

    #[test]
    fn sync_roundtrip_through_wire() {
        // Use a simple SyncMessage variant — RemoveSubscriptions
        let sync_msg = SyncMessage::RemoveSubscriptions(
            subduction_core::connection::message::RemoveSubscriptions {
                ids: vec![SedimentreeId::new([0x01; 32])],
            },
        );

        let wire = WireMessage::from(sync_msg.clone());
        let encoded = wire.encode();
        let decoded = WireMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, WireMessage::Sync(Box::new(sync_msg)));
    }

    #[test]
    fn ephemeral_roundtrip_through_wire() {
        let eph_msg = EphemeralMessage::Ephemeral {
            id: SedimentreeId::new([0xAB; 32]),
            payload: vec![10, 20, 30],
        };

        let wire = WireMessage::from(eph_msg.clone());
        let encoded = wire.encode();
        let decoded = WireMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, WireMessage::Ephemeral(eph_msg));
    }

    #[test]
    fn unknown_schema_rejected() {
        let mut buf = vec![0u8; 20];
        buf[0..4].copy_from_slice(b"XXX\x00");
        // Set total_size to match buf length
        buf[4..8].copy_from_slice(&20u32.to_be_bytes());

        let err = WireMessage::try_decode(&buf).unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidSchema(_)),
            "expected InvalidSchema, got {err:?}"
        );
    }

    #[test]
    fn wire_encode_is_transparent() {
        // Encoding a WireMessage should produce identical bytes to encoding
        // the inner message directly.
        let eph_msg = EphemeralMessage::Subscribe {
            ids: vec![SedimentreeId::new([0x42; 32])],
        };

        let direct = eph_msg.encode();
        let via_wire = WireMessage::Ephemeral(eph_msg).encode();

        assert_eq!(direct, via_wire);
    }

    #[test]
    fn keyhive_roundtrip_through_wire() {
        // Build a minimal SUK frame: schema(4) + total_size(4) + payload
        let payload = vec![0xCA, 0xFE];
        let total_size = (8 + payload.len()) as u32;
        let mut raw = Vec::with_capacity(total_size as usize);
        raw.extend_from_slice(&KEYHIVE_SCHEMA);
        raw.extend_from_slice(&total_size.to_be_bytes());
        raw.extend_from_slice(&payload);

        let wire = WireMessage::Keyhive(raw.clone());
        let encoded = wire.encode();
        let decoded = WireMessage::try_decode(&encoded).expect("decode");

        assert_eq!(decoded, WireMessage::Keyhive(raw));
    }

    #[test]
    fn keyhive_encode_is_passthrough() {
        let raw = {
            let payload = vec![0x01, 0x02, 0x03];
            let total_size = (8 + payload.len()) as u32;
            let mut buf = Vec::new();
            buf.extend_from_slice(&KEYHIVE_SCHEMA);
            buf.extend_from_slice(&total_size.to_be_bytes());
            buf.extend_from_slice(&payload);
            buf
        };

        let wire = WireMessage::Keyhive(raw.clone());
        assert_eq!(wire.encode(), raw);
    }
}
