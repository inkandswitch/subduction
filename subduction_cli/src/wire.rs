//! Wire message enum for multiplexing sync, ephemeral, and keyhive
//! traffic over a single physical connection.
//!
//! This is an application-level type defined in the CLI crate. The
//! transport layer is generic over message types via `ChannelMessage`;
//! this module provides the concrete enum and trait impls for all three
//! transport crates (websocket, iroh, HTTP long-poll).

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema, SizeMismatch},
};
use subduction_core::connection::message::{BatchSyncResponse, MESSAGE_SCHEMA, SyncMessage};
use subduction_ephemeral::message::{EPHEMERAL_SCHEMA, EphemeralMessage};

/// Schema header for keyhive messages: **SU**bduction **K**eyhive v0.
const KEYHIVE_SCHEMA: [u8; 4] = *b"SUK\x00";

/// Composed wire message carrying sync, ephemeral, or keyhive traffic.
///
/// Encode delegates to the inner variant (schema headers are already
/// distinct: `SUM\x00` vs `SUE\x00` vs `SUK\x00`). Decode reads the
/// 4-byte schema header and dispatches to the appropriate decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliWireMessage {
    /// A sync-protocol message.
    Sync(Box<SyncMessage>),

    /// An ephemeral-protocol message.
    Ephemeral(EphemeralMessage),

    /// A keyhive-protocol message (raw framed bytes).
    ///
    /// Carries the complete `SUK\x00`-framed wire bytes. The handler
    /// is responsible for decoding the inner payload.
    Keyhive(Vec<u8>),
}

impl From<SyncMessage> for CliWireMessage {
    fn from(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }
}

impl From<EphemeralMessage> for CliWireMessage {
    fn from(msg: EphemeralMessage) -> Self {
        Self::Ephemeral(msg)
    }
}

impl Encode for CliWireMessage {
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

impl Decode for CliWireMessage {
    const MIN_SIZE: usize = 8; // schema(4) + total_size(4)

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < 4 {
            return Err(DecodeError::MessageTooShort {
                type_name: "CliWireMessage schema",
                need: 4,
                have: buf.len(),
            });
        }

        let schema: [u8; 4] =
            buf.get(0..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "CliWireMessage schema",
                    need: 4,
                    have: buf.len(),
                })?;

        match schema {
            MESSAGE_SCHEMA => {
                SyncMessage::try_decode(buf).map(|m| CliWireMessage::Sync(Box::new(m)))
            }
            EPHEMERAL_SCHEMA => EphemeralMessage::try_decode(buf).map(CliWireMessage::Ephemeral),
            KEYHIVE_SCHEMA => decode_keyhive_frame(buf).map(CliWireMessage::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

// ── ChannelMessage impls ───────────────────────────────────────────────

impl subduction_http_longpoll::connection::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(sync_msg) => match sync_msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::LooseCommit { .. }
                | SyncMessage::Fragment { .. }
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::BatchSyncRequest(_)
                | SyncMessage::RemoveSubscriptions(_)
                | SyncMessage::DataRequestRejected(_) => None,
            },
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }
}

impl subduction_iroh::connection::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(sync_msg) => match sync_msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::LooseCommit { .. }
                | SyncMessage::Fragment { .. }
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::BatchSyncRequest(_)
                | SyncMessage::RemoveSubscriptions(_)
                | SyncMessage::DataRequestRejected(_) => None,
            },
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }
}

impl subduction_websocket::websocket::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(sync_msg) => match sync_msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::LooseCommit { .. }
                | SyncMessage::Fragment { .. }
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::BatchSyncRequest(_)
                | SyncMessage::RemoveSubscriptions(_)
                | SyncMessage::DataRequestRejected(_) => None,
            },
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }
}

/// Validate the `SUK\x00` frame envelope and return the complete framed bytes.
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
mod tests {
    use super::*;
    use sedimentree_core::{
        codec::{decode::Decode, encode::Encode},
        sedimentree::FingerprintSummary,
    };
    use std::collections::BTreeSet;
    use subduction_core::connection::message::SyncMessage;
    use testresult::TestResult;

    fn empty_fingerprint_summary() -> FingerprintSummary {
        FingerprintSummary::new(
            sedimentree_core::crypto::fingerprint::FingerprintSeed::new(0, 0),
            BTreeSet::new(),
            BTreeSet::new(),
        )
    }

    /// Build a minimal keyhive frame: `SUK\x00` + big-endian total size.
    fn keyhive_frame(payload: &[u8]) -> Vec<u8> {
        let total: u32 = (8 + payload.len()) as u32;
        let mut buf = Vec::with_capacity(total as usize);
        buf.extend_from_slice(b"SUK\x00");
        buf.extend_from_slice(&total.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    #[test]
    fn roundtrip_sync() -> TestResult {
        let req_id = subduction_core::connection::message::RequestId {
            requestor: subduction_core::peer::id::PeerId::new([1u8; 32]),
            nonce: 42,
        };
        let inner =
            SyncMessage::BatchSyncRequest(subduction_core::connection::message::BatchSyncRequest {
                id: sedimentree_core::id::SedimentreeId::new([0u8; 32]),
                req_id,
                fingerprint_summary: empty_fingerprint_summary(),
                subscribe: false,
            });
        let wire = CliWireMessage::from(inner.clone());
        let encoded = wire.encode();
        let decoded = CliWireMessage::try_decode(&encoded)?;

        match decoded {
            CliWireMessage::Sync(msg) => assert_eq!(*msg, inner),
            other => panic!("expected Sync, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn roundtrip_keyhive() -> TestResult {
        let payload = b"hello keyhive";
        let frame = keyhive_frame(payload);
        let wire = CliWireMessage::Keyhive(frame.clone());
        let encoded = wire.encode();
        let decoded = CliWireMessage::try_decode(&encoded)?;

        assert_eq!(decoded, CliWireMessage::Keyhive(frame));
        Ok(())
    }

    #[test]
    fn decode_too_short_fails() {
        let result = CliWireMessage::try_decode(&[0, 1, 2]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_unknown_schema_fails() {
        let mut buf = vec![0xFF, 0xFF, 0xFF, 0xFF];
        buf.extend_from_slice(&[0, 0, 0, 8]); // total_size = 8
        let result = CliWireMessage::try_decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn keyhive_size_mismatch_fails() {
        let mut frame = keyhive_frame(b"data");
        frame.push(0xFF); // extra byte → size mismatch
        let result = CliWireMessage::try_decode(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn from_sync_message() {
        let req_id = subduction_core::connection::message::RequestId {
            requestor: subduction_core::peer::id::PeerId::new([0u8; 32]),
            nonce: 0,
        };
        let msg =
            SyncMessage::BatchSyncRequest(subduction_core::connection::message::BatchSyncRequest {
                id: sedimentree_core::id::SedimentreeId::new([0u8; 32]),
                req_id,
                fingerprint_summary: empty_fingerprint_summary(),
                subscribe: false,
            });
        let wire = CliWireMessage::from(msg);
        assert!(matches!(wire, CliWireMessage::Sync(_)));
    }
}
