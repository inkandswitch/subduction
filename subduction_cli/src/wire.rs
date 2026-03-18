//! Wire message enum for multiplexing sync, ephemeral, and keyhive
//! traffic over a single physical connection.
//!
//! This is an application-level type. The transport layer is generic
//! over message types via `MessageTransport`; this module provides the
//! concrete enum and trait impls for the CLI server.

use std::vec::Vec;

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema},
};
use subduction_core::connection::message::{SyncMessage, MESSAGE_SCHEMA};
use subduction_ephemeral::message::{EphemeralMessage, EPHEMERAL_SCHEMA};
use subduction_keyhive::{KeyhiveMessage, KEYHIVE_SCHEMA};

/// Composed wire message for the CLI server.
///
/// Carries sync, ephemeral, or keyhive traffic. Decode reads the 4-byte
/// schema header and dispatches to the appropriate decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliWireMessage {
    /// A sync-protocol message (`SUM\x00`).
    Sync(Box<SyncMessage>),

    /// An ephemeral-protocol message (`SUE\x00`).
    Ephemeral(EphemeralMessage),

    /// A keyhive-protocol message (`SUK\x00`).
    Keyhive(KeyhiveMessage),
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

impl From<KeyhiveMessage> for CliWireMessage {
    fn from(msg: KeyhiveMessage) -> Self {
        Self::Keyhive(msg)
    }
}

impl Encode for CliWireMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync(msg) => Encode::encode(msg.as_ref()),
            Self::Ephemeral(msg) => msg.encode(),
            Self::Keyhive(msg) => msg.encode(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Ephemeral(msg) => msg.encoded_size(),
            Self::Keyhive(msg) => msg.encoded_size(),
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
            KEYHIVE_SCHEMA => KeyhiveMessage::try_decode(buf).map(CliWireMessage::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

impl subduction_ephemeral::composed::WireEnvelope for CliWireMessage {
    fn dispatch(self) -> subduction_ephemeral::composed::Dispatched {
        match self {
            Self::Sync(msg) => subduction_ephemeral::composed::Dispatched::Sync(msg),
            Self::Ephemeral(msg) => subduction_ephemeral::composed::Dispatched::Ephemeral(msg),
            Self::Keyhive(_) => {
                // Keyhive messages are handled directly by CliHandler,
                // not through the WireEnvelope/ComposedHandler dispatch path.
                unreachable!("keyhive messages should not be dispatched via WireEnvelope")
            }
        }
    }

    fn as_batch_sync_response(
        &self,
    ) -> Option<&subduction_core::connection::message::BatchSyncResponse> {
        match self {
            Self::Sync(msg) => match msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::BatchSyncRequest(_)
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::DataRequestRejected(_)
                | SyncMessage::Fragment { .. }
                | SyncMessage::LooseCommit { .. }
                | SyncMessage::RemoveSubscriptions(_) => None,
            },
            Self::Ephemeral(_) | Self::Keyhive(_) => None,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use sedimentree_core::{codec::encode::Encode, id::SedimentreeId};
    use subduction_core::connection::message::{
        BatchSyncResponse, RemoveSubscriptions, RequestId, SyncResult,
    };
    use subduction_ephemeral::composed::{Dispatched, WireEnvelope};

    fn test_peer_id() -> subduction_core::peer::id::PeerId {
        subduction_core::peer::id::PeerId::new([42u8; 32])
    }

    fn test_request_id() -> RequestId {
        RequestId {
            requestor: test_peer_id(),
            nonce: 99,
        }
    }

    // ── Encode / Decode roundtrips ──────────────────────────────────────

    #[test]
    fn sync_message_roundtrip() {
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::RemoveSubscriptions(
            RemoveSubscriptions {
                ids: vec![
                    SedimentreeId::new([0x01; 32]),
                    SedimentreeId::new([0x02; 32]),
                ],
            },
        )));

        let encoded = msg.encode();
        let decoded = CliWireMessage::try_decode(&encoded).expect("decode sync");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn ephemeral_message_roundtrip() {
        let msg = CliWireMessage::Ephemeral(EphemeralMessage::Ephemeral {
            id: SedimentreeId::new([0xAA; 32]),
            payload: vec![10, 20, 30],
        });

        let encoded = msg.encode();
        let decoded = CliWireMessage::try_decode(&encoded).expect("decode ephemeral");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn batch_sync_response_roundtrip() {
        let resp = BatchSyncResponse {
            req_id: test_request_id(),
            id: SedimentreeId::new([0xFF; 32]),
            result: SyncResult::NotFound,
        };
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp)));

        let encoded = msg.encode();
        let decoded = CliWireMessage::try_decode(&encoded).expect("decode batch sync response");
        assert_eq!(decoded, msg);
    }

    // ── WireEnvelope::dispatch ──────────────────────────────────────────

    #[test]
    fn dispatch_sync_returns_sync() {
        let inner = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0x11; 32]),
            digests: vec![],
        };
        let wire = CliWireMessage::Sync(Box::new(inner.clone()));

        match wire.dispatch() {
            Dispatched::Sync(msg) => assert_eq!(*msg, inner),
            Dispatched::Ephemeral(_) => panic!("expected Sync variant"),
        }
    }

    #[test]
    fn dispatch_ephemeral_returns_ephemeral() {
        let inner = EphemeralMessage::Subscribe {
            ids: vec![SedimentreeId::new([0x22; 32])],
        };
        let wire = CliWireMessage::Ephemeral(inner.clone());

        match wire.dispatch() {
            Dispatched::Ephemeral(msg) => assert_eq!(msg, inner),
            Dispatched::Sync(_) => panic!("expected Ephemeral variant"),
        }
    }

    // ── WireEnvelope::as_batch_sync_response ────────────────────────────

    #[test]
    fn as_batch_sync_response_extracts_response() {
        let resp = BatchSyncResponse {
            req_id: test_request_id(),
            id: SedimentreeId::new([0x33; 32]),
            result: SyncResult::NotFound,
        };
        let wire = CliWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp.clone())));

        assert_eq!(wire.as_batch_sync_response(), Some(&resp));
    }

    #[test]
    fn as_batch_sync_response_none_for_other_sync() {
        let wire = CliWireMessage::Sync(Box::new(SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0x44; 32]),
            digests: vec![],
        }));

        assert_eq!(wire.as_batch_sync_response(), None);
    }

    #[test]
    fn as_batch_sync_response_none_for_ephemeral() {
        let wire = CliWireMessage::Ephemeral(EphemeralMessage::Ephemeral {
            id: SedimentreeId::new([0x55; 32]),
            payload: vec![1],
        });

        assert_eq!(wire.as_batch_sync_response(), None);
    }
}
