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
use subduction_core::connection::message::{MESSAGE_SCHEMA, SyncMessage};
use subduction_ephemeral::message::{EPHEMERAL_SCHEMA, EphemeralMessage};
use subduction_keyhive::{KEYHIVE_SCHEMA, KeyhiveMessage};

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

#[cfg(test)]
mod tests {
    use super::*;
    use future_form::Sendable;
    use sedimentree_core::{codec::encode::Encode, id::SedimentreeId};
    use subduction_core::{
        connection::message::{BatchSyncResponse, RemoveSubscriptions, RequestId, SyncResult},
        remote_heads::RemoteHeads,
        timestamp::TimestampSeconds,
    };
    use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
    use subduction_ephemeral::{message::EphemeralPayload, topic::Topic};
    use testresult::TestResult;

    fn test_peer_id() -> subduction_core::peer::id::PeerId {
        subduction_core::peer::id::PeerId::new([42u8; 32])
    }

    fn test_request_id() -> RequestId {
        RequestId {
            requestor: test_peer_id(),
            nonce: 99,
        }
    }

    mod roundtrip {
        use super::*;

        #[test]
        fn sync() -> TestResult {
            let msg = CliWireMessage::Sync(Box::new(SyncMessage::RemoveSubscriptions(
                RemoveSubscriptions {
                    ids: vec![
                        SedimentreeId::new([0x01; 32]),
                        SedimentreeId::new([0x02; 32]),
                    ],
                },
            )));

            let encoded = msg.encode();
            let decoded = CliWireMessage::try_decode(&encoded)?;
            assert_eq!(decoded, msg);
            Ok(())
        }

        #[tokio::test]
        async fn ephemeral() -> TestResult {
            let signer = MemorySigner::generate();
            let ep = EphemeralPayload {
                id: Topic::new([0xAA; 32]),
                nonce: 42,
                timestamp: TimestampSeconds::new(1_700_000_000),
                payload: vec![10, 20, 30],
            };
            let verified = Signed::seal::<Sendable, _>(&signer, ep).await;
            let msg = CliWireMessage::Ephemeral(EphemeralMessage::Ephemeral(Box::new(
                verified.into_signed(),
            )));

            let encoded = msg.encode();
            let decoded = CliWireMessage::try_decode(&encoded)?;
            assert_eq!(decoded, msg);
            Ok(())
        }

        #[test]
        fn batch_sync_response() -> TestResult {
            let resp = BatchSyncResponse {
                req_id: test_request_id(),
                id: SedimentreeId::new([0xFF; 32]),
                result: SyncResult::NotFound,
                responder_heads: RemoteHeads::default(),
            };
            let msg = CliWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp)));

            let encoded = msg.encode();
            let decoded = CliWireMessage::try_decode(&encoded)?;
            assert_eq!(decoded, msg);
            Ok(())
        }

        #[test]
        fn keyhive() -> TestResult {
            let msg = CliWireMessage::Keyhive(KeyhiveMessage::new(vec![0xCA, 0xFE, 0xBA, 0xBE]));

            let encoded = msg.encode();
            let decoded = CliWireMessage::try_decode(&encoded)?;
            assert_eq!(decoded, msg);
            Ok(())
        }
    }
}
