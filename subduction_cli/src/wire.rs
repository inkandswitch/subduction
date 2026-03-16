//! Wire message enum for multiplexing sync and ephemeral traffic over
//! a single physical connection.
//!
//! This is an application-level type. The transport layer is generic
//! over message types via `ChannelMessage`; this module provides the
//! concrete enum and trait impls for the CLI server.

use std::vec::Vec;

use sedimentree_core::codec::{
    decode::Decode,
    encode::Encode,
    error::{DecodeError, InvalidSchema},
};
use subduction_core::connection::message::{BatchSyncResponse, MESSAGE_SCHEMA, SyncMessage};
use subduction_ephemeral::message::{EPHEMERAL_SCHEMA, EphemeralMessage};

/// Composed wire message for the CLI server.
///
/// Carries sync or ephemeral traffic. Decode reads the 4-byte schema
/// header and dispatches to the appropriate decoder.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CliWireMessage {
    /// A sync-protocol message.
    Sync(Box<SyncMessage>),

    /// An ephemeral-protocol message.
    Ephemeral(EphemeralMessage),
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
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Ephemeral(msg) => msg.encoded_size(),
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
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

// ── ChannelMessage helpers ──────────────────────────────────────────────

#[allow(dead_code)]
const fn as_batch_sync_response_inner(msg: &SyncMessage) -> Option<&BatchSyncResponse> {
    match msg {
        SyncMessage::BatchSyncResponse(resp) => Some(resp),
        SyncMessage::BatchSyncRequest(_)
        | SyncMessage::BlobsRequest { .. }
        | SyncMessage::BlobsResponse { .. }
        | SyncMessage::DataRequestRejected(_)
        | SyncMessage::Fragment { .. }
        | SyncMessage::LooseCommit { .. }
        | SyncMessage::RemoveSubscriptions(_) => None,
    }
}

// ── WebSocket ChannelMessage ────────────────────────────────────────────

impl subduction_websocket::websocket::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(msg) => as_batch_sync_response_inner(msg),
            Self::Ephemeral(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) => None,
        }
    }
}

// ── HTTP Long-Poll ChannelMessage ───────────────────────────────────────

impl subduction_http_longpoll::connection::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(msg) => as_batch_sync_response_inner(msg),
            Self::Ephemeral(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) => None,
        }
    }
}

// ── Iroh ChannelMessage ─────────────────────────────────────────────────

impl subduction_iroh::connection::ChannelMessage for CliWireMessage {
    fn wrap_sync(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(msg) => as_batch_sync_response_inner(msg),
            Self::Ephemeral(_) => None,
        }
    }

    fn into_sync(self) -> Option<SyncMessage> {
        match self {
            Self::Sync(msg) => Some(*msg),
            Self::Ephemeral(_) => None,
        }
    }
}
