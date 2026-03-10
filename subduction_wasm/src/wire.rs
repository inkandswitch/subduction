//! Wire message enum for multiplexing sync, ephemeral, and keyhive
//! traffic over a single physical connection.
//!
//! This is an application-level type defined in the Wasm crate. The
//! transport layer is generic over message types via `ChannelMessage`;
//! this module provides the concrete enum and trait impls.

use alloc::{boxed::Box, vec::Vec};

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
pub enum WireMessage {
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
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

// ── ChannelMessage impl for HTTP long-poll transport ────────────────────

impl subduction_http_longpoll::connection::ChannelMessage for WireMessage {
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
