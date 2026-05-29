//! Wire message enum for multiplexing sync, ephemeral, and keyhive
//! traffic over a single physical connection.
//!
//! This is an application-level type defined in the Wasm crate. The
//! transport layer is generic over message types via `ChannelMessage`;
//! this module provides the concrete enum and trait impls.

use alloc::{boxed::Box, vec::Vec};

use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidSchema},
    },
    id::SedimentreeId,
};
use subduction_core::connection::message::{
    BatchSyncResponse, MESSAGE_SCHEMA, SyncMessage, TryAsBatchSyncResponse, TryAsSubscribeRequest,
};
use subduction_ephemeral::message::{EPHEMERAL_SCHEMA, EphemeralMessage};
use subduction_keyhive::{KEYHIVE_SCHEMA, KeyhiveMessage};

/// Composed wire message carrying sync, ephemeral, or keyhive traffic.
///
/// Encode delegates to the inner variant (schema headers are already
/// distinct: `SUM\x00` vs `SUE\x00` vs `SUK\x00`). Decode reads the
/// 4-byte schema header and dispatches to the appropriate decoder.
/// Unrecognized schemas produce a [`DecodeError::InvalidSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    /// A sync-protocol message.
    Sync(Box<SyncMessage>),

    /// An ephemeral-protocol message.
    Ephemeral(EphemeralMessage),

    /// A keyhive-protocol message.
    Keyhive(KeyhiveMessage),
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

impl From<KeyhiveMessage> for WireMessage {
    fn from(msg: KeyhiveMessage) -> Self {
        Self::Keyhive(msg)
    }
}

/// Borrow a [`BatchSyncResponse`] from the wire envelope. Returns
/// `None` for ephemeral or keyhive messages so the [`Subduction`]
/// listen loop dispatches them through their respective handlers.
///
/// [`Subduction`]: subduction_core::subduction::Subduction
impl TryAsBatchSyncResponse for WireMessage {
    fn try_as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            WireMessage::Sync(sync) => sync.try_as_batch_sync_response(),
            WireMessage::Ephemeral(_) | WireMessage::Keyhive(_) => None,
        }
    }
}

/// Borrow the subscription target from the wire envelope. Delegates
/// to the inner [`SyncMessage`] for sync traffic; ephemeral and
/// keyhive messages never carry subscription requests.
impl TryAsSubscribeRequest for WireMessage {
    fn try_as_subscribe_request(&self) -> Option<SedimentreeId> {
        match self {
            WireMessage::Sync(sync) => sync.try_as_subscribe_request(),
            WireMessage::Ephemeral(_) | WireMessage::Keyhive(_) => None,
        }
    }
}

impl Encode for WireMessage {
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
            KEYHIVE_SCHEMA => KeyhiveMessage::try_decode(buf).map(WireMessage::Keyhive),
            _ => Err(InvalidSchema {
                expected: MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}
