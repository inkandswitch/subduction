//! Wire message enum for multiplexing sync and ephemeral traffic over
//! a single physical connection.
//!
//! This is an application-level type defined in the Wasm crate. The
//! transport layer is generic over message types via `ChannelMessage`;
//! this module provides the concrete enum and trait impls.

use alloc::{boxed::Box, vec::Vec};

use sedimentree_core::codec::{decode::Decode, encode::Encode, error::DecodeError};
use subduction_core::connection::message::{MESSAGE_SCHEMA, SyncMessage};
use subduction_ephemeral::message::{EPHEMERAL_SCHEMA, EphemeralMessage};

/// Composed wire message carrying sync, ephemeral, or unknown-protocol traffic.
///
/// Encode delegates to the inner variant (schema headers are already
/// distinct: `SUM\x00` vs `SUE\x00`). Decode reads the 4-byte schema
/// header and dispatches to the appropriate decoder. Unrecognized schemas
/// are captured as [`Unknown`](Self::Unknown) for forwarding to JS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    /// A sync-protocol message.
    Sync(Box<SyncMessage>),

    /// An ephemeral-protocol message.
    Ephemeral(EphemeralMessage),

    /// A frame with an unrecognized 4-byte schema header.
    Unknown(Vec<u8>),
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
            Self::Unknown(bytes) => bytes.clone(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Ephemeral(msg) => msg.encoded_size(),
            Self::Unknown(bytes) => bytes.len(),
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
            _ => Ok(WireMessage::Unknown(buf.to_vec())),
        }
    }
}
