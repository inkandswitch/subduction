//! Unsigned rejection messages for the handshake protocol.
//!
//! A [`Rejection`] is sent by the responder when the handshake fails
//! (bad audience, clock drift, replayed nonce, invalid signature).
//! It is deliberately unsigned — the server cannot prove its identity
//! until _after_ it verifies the challenge.

use alloc::vec::Vec;

use sedimentree_core::codec::error::{DecodeError, InvalidEnumTag};
use thiserror::Error;

use crate::timestamp::TimestampSeconds;

/// Reasons for rejecting a handshake (sent unsigned).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[repr(u8)]
pub enum RejectionReason {
    /// Client's timestamp is too far from server's clock.
    ClockDrift = 0x00,

    /// The audience field doesn't match this server.
    InvalidAudience = 0x01,

    /// This nonce was already used (replay attack detected).
    ReplayedNonce = 0x02,

    /// The signature on the challenge is invalid.
    InvalidSignature = 0x03,
}

impl TryFrom<u8> for RejectionReason {
    type Error = InvalidEnumTag;

    fn try_from(tag: u8) -> Result<Self, Self::Error> {
        match tag {
            0x00 => Ok(Self::ClockDrift),
            0x01 => Ok(Self::InvalidAudience),
            0x02 => Ok(Self::ReplayedNonce),
            0x03 => Ok(Self::InvalidSignature),
            _ => Err(InvalidEnumTag {
                tag,
                type_name: "RejectionReason",
            }),
        }
    }
}

/// An unsigned rejection message.
///
/// # Security Note
///
/// This message is unsigned. Clients should NOT use the `server_timestamp`
/// for drift correction if the drift is implausible (> [`super::MAX_PLAUSIBLE_DRIFT`]).
/// An attacker could send fake rejections with manipulated timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Rejection {
    /// Why the handshake was rejected.
    pub reason: RejectionReason,

    /// Server's current timestamp (informational only).
    pub server_timestamp: TimestampSeconds,
}

/// Size of a rejection payload: 1-byte reason + 8-byte timestamp.
///
/// This does _not_ include the envelope (schema + tag) which is handled
/// by [`super::HandshakeMessage`].
pub const REJECTION_SIZE: usize = 1 + 8;

impl Rejection {
    /// Create a new rejection.
    #[must_use]
    pub const fn new(reason: RejectionReason, now: TimestampSeconds) -> Self {
        Self {
            reason,
            server_timestamp: now,
        }
    }

    /// Encode the rejection payload (reason + timestamp).
    ///
    /// This does _not_ include the envelope (schema + tag) which is handled
    /// by [`super::HandshakeMessage::encode`].
    #[must_use]
    pub(super) fn encode_payload(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(REJECTION_SIZE);
        buf.push(self.reason as u8);
        buf.extend_from_slice(&self.server_timestamp.as_secs().to_be_bytes());
        buf
    }

    /// Decode from the payload bytes after the envelope (schema + tag).
    ///
    /// `payload` is the bytes _after_ the `SUH\0` schema and variant tag.
    pub(super) fn try_decode_payload(payload: &[u8]) -> Result<Self, RejectionDecodeError> {
        if payload.len() < REJECTION_SIZE {
            return Err(RejectionDecodeError::TooShort {
                have: payload.len(),
            });
        }

        let &reason_byte = payload
            .first()
            .ok_or(RejectionDecodeError::TooShort { have: 0 })?;

        let reason = RejectionReason::try_from(reason_byte)
            .map_err(|_| RejectionDecodeError::InvalidReason(reason_byte))?;

        let timestamp_bytes: [u8; 8] = payload.get(1..9).and_then(|s| s.try_into().ok()).ok_or(
            RejectionDecodeError::TooShort {
                have: payload.len(),
            },
        )?;

        Ok(Self {
            reason,
            server_timestamp: TimestampSeconds::new(u64::from_be_bytes(timestamp_bytes)),
        })
    }
}

/// Errors when decoding a [`Rejection`] message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum RejectionDecodeError {
    /// Payload is shorter than [`REJECTION_SIZE`] (9 bytes).
    #[error("rejection payload too short: need {REJECTION_SIZE} bytes, have {have}")]
    TooShort {
        /// Actual number of payload bytes available (after envelope).
        have: usize,
    },

    /// The reason tag byte is not a recognized [`RejectionReason`].
    #[error("invalid rejection reason tag: {0:#04x}")]
    InvalidReason(u8),
}

impl From<RejectionDecodeError> for DecodeError {
    fn from(err: RejectionDecodeError) -> Self {
        match err {
            RejectionDecodeError::TooShort { have } => DecodeError::MessageTooShort {
                type_name: "Rejection",
                need: REJECTION_SIZE,
                have,
            },
            RejectionDecodeError::InvalidReason(tag) => InvalidEnumTag {
                tag,
                type_name: "RejectionReason",
            }
            .into(),
        }
    }
}
