//! Pre-verify header view of an [`EphemeralPayload`].
//!
//! [`EphemeralPayloadHeader`] is the subset of payload fields that can
//! be read out of the wire bytes without copying the variable-length
//! `payload` `Vec`: `id`, `nonce`, `timestamp`, and the declared
//! `payload_len`.
//!
//! It exists so that pre-verify code paths in
//! [`EphemeralHandler::recv_ephemeral`] and
//! [`EphemeralHandler::publish`] can read the fixed-size fields they
//! need for size / age / dedup checks _before_ paying for a full
//! payload allocation or an Ed25519 verification. The full
//! [`EphemeralPayload`] (with its owned `payload: Vec<u8>`) is
//! materialised exactly once, post-verify, via
//! [`Signed::try_verify`].
//!
//! [`EphemeralPayload`]: crate::message::EphemeralPayload
//! [`EphemeralHandler::recv_ephemeral`]: crate::handler::EphemeralHandler
//! [`EphemeralHandler::publish`]: crate::handler::EphemeralHandler
//! [`Signed::try_verify`]: subduction_crypto::signed::Signed::try_verify

use sedimentree_core::codec::{decode, error::DecodeError};
use subduction_core::timestamp::TimestampSeconds;

use crate::topic::Topic;

/// Size of an [`EphemeralPayloadHeader`] as encoded on the wire,
/// excluding the variable-length payload bytes themselves:
/// `id(32) + nonce(8) + timestamp(8) + payload_len(bijou64, min 1B)`.
pub(crate) const EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE: usize = 32 + 8 + 8 + 1;

/// Header fields of an [`EphemeralPayload`] decoded without copying
/// the variable-length payload bytes: `id`, `nonce`, `timestamp`,
/// declared `payload_len`.
///
/// Returned by [`EphemeralPayloadHeader::try_decode`] for pre-verify
/// code paths that need the fixed-size fields but should not allocate
/// or copy the payload `Vec`.
///
/// [`EphemeralPayload`]: crate::message::EphemeralPayload
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EphemeralPayloadHeader {
    /// The topic this message is published to.
    pub id: Topic,
    /// Random nonce for deduplication.
    pub nonce: u64,
    /// UTC time at message creation.
    pub timestamp: TimestampSeconds,
    /// Declared length of the payload bytes (validated to fit in the
    /// decode buffer).
    pub payload_len: usize,
}

impl EphemeralPayloadHeader {
    /// Decode the header fields of an
    /// [`EphemeralPayload`](crate::message::EphemeralPayload)
    /// _without_ verifying the surrounding signature and _without_
    /// allocating or copying the payload bytes.
    ///
    /// `buf` must be the fields region of a `Signed<EphemeralPayload>`
    /// — i.e. [`Signed::fields_bytes`]. The declared payload length is
    /// bounds-checked against `buf`, so truncated wire bytes are
    /// rejected here rather than being passed downstream.
    ///
    /// The fields here are **untrusted** until the surrounding
    /// signature is verified — only use them for read-only checks
    /// (size, age, dedup probe).
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::MessageTooShort`] if `buf` is shorter
    /// than the minimum header size or if the declared payload length
    /// extends past `buf`'s end, and [`Bijou64Error`] if the payload
    /// length prefix is not a well-formed bijou64.
    ///
    /// [`Signed::fields_bytes`]: subduction_crypto::signed::Signed::fields_bytes
    /// [`Bijou64Error`]: sedimentree_core::codec::error::Bijou64Error
    pub fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Self::try_decode_with_offset(buf).map(|(header, _)| header)
    }

    /// Same as [`try_decode`](Self::try_decode), but also returns the
    /// byte offset immediately after the bijou64 payload-length prefix
    /// — i.e. the offset at which the payload bytes begin.
    ///
    /// Useful for full-payload decoders (e.g. [`DecodeFields`] for
    /// [`EphemeralPayload`](crate::message::EphemeralPayload)) that
    /// want to walk the header once and pick up where it left off
    /// without recomputing offsets.
    ///
    /// # Errors
    ///
    /// Same conditions as [`try_decode`](Self::try_decode).
    pub fn try_decode_with_offset(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "EphemeralPayloadHeader",
                need: EPHEMERAL_PAYLOAD_MIN_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        let id_bytes: [u8; 32] = decode::array(buf, offset)?;
        offset += 32;
        let id = Topic::new(id_bytes);

        let nonce = decode::u64(buf, offset)?;
        offset += 8;

        let timestamp_secs = decode::u64(buf, offset)?;
        offset += 8;
        let timestamp = TimestampSeconds::new(timestamp_secs);

        let remaining = buf.get(offset..).ok_or(DecodeError::MessageTooShort {
            type_name: "EphemeralPayloadHeader payload_len",
            need: offset + 1,
            have: buf.len(),
        })?;
        let (payload_len_u64, consumed) = bijou64::decode(remaining)
            .map_err(|kind| sedimentree_core::codec::error::Bijou64Error { offset, kind })?;
        offset += consumed;

        #[allow(clippy::cast_possible_truncation)]
        let payload_len = payload_len_u64 as usize;

        // Bounds-check the declared payload length against the actual
        // remaining buffer so a truncated wire message is caught here
        // rather than later during full-payload decode.
        if buf.len() < offset.saturating_add(payload_len) {
            return Err(DecodeError::MessageTooShort {
                type_name: "EphemeralPayloadHeader payload data",
                need: offset + payload_len,
                have: buf.len(),
            });
        }

        Ok((
            Self {
                id,
                nonce,
                timestamp,
                payload_len,
            },
            offset,
        ))
    }
}
