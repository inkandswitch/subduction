//! Response types for the handshake protocol.
//!
//! A [`Response`] is signed and sent by the responder to bind itself to
//! the initiator's challenge (via [`Digest`]) and provide a server timestamp
//! for client-side drift correction.

use alloc::vec::Vec;

use thiserror::Error;

use crate::timestamp::TimestampSeconds;
use sedimentree_core::{
    codec::{
        decode, decode::DecodeFields, encode, encode::EncodeFields, error::DecodeError, schema,
        schema::Schema,
    },
    crypto::digest::Digest,
};

use super::challenge::Challenge;

/// A handshake response from the server.
///
/// This is signed by the server. The client extracts the server's identity
/// from the signature's issuer and uses the server timestamp for drift correction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Response {
    /// Hash of the challenge being responded to (binds response to request).
    pub challenge_digest: Digest<Challenge>,

    /// Server's current timestamp (for client-side drift correction).
    pub server_timestamp: TimestampSeconds,
}

impl Response {
    /// Create a new response for a challenge.
    #[must_use]
    pub const fn new(challenge_digest: Digest<Challenge>, now: TimestampSeconds) -> Self {
        Self {
            challenge_digest,
            server_timestamp: now,
        }
    }

    /// Create a response directly from a challenge.
    #[must_use]
    pub fn for_challenge(challenge: &Challenge, now: TimestampSeconds) -> Self {
        Self::new(Digest::hash(challenge), now)
    }

    /// Validate that this response matches the expected challenge.
    ///
    /// # Errors
    ///
    /// Returns an error if the challenge digest doesn't match.
    pub fn validate(&self, expected_challenge: &Challenge) -> Result<(), ResponseValidationError> {
        let expected_digest = Digest::hash(expected_challenge);
        if self.challenge_digest != expected_digest {
            return Err(ResponseValidationError::ChallengeMismatch);
        }
        Ok(())
    }
}

/// Size of Response fields (after schema + issuer, before signature).
pub(crate) const RESPONSE_FIELDS_SIZE: usize = 32 + 8; // 40 bytes

/// Minimum size of a signed Response message.
pub const RESPONSE_MIN_SIZE: usize = 4 + 32 + RESPONSE_FIELDS_SIZE + 64; // 140 bytes

impl Schema for Response {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'R'; // Response
    const VERSION: u8 = 0;
}

impl EncodeFields for Response {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        // ChallengeDigest (32 bytes)
        encode::array(self.challenge_digest.as_bytes(), buf);

        // ServerTimestamp (8 bytes, big-endian)
        encode::u64(self.server_timestamp.as_secs(), buf);
    }

    fn fields_size(&self) -> usize {
        RESPONSE_FIELDS_SIZE
    }
}

impl DecodeFields for Response {
    const MIN_SIGNED_SIZE: usize = RESPONSE_MIN_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<(Self, usize), DecodeError> {
        if buf.len() < RESPONSE_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "Response",
                need: RESPONSE_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        let mut offset = 0;

        // ChallengeDigest (32 bytes)
        let challenge_digest_bytes: [u8; 32] = decode::array(buf, offset)?;
        offset += 32;
        let challenge_digest = Digest::force_from_bytes(challenge_digest_bytes);

        // ServerTimestamp (8 bytes)
        let server_timestamp_secs = decode::u64(buf, offset)?;
        offset += 8;
        let server_timestamp = TimestampSeconds::new(server_timestamp_secs);

        Ok((
            Self {
                challenge_digest,
                server_timestamp,
            },
            offset,
        ))
    }
}

/// Errors when validating a [`Response`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ResponseValidationError {
    /// The challenge digest doesn't match.
    #[error("challenge digest mismatch")]
    ChallengeMismatch,
}
