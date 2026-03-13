//! Challenge types for the handshake protocol.
//!
//! A [`Challenge`] is signed and sent by the initiator to prove its identity
//! and declare the intended recipient ([`Audience`]).

use alloc::vec::Vec;
use core::time::Duration;

use thiserror::Error;

use crate::{peer::id::PeerId, timestamp::TimestampSeconds};
use sedimentree_core::codec::{
    decode::{self, DecodeFields},
    encode::{self, EncodeFields},
    error::{DecodeError, InvalidEnumTag},
    schema::{self, Schema},
};
use subduction_crypto::nonce::Nonce;

use super::{
    audience::{Audience, DiscoveryId},
    rejection::RejectionReason,
};

/// A handshake challenge sent by the client.
///
/// This is signed by the client and sent to the server. The server extracts
/// the client's identity from the signature's issuer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Challenge {
    /// Who the client is connecting to.
    pub audience: Audience,

    /// Client's timestamp (used for replay protection).
    pub timestamp: TimestampSeconds,

    /// Random nonce for uniqueness.
    pub nonce: Nonce,
}

impl Challenge {
    /// Create a new challenge.
    #[must_use]
    pub const fn new(audience: Audience, now: TimestampSeconds, nonce: Nonce) -> Self {
        Self {
            audience,
            timestamp: now,
            nonce,
        }
    }

    /// Check if this challenge is fresh (timestamp within acceptable drift).
    #[must_use]
    pub fn is_fresh(&self, now: TimestampSeconds, max_drift: Duration) -> bool {
        self.timestamp.abs_diff(now) <= max_drift
    }

    /// Validate the challenge against expected parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The audience doesn't match the expected audience
    /// - The timestamp is outside the acceptable drift window
    pub fn validate(
        &self,
        expected_audience: &Audience,
        now: TimestampSeconds,
        max_drift: Duration,
    ) -> Result<(), ChallengeValidationError> {
        if &self.audience != expected_audience {
            return Err(ChallengeValidationError::InvalidAudience);
        }

        if !self.is_fresh(now, max_drift) {
            return Err(ChallengeValidationError::ClockDrift {
                client_timestamp: self.timestamp,
                server_timestamp: now,
            });
        }

        Ok(())
    }
}

/// Size of Challenge fields (after schema + issuer, before signature).
pub(crate) const CHALLENGE_FIELDS_SIZE: usize = 1 + 32 + 8 + 16; // 57 bytes

/// Minimum size of a signed Challenge message.
pub const CHALLENGE_MIN_SIZE: usize = 4 + 32 + CHALLENGE_FIELDS_SIZE + 64; // 157 bytes

impl Schema for Challenge {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'H'; // Handshake
    const VERSION: u8 = 0;
}

impl EncodeFields for Challenge {
    fn encode_fields(&self, buf: &mut Vec<u8>) {
        // AudienceTag (1 byte)
        match &self.audience {
            Audience::Known(_) => encode::u8(0x00, buf),
            Audience::Discover(_) => encode::u8(0x01, buf),
        }

        // AudienceValue (32 bytes)
        match &self.audience {
            Audience::Known(peer_id) => encode::array(peer_id.as_bytes(), buf),
            Audience::Discover(disc_id) => encode::array(disc_id.as_bytes(), buf),
        }

        // Timestamp (8 bytes, big-endian)
        encode::u64(self.timestamp.as_secs(), buf);

        // Nonce (16 bytes)
        encode::array(self.nonce.as_bytes(), buf);
    }

    fn fields_size(&self) -> usize {
        CHALLENGE_FIELDS_SIZE
    }
}

impl DecodeFields for Challenge {
    const MIN_SIGNED_SIZE: usize = CHALLENGE_MIN_SIZE;

    fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < CHALLENGE_FIELDS_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "Challenge",
                need: CHALLENGE_FIELDS_SIZE,
                have: buf.len(),
            });
        }

        // AudienceTag (1 byte)
        let audience_tag = decode::u8(buf, 0)?;

        // AudienceValue (32 bytes)
        let audience_value: [u8; 32] = decode::array(buf, 1)?;

        let audience = match audience_tag {
            0x00 => Audience::Known(PeerId::new(audience_value)),
            0x01 => Audience::Discover(DiscoveryId::from_raw(audience_value)),
            tag => {
                return Err(InvalidEnumTag {
                    tag,
                    type_name: "Audience",
                }
                .into());
            }
        };

        // Timestamp (8 bytes)
        let timestamp_secs = decode::u64(buf, 33)?;
        let timestamp = TimestampSeconds::new(timestamp_secs);

        // Nonce (16 bytes)
        let nonce_bytes: [u8; 16] = decode::array(buf, 41)?;
        let nonce = Nonce::from_bytes(nonce_bytes);

        Ok(Self {
            audience,
            timestamp,
            nonce,
        })
    }
}

/// Errors when validating a [`Challenge`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum ChallengeValidationError {
    /// The audience field doesn't match.
    #[error("invalid audience")]
    InvalidAudience,

    /// The timestamp is outside acceptable drift.
    #[error("clock drift too large: client={client_timestamp:?}, server={server_timestamp:?}")]
    ClockDrift {
        /// The timestamp from the client's challenge.
        client_timestamp: TimestampSeconds,

        /// The server's current timestamp.
        server_timestamp: TimestampSeconds,
    },

    /// The nonce has already been used (replay attack detected).
    #[error("replayed nonce")]
    ReplayedNonce,
}

impl ChallengeValidationError {
    /// Convert to a rejection reason.
    #[must_use]
    pub const fn to_rejection_reason(&self) -> RejectionReason {
        match self {
            Self::InvalidAudience => RejectionReason::InvalidAudience,
            Self::ClockDrift { .. } => RejectionReason::ClockDrift,
            Self::ReplayedNonce => RejectionReason::ReplayedNonce,
        }
    }
}
