//! Handshake protocol types for authenticating new connections.
//!
//! The handshake establishes mutual identity between peers. It answers
//! "_who_ is connecting?" — authorization ("_can_ they connect?") is a
//! policy question handled elsewhere.
//!
//! # Protocol Flow
//!
//! ```text
//!     Initiator                                       Responder
//!       │                                               │
//!       │  1. Signed<Challenge>                         │
//!       │  ─────────────────────────────────────────►   │
//!       │     { audience, timestamp, nonce }            │
//!       │     Initiator identity: challenge.issuer()    │
//!       │                                               │
//!       │                      2. Signed<Response>      │
//!       │  ◄─────────────────────────────────────────   │
//!       │     { challenge_digest, server_timestamp }    │
//!       │     Responder identity: response.issuer()     │
//!       │     Binding: challenge_digest includes nonce  │
//!       │                                               │
//!       ▼                                               ▼
//!    Knows responder_id                           Knows initiator_id
//! ```
//!
//! # Sans-io split
//!
//! This module holds the _pure_ handshake vocabulary: message types,
//! codec, validation math, drift correction, and errors. The handshake
//! _flow_ lives in the per-connection machine: signing is emitted as
//! `Sign` effects, while verification (`try_verify`) and the pure checks
//! (`Challenge::validate`, `Response::validate`) run inline.

pub mod audience;
pub mod challenge;
pub mod rejection;
pub mod response;

use alloc::vec::Vec;
use core::time::Duration;

use sedimentree_core::codec::{
    error::{DecodeError, InvalidEnumTag, InvalidSchema},
    schema::Schema,
};
use subduction_crypto::signed::Signed;
use thiserror::Error;

use crate::wall_clock::TimestampSeconds;
use audience::Audience;
use challenge::{Challenge, ChallengeValidationError};
use rejection::Rejection;
use response::ResponseValidationError;

pub use challenge::Challenge as HandshakeChallenge;

/// Maximum plausible clock drift for rejecting implausible timestamps (±10 minutes).
// `Duration::from_mins` is not yet const-stable (rust-lang/rust#140881), so
// stay on `from_secs` until the MSRV catches up. The `unknown_lints` allow
// keeps older toolchains (pre-1.95) quiet about the unrecognized lint name.
#[allow(unknown_lints, clippy::duration_suboptimal_units)]
pub const MAX_PLAUSIBLE_DRIFT: Duration = Duration::from_secs(10 * 60);

/// Maximum clock drift tolerated during simultaneous open handshakes.
#[allow(unknown_lints, clippy::duration_suboptimal_units)]
pub const SIMULTANEOUS_OPEN_MAX_DRIFT: Duration = Duration::from_secs(10 * 60);

/// Client-side drift correction.
///
/// Tracks clock drift learned from server responses and applies bounded
/// corrections to future timestamps. Retry logic (e.g., "try adjusted once,
/// then fall back to original") belongs in the caller.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DriftCorrection {
    /// The computed drift offset (`server_time` - `client_time`).
    offset_secs: i32,
}

impl DriftCorrection {
    /// Create a new drift correction with no offset.
    #[must_use]
    pub const fn new() -> Self {
        Self { offset_secs: 0 }
    }

    /// Adjust drift based on a server timestamp.
    ///
    /// Returns `true` if the drift was plausible and applied.
    /// Returns `false` if the drift exceeds [`MAX_PLAUSIBLE_DRIFT`].
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    pub fn adjust(
        &mut self,
        server_timestamp: TimestampSeconds,
        client_timestamp: TimestampSeconds,
    ) -> bool {
        let drift = server_timestamp.signed_diff(client_timestamp);
        let max_drift_secs = i64::from(i32::MAX).min(MAX_PLAUSIBLE_DRIFT.as_secs() as i64);

        if drift.abs() > max_drift_secs {
            return false;
        }

        self.offset_secs = drift as i32;
        true
    }

    /// Apply the drift correction to a timestamp.
    #[must_use]
    pub fn apply(&self, timestamp: TimestampSeconds) -> TimestampSeconds {
        timestamp.add_signed(i64::from(self.offset_secs))
    }

    /// Get the current drift offset in seconds.
    #[must_use]
    pub const fn offset_secs(&self) -> i32 {
        self.offset_secs
    }
}

/// Wire format for handshake messages.
///
/// All handshake types share the `SUH\x00` schema. Byte 4 (the
/// [`DISCRIMINANT`](Schema::DISCRIMINANT) for signed variants, or a
/// tag byte for unsigned control messages) distinguishes them:
///
/// | Byte 4 | Variant              | Wire layout                                              |
/// |--------|----------------------|----------------------------------------------------------|
/// | `0x00` | `Signed<Challenge>`  | `SUH\x00` + `0x00` + issuer(32) + fields(57) + sig(64)  |
/// | `0x01` | `Signed<Response>`   | `SUH\x00` + `0x01` + issuer(32) + fields(40) + sig(64)  |
/// | `0x02` | `Rejection`          | `SUH\x00` + `0x02` + reason(1) + timestamp(8)           |
///
/// For signed variants, the full bytes are `Signed<T>::as_bytes()`
/// — no outer envelope, no stripping. The discriminant at byte 4 is
/// part of the signed region.
#[derive(Debug)]
pub enum HandshakeMessage {
    /// A signed challenge from the initiator.
    SignedChallenge(Signed<Challenge>),

    /// A signed response from the responder.
    SignedResponse(Signed<response::Response>),

    /// An unsigned rejection from the responder.
    Rejection(Rejection),
}

/// The shared handshake schema — `Challenge` and `Response` both use
/// this with their respective [`DISCRIMINANT`](Schema::DISCRIMINANT).
pub(crate) const HANDSHAKE_SCHEMA: [u8; 4] = Challenge::SCHEMA;

/// Variant tag bytes within the `SUH\0` handshake protocol.
mod handshake_tags {
    use super::{response::Response, Challenge};

    pub(super) const CHALLENGE: u8 = Challenge::TAG;
    pub(super) const RESPONSE: u8 = Response::TAG;
    pub(super) const REJECTION: u8 = 0x02;
}

impl HandshakeMessage {
    /// Minimum size: schema (4) + tag (1).
    const MIN_SIZE: usize = 5;

    /// Encode the handshake message to wire bytes.
    ///
    /// For signed variants (`Challenge`, `Response`), the `Signed<T>`
    /// bytes _are_ the wire message — the schema + discriminant + issuer
    /// + fields + signature. No outer envelope wrapping needed.
    ///
    /// For `Rejection` (unsigned), a manual `SUH\0 + tag + payload`
    /// envelope is constructed.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            HandshakeMessage::SignedChallenge(signed) => signed.as_bytes().to_vec(),
            HandshakeMessage::SignedResponse(signed) => signed.as_bytes().to_vec(),
            HandshakeMessage::Rejection(rejection) => {
                let payload = rejection.encode_payload();
                let mut buf = Vec::with_capacity(4 + 1 + payload.len());
                buf.extend_from_slice(&HANDSHAKE_SCHEMA);
                buf.push(handshake_tags::REJECTION);
                buf.extend_from_slice(&payload);
                buf
            }
        }
    }

    /// Decode a handshake message from wire bytes.
    ///
    /// All handshake messages start with `SUH\x00`. The byte at position 4
    /// is the variant discriminant/tag:
    /// - `0x00` → `Signed<Challenge>` (full `Signed<T>` bytes)
    /// - `0x01` → `Signed<Response>` (full `Signed<T>` bytes)
    /// - `0x02` → `Rejection` (unsigned, manual envelope)
    ///
    /// For signed variants, the entire byte slice is the `Signed<T>` — the
    /// discriminant at byte 4 is validated by [`Signed::try_decode`].
    /// _Decoding does not verify signatures_ — the machine emits a
    /// dedicated effect for that: verification is computation.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema is unrecognized, the tag is invalid,
    /// or the payload is malformed.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < Self::MIN_SIZE {
            return Err(DecodeError::MessageTooShort {
                type_name: "HandshakeMessage",
                need: Self::MIN_SIZE,
                have: bytes.len(),
            });
        }

        let got_schema: [u8; 4] =
            bytes
                .get(..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(DecodeError::MessageTooShort {
                    type_name: "HandshakeMessage",
                    need: 4,
                    have: bytes.len(),
                })?;

        if got_schema != HANDSHAKE_SCHEMA {
            return Err(InvalidSchema {
                expected: HANDSHAKE_SCHEMA,
                got: got_schema,
            }
            .into());
        }

        let tag = bytes.get(4).copied().ok_or(DecodeError::MessageTooShort {
            type_name: "HandshakeMessage",
            need: Self::MIN_SIZE,
            have: bytes.len(),
        })?;

        match tag {
            handshake_tags::CHALLENGE => {
                // Full bytes are Signed<Challenge> — discriminant validated by try_decode.
                let signed = Signed::<Challenge>::try_decode(bytes)?;
                Ok(HandshakeMessage::SignedChallenge(signed))
            }
            handshake_tags::RESPONSE => {
                let signed = Signed::<response::Response>::try_decode(bytes)?;
                Ok(HandshakeMessage::SignedResponse(signed))
            }
            handshake_tags::REJECTION => {
                let payload = bytes.get(5..).ok_or(DecodeError::MessageTooShort {
                    type_name: "HandshakeMessage::Rejection",
                    need: Self::MIN_SIZE + 1,
                    have: bytes.len(),
                })?;
                let rejection = Rejection::try_decode_payload(payload)?;
                Ok(HandshakeMessage::Rejection(rejection))
            }
            tag => Err(InvalidEnumTag {
                tag,
                type_name: "HandshakeMessage",
            }
            .into()),
        }
    }
}

/// Errors that can occur during the handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum HandshakeError {
    /// The signature on the message was invalid.
    #[error("invalid signature")]
    InvalidSignature,

    /// Challenge validation failed.
    #[error("challenge validation failed: {0}")]
    ChallengeValidation(#[from] ChallengeValidationError),

    /// Response validation failed.
    #[error("response validation failed: {0}")]
    ResponseValidation(#[from] ResponseValidationError),
}

/// The identity pin implied by a challenge's audience: dialing a
/// [`Audience::Known`] peer pins the authenticated identity to it.
#[must_use]
pub(crate) const fn pinned_peer(challenge: &Challenge) -> Option<crate::peer_id::PeerId> {
    match challenge.audience {
        Audience::Known(peer) => Some(peer),
        Audience::Discover(_) => None,
    }
}

/// Build the byte preimage that [`Signed::seal`] signs:
/// `schema + discriminant? + issuer + fields`. Appending an ed25519
/// signature over these bytes yields valid `Signed<T>` wire bytes.
///
/// This duplicates [`Signed::sign_preimage`]'s canonical layout because
/// the issuer here is a [`PeerId`](crate::peer_id::PeerId) (infallible
/// bytes), not a parsed `VerifyingKey` — converting would be fallible in
/// runtime sign-effect code. Any layout change is a wire break: the two
/// builders are pinned byte-identical by the `preimage_parity_with_crypto`
/// test below.
pub(crate) fn signed_preimage<T: Schema + sedimentree_core::codec::encode::EncodeFields>(
    issuer: &crate::peer_id::PeerId,
    payload: &T,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&T::SCHEMA);
    if let Some(disc) = T::DISCRIMINANT {
        buf.push(disc);
    }
    buf.extend_from_slice(issuer.as_bytes());
    payload.encode_fields(&mut buf);
    buf
}

#[cfg(test)]
mod tests {
    use super::{audience::Audience, response::Response, *};
    use ed25519_dalek::SigningKey;
    use subduction_crypto::nonce::Nonce;
    use testresult::TestResult;

    fn seal<T>(signing_key: &SigningKey, payload: T) -> Signed<T>
    where
        T: Schema
            + sedimentree_core::codec::encode::EncodeFields
            + sedimentree_core::codec::decode::DecodeFields,
    {
        Signed::seal_sync(signing_key, payload).into_signed()
    }

    /// `handshake::signed_preimage` must stay byte-identical to the
    /// canonical `Signed::sign_preimage` — drift here is a wire/security
    /// bug (the driver signs these exact bytes).
    #[test]
    fn preimage_parity_with_crypto() {
        let signing_key = SigningKey::from_bytes(&[7u8; 32]);
        let verifying_key = signing_key.verifying_key();
        let peer = crate::peer_id::PeerId::from(verifying_key);

        let challenge = Challenge::new(
            Audience::discover(b"parity"),
            TimestampSeconds::new(1234),
            Nonce::from_u128(99),
        );
        assert_eq!(
            signed_preimage(&peer, &challenge),
            Signed::sign_preimage(&verifying_key, &challenge),
            "challenge preimages must match"
        );

        let response = Response::for_challenge(&challenge, TimestampSeconds::new(1235));
        assert_eq!(
            signed_preimage(&peer, &response),
            Signed::sign_preimage(&verifying_key, &response),
            "response preimages must match"
        );
    }

    mod codec {
        use super::*;

        #[test]
        fn challenge_roundtrips() -> TestResult {
            let signing_key = SigningKey::from_bytes(&[1u8; 32]);
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::from_u128(42),
            );
            let sealed = seal(&signing_key, challenge);
            let encoded = HandshakeMessage::SignedChallenge(sealed.clone()).encode();
            let decoded = HandshakeMessage::try_decode(&encoded)?;
            let HandshakeMessage::SignedChallenge(got) = decoded else {
                return Err("expected SignedChallenge variant".into());
            };
            assert_eq!(got.as_bytes(), sealed.as_bytes());
            Ok(())
        }

        #[test]
        fn response_roundtrips() -> TestResult {
            let signing_key = SigningKey::from_bytes(&[2u8; 32]);
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::from_u128(42),
            );
            let response = Response::for_challenge(&challenge, TimestampSeconds::new(1001));
            let sealed = seal(&signing_key, response);
            let encoded = HandshakeMessage::SignedResponse(sealed.clone()).encode();
            let decoded = HandshakeMessage::try_decode(&encoded)?;
            let HandshakeMessage::SignedResponse(got) = decoded else {
                return Err("expected SignedResponse variant".into());
            };
            assert_eq!(got.as_bytes(), sealed.as_bytes());
            Ok(())
        }

        #[test]
        fn rejection_roundtrips() -> TestResult {
            let rejection = Rejection::new(
                rejection::RejectionReason::ClockDrift,
                TimestampSeconds::new(1234),
            );
            let encoded = HandshakeMessage::Rejection(rejection).encode();
            let decoded = HandshakeMessage::try_decode(&encoded)?;
            let HandshakeMessage::Rejection(got) = decoded else {
                return Err("expected Rejection variant".into());
            };
            assert_eq!(got, rejection);
            Ok(())
        }

        #[test]
        fn bad_schema_rejected() {
            let bytes = b"BAD\x00\x00rest of message padding".to_vec();
            assert!(matches!(
                HandshakeMessage::try_decode(&bytes),
                Err(DecodeError::InvalidSchema(_))
            ));
        }
    }

    mod drift_correction {
        use super::*;

        #[test]
        fn no_adjustment_by_default() {
            let dc = DriftCorrection::new();
            let ts = TimestampSeconds::new(1000);
            assert_eq!(dc.apply(ts), ts);
        }

        #[test]
        fn applies_positive_drift() {
            let mut dc = DriftCorrection::new();
            let client_ts = TimestampSeconds::new(1000);
            let server_ts = TimestampSeconds::new(1010);

            assert!(dc.adjust(server_ts, client_ts));
            assert_eq!(dc.apply(client_ts), server_ts);
        }

        #[test]
        fn rejects_implausible_drift() {
            let mut dc = DriftCorrection::new();
            let client_ts = TimestampSeconds::new(1000);
            let server_ts = TimestampSeconds::new(1000 + MAX_PLAUSIBLE_DRIFT.as_secs() + 1);

            assert!(!dc.adjust(server_ts, client_ts));
            assert_eq!(dc.offset_secs(), 0);
        }
    }

    mod validation {
        use super::*;

        #[test]
        fn wrong_challenge_fails_validation() {
            let challenge1 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::from_u128(1),
            );
            let challenge2 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::from_u128(2),
            );
            let response = Response::for_challenge(&challenge1, TimestampSeconds::new(1001));
            assert!(response.validate(&challenge2).is_err());
        }

        #[test]
        fn stale_challenge_fails_freshness() {
            let audience = Audience::discover(b"test");
            let challenge =
                Challenge::new(audience, TimestampSeconds::new(1000), Nonce::from_u128(1));
            let now = TimestampSeconds::new(1000 + MAX_PLAUSIBLE_DRIFT.as_secs() + 1);
            assert!(matches!(
                challenge.validate(&audience, now, MAX_PLAUSIBLE_DRIFT),
                Err(ChallengeValidationError::ClockDrift { .. })
            ));
        }
    }
}
