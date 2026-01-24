//! Handshake protocol for authenticating new connections.
//!
//! The handshake establishes mutual identity between peers.
//! It answers "_who_ is connecting?" but does not answer
//! "_can_ they connect?" — that's the job of [`ConnectionPolicy`](crate::policy::ConnectionPolicy).
//!
//! # Protocol Flow
//!
//! ```text
//!     Client                                          Server
//!       │                                               │
//!       │  1. Signed<Challenge>                         │
//!       │  ─────────────────────────────────────────►   │
//!       │     { audience, timestamp, nonce }            │
//!       │     Client identity: challenge.issuer()       │
//!       │                                               │
//!       │                                               │
//!       │                      2. Signed<Response>      │
//!       │  ◄─────────────────────────────────────────   │
//!       │     { challenge_digest, server_timestamp }    │
//!       │     Server identity: response.issuer()        │
//!       │     Binding: challenge_digest includes nonce  │
//!       │                                               │
//!       ▼                                               ▼
//!    Knows server_id                              Knows client_id
//! ```

use core::time::Duration;

use futures_kind::FutureKind;
use sedimentree_core::blob::Digest as RawDigest;
use thiserror::Error;

use crate::{
    crypto::{digest::Digest, signed::Signed, signer::Signer},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};

pub use crate::crypto::nonce::Nonce;

/// Maximum plausible clock drift for rejecting implausible timestamps.
pub const MAX_PLAUSIBLE_DRIFT: Duration = Duration::from_secs(10 * 60);

/// The intended recipient of a challenge.
///
/// Supports two modes:
/// - `Peer`: Client knows the server's [`PeerId`] ahead of time
/// - `Discovery`: Client knows the URL/endpoint but not the peer identity
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Audience {
    /// Known peer identity.
    #[n(0)]
    Known(#[n(0)] PeerId),

    /// Discovery mode: hash of URL or similar service identifier.
    #[n(1)]
    Discover(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 32],
    ),
}

impl Audience {
    /// Create an audience from a known peer ID.
    #[must_use]
    pub const fn known(id: PeerId) -> Self {
        Self::Known(id)
    }

    /// Create a discovery audience from a service identifier.
    ///
    /// The identifier is hashed with BLAKE3 to produce a 32-byte value.
    #[must_use]
    pub fn discover(service_identifier: &[u8]) -> Self {
        let digest = RawDigest::hash(service_identifier);
        Self::Discover(*digest.as_bytes())
    }

    /// Create a discovery audience from a pre-hashed value.
    #[must_use]
    pub const fn discover_raw(hash: [u8; 32]) -> Self {
        Self::Discover(hash)
    }
}


/// A handshake challenge sent by the client.
///
/// This is signed by the client and sent to the server. The server extracts
/// the client's identity from the signature's issuer.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Challenge {
    /// Who the client is connecting to.
    #[n(0)]
    pub audience: Audience,

    /// Client's timestamp (used for replay protection).
    #[n(1)]
    pub timestamp: TimestampSeconds,

    /// Random nonce for uniqueness.
    #[n(2)]
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

/// A handshake response from the server.
///
/// This is signed by the server. The client extracts the server's identity
/// from the signature's issuer and uses the server timestamp for drift correction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Response {
    /// Hash of the challenge being responded to (binds response to request).
    #[n(0)]
    pub challenge_digest: Digest<Challenge>,

    /// Server's current timestamp (for client-side drift correction).
    #[n(1)]
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

/// Reasons for rejecting a handshake (sent unsigned).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RejectionReason {
    /// Client's timestamp is too far from server's clock.
    #[n(0)]
    ClockDrift,

    /// The audience field doesn't match this server.
    #[n(1)]
    InvalidAudience,

    /// This nonce was already used (replay attack detected).
    #[n(2)]
    ReplayedNonce,

    /// The signature on the challenge is invalid.
    #[n(3)]
    InvalidSignature,
}

/// An unsigned rejection message.
///
/// # Security Note
///
/// This message is unsigned. Clients should NOT use the `server_timestamp`
/// for drift correction if the drift is implausible (> [`MAX_PLAUSIBLE_DRIFT`]).
/// An attacker could send fake rejections with manipulated timestamps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, minicbor::Encode, minicbor::Decode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Rejection {
    /// Why the handshake was rejected.
    #[n(0)]
    pub reason: RejectionReason,

    /// Server's current timestamp (informational only).
    #[n(1)]
    pub server_timestamp: TimestampSeconds,
}

impl Rejection {
    /// Create a new rejection.
    #[must_use]
    pub const fn new(reason: RejectionReason, now: TimestampSeconds) -> Self {
        Self {
            reason,
            server_timestamp: now,
        }
    }
}

/// Errors when validating a [`Challenge`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
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
}

impl ChallengeValidationError {
    /// Convert to a rejection reason.
    #[must_use]
    pub const fn to_rejection_reason(&self) -> RejectionReason {
        match self {
            Self::InvalidAudience => RejectionReason::InvalidAudience,
            Self::ClockDrift { .. } => RejectionReason::ClockDrift,
        }
    }
}

/// Errors when validating a [`Response`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ResponseValidationError {
    /// The challenge digest doesn't match.
    #[error("challenge digest mismatch")]
    ChallengeMismatch,
}

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

/// Create a signed challenge for initiating a handshake.
///
/// The caller must provide the current timestamp and a random nonce.
/// For `no_std` compatibility, these are not generated internally.
pub async fn create_challenge<K: FutureKind>(
    signer: &impl Signer<K>,
    audience: Audience,
    now: TimestampSeconds,
    nonce: Nonce,
) -> Signed<Challenge> {
    let challenge = Challenge::new(audience, now, nonce);
    Signed::sign(signer, challenge).await
}

/// Result of verifying a challenge on the server side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifiedChallenge {
    /// The client's peer ID (extracted from the signature).
    pub client_id: PeerId,
    /// The verified challenge payload.
    pub challenge: Challenge,
}

/// Verify a signed challenge from a client.
///
/// # Errors
///
/// Returns an error if:
/// - The signature is invalid
/// - The audience doesn't match
/// - The timestamp is outside the acceptable drift window
pub fn verify_challenge(
    signed_challenge: &Signed<Challenge>,
    expected_audience: &Audience,
    now: TimestampSeconds,
    max_drift: Duration,
) -> Result<VerifiedChallenge, HandshakeError> {
    // Verify signature and decode
    let verified = signed_challenge
        .try_verify()
        .map_err(|_| HandshakeError::InvalidSignature)?;

    let challenge = verified.payload();

    // Validate the challenge
    challenge
        .validate(expected_audience, now, max_drift)
        .map_err(HandshakeError::ChallengeValidation)?;

    Ok(VerifiedChallenge {
        client_id: PeerId::from(verified.issuer()),
        challenge: *challenge,
    })
}

/// Create a signed response for a verified challenge.
pub async fn create_response<K: FutureKind>(
    signer: &impl Signer<K>,
    challenge: &Challenge,
    now: TimestampSeconds,
) -> Signed<Response> {
    let response = Response::for_challenge(challenge, now);
    Signed::sign(signer, response).await
}

/// Result of verifying a response on the client side.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VerifiedResponse {
    /// The server's peer ID (extracted from the signature).
    pub server_id: PeerId,
    /// The verified response payload.
    pub response: Response,
}

/// Verify a signed response from a server.
///
/// # Errors
///
/// Returns an error if:
/// - The signature is invalid
/// - The challenge digest doesn't match the original challenge
pub fn verify_response(
    signed_response: &Signed<Response>,
    original_challenge: &Challenge,
) -> Result<VerifiedResponse, HandshakeError> {
    // Verify signature and decode
    let verified = signed_response
        .try_verify()
        .map_err(|_| HandshakeError::InvalidSignature)?;

    let response = verified.payload();

    // Validate the response matches our challenge
    response
        .validate(original_challenge)
        .map_err(HandshakeError::ResponseValidation)?;

    Ok(VerifiedResponse {
        server_id: PeerId::from(verified.issuer()),
        response: *response,
    })
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    mod nonce {
        use super::*;

        #[test]
        fn roundtrip_from_bytes() {
            let nonce = Nonce::new(0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0);
            let bytes = *nonce.as_bytes();
            let recovered = Nonce::from_bytes(bytes);
            assert_eq!(nonce, recovered);
        }

        #[test]
        fn roundtrip_from_u128() {
            let value = 0x1234_5678_9ABC_DEF0_1234_5678_9ABC_DEF0u128;
            let nonce = Nonce::new(value);
            assert_eq!(nonce.as_u128(), value);
        }
    }

    mod challenge {
        use super::*;

        #[test]
        fn digest_is_deterministic() {
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            assert_eq!(
                Digest::<Challenge>::hash(&challenge),
                Digest::<Challenge>::hash(&challenge)
            );
        }

        #[test]
        fn different_nonces_different_digests() {
            let c1 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(1),
            );
            let c2 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(2),
            );
            assert_ne!(
                Digest::<Challenge>::hash(&c1),
                Digest::<Challenge>::hash(&c2)
            );
        }

        #[test]
        fn is_fresh_within_drift() {
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            let now = TimestampSeconds::new(1005);
            assert!(challenge.is_fresh(now, Duration::from_secs(10)));
        }

        #[test]
        fn is_not_fresh_outside_drift() {
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            let now = TimestampSeconds::new(2000);
            assert!(!challenge.is_fresh(now, Duration::from_secs(10)));
        }
    }

    mod response {
        use super::*;

        #[test]
        fn for_challenge_matches_digest() {
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            let response = Response::for_challenge(&challenge, TimestampSeconds::new(1001));
            assert!(response.validate(&challenge).is_ok());
        }

        #[test]
        fn wrong_challenge_fails_validation() {
            let challenge1 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(1),
            );
            let challenge2 = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(2),
            );
            let response = Response::for_challenge(&challenge1, TimestampSeconds::new(1001));
            assert!(response.validate(&challenge2).is_err());
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
        fn applies_negative_drift() {
            let mut dc = DriftCorrection::new();
            let client_ts = TimestampSeconds::new(1010);
            let server_ts = TimestampSeconds::new(1000);

            assert!(dc.adjust(server_ts, client_ts));
            assert_eq!(dc.apply(client_ts), server_ts);
        }

        #[test]
        fn rejects_implausible_drift() {
            let mut dc = DriftCorrection::new();
            let client_ts = TimestampSeconds::new(1000);
            let server_ts = TimestampSeconds::new(1_000_000); // Way off

            assert!(!dc.adjust(server_ts, client_ts));
            assert_eq!(dc.offset_secs(), 0);
        }

        #[test]
        fn overwrites_previous_adjustment() {
            let mut dc = DriftCorrection::new();
            let client_ts = TimestampSeconds::new(1000);

            assert!(dc.adjust(TimestampSeconds::new(1010), client_ts));
            assert_eq!(dc.offset_secs(), 10);

            assert!(dc.adjust(TimestampSeconds::new(1020), client_ts));
            assert_eq!(dc.offset_secs(), 20);
        }
    }

    mod executor {
        use super::*;
        use crate::crypto::signer::LocalSigner;

        fn test_signer(seed: u8) -> LocalSigner {
            LocalSigner::from_bytes(&[seed; 32])
        }

        #[tokio::test]
        async fn full_handshake_round_trip() {
            let client_signer = test_signer(1);
            let server_signer = test_signer(2);

            let now = TimestampSeconds::new(1000);
            let audience = Audience::discover(b"https://example.com");
            let nonce = Nonce::new(12345);

            // Client creates challenge
            let signed_challenge = create_challenge(&client_signer, audience, now, nonce).await;

            // Server verifies challenge
            let verified_challenge =
                verify_challenge(&signed_challenge, &audience, now, MAX_PLAUSIBLE_DRIFT)
                    .expect("challenge should verify");

            assert_eq!(verified_challenge.client_id, client_signer.peer_id());
            assert_eq!(verified_challenge.challenge.nonce, nonce);

            // Server creates response
            let signed_response =
                create_response(&server_signer, &verified_challenge.challenge, now).await;

            // Client verifies response
            let original_challenge = Challenge::new(audience, now, nonce);
            let verified_response = verify_response(&signed_response, &original_challenge)
                .expect("response should verify");

            assert_eq!(verified_response.server_id, server_signer.peer_id());
        }

        #[tokio::test]
        async fn wrong_audience_rejected() {
            let client_signer = test_signer(1);

            let now = TimestampSeconds::new(1000);
            let client_audience = Audience::discover(b"https://example.com");
            let server_audience = Audience::discover(b"https://other.com");
            let nonce = Nonce::new(12345);

            let signed_challenge =
                create_challenge(&client_signer, client_audience, now, nonce).await;

            let result = verify_challenge(
                &signed_challenge,
                &server_audience,
                now,
                MAX_PLAUSIBLE_DRIFT,
            );

            assert!(matches!(
                result,
                Err(HandshakeError::ChallengeValidation(
                    ChallengeValidationError::InvalidAudience
                ))
            ));
        }

        #[tokio::test]
        async fn stale_timestamp_rejected() {
            let client_signer = test_signer(1);

            let client_now = TimestampSeconds::new(1000);
            let server_now = TimestampSeconds::new(2000); // 1000 seconds later
            let audience = Audience::discover(b"https://example.com");
            let nonce = Nonce::new(12345);

            let signed_challenge =
                create_challenge(&client_signer, audience, client_now, nonce).await;

            // Use a short max drift to trigger rejection
            let result = verify_challenge(
                &signed_challenge,
                &audience,
                server_now,
                Duration::from_secs(60),
            );

            assert!(matches!(
                result,
                Err(HandshakeError::ChallengeValidation(
                    ChallengeValidationError::ClockDrift { .. }
                ))
            ));
        }

        #[tokio::test]
        async fn wrong_challenge_digest_rejected() {
            let server_signer = test_signer(2);

            let now = TimestampSeconds::new(1000);
            let audience = Audience::discover(b"https://example.com");
            let nonce1 = Nonce::new(11111);
            let nonce2 = Nonce::new(22222);

            // Client creates challenge with nonce1
            let challenge1 = Challenge::new(audience, now, nonce1);

            // Server creates response for challenge1
            let signed_response = create_response(&server_signer, &challenge1, now).await;

            // Client tries to verify with different challenge (nonce2)
            let challenge2 = Challenge::new(audience, now, nonce2);
            let result = verify_response(&signed_response, &challenge2);

            assert!(matches!(
                result,
                Err(HandshakeError::ResponseValidation(
                    ResponseValidationError::ChallengeMismatch
                ))
            ));
        }
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_nonce_bytes_roundtrip() {
            bolero::check!().with_type::<Nonce>().for_each(|nonce| {
                let bytes = *nonce.as_bytes();
                let recovered = Nonce::from_bytes(bytes);
                assert_eq!(nonce, &recovered);
            });
        }

        #[test]
        fn prop_nonce_u128_roundtrip() {
            bolero::check!().with_type::<u128>().for_each(|value| {
                let nonce = Nonce::new(*value);
                assert_eq!(nonce.as_u128(), *value);
            });
        }

        #[test]
        fn prop_challenge_digest_deterministic() {
            bolero::check!()
                .with_type::<Challenge>()
                .for_each(|challenge| {
                    assert_eq!(
                        Digest::<Challenge>::hash(challenge),
                        Digest::<Challenge>::hash(challenge)
                    );
                });
        }

        #[test]
        fn prop_response_for_challenge_validates() {
            bolero::check!()
                .with_type::<(Challenge, TimestampSeconds)>()
                .for_each(|(challenge, now)| {
                    let response = Response::for_challenge(challenge, *now);
                    assert!(response.validate(challenge).is_ok());
                });
        }

        #[test]
        fn prop_challenge_validation_error_has_rejection_reason() {
            bolero::check!()
                .with_type::<ChallengeValidationError>()
                .for_each(|err| {
                    let _ = err.to_rejection_reason();
                });
        }

        #[test]
        fn prop_drift_correction_bounded() {
            bolero::check!()
                .with_type::<(TimestampSeconds, TimestampSeconds)>()
                .for_each(|(server_ts, client_ts)| {
                    let mut dc = DriftCorrection::new();
                    if dc.adjust(*server_ts, *client_ts) {
                        let max_drift = MAX_PLAUSIBLE_DRIFT.as_secs() as i32;
                        assert!(dc.offset_secs().abs() <= max_drift);
                    }
                });
        }
    }
}
