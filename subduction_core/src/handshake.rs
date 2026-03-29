//! Handshake protocol for authenticating new connections.
//!
//! The handshake establishes mutual identity between peers.
//! It answers "_who_ is connecting?" but does not answer
//! "_can_ they connect?" — that's the job of [`ConnectionPolicy`](crate::policy::ConnectionPolicy).
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
//! # Usage
//!
//! Use [`initiate`] for the side that sends first (traditional "client"),
//! and [`respond`] for the side that receives first (traditional "server").
//! These functions consume the transport and return an [`Authenticated`]
//! connection on success.
//!
//! ```ignore
//! use subduction_core::handshake;
//!
//! // Initiator side - transport is consumed, returned to build_connection
//! let authenticated = handshake::initiate(
//!     transport,  // consumed
//!     |transport, peer_id| MyConnection::new(transport, peer_id),
//!     &signer,
//!     audience,
//!     now,
//!     nonce,
//! ).await?;
//!
//! // Responder side - transport is consumed, returned to build_connection
//! let authenticated = handshake::respond(
//!     transport,  // consumed
//!     |transport, peer_id| MyConnection::new(transport, peer_id),
//!     &signer,
//!     &nonce_cache,
//!     our_peer_id,
//!     discovery_audience,
//!     now,
//!     max_drift,
//! ).await?;
//! ```

pub mod audience;
pub mod challenge;
pub mod rejection;
pub mod response;

use alloc::vec::Vec;
use core::time::Duration;

use future_form::FutureForm;
use thiserror::Error;

use crate::{
    authenticated::Authenticated, nonce_cache::NonceCache, peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use sedimentree_core::codec::{
    error::{DecodeError, InvalidEnumTag, InvalidSchema},
    schema::{self, Schema},
};
use subduction_crypto::{nonce::Nonce, signed::Signed, signer::Signer};

use audience::Audience;
use challenge::{Challenge, ChallengeValidationError};
use rejection::{Rejection, RejectionReason};
use response::{Response, ResponseValidationError};

/// Maximum plausible clock drift for rejecting implausible timestamps (±10 minutes).
pub const MAX_PLAUSIBLE_DRIFT: Duration = Duration::from_secs(10 * 60);

/// Maximum clock drift tolerated during simultaneous open handshakes.
const SIMULTANEOUS_OPEN_MAX_DRIFT: Duration = Duration::from_secs(600);

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

/// A transport capable of exchanging handshake messages.
///
/// Implementors provide raw byte send/recv over their transport layer.
/// The handshake protocol handles encoding/decoding of [`HandshakeMessage`].
pub trait Handshake<K: FutureForm> {
    /// Transport-level error type.
    type Error;

    /// Send raw bytes over the transport.
    fn send(&mut self, bytes: Vec<u8>) -> K::Future<'_, Result<(), Self::Error>>;

    /// Receive raw bytes from the transport.
    fn recv(&mut self) -> K::Future<'_, Result<Vec<u8>, Self::Error>>;
}

/// Wire format for handshake messages.
///
/// All handshake messages share a common `SUH\0` schema envelope, followed
/// by a 1-byte variant tag:
///
/// ```text
/// ┌──────────┬─────┬───────────────────────────────────┐
/// │ Schema   │ Tag │ Payload                           │
/// │ SUH\0    │ 1B  │ (variant-specific)                │
/// │ (4B)     │     │                                   │
/// └──────────┴─────┴───────────────────────────────────┘
/// ```
///
/// For signed variants, the inner `Signed<T>` schema prefix is elided
/// on the wire (the envelope schema identifies the protocol, and the tag
/// identifies the variant). The schema is reconstructed on decode before
/// passing to [`Signed::try_decode`].
///
/// | Tag  | Variant              | Payload                                    |
/// |------|----------------------|--------------------------------------------|
/// | 0x00 | `Signed<Challenge>`  | issuer(32) + fields(57) + sig(64) = 153B   |
/// | 0x01 | `Signed<Response>`   | issuer(32) + fields(40) + sig(64) = 136B   |
/// | 0x02 | `Rejection`          | reason (1B) + timestamp (8B)               |
///
/// [`Signed::try_decode`]: subduction_crypto::signed::Signed::try_decode
#[derive(Debug)]
pub enum HandshakeMessage {
    /// A signed challenge from the initiator.
    SignedChallenge(Signed<Challenge>),

    /// A signed response from the responder.
    SignedResponse(Signed<Response>),

    /// An unsigned rejection from the responder.
    Rejection(Rejection),
}

impl Schema for HandshakeMessage {
    const PREFIX: [u8; 2] = schema::SUBDUCTION_PREFIX;
    const TYPE_BYTE: u8 = b'H'; // Handshake
    const VERSION: u8 = 0;
}

/// Variant tag bytes within the `SUH\0` handshake envelope.
mod handshake_tags {
    pub(super) const CHALLENGE: u8 = 0x00;
    pub(super) const RESPONSE: u8 = 0x01;
    pub(super) const REJECTION: u8 = 0x02;
}

impl HandshakeMessage {
    /// Minimum size: schema (4) + tag (1).
    const MIN_SIZE: usize = 5;

    /// Schema prefix size (elided from signed variant payloads on the wire).
    const SCHEMA_SIZE: usize = 4;

    /// Encode the handshake message to wire bytes.
    ///
    /// Emits `SUH\0` + variant tag + payload. For signed variants,
    /// the inner `Signed<T>` schema prefix is elided (the envelope
    /// schema already identifies the protocol).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match self {
            HandshakeMessage::SignedChallenge(signed) => {
                let inner = signed.as_bytes();
                let stripped = &inner[Self::SCHEMA_SIZE..];
                let mut buf = Vec::with_capacity(4 + 1 + stripped.len());
                buf.extend_from_slice(&Self::SCHEMA);
                buf.push(handshake_tags::CHALLENGE);
                buf.extend_from_slice(stripped);
                buf
            }
            HandshakeMessage::SignedResponse(signed) => {
                let inner = signed.as_bytes();
                let stripped = &inner[Self::SCHEMA_SIZE..];
                let mut buf = Vec::with_capacity(4 + 1 + stripped.len());
                buf.extend_from_slice(&Self::SCHEMA);
                buf.push(handshake_tags::RESPONSE);
                buf.extend_from_slice(stripped);
                buf
            }
            HandshakeMessage::Rejection(rejection) => {
                let payload = rejection.encode_payload();
                let mut buf = Vec::with_capacity(4 + 1 + payload.len());
                buf.extend_from_slice(&Self::SCHEMA);
                buf.push(handshake_tags::REJECTION);
                buf.extend_from_slice(&payload);
                buf
            }
        }
    }

    /// Decode a handshake message from wire bytes.
    ///
    /// Expects `SUH\0` schema prefix followed by a variant tag byte:
    /// - `0x00` → [`Signed<Challenge>`]
    /// - `0x01` → [`Signed<Response>`]
    /// - `0x02` → [`Rejection`]
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

        if got_schema != Self::SCHEMA {
            return Err(InvalidSchema {
                expected: Self::SCHEMA,
                got: got_schema,
            }
            .into());
        }

        let tag = bytes.get(4).copied().ok_or(DecodeError::MessageTooShort {
            type_name: "HandshakeMessage",
            need: Self::MIN_SIZE,
            have: bytes.len(),
        })?;

        let payload = bytes.get(5..).ok_or(DecodeError::MessageTooShort {
            type_name: "HandshakeMessage",
            need: Self::MIN_SIZE + 1,
            have: bytes.len(),
        })?;

        match tag {
            handshake_tags::CHALLENGE => {
                // Reconstruct the Signed<Challenge> schema prefix that was
                // elided on the wire (the envelope schema identifies the protocol).
                let mut full = Vec::with_capacity(4 + payload.len());
                full.extend_from_slice(&Challenge::SCHEMA);
                full.extend_from_slice(payload);
                let signed = Signed::<Challenge>::try_decode(full)?;
                Ok(HandshakeMessage::SignedChallenge(signed))
            }
            handshake_tags::RESPONSE => {
                let mut full = Vec::with_capacity(4 + payload.len());
                full.extend_from_slice(&Response::SCHEMA);
                full.extend_from_slice(payload);
                let signed = Signed::<Response>::try_decode(full)?;
                Ok(HandshakeMessage::SignedResponse(signed))
            }
            handshake_tags::REJECTION => {
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

/// Errors that can occur during authentication.
#[derive(Debug, Error)]
pub enum AuthenticateError<E> {
    /// Transport-level error.
    #[error("transport error: {0}")]
    Transport(E),

    /// `SyncMessage` decoding error.
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),

    /// Handshake protocol error (signature or validation failure).
    #[error("handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    /// The responder rejected the handshake.
    #[error("handshake rejected: {reason:?}")]
    Rejected {
        /// The rejection reason.
        reason: RejectionReason,
        /// The responder's timestamp (for drift correction).
        responder_timestamp: TimestampSeconds,
    },

    /// Received an unexpected message type.
    #[error("unexpected message type")]
    UnexpectedMessage,

    /// Connection closed before handshake completed.
    #[error("connection closed during handshake")]
    ConnectionClosed,

    /// Simultaneous open detected a reflected challenge (same bytes as ours).
    /// This indicates a reflection attack where the remote side replayed our
    /// challenge back to us without signing their own.
    #[error("reflected challenge detected (possible reflection attack)")]
    ReflectedChallenge,

    /// Simultaneous open received a challenge signed by our own key.
    /// This indicates a reflection attack or a self-connection attempt.
    #[error("challenge signed by our own key (reflection attack)")]
    ReflectionAttack,

    /// Simultaneous open: the peer ID from the challenge doesn't match the
    /// peer ID from the response. This indicates a MITM or protocol violation.
    #[error("peer ID mismatch between simultaneous open challenge and response")]
    SimultaneousOpenPeerMismatch,
}

/// Result of a successful initiator-side handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitiateResult {
    /// The verified responder's peer ID.
    pub responder_id: PeerId,
    /// The responder's timestamp (for drift correction).
    pub responder_timestamp: TimestampSeconds,
}

/// Result of a successful responder-side handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RespondResult {
    /// The verified initiator's peer ID.
    pub initiator_id: PeerId,
    /// The verified challenge (for logging/debugging).
    pub challenge: Challenge,
}

/// Receive and decode a handshake message from the transport.
async fn recv_handshake_message<K: FutureForm, H: Handshake<K>>(
    handshake: &mut H,
) -> Result<HandshakeMessage, AuthenticateError<H::Error>> {
    let bytes = handshake
        .recv()
        .await
        .map_err(AuthenticateError::Transport)?;
    if bytes.is_empty() {
        return Err(AuthenticateError::ConnectionClosed);
    }
    Ok(HandshakeMessage::try_decode(&bytes)?)
}

/// Receive a message from the transport, verify it is a [`Signed<Response>`]
/// that matches our original challenge, and return the responder's peer ID.
///
/// Also handles [`Rejection`] messages and unexpected message types.
async fn recv_verified_response<K: FutureForm, H: Handshake<K>>(
    handshake: &mut H,
    original_challenge: &Challenge,
) -> Result<PeerId, AuthenticateError<H::Error>> {
    let msg = recv_handshake_message(handshake).await?;
    match msg {
        HandshakeMessage::SignedResponse(signed_response) => {
            let verified = verify_response(&signed_response, original_challenge)?;
            Ok(verified.server_id)
        }
        HandshakeMessage::Rejection(rejection) => Err(AuthenticateError::Rejected {
            reason: rejection.reason,
            responder_timestamp: rejection.server_timestamp,
        }),
        HandshakeMessage::SignedChallenge(_) => Err(AuthenticateError::UnexpectedMessage),
    }
}

/// Perform the initiator side of the handshake (sends first).
///
/// Sends a signed challenge and waits for a signed response.
/// On success, returns an [`Authenticated`] connection wrapping the result
/// of `build_connection`, along with any extra data returned by the factory.
///
/// # Arguments
///
/// * `handshake` - The transport implementing [`Handshake`] (consumed)
/// * `build_connection` - Factory to create the connection (and optional extra data) from the transport and verified peer ID
/// * `signer` - The initiator's signer for creating the challenge
/// * `audience` - The intended recipient (known peer ID or discovery hash)
/// * `now` - The current timestamp
/// * `nonce` - A random nonce for replay protection
///
/// # Errors
///
/// Returns an error if:
/// - The transport fails to send/receive
/// - The responder rejects the handshake
/// - The response signature is invalid
/// - The response doesn't match the challenge
///
/// # Panics
///
/// Panics if encoding of the challenge message fails (should never happen
/// with well-formed types).
pub async fn initiate<K: FutureForm, H: Handshake<K>, C: Clone, E, S: Signer<K>>(
    mut handshake: H,
    build_connection: impl FnOnce(H, PeerId) -> (C, E),
    signer: &S,
    audience: Audience,
    now: TimestampSeconds,
    nonce: Nonce,
) -> Result<(Authenticated<C, K>, E), AuthenticateError<H::Error>> {
    // Create and send challenge
    let challenge = Challenge::new(audience, now, nonce);
    let signed_challenge = Signed::seal::<K, _>(signer, challenge).await.into_signed();
    let msg = HandshakeMessage::SignedChallenge(signed_challenge.clone());
    handshake
        .send(msg.encode())
        .await
        .map_err(AuthenticateError::Transport)?;

    // Receive first message (response, rejection, or simultaneous challenge)
    let first_msg = recv_handshake_message(&mut handshake).await?;

    match first_msg {
        HandshakeMessage::SignedResponse(signed_response) => {
            // Normal case: responder sent a response to our challenge.
            let verified = verify_response(&signed_response, &challenge)?;
            let peer_id = verified.server_id;
            let (conn, extra) = build_connection(handshake, peer_id);
            Ok((Authenticated::from_handshake(conn, peer_id), extra))
        }
        HandshakeMessage::Rejection(rejection) => Err(AuthenticateError::Rejected {
            reason: rejection.reason,
            responder_timestamp: rejection.server_timestamp,
        }),
        HandshakeMessage::SignedChallenge(their_signed_challenge) => {
            // Simultaneous open: both sides sent challenges.

            // Guard: detect reflected challenge (exact replay of our bytes).
            if signed_challenge.as_bytes() == their_signed_challenge.as_bytes() {
                return Err(AuthenticateError::ReflectedChallenge);
            }

            // Verify signature, audience, and clock drift using the same
            // validation as `respond()`. Try Known(our_peer_id) first,
            // then fall back to our discovery audience if applicable.
            let our_peer_id = PeerId::from(signer.verifying_key());
            let known_audience = Audience::known(our_peer_id);

            let their_verified = match verify_challenge(
                &their_signed_challenge,
                &known_audience,
                now,
                SIMULTANEOUS_OPEN_MAX_DRIFT,
            ) {
                Ok(v) => v,
                Err(HandshakeError::ChallengeValidation(
                    ChallengeValidationError::InvalidAudience,
                )) => {
                    // Fall back to discovery audience if we initiated with one.
                    let discovery = match &audience {
                        Audience::Discover(_) => Some(&audience),
                        Audience::Known(_) => None,
                    };
                    match discovery {
                        Some(disc) => verify_challenge(
                            &their_signed_challenge,
                            disc,
                            now,
                            SIMULTANEOUS_OPEN_MAX_DRIFT,
                        )?,
                        None => {
                            return Err(AuthenticateError::Handshake(
                                HandshakeError::ChallengeValidation(
                                    ChallengeValidationError::InvalidAudience,
                                ),
                            ));
                        }
                    }
                }
                Err(e) => return Err(e.into()),
            };

            // Guard: detect challenge signed by our own key (reflection
            // attack or self-connection). Checked AFTER signature verification
            // to prevent DoS via spoofed issuer field.
            if their_verified.client_id == our_peer_id {
                return Err(AuthenticateError::ReflectionAttack);
            }

            let expected_peer_id = their_verified.client_id;
            let their_challenge = their_verified.challenge;

            // Break the tie deterministically — the side whose signed
            // challenge is lexicographically greater wins (keeps initiator role).
            let we_win = signed_challenge.as_bytes() > their_signed_challenge.as_bytes();

            if we_win {
                // Winner: receive the loser's response first (verify they're
                // legit), then send our response to their challenge.
                let peer_id = recv_verified_response(&mut handshake, &challenge).await?;

                // The peer who signed the challenge must be the same peer
                // who signed the response — otherwise MITM.
                if peer_id != expected_peer_id {
                    return Err(AuthenticateError::SimultaneousOpenPeerMismatch);
                }

                let our_response = create_response::<K, _>(signer, &their_challenge, now).await;
                handshake
                    .send(HandshakeMessage::SignedResponse(our_response).encode())
                    .await
                    .map_err(AuthenticateError::Transport)?;

                let (conn, extra) = build_connection(handshake, peer_id);
                Ok((Authenticated::from_handshake(conn, peer_id), extra))
            } else {
                // Loser: send our response to their challenge, then wait
                // for the winner to respond to ours.
                let our_response = create_response::<K, _>(signer, &their_challenge, now).await;
                handshake
                    .send(HandshakeMessage::SignedResponse(our_response).encode())
                    .await
                    .map_err(AuthenticateError::Transport)?;

                let peer_id = recv_verified_response(&mut handshake, &challenge).await?;

                if peer_id != expected_peer_id {
                    return Err(AuthenticateError::SimultaneousOpenPeerMismatch);
                }

                let (conn, extra) = build_connection(handshake, peer_id);
                Ok((Authenticated::from_handshake(conn, peer_id), extra))
            }
        }
    }
}

/// Perform the responder side of the handshake (receives first).
///
/// Receives a signed challenge, verifies it, and sends a signed response.
/// On success, returns an [`Authenticated`] connection wrapping the result
/// of `build_connection`, along with any extra data returned by the factory.
///
/// # Arguments
///
/// * `handshake` - The transport implementing [`Handshake`] (consumed)
/// * `build_connection` - Factory to create the connection (and optional extra data) from the transport and verified peer ID
/// * `signer` - The responder's signer for creating the response
/// * `nonce_cache` - Cache for replay protection
/// * `our_peer_id` - Our peer ID (always accepted as `Audience::Known`)
/// * `discovery_audience` - Optional discovery audience (also accepted if provided)
/// * `now` - The current timestamp
/// * `max_drift` - Maximum acceptable clock drift
///
/// # Errors
///
/// Returns an error if:
/// - The transport fails to send/receive
/// - The challenge signature is invalid
/// - The audience doesn't match
/// - The timestamp is outside the acceptable drift window
/// - The nonce has already been used (replay attack)
///
/// # Panics
///
/// Panics if encoding of the response or rejection message fails (should
/// never happen with well-formed types).
#[allow(clippy::expect_used, clippy::too_many_arguments)]
pub async fn respond<K: FutureForm, H: Handshake<K>, C: Clone, E, S: Signer<K>>(
    mut handshake: H,
    build_connection: impl FnOnce(H, PeerId) -> (C, E),
    signer: &S,
    nonce_cache: &NonceCache,
    our_peer_id: PeerId,
    discovery_audience: Option<Audience>,
    now: TimestampSeconds,
    max_drift: Duration,
) -> Result<(Authenticated<C, K>, E), AuthenticateError<H::Error>> {
    // Receive challenge
    let challenge_msg = recv_handshake_message(&mut handshake).await?;

    let HandshakeMessage::SignedChallenge(signed_challenge) = challenge_msg else {
        return Err(AuthenticateError::UnexpectedMessage);
    };

    // Verify the challenge - try Known(our_peer_id) first, then discovery audience
    let known_audience = Audience::known(our_peer_id);
    let verified = match verify_challenge(&signed_challenge, &known_audience, now, max_drift) {
        Ok(v) => v,
        Err(HandshakeError::ChallengeValidation(ChallengeValidationError::InvalidAudience))
            if discovery_audience.is_some() =>
        {
            // Try discovery audience as fallback
            match verify_challenge(
                &signed_challenge,
                discovery_audience.as_ref().expect("checked is_some"),
                now,
                max_drift,
            ) {
                Ok(v) => v,
                Err(e) => {
                    send_rejection(&mut handshake, &e, now).await?;
                    return Err(e.into());
                }
            }
        }
        Err(e) => {
            send_rejection(&mut handshake, &e, now).await?;
            return Err(e.into());
        }
    };

    // Claim the nonce for replay protection
    // Only do this after signature verification succeeds (to prevent DoS via cache filling)
    if nonce_cache
        .try_claim(verified.client_id, verified.challenge.nonce, now)
        .await
        .is_err()
    {
        let rejection = Rejection::new(RejectionReason::ReplayedNonce, now);
        let msg = HandshakeMessage::Rejection(rejection);
        handshake
            .send(msg.encode())
            .await
            .map_err(AuthenticateError::Transport)?;
        return Err(AuthenticateError::Handshake(
            HandshakeError::ChallengeValidation(ChallengeValidationError::ReplayedNonce),
        ));
    }

    // Create and send response
    let signed_response = create_response(signer, &verified.challenge, now).await;
    let response_msg = HandshakeMessage::SignedResponse(signed_response);
    handshake
        .send(response_msg.encode())
        .await
        .map_err(AuthenticateError::Transport)?;

    let peer_id = verified.client_id;
    let (conn, extra) = build_connection(handshake, peer_id);
    Ok((Authenticated::from_handshake(conn, peer_id), extra))
}

/// Helper to send a rejection message.
#[allow(clippy::expect_used)]
async fn send_rejection<K: FutureForm, H: Handshake<K>>(
    handshake: &mut H,
    error: &HandshakeError,
    now: TimestampSeconds,
) -> Result<(), AuthenticateError<H::Error>> {
    let reason = match error {
        HandshakeError::InvalidSignature | HandshakeError::ResponseValidation(_) => {
            RejectionReason::InvalidSignature
        }
        HandshakeError::ChallengeValidation(cv) => cv.to_rejection_reason(),
    };
    let rejection = Rejection::new(reason, now);
    let msg = HandshakeMessage::Rejection(rejection);
    handshake
        .send(msg.encode())
        .await
        .map_err(AuthenticateError::Transport)
}

/// Create a signed challenge for initiating a handshake.
///
/// The caller must provide the current timestamp and a random nonce.
/// For `no_std` compatibility, these are not generated internally.
pub async fn create_challenge<K: FutureForm, S: Signer<K>>(
    signer: &S,
    audience: Audience,
    now: TimestampSeconds,
    nonce: Nonce,
) -> Signed<Challenge> {
    let challenge = Challenge::new(audience, now, nonce);
    Signed::seal::<K, _>(signer, challenge).await.into_signed()
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
pub async fn create_response<K: FutureForm, S: Signer<K>>(
    signer: &S,
    challenge: &Challenge,
    now: TimestampSeconds,
) -> Signed<Response> {
    let response = Response::for_challenge(challenge, now);
    Signed::seal::<K, _>(signer, response).await.into_signed()
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

    use super::{
        audience::DiscoveryId,
        challenge::{CHALLENGE_FIELDS_SIZE, CHALLENGE_MIN_SIZE},
        response::{RESPONSE_FIELDS_SIZE, RESPONSE_MIN_SIZE},
    };
    use sedimentree_core::{
        codec::{decode::DecodeFields, encode::EncodeFields, error::InvalidEnumTag},
        crypto::digest::Digest,
    };

    mod response {
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
        use crate::peer::id::PeerId;
        use future_form::Sendable;
        use subduction_crypto::signer::memory::MemorySigner;

        fn test_signer(seed: u8) -> MemorySigner {
            MemorySigner::from_bytes(&[seed; 32])
        }

        #[tokio::test]
        async fn full_handshake_round_trip() {
            let client_signer = test_signer(1);
            let server_signer = test_signer(2);

            let now = TimestampSeconds::new(1000);
            let audience = Audience::discover(b"https://example.com");
            let nonce = Nonce::from_u128(12345);

            // Client creates challenge
            let signed_challenge =
                create_challenge::<Sendable, _>(&client_signer, audience, now, nonce).await;

            // Server verifies challenge
            let verified_challenge =
                verify_challenge(&signed_challenge, &audience, now, MAX_PLAUSIBLE_DRIFT)
                    .expect("challenge should verify");

            assert_eq!(
                verified_challenge.client_id,
                PeerId::from(client_signer.verifying_key())
            );
            assert_eq!(verified_challenge.challenge.nonce, nonce);

            // Server creates response
            let signed_response =
                create_response::<Sendable, _>(&server_signer, &verified_challenge.challenge, now)
                    .await;

            // Client verifies response
            let original_challenge = Challenge::new(audience, now, nonce);
            let verified_response = verify_response(&signed_response, &original_challenge)
                .expect("response should verify");

            assert_eq!(
                verified_response.server_id,
                PeerId::from(server_signer.verifying_key())
            );
        }

        #[tokio::test]
        async fn wrong_audience_rejected() {
            let client_signer = test_signer(1);

            let now = TimestampSeconds::new(1000);
            let client_audience = Audience::discover(b"https://example.com");
            let server_audience = Audience::discover(b"https://other.com");
            let nonce = Nonce::from_u128(12345);

            let signed_challenge =
                create_challenge::<Sendable, _>(&client_signer, client_audience, now, nonce).await;

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
            let nonce = Nonce::from_u128(12345);

            let signed_challenge =
                create_challenge::<Sendable, _>(&client_signer, audience, client_now, nonce).await;

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
            let nonce1 = Nonce::from_u128(11111);
            let nonce2 = Nonce::from_u128(22222);

            // Client creates challenge with nonce1
            let challenge1 = Challenge::new(audience, now, nonce1);

            // Server creates response for challenge1
            let signed_response =
                create_response::<Sendable, _>(&server_signer, &challenge1, now).await;

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
                let nonce = Nonce::from_u128(*value);
                assert_eq!(nonce.as_u128(), *value);
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
        fn prop_drift_correction_bounded() {
            bolero::check!()
                .with_type::<(TimestampSeconds, TimestampSeconds)>()
                .for_each(|(server_ts, client_ts)| {
                    let mut dc = DriftCorrection::new();
                    if dc.adjust(*server_ts, *client_ts) {
                        #[allow(clippy::cast_possible_truncation)]
                        let max_drift = MAX_PLAUSIBLE_DRIFT.as_secs() as i32;
                        assert!(dc.offset_secs().abs() <= max_drift);
                    }
                });
        }

        #[test]
        fn prop_challenge_codec_roundtrip() {
            bolero::check!()
                .with_type::<Challenge>()
                .for_each(|challenge| {
                    let mut buf = Vec::new();
                    challenge.encode_fields(&mut buf);
                    let (decoded, consumed) =
                        Challenge::try_decode_fields(&buf).expect("decode should succeed");
                    assert_eq!(challenge, &decoded);
                    assert_eq!(consumed, buf.len());
                });
        }

        #[test]
        fn prop_response_codec_roundtrip() {
            bolero::check!()
                .with_type::<Response>()
                .for_each(|response| {
                    let mut buf = Vec::new();
                    response.encode_fields(&mut buf);
                    let (decoded, consumed) =
                        Response::try_decode_fields(&buf).expect("decode should succeed");
                    assert_eq!(response, &decoded);
                    assert_eq!(consumed, buf.len());
                });
        }

        #[test]
        fn prop_discovery_id_bytes_roundtrip() {
            bolero::check!().with_type::<[u8; 32]>().for_each(|bytes| {
                let id = DiscoveryId::from_raw(*bytes);
                assert_eq!(*id.as_bytes(), *bytes);
            });
        }

        #[test]
        fn prop_different_nonces_different_digests() {
            bolero::check!()
                .with_type::<(Audience, TimestampSeconds, Nonce, Nonce)>()
                .for_each(|(audience, timestamp, nonce1, nonce2)| {
                    if nonce1 != nonce2 {
                        let c1 = Challenge::new(*audience, *timestamp, *nonce1);
                        let c2 = Challenge::new(*audience, *timestamp, *nonce2);
                        assert_ne!(
                            Digest::<Challenge>::hash(&c1),
                            Digest::<Challenge>::hash(&c2)
                        );
                    }
                });
        }

        #[test]
        fn prop_is_fresh_within_drift() {
            bolero::check!()
                .with_type::<(Audience, Nonce, u32, u32)>()
                .for_each(|(audience, nonce, base, delta)| {
                    let base = u64::from(*base);
                    let delta = u64::from(*delta);
                    let drift = Duration::from_secs(delta);
                    let ts = TimestampSeconds::new(base);
                    let now = TimestampSeconds::new(base.saturating_add(delta));
                    let challenge = Challenge::new(*audience, ts, *nonce);
                    assert!(challenge.is_fresh(now, drift));
                });
        }

        #[test]
        fn prop_is_not_fresh_outside_drift() {
            bolero::check!()
                .with_type::<(Audience, Nonce, u32, u16)>()
                .for_each(|(audience, nonce, base, drift_secs)| {
                    let base = u64::from(*base);
                    let drift_secs = u64::from(*drift_secs);
                    let drift = Duration::from_secs(drift_secs);
                    let ts = TimestampSeconds::new(base);
                    // Place `now` strictly outside the drift window
                    let now =
                        TimestampSeconds::new(base.saturating_add(drift_secs).saturating_add(1));
                    let challenge = Challenge::new(*audience, ts, *nonce);
                    if now.abs_diff(ts) > drift {
                        assert!(!challenge.is_fresh(now, drift));
                    }
                });
        }

        #[test]
        fn prop_drift_correction_apply_correct() {
            bolero::check!()
                .with_type::<(TimestampSeconds, TimestampSeconds)>()
                .for_each(|(server_ts, client_ts)| {
                    let mut dc = DriftCorrection::new();
                    if dc.adjust(*server_ts, *client_ts) {
                        let corrected = dc.apply(*client_ts);
                        let expected = client_ts.add_signed(i64::from(dc.offset_secs()));
                        assert_eq!(corrected, expected);
                    }
                });
        }
    }

    mod codec {
        use super::*;

        fn sample_challenge() -> Challenge {
            Challenge {
                audience: Audience::Known(PeerId::new([0x42; 32])),
                timestamp: TimestampSeconds::new(1_234_567_890),
                nonce: Nonce::from_u128(0xDEAD_BEEF_CAFE_BABE_1234_5678_9ABC_DEF0),
            }
        }

        fn sample_response() -> Response {
            Response {
                challenge_digest: Digest::force_from_bytes([0xAB; 32]),
                server_timestamp: TimestampSeconds::new(1_234_567_890),
            }
        }

        type TestResult = Result<(), Box<dyn std::error::Error>>;

        #[test]
        fn challenge_known_audience_tag() -> TestResult {
            let challenge = Challenge {
                audience: Audience::Known(PeerId::new([0x00; 32])),
                timestamp: TimestampSeconds::new(0),
                nonce: Nonce::from_u128(0),
            };

            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);

            assert_eq!(*buf.first().ok_or("empty buffer")?, 0x00); // Known tag
            Ok(())
        }

        #[test]
        fn challenge_discover_audience_tag() -> TestResult {
            let challenge = Challenge {
                audience: Audience::Discover(DiscoveryId::from_raw([0xFF; 32])),
                timestamp: TimestampSeconds::new(0),
                nonce: Nonce::from_u128(0),
            };

            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);

            assert_eq!(*buf.first().ok_or("empty buffer")?, 0x01); // Discover tag
            Ok(())
        }

        #[test]
        fn challenge_invalid_audience_tag_rejected() -> TestResult {
            let mut buf = vec![0u8; CHALLENGE_FIELDS_SIZE];
            *buf.first_mut().ok_or("empty buffer")? = 0x02; // Invalid tag

            let result = Challenge::try_decode_fields(&buf);
            assert!(matches!(
                result,
                Err(DecodeError::InvalidEnumTag(InvalidEnumTag {
                    tag: 0x02,
                    type_name: "Audience"
                }))
            ));
            Ok(())
        }

        #[test]
        fn challenge_buffer_too_short() {
            let buf = vec![0u8; CHALLENGE_FIELDS_SIZE - 1];
            let result = Challenge::try_decode_fields(&buf);
            assert!(matches!(result, Err(DecodeError::MessageTooShort { .. })));
        }

        #[test]
        fn challenge_timestamp_big_endian() -> TestResult {
            let challenge = Challenge {
                audience: Audience::Known(PeerId::new([0x00; 32])),
                timestamp: TimestampSeconds::new(0x0102_0304_0506_0708),
                nonce: Nonce::from_u128(0),
            };

            let mut buf = Vec::new();
            challenge.encode_fields(&mut buf);

            // Timestamp is at offset 33 (1 + 32)
            let timestamp_bytes = buf.get(33..41).ok_or("buffer too short")?;
            assert_eq!(
                timestamp_bytes,
                &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
            );
            Ok(())
        }

        #[test]
        fn challenge_fields_size_constant() {
            let challenge = sample_challenge();
            assert_eq!(challenge.fields_size(), CHALLENGE_FIELDS_SIZE);
        }

        #[test]
        fn challenge_signed_size_correct() {
            let challenge = sample_challenge();
            assert_eq!(challenge.signed_size(), CHALLENGE_MIN_SIZE);
        }

        #[test]
        fn response_challenge_digest_preserved() -> TestResult {
            let digest_bytes = [0x42; 32];
            let response = Response {
                challenge_digest: Digest::force_from_bytes(digest_bytes),
                server_timestamp: TimestampSeconds::new(0),
            };

            let mut buf = Vec::new();
            response.encode_fields(&mut buf);

            // Digest is at offset 0
            assert_eq!(buf.get(0..32).ok_or("buffer too short")?, &digest_bytes);
            Ok(())
        }

        #[test]
        fn response_timestamp_big_endian() -> TestResult {
            let response = Response {
                challenge_digest: Digest::force_from_bytes([0x00; 32]),
                server_timestamp: TimestampSeconds::new(0x0102_0304_0506_0708),
            };

            let mut buf = Vec::new();
            response.encode_fields(&mut buf);

            // Timestamp is at offset 32
            let timestamp_bytes = buf.get(32..40).ok_or("buffer too short")?;
            assert_eq!(
                timestamp_bytes,
                &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
            );
            Ok(())
        }

        #[test]
        fn response_buffer_too_short() {
            let buf = vec![0u8; RESPONSE_FIELDS_SIZE - 1];
            let result = Response::try_decode_fields(&buf);
            assert!(matches!(result, Err(DecodeError::MessageTooShort { .. })));
        }

        #[test]
        fn response_fields_size_constant() {
            let response = sample_response();
            assert_eq!(response.fields_size(), RESPONSE_FIELDS_SIZE);
        }

        #[test]
        fn response_signed_size_correct() {
            let response = sample_response();
            assert_eq!(response.signed_size(), RESPONSE_MIN_SIZE);
        }

        #[test]
        fn handshake_message_wrong_schema_rejected() {
            let mut bytes = vec![0u8; 20];
            bytes
                .get_mut(..4)
                .expect("test buffer is 20 bytes")
                .copy_from_slice(b"BAD\x00");
            let result = HandshakeMessage::try_decode(&bytes);
            assert!(
                matches!(result, Err(DecodeError::InvalidSchema(_))),
                "expected InvalidSchema, got {result:?}"
            );
        }

        #[test]
        fn handshake_message_too_short_rejected() {
            let bytes = vec![0u8; 3]; // less than 5 bytes (schema + tag)
            let result = HandshakeMessage::try_decode(&bytes);
            assert!(
                matches!(result, Err(DecodeError::MessageTooShort { .. })),
                "expected MessageTooShort, got {result:?}"
            );
        }

        #[test]
        fn handshake_message_unknown_schema_rejected() {
            let mut bytes = vec![0u8; 20];
            bytes
                .get_mut(..4)
                .expect("test buffer is 20 bytes")
                .copy_from_slice(b"SUZ\x00"); // valid prefix, wrong type
            let result = HandshakeMessage::try_decode(&bytes);
            assert!(
                matches!(result, Err(DecodeError::InvalidSchema(_))),
                "expected InvalidSchema, got {result:?}"
            );
        }

        #[test]
        fn rejection_encode_has_envelope_and_tag() {
            let rejection =
                Rejection::new(RejectionReason::ClockDrift, TimestampSeconds::new(1000));
            let msg = HandshakeMessage::Rejection(rejection);
            let bytes = msg.encode();
            // SUH\0 envelope
            assert_eq!(bytes.get(..4), Some(HandshakeMessage::SCHEMA.as_slice()));
            assert_eq!(bytes.get(..4), Some(b"SUH\x00".as_slice()));
            // Rejection variant tag
            assert_eq!(bytes.get(4).copied(), Some(0x02));
            // Total: 4 (schema) + 1 (tag) + 1 (reason) + 8 (timestamp) = 14
            assert_eq!(bytes.len(), 14);
        }

        #[test]
        fn handshake_message_invalid_tag_rejected() {
            let mut bytes = vec![0u8; 20];
            bytes
                .get_mut(..4)
                .expect("test buffer is 20 bytes")
                .copy_from_slice(b"SUH\x00");
            *bytes.get_mut(4).expect("test buffer is 20 bytes") = 0xFF;
            let result = HandshakeMessage::try_decode(&bytes);
            assert!(
                matches!(result, Err(DecodeError::InvalidEnumTag(_))),
                "expected InvalidEnumTag, got {result:?}"
            );
        }

        #[test]
        fn rejection_round_trips_through_envelope() -> TestResult {
            let rejection =
                Rejection::new(RejectionReason::InvalidSignature, TimestampSeconds::new(42));
            let msg = HandshakeMessage::Rejection(rejection);
            let bytes = msg.encode();
            let decoded = HandshakeMessage::try_decode(&bytes)?;
            let HandshakeMessage::Rejection(r) = decoded else {
                unreachable!("expected Rejection variant");
            };
            assert_eq!(r.reason, RejectionReason::InvalidSignature);
            assert_eq!(r.server_timestamp, TimestampSeconds::new(42));
            Ok(())
        }

        #[test]
        fn handshake_message_raw_cbor_rejected() {
            // Simulated raw automerge/CBOR bytes: starts with 0x85 (CBOR array)
            let raw_cbor = vec![0x85, 0x6F, 0x4A, 0x83, 0x00, 0x01, 0x02, 0x03];
            let result = HandshakeMessage::try_decode(&raw_cbor);
            assert!(
                matches!(result, Err(DecodeError::InvalidSchema(_))),
                "expected InvalidSchema, got {result:?}"
            );
        }
    }

    mod simultaneous_open {
        use super::*;
        use future_form::Sendable;
        use futures::FutureExt;
        use subduction_crypto::signer::memory::MemorySigner;
        use testresult::TestResult;

        /// A paired in-memory handshake transport for testing.
        ///
        /// Two `ChannelHandshake` instances share crossed channels:
        /// A's send goes to B's recv and vice versa.
        #[derive(Debug)]
        struct ChannelHandshake {
            tx: async_channel::Sender<Vec<u8>>,
            rx: async_channel::Receiver<Vec<u8>>,
        }

        impl ChannelHandshake {
            fn pair() -> (Self, Self) {
                let (tx_a, rx_a) = async_channel::bounded(16);
                let (tx_b, rx_b) = async_channel::bounded(16);
                (Self { tx: tx_a, rx: rx_b }, Self { tx: tx_b, rx: rx_a })
            }
        }

        #[derive(Debug, thiserror::Error)]
        enum ChannelError {
            #[error("send: {0}")]
            Send(#[from] async_channel::SendError<Vec<u8>>),
            #[error("recv: {0}")]
            Recv(#[from] async_channel::RecvError),
        }

        impl Handshake<Sendable> for ChannelHandshake {
            type Error = ChannelError;

            fn send(
                &mut self,
                bytes: Vec<u8>,
            ) -> futures::future::BoxFuture<'_, Result<(), Self::Error>> {
                let tx = self.tx.clone();
                async move { Ok(tx.send(bytes).await?) }.boxed()
            }

            fn recv(&mut self) -> futures::future::BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
                let rx = self.rx.clone();
                async move { Ok(rx.recv().await?) }.boxed()
            }
        }

        fn test_signer(seed: u8) -> MemorySigner {
            MemorySigner::from_bytes(&[seed; 32])
        }

        /// Both sides call `initiate` simultaneously — one wins, one loses,
        /// both complete successfully with the correct peer IDs.
        #[tokio::test]
        async fn simultaneous_initiate_completes() -> TestResult {
            let (transport_a, transport_b) = ChannelHandshake::pair();
            let signer_a = test_signer(1);
            let signer_b = test_signer(2);
            let peer_id_a = PeerId::from(signer_a.verifying_key());
            let peer_id_b = PeerId::from(signer_b.verifying_key());

            let now = TimestampSeconds::new(1000);

            let a_handle = tokio::spawn(async move {
                initiate::<Sendable, _, _, _, _>(
                    transport_a,
                    |_hs, peer_id| (peer_id, ()),
                    &signer_a,
                    Audience::known(peer_id_b),
                    now,
                    Nonce::random(),
                )
                .await
            });

            let b_handle = tokio::spawn(async move {
                initiate::<Sendable, _, _, _, _>(
                    transport_b,
                    |_hs, peer_id| (peer_id, ()),
                    &signer_b,
                    Audience::known(peer_id_a),
                    now,
                    Nonce::random(),
                )
                .await
            });

            let (a_result, b_result) = tokio::join!(a_handle, b_handle);
            let (a_auth, ()) = a_result??;
            let (b_auth, ()) = b_result??;

            assert_eq!(a_auth.peer_id(), peer_id_b, "A should authenticate B");
            assert_eq!(b_auth.peer_id(), peer_id_a, "B should authenticate A");

            Ok(())
        }

        /// Reflection attack: Mallory reflects Alice's challenge back.
        /// Alice should detect the reflected challenge and reject.
        #[tokio::test]
        async fn reflected_challenge_is_rejected() -> TestResult {
            let (mut alice_hs, mut mallory_hs) = ChannelHandshake::pair();
            let signer_alice = test_signer(1);

            let now = TimestampSeconds::new(1000);
            let challenge = Challenge::new(
                Audience::known(PeerId::new([0xAA; 32])),
                now,
                Nonce::random(),
            );
            let signed_challenge = Signed::seal::<Sendable, _>(&signer_alice, challenge)
                .await
                .into_signed();

            // Alice sends her challenge
            let msg = HandshakeMessage::SignedChallenge(signed_challenge.clone());
            alice_hs.send(msg.encode()).await?;

            // Mallory receives and reflects it back
            let reflected_bytes = mallory_hs.recv().await?;
            mallory_hs.send(reflected_bytes).await?;

            // Alice tries to complete the handshake
            let first_msg = recv_handshake_message::<Sendable, _>(&mut alice_hs).await?;

            // Simulate what initiate does: check if it's a challenge
            match first_msg {
                HandshakeMessage::SignedChallenge(their_challenge) => {
                    // The reflected challenge has identical bytes
                    assert_eq!(
                        signed_challenge.as_bytes(),
                        their_challenge.as_bytes(),
                        "reflected challenge should have identical bytes"
                    );

                    // The issuer is our own key
                    assert_eq!(
                        their_challenge.issuer(),
                        signer_alice.verifying_key(),
                        "reflected challenge issuer should be our own key"
                    );
                }
                HandshakeMessage::SignedResponse(_) | HandshakeMessage::Rejection(_) => {
                    return Err("expected SignedChallenge".into());
                }
            }

            // Now test that `initiate` itself rejects this
            let (alice_hs2, mut mallory_hs2) = ChannelHandshake::pair();

            // Spawn Alice's initiate
            let signer_clone = test_signer(1);
            let alice_handle = tokio::spawn(async move {
                initiate::<Sendable, _, _, _, _>(
                    alice_hs2,
                    |_hs, peer_id| (peer_id, ()),
                    &signer_clone,
                    Audience::known(PeerId::new([0xAA; 32])),
                    now,
                    Nonce::random(),
                )
                .await
            });

            // Mallory reflects Alice's challenge
            let alice_challenge = mallory_hs2.recv().await?;
            mallory_hs2.send(alice_challenge).await?;

            let result = alice_handle.await?;
            assert!(
                matches!(
                    result,
                    Err(AuthenticateError::ReflectedChallenge | AuthenticateError::ReflectionAttack)
                ),
                "expected reflection error, got {result:?}"
            );

            Ok(())
        }

        /// Simultaneous open with discovery audience should succeed.
        #[tokio::test]
        async fn simultaneous_initiate_with_discovery() -> TestResult {
            let (transport_a, transport_b) = ChannelHandshake::pair();
            let signer_a = test_signer(3);
            let signer_b = test_signer(4);
            let peer_id_a = PeerId::from(signer_a.verifying_key());
            let peer_id_b = PeerId::from(signer_b.verifying_key());

            let now = TimestampSeconds::new(1000);
            let service = Audience::discover(b"my-service");

            let service_a = service;
            let service_b = service;

            let a_handle = tokio::spawn(async move {
                initiate::<Sendable, _, _, _, _>(
                    transport_a,
                    |_hs, peer_id| (peer_id, ()),
                    &signer_a,
                    service_a,
                    now,
                    Nonce::random(),
                )
                .await
            });

            let b_handle = tokio::spawn(async move {
                initiate::<Sendable, _, _, _, _>(
                    transport_b,
                    |_hs, peer_id| (peer_id, ()),
                    &signer_b,
                    service_b,
                    now,
                    Nonce::random(),
                )
                .await
            });

            let (a_result, b_result) = tokio::join!(a_handle, b_handle);
            let (a_auth, ()) = a_result??;
            let (b_auth, ()) = b_result??;

            assert_eq!(a_auth.peer_id(), peer_id_b);
            assert_eq!(b_auth.peer_id(), peer_id_a);

            Ok(())
        }
    }
}
