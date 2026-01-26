//! Handshake protocol implementation for WebSocket connections.
//!
//! This module provides the transport-layer integration for the handshake
//! protocol defined in [`subduction_core::connection::handshake`]. It handles
//! the actual sending and receiving of signed challenges and responses over
//! WebSocket connections.
//!
//! # Usage
//!
//! The handshake occurs after the WebSocket protocol upgrade but before
//! creating the `WebSocket` wrapper:
//!
//! 1. TCP connection established
//! 2. WebSocket upgrade handshake (tungstenite)
//! 3. Subduction handshake (this module)
//!    - Server: receive Challenge, send Response
//!    - Client: send Challenge, receive Response
//! 4. Create WebSocket wrapper with verified `PeerId`
//! 5. Register connection with Subduction

use core::time::Duration;

use async_tungstenite::WebSocketStream;
use future_form::FutureForm;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use subduction_core::{
    connection::{
        handshake::{self, Audience, Challenge, HandshakeError, Nonce, Rejection, RejectionReason},
        nonce_cache::NonceCache,
    },
    crypto::{signed::Signed, signer::Signer},
    peer::id::PeerId,
    timestamp::TimestampSeconds,
};
use thiserror::Error;

/// Errors that can occur during WebSocket handshake.
#[derive(Debug, Error)]
pub enum WebSocketHandshakeError {
    /// WebSocket transport error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Received an unexpected message type (not binary).
    #[error("expected binary message, got: {0}")]
    UnexpectedMessageType(&'static str),

    /// Connection closed before handshake completed.
    #[error("connection closed during handshake")]
    ConnectionClosed,

    /// CBOR decoding error.
    #[error("CBOR decode error: {0}")]
    DecodeError(#[from] minicbor::decode::Error),

    /// Handshake protocol error.
    #[error("handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    /// Server rejected the handshake.
    #[error("handshake rejected: {reason:?}")]
    Rejected {
        /// The rejection reason.
        reason: RejectionReason,

        /// The server's timestamp (for drift correction).
        server_timestamp: TimestampSeconds,
    },
}

/// Result of a successful server handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerHandshakeResult {
    /// The verified client peer ID.
    pub client_id: PeerId,

    /// The verified challenge (for logging/debugging).
    pub challenge: Challenge,
}

/// Result of a successful client handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientHandshakeResult {
    /// The verified server peer ID.
    pub server_id: PeerId,

    /// The server's timestamp (for drift correction).
    pub server_timestamp: TimestampSeconds,
}

/// Handshake message types for WebSocket transport.
///
/// The handshake uses a simple protocol where:
/// - Client sends a `SignedChallenge`
/// - Server responds with either `SignedResponse` or `Rejection`
#[derive(Debug, minicbor::Encode, minicbor::Decode)]
enum HandshakeMessage {
    /// A signed challenge from the client.
    #[n(0)]
    SignedChallenge(#[n(0)] Signed<Challenge>),

    /// A signed response from the server.
    #[n(1)]
    SignedResponse(#[n(0)] Signed<handshake::Response>),

    /// An unsigned rejection from the server.
    #[n(2)]
    Rejection(#[n(0)] Rejection),
}

/// Perform the server side of the handshake.
///
/// Receives a signed challenge from the client, verifies it, and sends
/// a signed response. Returns the verified client peer ID on success.
///
/// # Arguments
///
/// * `ws` - The WebSocket stream (after protocol upgrade)
/// * `signer` - The server's signer for creating the response
/// * `server_peer_id` - The server's peer ID (always accepted as `Audience::Known`)
/// * `discovery_audience` - Optional discovery audience (also accepted if provided)
/// * `nonce_tracker` - Nonce tracker for replay protection
/// * `now` - The current timestamp
/// * `max_drift` - Maximum acceptable clock drift
///
/// # Errors
///
/// Returns an error if:
/// - The WebSocket message could not be received/sent
/// - The challenge signature is invalid
/// - The audience doesn't match
/// - The timestamp is outside the acceptable drift window
/// - The nonce has already been used (replay attack)
///
/// # Panics
///
/// Panics if CBOR encoding fails (should never happen for well-formed types).
#[allow(clippy::expect_used)]
pub async fn server_handshake<T, S, K>(
    ws: &mut WebSocketStream<T>,
    signer: &S,
    server_peer_id: PeerId,
    discovery_audience: Option<Audience>,
    nonce_cache: &NonceCache,
    now: TimestampSeconds,
    max_drift: Duration,
) -> Result<ServerHandshakeResult, WebSocketHandshakeError>
where
    T: AsyncRead + AsyncWrite + Unpin,
    S: Signer<K>,
    K: FutureForm,
{
    // Receive the challenge
    let challenge_msg = ws
        .next()
        .await
        .ok_or(WebSocketHandshakeError::ConnectionClosed)??;

    let challenge_bytes = match challenge_msg {
        tungstenite::Message::Binary(bytes) => bytes,
        tungstenite::Message::Text(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("text"));
        }
        tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("ping/pong"));
        }
        tungstenite::Message::Close(_) => return Err(WebSocketHandshakeError::ConnectionClosed),
        tungstenite::Message::Frame(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("frame"));
        }
    };

    let handshake_msg: HandshakeMessage = minicbor::decode(&challenge_bytes)?;

    let HandshakeMessage::SignedChallenge(signed_challenge) = handshake_msg else {
        return Err(WebSocketHandshakeError::UnexpectedMessageType(
            "expected SignedChallenge",
        ));
    };

    // Verify the challenge - try Known(peer_id) first, then discovery audience
    let known_audience = Audience::known(server_peer_id);
    let verified =
        match handshake::verify_challenge(&signed_challenge, &known_audience, now, max_drift) {
            Ok(v) => v,
            Err(HandshakeError::ChallengeValidation(
                handshake::ChallengeValidationError::InvalidAudience,
            )) if discovery_audience.is_some() => {
                // Try discovery audience as fallback
                match handshake::verify_challenge(
                    &signed_challenge,
                    discovery_audience.as_ref().expect("checked is_some"),
                    now,
                    max_drift,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        let reason = match &e {
                            HandshakeError::ChallengeValidation(cv) => cv.to_rejection_reason(),
                            HandshakeError::InvalidSignature
                            | HandshakeError::ResponseValidation(_) => {
                                RejectionReason::InvalidSignature
                            }
                        };
                        let rejection = Rejection::new(reason, now);
                        let msg = HandshakeMessage::Rejection(rejection);
                        let bytes =
                            minicbor::to_vec(&msg).expect("rejection encoding should not fail");
                        ws.send(tungstenite::Message::Binary(bytes.into())).await?;
                        return Err(e.into());
                    }
                }
            }
            Err(e) => {
                // Send rejection
                let reason = match &e {
                    HandshakeError::InvalidSignature => RejectionReason::InvalidSignature,
                    HandshakeError::ChallengeValidation(cv) => cv.to_rejection_reason(),
                    HandshakeError::ResponseValidation(_) => {
                        // This shouldn't happen on server side, but handle it
                        RejectionReason::InvalidSignature
                    }
                };
                let rejection = Rejection::new(reason, now);
                let msg = HandshakeMessage::Rejection(rejection);
                let bytes = minicbor::to_vec(&msg).expect("rejection encoding should not fail");
                ws.send(tungstenite::Message::Binary(bytes.into())).await?;
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
        let bytes = minicbor::to_vec(&msg).expect("rejection encoding should not fail");
        ws.send(tungstenite::Message::Binary(bytes.into())).await?;
        return Err(WebSocketHandshakeError::Handshake(
            HandshakeError::ChallengeValidation(handshake::ChallengeValidationError::ReplayedNonce),
        ));
    }

    // Create and send response
    let signed_response = handshake::create_response(signer, &verified.challenge, now).await;
    let response_msg = HandshakeMessage::SignedResponse(signed_response);
    let response_bytes =
        minicbor::to_vec(&response_msg).expect("response encoding should not fail");
    ws.send(tungstenite::Message::Binary(response_bytes.into()))
        .await?;

    Ok(ServerHandshakeResult {
        client_id: verified.client_id,
        challenge: verified.challenge,
    })
}

/// Perform the client side of the handshake.
///
/// Sends a signed challenge to the server and waits for a signed response.
/// Returns the verified server peer ID on success.
///
/// # Arguments
///
/// * `ws` - The WebSocket stream (after protocol upgrade)
/// * `signer` - The client's signer for creating the challenge
/// * `audience` - The intended audience (server identity or discovery hash)
/// * `now` - The current timestamp
/// * `nonce` - A random nonce for replay protection
///
/// # Errors
///
/// Returns an error if:
/// - The WebSocket message could not be sent/received
/// - The server rejected the handshake
/// - The response signature is invalid
/// - The response doesn't match the challenge
///
/// # Panics
///
/// Panics if CBOR encoding fails (should never happen for well-formed types).
#[allow(clippy::expect_used)]
pub async fn client_handshake<T, S, K>(
    ws: &mut WebSocketStream<T>,
    signer: &S,
    audience: Audience,
    now: TimestampSeconds,
    nonce: Nonce,
) -> Result<ClientHandshakeResult, WebSocketHandshakeError>
where
    T: AsyncRead + AsyncWrite + Unpin,
    S: Signer<K>,
    K: FutureForm,
{
    // Create and send challenge
    let challenge = Challenge::new(audience, now, nonce);
    let signed_challenge = Signed::seal(signer, challenge).await.into_signed();
    let challenge_msg = HandshakeMessage::SignedChallenge(signed_challenge);
    let challenge_bytes =
        minicbor::to_vec(&challenge_msg).expect("challenge encoding should not fail");
    ws.send(tungstenite::Message::Binary(challenge_bytes.into()))
        .await?;

    // Receive response
    let response_msg = ws
        .next()
        .await
        .ok_or(WebSocketHandshakeError::ConnectionClosed)??;

    let response_bytes = match response_msg {
        tungstenite::Message::Binary(bytes) => bytes,
        tungstenite::Message::Text(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("text"));
        }
        tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("ping/pong"));
        }
        tungstenite::Message::Close(_) => return Err(WebSocketHandshakeError::ConnectionClosed),
        tungstenite::Message::Frame(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("frame"));
        }
    };

    let handshake_msg: HandshakeMessage = minicbor::decode(&response_bytes)?;

    match handshake_msg {
        HandshakeMessage::SignedResponse(signed_response) => {
            // Verify the response
            let verified = handshake::verify_response(&signed_response, &challenge)?;

            Ok(ClientHandshakeResult {
                server_id: verified.server_id,
                server_timestamp: verified.response.server_timestamp,
            })
        }
        HandshakeMessage::Rejection(rejection) => Err(WebSocketHandshakeError::Rejected {
            reason: rejection.reason,
            server_timestamp: rejection.server_timestamp,
        }),
        HandshakeMessage::SignedChallenge(_) => Err(
            WebSocketHandshakeError::UnexpectedMessageType("expected SignedResponse or Rejection"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use future_form::Sendable;
    use subduction_core::crypto::signer::MemorySigner;

    fn test_signer(seed: u8) -> MemorySigner {
        MemorySigner::from_bytes(&[seed; 32])
    }

    mod handshake_message {
        use super::*;

        #[tokio::test]
        async fn signed_challenge_roundtrips() {
            let test_signer = test_signer(1);
            let challenge = Challenge::new(
                Audience::discover(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            let signed_challenge = Signed::seal::<Sendable, _>(&test_signer, challenge)
                .await
                .into_signed();
            let msg = HandshakeMessage::SignedChallenge(signed_challenge.clone());

            let bytes = minicbor::to_vec(&msg)
                .unwrap_or_else(|e| unreachable!("encoding should succeed: {e}"));
            let decoded: HandshakeMessage = minicbor::decode(&bytes)
                .unwrap_or_else(|e| unreachable!("decoding should succeed: {e}"));

            let HandshakeMessage::SignedChallenge(decoded_signed) = decoded else {
                unreachable!(
                    "expected SignedChallenge, got {:?}",
                    core::mem::discriminant(&decoded)
                );
            };
            assert_eq!(decoded_signed.issuer(), signed_challenge.issuer());
        }

        #[test]
        fn rejection_roundtrips() {
            let rejection =
                Rejection::new(RejectionReason::ClockDrift, TimestampSeconds::new(1000));
            let msg = HandshakeMessage::Rejection(rejection);

            let bytes = minicbor::to_vec(&msg)
                .unwrap_or_else(|e| unreachable!("encoding should succeed: {e}"));
            let decoded: HandshakeMessage = minicbor::decode(&bytes)
                .unwrap_or_else(|e| unreachable!("decoding should succeed: {e}"));

            let HandshakeMessage::Rejection(decoded_rejection) = decoded else {
                unreachable!(
                    "expected Rejection, got {:?}",
                    core::mem::discriminant(&decoded)
                );
            };
            assert_eq!(decoded_rejection.reason, rejection.reason);
            assert_eq!(
                decoded_rejection.server_timestamp,
                rejection.server_timestamp
            );
        }
    }
}
