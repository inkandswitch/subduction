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
//! 4. Create WebSocket wrapper with verified PeerId
//! 5. Register connection with Subduction

use core::time::Duration;

use async_tungstenite::WebSocketStream;
use futures_util::{AsyncRead, AsyncWrite, StreamExt};
use subduction_core::{
    connection::handshake::{
        self, Audience, Challenge, HandshakeError, Nonce, Rejection, RejectionReason,
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
/// * `expected_audience` - The audience the server expects to see
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
pub async fn server_handshake<T: AsyncRead + AsyncWrite + Unpin>(
    ws: &mut WebSocketStream<T>,
    signer: &impl Signer,
    expected_audience: &Audience,
    now: TimestampSeconds,
    max_drift: Duration,
) -> Result<ServerHandshakeResult, WebSocketHandshakeError> {
    // Receive the challenge
    let challenge_msg = ws
        .next()
        .await
        .ok_or(WebSocketHandshakeError::ConnectionClosed)??;

    let challenge_bytes = match challenge_msg {
        tungstenite::Message::Binary(bytes) => bytes,
        tungstenite::Message::Text(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("text"))
        }
        tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("ping/pong"))
        }
        tungstenite::Message::Close(_) => {
            return Err(WebSocketHandshakeError::ConnectionClosed)
        }
        tungstenite::Message::Frame(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("frame"))
        }
    };

    let handshake_msg: HandshakeMessage = minicbor::decode(&challenge_bytes)?;

    let signed_challenge = match handshake_msg {
        HandshakeMessage::SignedChallenge(c) => c,
        _ => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType(
                "expected SignedChallenge",
            ))
        }
    };

    // Verify the challenge
    let verified = match handshake::verify_challenge(&signed_challenge, expected_audience, now, max_drift) {
        Ok(v) => v,
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

    // Create and send response
    let signed_response = handshake::create_response(signer, &verified.challenge, now);
    let response_msg = HandshakeMessage::SignedResponse(signed_response);
    let response_bytes = minicbor::to_vec(&response_msg).expect("response encoding should not fail");
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
pub async fn client_handshake<T: AsyncRead + AsyncWrite + Unpin>(
    ws: &mut WebSocketStream<T>,
    signer: &impl Signer,
    audience: Audience,
    now: TimestampSeconds,
    nonce: Nonce,
) -> Result<ClientHandshakeResult, WebSocketHandshakeError> {
    // Create and send challenge
    let challenge = Challenge::new(audience, now, nonce);
    let signed_challenge = Signed::sign(signer, challenge);
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
            return Err(WebSocketHandshakeError::UnexpectedMessageType("text"))
        }
        tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("ping/pong"))
        }
        tungstenite::Message::Close(_) => {
            return Err(WebSocketHandshakeError::ConnectionClosed)
        }
        tungstenite::Message::Frame(_) => {
            return Err(WebSocketHandshakeError::UnexpectedMessageType("frame"))
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
        HandshakeMessage::SignedChallenge(_) => Err(WebSocketHandshakeError::UnexpectedMessageType(
            "expected SignedResponse or Rejection",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use subduction_core::crypto::signer::LocalSigner;

    fn test_signer(seed: u8) -> LocalSigner {
        LocalSigner::from_bytes(&[seed; 32])
    }

    mod handshake_message {
        use super::*;

        #[test]
        fn signed_challenge_roundtrips() {
            let signer = test_signer(1);
            let challenge = Challenge::new(
                Audience::discovery(b"test"),
                TimestampSeconds::new(1000),
                Nonce::new(42),
            );
            let signed = Signed::sign(&signer, challenge);
            let msg = HandshakeMessage::SignedChallenge(signed.clone());

            let bytes = minicbor::to_vec(&msg).expect("encoding should succeed");
            let decoded: HandshakeMessage = minicbor::decode(&bytes).expect("decoding should succeed");

            match decoded {
                HandshakeMessage::SignedChallenge(decoded_signed) => {
                    assert_eq!(decoded_signed.issuer(), signed.issuer());
                }
                _ => panic!("expected SignedChallenge"),
            }
        }

        #[test]
        fn rejection_roundtrips() {
            let rejection = Rejection::new(RejectionReason::ClockDrift, TimestampSeconds::new(1000));
            let msg = HandshakeMessage::Rejection(rejection);

            let bytes = minicbor::to_vec(&msg).expect("encoding should succeed");
            let decoded: HandshakeMessage = minicbor::decode(&bytes).expect("decoding should succeed");

            match decoded {
                HandshakeMessage::Rejection(decoded_rejection) => {
                    assert_eq!(decoded_rejection.reason, rejection.reason);
                    assert_eq!(decoded_rejection.server_timestamp, rejection.server_timestamp);
                }
                _ => panic!("expected Rejection"),
            }
        }
    }
}
