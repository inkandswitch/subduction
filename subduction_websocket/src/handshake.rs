//! Handshake transport implementation for WebSocket connections.
//!
//! This module provides the [`Handshake`] trait implementation for
//! [`WebSocketStream`], enabling the handshake protocol from
//! [`subduction_core::connection::handshake`] to operate over WebSocket.
//!
//! # Usage
//!
//! Use [`handshake::initiate`] or [`handshake::respond`] from `subduction_core`
//! with a `WebSocketStream` to perform authentication:
//!
//! ```ignore
//! use subduction_core::connection::handshake;
//!
//! // After WebSocket upgrade, perform Subduction handshake
//! let authenticated = handshake::initiate(
//!     &mut ws_stream,
//!     |peer_id| WebSocket::new(ws_stream, timeout, default_timeout, peer_id),
//!     &signer,
//!     audience,
//!     now,
//!     nonce,
//! ).await?;
//! ```
//!
//! [`handshake::initiate`]: subduction_core::connection::handshake::initiate
//! [`handshake::respond`]: subduction_core::connection::handshake::respond

use alloc::vec::Vec;
use core::ops::{Deref, DerefMut};

use async_tungstenite::WebSocketStream;
use future_form::Sendable;
use futures_util::{AsyncRead, AsyncWrite, SinkExt, StreamExt, future::BoxFuture};
use subduction_core::connection::handshake::Handshake;
use thiserror::Error;

/// Errors that can occur during WebSocket handshake transport.
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
}

/// Newtype wrapper around [`WebSocketStream`] for handshake.
///
/// This wrapper enables implementing [`Handshake`] for `WebSocketStream`
/// while respecting Rust's orphan rules.
///
/// Use [`WebSocketHandshake::new`] to wrap a `WebSocketStream`, then pass
/// it to [`handshake::initiate`] or [`handshake::respond`]. After the
/// handshake completes, use [`into_inner`] to retrieve the underlying stream.
///
/// [`handshake::initiate`]: subduction_core::connection::handshake::initiate
/// [`handshake::respond`]: subduction_core::connection::handshake::respond
/// [`into_inner`]: WebSocketHandshake::into_inner
#[derive(Debug)]
pub struct WebSocketHandshake<T>(pub WebSocketStream<T>);

impl<T> WebSocketHandshake<T> {
    /// Wrap a `WebSocketStream` for handshake.
    pub const fn new(stream: WebSocketStream<T>) -> Self {
        Self(stream)
    }

    /// Unwrap and return the inner `WebSocketStream`.
    pub fn into_inner(self) -> WebSocketStream<T> {
        self.0
    }
}

impl<T> Deref for WebSocketHandshake<T> {
    type Target = WebSocketStream<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for WebSocketHandshake<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Handshake<Sendable> for WebSocketHandshake<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send,
{
    type Error = WebSocketHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> BoxFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            SinkExt::send(&mut self.0, tungstenite::Message::Binary(bytes.into())).await?;
            Ok(())
        })
    }

    fn recv(&mut self) -> BoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        Box::pin(async move {
            loop {
                let msg = self
                    .0
                    .next()
                    .await
                    .ok_or(WebSocketHandshakeError::ConnectionClosed)??;

                match msg {
                    tungstenite::Message::Binary(bytes) => return Ok(bytes.to_vec()),
                    tungstenite::Message::Text(_) => {
                        return Err(WebSocketHandshakeError::UnexpectedMessageType("text"));
                    }
                    // Skip ping/pong, continue waiting for binary
                    tungstenite::Message::Ping(_) | tungstenite::Message::Pong(_) => {}
                    tungstenite::Message::Close(_) => {
                        return Err(WebSocketHandshakeError::ConnectionClosed);
                    }
                    tungstenite::Message::Frame(_) => {
                        return Err(WebSocketHandshakeError::UnexpectedMessageType("frame"));
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use future_form::Sendable;
    use subduction_core::{
        connection::handshake::{
            Audience, Challenge, HandshakeMessage, Rejection, RejectionReason,
        },
        crypto::{nonce::Nonce, signed::Signed, signer::MemorySigner},
        timestamp::TimestampSeconds,
    };

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
