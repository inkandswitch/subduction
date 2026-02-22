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
//! use subduction_websocket::handshake::WebSocketHandshake;
//!
//! // After WebSocket upgrade, perform Subduction handshake
//! // The transport is consumed and passed back to build_connection
//! let (authenticated, ()) = handshake::initiate(
//!     WebSocketHandshake::new(ws_stream),  // consumed
//!     |ws_handshake, peer_id| {
//!         let ws_stream = ws_handshake.into_inner();
//!         (WebSocket::new(ws_stream, timeout, default_timeout, peer_id), ())
//!     },
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
use future_form::{FutureForm, Local, Sendable, future_form};
use futures_util::{AsyncRead, AsyncWrite, SinkExt, StreamExt};
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

#[future_form(
    Sendable where T: AsyncRead + AsyncWrite + Unpin + Send,
    Local where T: AsyncRead + AsyncWrite + Unpin
)]
impl<K: FutureForm, T> Handshake<K> for WebSocketHandshake<T> {
    type Error = WebSocketHandshakeError;

    fn send(&mut self, bytes: Vec<u8>) -> K::Future<'_, Result<(), Self::Error>> {
        K::from_future(async move {
            SinkExt::send(&mut self.0, tungstenite::Message::Binary(bytes.into())).await?;
            Ok(())
        })
    }

    fn recv(&mut self) -> K::Future<'_, Result<Vec<u8>, Self::Error>> {
        K::from_future(async move {
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
        timestamp::TimestampSeconds,
    };
    use subduction_crypto::{nonce::Nonce, signed::Signed, signer::memory::MemorySigner};

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
                Nonce::from_u128(42),
            );
            let signed_challenge = Signed::seal::<Sendable, _>(&test_signer, challenge)
                .await
                .into_signed();
            let msg = HandshakeMessage::SignedChallenge(signed_challenge.clone());

            let bytes = msg.encode();
            let decoded = HandshakeMessage::try_decode(&bytes)
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

            let bytes = msg.encode();
            let decoded = HandshakeMessage::try_decode(&bytes)
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
