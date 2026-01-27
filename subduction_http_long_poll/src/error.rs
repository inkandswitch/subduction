//! Error types for HTTP long-polling transport.

use subduction_core::connection::{
    handshake::{HandshakeError, RejectionReason},
    message::Message,
};
use thiserror::Error;

/// Problem while attempting to send a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// HTTP request failed.
    #[cfg(feature = "client")]
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    /// Session has expired or is invalid.
    #[error("Session expired or invalid")]
    SessionExpired,

    /// CBOR encoding failed.
    #[error("CBOR encoding error: {0}")]
    Encode(#[from] minicbor::encode::Error<core::convert::Infallible>),

    /// Connection has been closed.
    #[error("Connection closed")]
    ConnectionClosed,
}

/// Problem while attempting to receive a message.
#[derive(Debug, Clone, Copy, Error)]
pub enum RecvError {
    /// The inbound channel was closed.
    #[error("Inbound channel closed")]
    ChannelClosed,

    /// Poll loop terminated unexpectedly.
    #[error("Poll loop terminated")]
    PollLoopTerminated,
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// HTTP request failed.
    #[cfg(feature = "client")]
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    /// Session has expired or is invalid.
    #[error("Session expired or invalid")]
    SessionExpired,

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    Timeout,

    /// Response channel was canceled.
    #[error("Response channel canceled")]
    ChannelCanceled,

    /// CBOR encoding failed.
    #[error("CBOR encoding error: {0}")]
    Encode(#[from] minicbor::encode::Error<core::convert::Infallible>),

    /// CBOR decoding failed.
    #[error("CBOR decoding error: {0}")]
    Decode(#[from] minicbor::decode::Error),

    /// Connection has been closed.
    #[error("Connection closed")]
    ConnectionClosed,
}

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("Disconnection error")]
pub struct DisconnectionError;

/// Errors during HTTP handshake.
#[derive(Debug, Error)]
pub enum HttpHandshakeError {
    /// HTTP request failed.
    #[cfg(feature = "client")]
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    /// CBOR encoding failed.
    #[error("CBOR encoding error: {0}")]
    Encode(#[from] minicbor::encode::Error<core::convert::Infallible>),

    /// CBOR decoding failed.
    #[error("CBOR decoding error: {0}")]
    Decode(#[from] minicbor::decode::Error),

    /// Handshake protocol error.
    #[error("Handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    /// Server rejected the handshake.
    #[error("Handshake rejected: {0:?}")]
    Rejected(RejectionReason),

    /// Invalid URL.
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Missing session ID in response.
    #[error("Missing session ID in response")]
    MissingSessionId,

    /// Invalid session ID format.
    #[error("Invalid session ID format")]
    InvalidSessionId,

    /// Server returned an error status.
    #[error("Server error: {status} - {message}")]
    ServerError {
        /// HTTP status code.
        status: u16,
        /// Error message from server.
        message: String,
    },
}

/// Server-side rejection reasons.
#[derive(Debug, Clone, Copy, PartialEq, Eq, minicbor::Encode, minicbor::Decode)]
pub enum ServerRejection {
    /// Session not found or expired.
    #[n(0)]
    SessionNotFound,

    /// Invalid request format.
    #[n(1)]
    InvalidRequest,

    /// Rate limited.
    #[n(2)]
    RateLimited,

    /// Internal server error.
    #[n(3)]
    InternalError,
}

/// Errors while running the server.
#[cfg(feature = "server")]
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ServerError {
    /// CBOR decoding failed.
    #[error("CBOR decoding error: {0}")]
    Decode(#[from] minicbor::decode::Error),

    /// CBOR encoding failed.
    #[error("CBOR encoding error: {0}")]
    Encode(#[from] minicbor::encode::Error<core::convert::Infallible>),

    /// Handshake error.
    #[error("Handshake error: {0}")]
    Handshake(#[from] HandshakeError),

    /// Session not found.
    #[error("Session not found")]
    SessionNotFound,

    /// Invalid session ID header.
    #[error("Invalid session ID header")]
    InvalidSessionId,

    /// Missing session ID header.
    #[error("Missing session ID header")]
    MissingSessionId,

    /// Internal channel error.
    #[error("Channel send error: {0}")]
    ChannelSend(async_channel::SendError<Message>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_error_displays() {
        let err = SendError::SessionExpired;
        assert_eq!(format!("{err}"), "Session expired or invalid");
    }

    #[test]
    fn recv_error_displays() {
        let err = RecvError::ChannelClosed;
        assert_eq!(format!("{err}"), "Inbound channel closed");
    }

    #[test]
    fn call_error_displays() {
        let err = CallError::Timeout;
        assert_eq!(format!("{err}"), "Timed out waiting for response");
    }

    #[test]
    fn disconnection_error_displays() {
        let err = DisconnectionError;
        assert_eq!(format!("{err}"), "Disconnection error");
    }

    #[test]
    fn server_rejection_roundtrips() {
        let rejection = ServerRejection::SessionNotFound;
        let bytes = minicbor::to_vec(&rejection).expect("encode");
        let decoded: ServerRejection = minicbor::decode(&bytes).expect("decode");
        assert_eq!(rejection, decoded);
    }
}
