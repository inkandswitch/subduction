//! Error types for the HTTP long-poll transport.

use thiserror::Error;

/// Outbound channel closed — no more messages can be sent to the client.
#[derive(Debug, Clone, Copy, Error)]
#[error("outbound channel closed")]
pub struct SendError;

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Clone, Copy, Error)]
pub enum CallError {
    /// Response oneshot was dropped before a reply arrived.
    #[error("response dropped")]
    ResponseDropped,

    /// Timed out waiting for response.
    #[error("timed out waiting for response")]
    Timeout,

    /// Outbound channel closed.
    #[error("outbound channel closed")]
    ChannelClosed,
}

/// Inbound channel closed — no more messages from the client.
#[derive(Debug, Clone, Copy, Error)]
#[error("inbound channel closed")]
pub struct RecvError;

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("disconnected")]
pub struct DisconnectionError;

/// Errors while processing an HTTP long-poll request on the server.
#[cfg(feature = "server")]
#[derive(Debug, Error)]
pub enum ServerError {
    /// The session ID header is missing or malformed.
    #[error("missing or invalid session ID")]
    InvalidSessionId,

    /// No session found for the given ID.
    #[error("session not found")]
    SessionNotFound,

    /// The request body exceeds the maximum allowed size.
    #[error("request body too large")]
    BodyTooLarge,

    /// The request body could not be read, for example because the client
    /// disconnected mid-upload or sent malformed chunked encoding.
    #[error("failed to read request body")]
    BodyRead(#[source] alloc::boxed::Box<dyn core::error::Error + Send + Sync>),

    /// Internal channel error.
    #[error("channel send error")]
    ChanSend,

    /// The HTTP handshake adapter produced no response bytes.
    #[error("handshake produced no response")]
    HandshakeNoResponse,

    /// The HTTP handshake adapter had no challenge bytes to yield.
    #[error("handshake has no challenge bytes")]
    HandshakeNoChallenge,

    /// Failed to build an HTTP response.
    #[error(transparent)]
    HttpBuild(#[from] hyper::http::Error),
}

#[cfg(feature = "server")]
impl ServerError {
    /// The HTTP status to answer with.
    ///
    /// A 4xx status means the request was at fault, not the server.
    #[must_use]
    pub const fn status(&self) -> hyper::StatusCode {
        use hyper::StatusCode;

        match self {
            Self::InvalidSessionId | Self::BodyRead(_) => StatusCode::BAD_REQUEST,
            Self::SessionNotFound => StatusCode::NOT_FOUND,
            Self::BodyTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ChanSend
            | Self::HandshakeNoResponse
            | Self::HandshakeNoChallenge
            | Self::HttpBuild(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Errors while connecting as a client.
#[derive(Debug, Error)]
pub enum ClientError {
    /// HTTP request failed.
    #[error("HTTP request error: {0}")]
    Request(alloc::string::String),

    /// Server returned an unexpected status code.
    #[error("unexpected status {status}: {body}")]
    UnexpectedStatus {
        /// The HTTP status code.
        status: u16,
        /// The response body as a string.
        body: alloc::string::String,
    },

    /// Failed to decode handshake response.
    #[error("handshake decode error: {0}")]
    HandshakeDecode(alloc::string::String),

    /// Handshake was rejected by the server.
    #[error("handshake rejected: {reason}")]
    HandshakeRejected {
        /// The rejection reason.
        reason: alloc::string::String,
    },

    /// Authentication error during handshake.
    #[error("authentication error: {0}")]
    Authentication(alloc::string::String),
}
