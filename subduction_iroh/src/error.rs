//! Error types for the Iroh transport.

use alloc::vec::Vec;

use subduction_core::handshake::AuthenticateError;
use thiserror::Error;

/// Outbound channel closed -- no more messages can be sent.
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

/// Inbound channel closed -- no more messages from the peer.
#[derive(Debug, Clone, Copy, Error)]
#[error("inbound channel closed")]
pub struct RecvError;

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("disconnected")]
pub struct DisconnectionError;

/// QUIC stream-level errors.
///
/// Extracted from [`RunError`] so that code that never touches the
/// internal channel (handshake, framing helpers) can use a concrete,
/// non-generic error type.
#[derive(Debug, Error)]
pub enum StreamError {
    /// The frame exceeds the maximum allowed size.
    #[error("frame too large: {actual} bytes (max {max})")]
    FrameTooLarge {
        /// Actual frame size (or attempted write size).
        actual: usize,
        /// Maximum allowed frame size.
        max: usize,
    },

    /// Failed to read from the QUIC stream.
    #[error("QUIC read error: {0}")]
    Read(#[from] iroh::endpoint::ReadExactError),

    /// Failed to write to the QUIC stream.
    #[error("QUIC write error: {0}")]
    Write(iroh::endpoint::WriteError),

    /// The QUIC connection was closed.
    #[error("connection closed: {0}")]
    ConnectionClosed(#[from] iroh::endpoint::ConnectionError),
}

/// Errors during listener/sender task execution.
#[derive(Debug, Error)]
pub enum RunError {
    /// QUIC stream-level error.
    #[error(transparent)]
    Stream(#[from] StreamError),

    /// Failed to send on internal channel (receiver dropped).
    #[error("channel send error: {0}")]
    ChanSend(Box<async_channel::SendError<Vec<u8>>>),
}

/// Errors when connecting to a peer.
#[derive(Debug, Error)]
pub enum ConnectError {
    /// Failed to establish the QUIC connection.
    #[error("QUIC connect error: {0}")]
    Connect(#[from] iroh::endpoint::ConnectError),

    /// Failed during connection establishment.
    #[error("connecting error: {0}")]
    Connecting(#[from] iroh::endpoint::ConnectingError),

    /// Failed to open the initial bi-directional stream.
    #[error("failed to open bi-stream: {0}")]
    OpenBi(#[from] iroh::endpoint::ConnectionError),

    /// Handshake authentication failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] Box<AuthenticateError<StreamError>>),
}

/// Errors when accepting a connection.
#[derive(Debug, Error)]
pub enum AcceptError {
    /// No incoming connection (endpoint shutting down).
    #[error("no incoming connection")]
    NoIncoming,

    /// Failed during connection establishment.
    #[error("connecting error: {0}")]
    Connecting(#[from] iroh::endpoint::ConnectingError),

    /// Failed to accept the bi-directional stream.
    #[error("failed to accept bi-stream: {0}")]
    AcceptBi(#[from] iroh::endpoint::ConnectionError),

    /// Handshake authentication failed.
    #[error("handshake error: {0}")]
    Handshake(#[from] Box<AuthenticateError<StreamError>>),
}
