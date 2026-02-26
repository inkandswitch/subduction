//! Error types for the Iroh transport.

use subduction_core::connection::handshake::AuthenticateError;
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
    ResponseDropped(futures::channel::oneshot::Canceled),

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

/// Errors during listener task execution.
#[derive(Debug, Error)]
pub enum RunError {
    /// Failed to read from the QUIC stream.
    #[error("QUIC read error: {0}")]
    Read(#[from] iroh::endpoint::ReadExactError),

    /// Failed to write to the QUIC stream.
    #[error("QUIC write error: {0}")]
    Write(#[from] iroh::endpoint::WriteError),

    /// Failed to decode an inbound message.
    #[error("message decode error: {0}")]
    Deserialize(#[from] sedimentree_core::codec::error::DecodeError),

    /// Failed to send on internal channel.
    #[error("channel send error")]
    ChanSend(Box<async_channel::SendError<subduction_core::connection::message::Message>>),

    /// The QUIC connection was closed.
    #[error("connection closed: {0}")]
    ConnectionClosed(#[from] iroh::endpoint::ConnectionError),
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
    Handshake(#[from] Box<AuthenticateError<RunError>>),
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
    Handshake(#[from] Box<AuthenticateError<RunError>>),
}
