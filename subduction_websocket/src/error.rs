//! Error types.

use futures::channel::oneshot;
use subduction_core::connection::message::Message;
use thiserror::Error;

/// Problem while attempting to send a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Outbound channel closed (sender task stopped).
    #[error("Outbound channel closed")]
    ChannelClosed,
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Problem receiving on the internal channel.
    #[error("Channel canceled")]
    ChanCanceled(oneshot::Canceled),

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    Timeout,

    /// Outbound channel closed (sender task stopped).
    #[error("Outbound channel closed")]
    ChannelClosed,
}

/// Problem while attempting to receive a message.
#[derive(Debug, Clone, Copy, Error)]
pub enum RecvError {
    /// Problem receiving on the internal channel.
    #[error("Channel receive error")]
    ChanCanceled(oneshot::Canceled),

    /// Attempted to read from a closed channel.
    #[error("Attempted to read from closed channel")]
    ReadFromClosed,
}

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("Disconnected")]
pub struct DisconnectionError;

/// Errors while running the connection loop.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum RunError {
    /// Internal MPSC channel error.
    #[error("Channel send error: {0}")]
    ChanSend(async_channel::SendError<Message>),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Deserialization error.
    #[error("Deserialize error")]
    Deserialize,
}
