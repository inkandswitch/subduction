//! Error types.

use futures::channel::oneshot;
use thiserror::Error;

/// Problem while attempting to send a message.
#[derive(Debug, Error)]
pub enum SendError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Serialization error.
    #[error("Bincode error: {0}")]
    Serialization(#[from] bincode::error::EncodeError),
}

/// Problem while attempting to make a roundtrip call.
#[derive(Debug, Error)]
pub enum CallError {
    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(bincode::error::EncodeError),

    /// Problem receiving on the internal channel.
    #[error("Channel canceled: {0}")]
    ChanCanceled(#[from] oneshot::Canceled),

    /// Timed out waiting for response.
    #[error("Timed out waiting for response")]
    Timeout,
}

/// Problem while attempting to receive a message.
#[derive(Debug, Clone, Copy, Error)]
pub enum RecvError {
    /// Problem receiving on the internal channel.
    #[error("Channel receive error: {0}")]
    ChanCanceled(#[from] oneshot::Canceled),

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
pub enum RunError {
    /// Internal MPSC channel error.
    #[error("Channel send error: {0}")]
    ChanSend(#[from] futures::channel::mpsc::SendError),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    /// Deserialization error.
    #[error("Bincode deserialize error: {0}")]
    Deserialize(#[from] bincode::error::DecodeError),
}
