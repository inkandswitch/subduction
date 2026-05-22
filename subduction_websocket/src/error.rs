//! Error types.

use alloc::vec::Vec;

use thiserror::Error;

/// Outbound channel closed — the sender task has stopped.
#[derive(Debug, Clone, Copy, Error)]
#[error("sender task stopped")]
pub struct SendError;

/// Inbound channel closed — the listener task has stopped.
#[derive(Debug, Clone, Copy, Error)]
#[error("inbound channel closed")]
pub struct RecvError;

/// Problem while attempting to gracefully disconnect.
#[derive(Debug, Clone, Copy, Error)]
#[error("disconnected")]
pub struct DisconnectionError;

/// Errors while running the connection loop.
///
/// [`ChanSend`]: RunError::ChanSend
#[derive(Debug, Error)]
pub enum RunError {
    /// Internal channel send failed (receiver dropped).
    #[error("channel send error: {0}")]
    ChanSend(Box<async_channel::SendError<Vec<u8>>>),

    /// WebSocket error.
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),
}
