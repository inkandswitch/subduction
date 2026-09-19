//! Error types.

use alloc::vec::Vec;

use thiserror::Error;

/// Outbound channel closed — the sender task has stopped.
#[derive(Debug, Clone, Copy, Error)]
#[error("sender task stopped")]
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

    /// Outbound channel closed — the sender task has stopped.
    #[error("sender task stopped")]
    SenderTaskStopped,
}

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
    ///
    /// Boxed: `tungstenite::Error` is 136 bytes and this variant is only
    /// built from `?`/`From` below, so the indirection costs nothing on the
    /// error path but keeps every `Result<_, RunError>` small.
    #[error(transparent)]
    WebSocket(Box<tungstenite::Error>),
}

impl From<tungstenite::Error> for RunError {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}
