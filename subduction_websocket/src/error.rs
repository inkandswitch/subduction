//! Error types.

use core::fmt;

use futures::channel::oneshot;
use sedimentree_core::codec::error::DecodeError;
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
    ResponseDropped(oneshot::Canceled),

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
/// Generic over the channel message type `M` so that the [`ChanSend`]
/// variant preserves the full [`async_channel::SendError<M>`].
///
/// [`ChanSend`]: RunError::ChanSend
#[derive(Debug, Error)]
pub enum RunError<M: fmt::Debug> {
    /// Internal channel send failed (receiver dropped).
    #[error("channel send error: {0}")]
    ChanSend(Box<async_channel::SendError<M>>),

    /// WebSocket error.
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),

    /// Message deserialization error.
    #[error("deserialize error: {0}")]
    Deserialize(DecodeError),
}
