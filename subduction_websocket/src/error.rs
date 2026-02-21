//! Error types.

use futures::channel::oneshot;
use sedimentree_core::codec::error::DecodeError;
use subduction_core::connection::message::Message;
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
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum RunError {
    /// Internal MPSC channel error.
    #[error("channel send error: {0}")]
    ChanSend(async_channel::SendError<Message>),

    /// WebSocket error.
    #[error(transparent)]
    WebSocket(#[from] tungstenite::Error),

    /// Message deserialization error.
    #[error("deserialize error: {0}")]
    Deserialize(DecodeError),
}
