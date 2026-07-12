//! The Durable Object side of a Subduction connection.
//!
//! Two adapters live here:
//!
//! - [`DoConnection`] — implements [`Connection<Local, SyncMessage>`]. It is a
//!   thin wrapper over a hibernatable [`worker::WebSocket`]. `send` encodes a
//!   [`SyncMessage`] and pushes it out the socket; `recv` is intentionally
//!   inert (the DO is event-driven — messages arrive via `websocket_message`,
//!   not by polling `recv`).
//!
//! - [`OneShot`] — implements the handshake transport [`Handshake<Local>`] for
//!   exactly one round trip. The responder handshake does a single
//!   `recv` (the challenge, already delivered by the runtime) followed by a
//!   single `send` (the response), which fits entirely inside one
//!   `websocket_message` invocation.

use std::sync::atomic::{AtomicU64, Ordering};

use future_form::{FutureForm, Local};
use futures::future::LocalBoxFuture;
use subduction_core::{
    connection::{message::SyncMessage, Connection},
    handshake::Handshake,
};
use thiserror::Error;
use worker::WebSocket;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(0);

/// A connection to a peer, backed by a hibernatable Durable Object WebSocket.
#[derive(Clone)]
pub struct DoConnection {
    ws: WebSocket,
    /// Process-local identity, used only for [`PartialEq`]. Clones share it.
    id: u64,
}

impl DoConnection {
    /// Wrap a WebSocket as a connection.
    #[must_use]
    pub fn new(ws: WebSocket) -> Self {
        Self {
            ws,
            id: NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl core::fmt::Debug for DoConnection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DoConnection")
            .field("id", &self.id)
            .finish()
    }
}

impl PartialEq for DoConnection {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

/// Failure sending a message over the DO WebSocket.
#[derive(Debug, Error)]
#[error("websocket send failed: {0}")]
pub struct SendError(pub String);

/// `recv` is not supported on a DO connection (messages are pushed by the
/// runtime, not pulled). Never produced in normal operation.
#[derive(Debug, Error)]
#[error("recv is not supported on a Durable Object connection")]
pub struct RecvUnsupported;

/// Failure closing the DO WebSocket.
#[derive(Debug, Error)]
#[error("websocket close failed: {0}")]
pub struct DisconnectError(pub String);

impl Connection<Local, SyncMessage> for DoConnection {
    type DisconnectionError = DisconnectError;
    type SendError = SendError;
    type RecvError = RecvUnsupported;

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        let res = self
            .ws
            .close(Some(1000), Some("closed by server"))
            .map_err(|e| DisconnectError(e.to_string()));
        Local::from_future(async move { res })
    }

    fn send(&self, message: &SyncMessage) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        let bytes = message.encode();
        let res = self
            .ws
            .send_with_bytes(&bytes)
            .map_err(|e| SendError(e.to_string()));
        Local::from_future(async move { res })
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<SyncMessage, Self::RecvError>> {
        Local::from_future(async move { Err(RecvUnsupported) })
    }
}

// ---------------------------------------------------------------------------
// One-shot handshake transport
// ---------------------------------------------------------------------------

/// Transport-level error during the handshake.
#[derive(Debug, Error)]
#[error("handshake transport error: {0}")]
pub struct HandshakeIoError(pub String);

/// A single-round-trip [`Handshake`] transport for the responder side.
///
/// The runtime has already delivered the challenge bytes (they are the first
/// message on a not-yet-authenticated socket), so `recv` yields them once and
/// then reports the connection closed. `send` writes the response (or a
/// rejection) straight to the socket.
pub struct OneShot {
    incoming: Option<Vec<u8>>,
    ws: WebSocket,
}

impl OneShot {
    /// Build a one-shot handshake transport from the buffered challenge bytes
    /// and the socket to answer on.
    #[must_use]
    pub fn new(challenge_bytes: Vec<u8>, ws: WebSocket) -> Self {
        Self {
            incoming: Some(challenge_bytes),
            ws,
        }
    }
}

impl Handshake<Local> for OneShot {
    type Error = HandshakeIoError;

    fn send(&mut self, bytes: Vec<u8>) -> LocalBoxFuture<'_, Result<(), Self::Error>> {
        let res = self
            .ws
            .send_with_bytes(&bytes)
            .map_err(|e| HandshakeIoError(e.to_string()));
        Local::from_future(async move { res })
    }

    fn recv(&mut self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::Error>> {
        let res = self
            .incoming
            .take()
            .ok_or_else(|| HandshakeIoError("connection closed during handshake".to_string()));
        Local::from_future(async move { res })
    }
}
