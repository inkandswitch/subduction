//! Identified connection for Wasm.
//!
//! Pairs a [`JsConnection`] (transport) with a verified [`PeerId`] (from the handshake).
//! This is the single connection type used by [`Subduction`] in Wasm — all transports
//! (WebSocket, HTTP long-poll, custom JS) are erased through the [`JsConnection`] interface.

use core::time::Duration;

use super::{JsConnection, JsConnectionError};
use future_form::Local;
use futures::future::LocalBoxFuture;
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    },
    peer::id::PeerId,
};

/// A [`JsConnection`] paired with the verified remote [`PeerId`].
///
/// Constructed by the `build_connection` closure inside [`handshake::initiate`] or
/// [`handshake::respond`], which provides the verified peer identity after the
/// handshake succeeds. This mirrors the native pattern where connection constructors
/// (e.g., `WebSocket::new(stream, ..., peer_id)`) take the peer ID as an argument.
///
/// [`handshake::initiate`]: subduction_core::connection::handshake::initiate
/// [`handshake::respond`]: subduction_core::connection::handshake::respond
#[derive(Debug, Clone)]
pub struct IdentifiedConnection {
    transport: JsConnection,
    peer_id: PeerId,
}

impl IdentifiedConnection {
    /// Construct from a [`JsConnection`] and a verified [`PeerId`].
    #[must_use]
    pub fn new(transport: JsConnection, peer_id: PeerId) -> Self {
        Self { transport, peer_id }
    }

    /// Access the inner [`JsConnection`].
    #[must_use]
    pub fn transport(&self) -> &JsConnection {
        &self.transport
    }

    /// Consume and return the inner [`JsConnection`].
    #[must_use]
    pub fn into_transport(self) -> JsConnection {
        self.transport
    }
}

impl PartialEq for IdentifiedConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.transport == other.transport
    }
}

impl Connection<Local> for IdentifiedConnection {
    type SendError = JsConnectionError;
    type RecvError = JsConnectionError;
    type CallError = JsConnectionError;
    type DisconnectionError = JsConnectionError;

    fn next_request_id(&self) -> LocalBoxFuture<'_, RequestId> {
        Connection::<Local>::next_request_id(&self.transport)
    }

    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Connection::<Local>::disconnect(&self.transport)
    }

    fn send(&self, message: &Message) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        Connection::<Local>::send(&self.transport, message)
    }

    fn recv(&self) -> LocalBoxFuture<'_, Result<Message, Self::RecvError>> {
        Connection::<Local>::recv(&self.transport)
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> LocalBoxFuture<'_, Result<BatchSyncResponse, Self::CallError>> {
        Connection::<Local>::call(&self.transport, req, timeout)
    }
}
