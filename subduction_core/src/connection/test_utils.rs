//! Test utilities for connection testing.
//!
//! This module provides mock connections and helpers for testing connection-related code.

use core::time::Duration;

use sedimentree_core::future::Sendable;

use super::{
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    Connection,
};
use crate::peer::id::PeerId;

/// A minimal mock connection for testing.
///
/// This connection implements all required methods with simple defaults:
/// - `peer_id()` returns an all-zeros PeerId
/// - `disconnect()` succeeds immediately
/// - `send()` succeeds immediately (messages are discarded)
/// - `recv()` fails immediately with an error
/// - `next_request_id()` returns a request ID with nonce 0
/// - `call()` fails immediately with an error
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct MockConnection {
    peer_id: PeerId,
}

impl MockConnection {
    /// Create a new mock connection with the default peer ID (all zeros).
    #[must_use]
    pub fn new() -> Self {
        Self {
            peer_id: PeerId::new([0u8; 32]),
        }
    }

    /// Create a new mock connection with a specific peer ID.
    #[must_use]
    pub fn with_peer_id(peer_id: PeerId) -> Self {
        Self { peer_id }
    }
}

impl Default for MockConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection<Sendable> for MockConnection {
    type DisconnectionError = core::fmt::Error;
    type SendError = core::fmt::Error;
    type RecvError = core::fmt::Error;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Sendable as sedimentree_core::future::FutureKind>::Future<
        '_,
        Result<(), Self::DisconnectionError>,
    > {
        Box::pin(async { Ok(()) })
    }

    fn send(
        &self,
        _message: Message,
    ) -> <Sendable as sedimentree_core::future::FutureKind>::Future<
        '_,
        Result<(), Self::SendError>,
    > {
        Box::pin(async { Ok(()) })
    }

    fn recv(
        &self,
    ) -> <Sendable as sedimentree_core::future::FutureKind>::Future<
        '_,
        Result<Message, Self::RecvError>,
    > {
        Box::pin(async { Err(core::fmt::Error) })
    }

    fn next_request_id(
        &self,
    ) -> <Sendable as sedimentree_core::future::FutureKind>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        Box::pin(async move {
            RequestId {
                requestor: peer_id,
                nonce: 0,
            }
        })
    }

    fn call(
        &self,
        _req: BatchSyncRequest,
        _timeout: Option<Duration>,
    ) -> <Sendable as sedimentree_core::future::FutureKind>::Future<
        '_,
        Result<BatchSyncResponse, Self::CallError>,
    > {
        Box::pin(async { Err(core::fmt::Error) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_connection_new() {
        let conn = MockConnection::new();
        assert_eq!(conn.peer_id(), PeerId::new([0u8; 32]));
    }

    #[test]
    fn test_mock_connection_with_peer_id() {
        let peer_id = PeerId::new([42u8; 32]);
        let conn = MockConnection::with_peer_id(peer_id);
        assert_eq!(conn.peer_id(), peer_id);
    }

    #[test]
    fn test_mock_connection_default() {
        let conn = MockConnection::default();
        assert_eq!(conn.peer_id(), PeerId::new([0u8; 32]));
    }

    #[test]
    fn test_mock_connection_clone() {
        let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
        let conn2 = conn1.clone();
        assert_eq!(conn1.peer_id(), conn2.peer_id());
    }

    #[test]
    fn test_mock_connection_equality() {
        let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
        let conn2 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
        let conn3 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

        assert_eq!(conn1, conn2);
        assert_ne!(conn1, conn3);
    }
}
