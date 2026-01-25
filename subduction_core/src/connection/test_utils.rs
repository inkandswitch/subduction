//! Test utilities for connection testing.
//!
//! This module provides mock connections and helpers for testing connection-related code.

use core::time::Duration;

use futures::FutureExt;
use futures_kind::{FutureKind, Local, Sendable};

use super::{
    Connection,
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
};
use crate::peer::id::PeerId;

/// A minimal mock connection for testing.
///
/// This connection implements all required methods with simple defaults:
/// - `peer_id()` returns an all-zeros `PeerId`
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
    pub const fn new() -> Self {
        Self {
            peer_id: PeerId::new([0u8; 32]),
        }
    }

    /// Create a new mock connection with a specific peer ID.
    #[must_use]
    pub const fn with_peer_id(peer_id: PeerId) -> Self {
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
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &Message,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        Box::pin(async { Ok(()) })
    }

    fn recv(&self) -> <Sendable as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        Box::pin(async { Err(core::fmt::Error) })
    }

    fn next_request_id(&self) -> <Sendable as FutureKind>::Future<'_, RequestId> {
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
    ) -> <Sendable as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        Box::pin(async { Err(core::fmt::Error) })
    }
}

/// A mock connection that always fails on send.
///
/// This is useful for testing connection cleanup when send operations fail.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct FailingSendMockConnection {
    peer_id: PeerId,
}

impl FailingSendMockConnection {
    /// Create a new failing mock connection with the default peer ID (all zeros).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            peer_id: PeerId::new([0u8; 32]),
        }
    }

    /// Create a new failing mock connection with a specific peer ID.
    #[must_use]
    pub const fn with_peer_id(peer_id: PeerId) -> Self {
        Self { peer_id }
    }
}

impl Default for FailingSendMockConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection<Sendable> for FailingSendMockConnection {
    type DisconnectionError = core::fmt::Error;
    type SendError = core::fmt::Error;
    type RecvError = core::fmt::Error;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &Message,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        Box::pin(async { Err(core::fmt::Error) })
    }

    fn recv(&self) -> <Sendable as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        Box::pin(async { Err(core::fmt::Error) })
    }

    fn next_request_id(&self) -> <Sendable as FutureKind>::Future<'_, RequestId> {
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
    ) -> <Sendable as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        Box::pin(async { Err(core::fmt::Error) })
    }
}

/// A channel-based mock connection for diagnostic testing.
///
/// This mock uses async channels to simulate real message flow, allowing
/// precise control over when messages arrive and observation of timing.
#[derive(Clone, Debug)]
pub struct ChannelMockConnection {
    peer_id: PeerId,

    /// Sender for outbound messages (from Subduction to "remote")
    outbound_tx: async_channel::Sender<Message>,

    /// Receiver for inbound messages (from "remote" to Subduction)
    inbound_rx: async_channel::Receiver<Message>,

    /// Sender for inbound messages (kept for potential direct access in complex tests)
    #[allow(dead_code)]
    inbound_tx: async_channel::Sender<Message>,

    /// Request ID counter
    request_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl PartialEq for ChannelMockConnection {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
    }
}

/// Handle for controlling a `ChannelMockConnection` from tests.
#[derive(Clone, Debug)]
pub struct ChannelMockConnectionHandle {
    /// Receiver for outbound messages (messages Subduction sends)
    pub outbound_rx: async_channel::Receiver<Message>,

    /// Sender for inbound messages (inject messages to Subduction)
    pub inbound_tx: async_channel::Sender<Message>,
}

impl ChannelMockConnection {
    /// Create a new channel mock connection with its control handle.
    ///
    /// Returns the connection and a handle that tests can use to:
    /// - Inject messages via `handle.inbound_tx.send(msg)`
    /// - Observe sent messages via `handle.outbound_rx.recv()`
    #[must_use]
    pub fn new_with_handle(peer_id: PeerId) -> (Self, ChannelMockConnectionHandle) {
        let (outbound_tx, outbound_rx) = async_channel::unbounded();
        let (inbound_tx, inbound_rx) = async_channel::unbounded();

        let conn = Self {
            peer_id,
            outbound_tx,
            inbound_rx,
            inbound_tx: inbound_tx.clone(),
            request_counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        let handle = ChannelMockConnectionHandle {
            outbound_rx,
            inbound_tx,
        };

        (conn, handle)
    }

    /// Create with default peer ID.
    #[must_use]
    pub fn new_default_with_handle() -> (Self, ChannelMockConnectionHandle) {
        Self::new_with_handle(PeerId::new([0u8; 32]))
    }
}

impl Connection<Sendable> for ChannelMockConnection {
    type DisconnectionError = core::convert::Infallible;
    type SendError = async_channel::SendError<Message>;
    type RecvError = async_channel::RecvError;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async { Ok(()) })
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        Box::pin(async move { tx.send(message).await })
    }

    fn recv(&self) -> <Sendable as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        Box::pin(async move { rx.recv().await })
    }

    fn next_request_id(&self) -> <Sendable as FutureKind>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        let counter = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async move {
            RequestId {
                requestor: peer_id,
                nonce: counter,
            }
        })
    }

    fn call(
        &self,
        _req: BatchSyncRequest,
        _timeout: Option<Duration>,
    ) -> <Sendable as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        // For now, call always fails. Tests can implement response handling if needed.
        async { Err(core::fmt::Error) }.boxed()
    }
}

impl Connection<Local> for ChannelMockConnection {
    type DisconnectionError = core::convert::Infallible;
    type SendError = async_channel::SendError<Message>;
    type RecvError = async_channel::RecvError;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        async { Ok(()) }.boxed_local()
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        async move { tx.send(message).await }.boxed_local()
    }

    fn recv(&self) -> <Local as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        async move { rx.recv().await }.boxed_local()
    }

    fn next_request_id(&self) -> <Local as FutureKind>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        let counter = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        async move {
            RequestId {
                requestor: peer_id,
                nonce: counter,
            }
        }
        .boxed_local()
    }

    fn call(
        &self,
        _req: BatchSyncRequest,
        _timeout: Option<Duration>,
    ) -> <Local as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        async { Err(core::fmt::Error) }.boxed_local()
    }
}

/// A connection wrapper that fires callbacks during `recv()`, mimicking WASM behavior.
///
/// This wrapper demonstrates the "one behind" bug: callbacks fire BEFORE the message
/// is dispatched and stored, so any data queries in the callback see stale state.
///
/// The callback receives the message and a sender to report what it observed.
#[derive(Clone, Debug)]
pub struct CallbackOnRecvConnection<C> {
    inner: C,

    /// Channel to send observation results from callback
    callback_result_tx: async_channel::Sender<CallbackObservation>,
}

/// What the callback observed when it fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallbackObservation {
    /// The sedimentree ID from the message.
    pub sedimentree_id: sedimentree_core::id::SedimentreeId,

    /// Whether the sedimentree was visible when callback fired.
    pub was_visible: bool,

    /// Number of commits visible when callback fired.
    pub commit_count: usize,
}

/// Handle for receiving callback observations in tests.
#[derive(Clone, Debug)]
pub struct CallbackObservationHandle {
    /// Receiver channel for callback observations.
    pub rx: async_channel::Receiver<CallbackObservation>,
}

impl<C> CallbackOnRecvConnection<C> {
    /// Create a new callback wrapper with observation channel.
    #[must_use]
    pub fn new(inner: C) -> (Self, CallbackObservationHandle) {
        let (tx, rx) = async_channel::unbounded();
        (
            Self {
                inner,
                callback_result_tx: tx,
            },
            CallbackObservationHandle { rx },
        )
    }

    /// Get a sender for reporting observations (used by external callback logic).
    #[must_use]
    pub fn observation_sender(&self) -> async_channel::Sender<CallbackObservation> {
        self.callback_result_tx.clone()
    }
}

impl<C: PartialEq> PartialEq for CallbackOnRecvConnection<C> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<C: Connection<Sendable> + Send> Connection<Sendable> for CallbackOnRecvConnection<C>
where
    C::RecvError: Send,
{
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;
    type CallError = C::CallError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id()
    }

    fn disconnect(
        &self,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Sendable as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Sendable as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        // Note: This mimics WASM behavior where callbacks would fire here,
        // but we can't actually fire callbacks here without access to Subduction.
        // The test will inject a callback-like check separately.
        self.inner.recv()
    }

    fn next_request_id(&self) -> <Sendable as FutureKind>::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> <Sendable as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

impl<C: Connection<Local>> Connection<Local> for CallbackOnRecvConnection<C> {
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;
    type CallError = C::CallError;

    fn peer_id(&self) -> PeerId {
        self.inner.peer_id()
    }

    fn disconnect(
        &self,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Local as FutureKind>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Local as FutureKind>::Future<'_, Result<Message, Self::RecvError>> {
        self.inner.recv()
    }

    fn next_request_id(&self) -> <Local as FutureKind>::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> <Local as FutureKind>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use testresult::TestResult;

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
    fn test_mock_connection_equality() {
        let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
        let conn2 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
        let conn3 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

        assert_eq!(conn1, conn2);
        assert_ne!(conn1, conn3);
    }

    #[tokio::test]
    async fn test_channel_mock_connection_send_recv() -> TestResult {
        let peer_id = PeerId::new([1u8; 32]);
        let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);

        let msg = Message::BlobsRequest(vec![]);
        Connection::<Sendable>::send(&conn, &msg).await?;
        let received = handle.outbound_rx.recv().await?;
        assert!(matches!(received, Message::BlobsRequest(_)));

        let inject_msg = Message::BlobsResponse(vec![]);
        handle.inbound_tx.send(inject_msg).await?;
        let received = Connection::<Sendable>::recv(&conn).await?;
        assert!(matches!(received, Message::BlobsResponse(_)));

        Ok(())
    }
}
