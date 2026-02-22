//! Test utilities for connection testing.
//!
//! This module provides mock connections and helpers for testing connection-related code.

use alloc::sync::Arc;
use core::{convert::Infallible, time::Duration};

use future_form::{FutureForm, Local, Sendable};
use futures::future::{AbortHandle, BoxFuture, LocalBoxFuture};
use sedimentree_core::commit::CountLeadingZeroBytes;
use subduction_crypto::signer::memory::MemorySigner;

use super::{
    authenticated::Authenticated,
    manager::Spawn,
    message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId},
    nonce_cache::NonceCache,
    Connection,
};
use crate::{
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    storage::memory::MemoryStorage,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
};

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

    /// Wrap this connection in an `Authenticated` wrapper for testing.
    ///
    /// Uses the connection's peer ID as the authenticated identity.
    #[must_use]
    pub fn authenticated(self) -> Authenticated<Self, Sendable> {
        Authenticated::new_for_test(self)
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &Message,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
    }

    fn next_request_id(&self) -> <Sendable as FutureForm>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        Sendable::from_future(async move {
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
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

    /// Wrap this connection in an `Authenticated` wrapper for testing.
    ///
    /// Uses the connection's peer ID as the authenticated identity.
    #[must_use]
    pub fn authenticated(self) -> Authenticated<Self, Sendable> {
        Authenticated::new_for_test(self)
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &Message,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
    }

    fn next_request_id(&self) -> <Sendable as FutureForm>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        Sendable::from_future(async move {
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
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

    /// Wrap this connection in an `Authenticated` wrapper for testing.
    ///
    /// Uses the connection's peer ID as the authenticated identity.
    #[must_use]
    pub fn authenticated<K: FutureForm>(self) -> Authenticated<Self, K>
    where
        Self: Connection<K>,
    {
        Authenticated::new_for_test(self)
    }
}

impl Connection<Sendable> for ChannelMockConnection {
    type DisconnectionError = Infallible;
    type SendError = async_channel::SendError<Message>;
    type RecvError = async_channel::RecvError;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        Sendable::from_future(async move { tx.send(message).await })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        Sendable::from_future(async move { rx.recv().await })
    }

    fn next_request_id(&self) -> <Sendable as FutureForm>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        let counter = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Sendable::from_future(async move {
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        // For now, call always fails. Tests can implement response handling if needed.
        Sendable::from_future(async { Err(core::fmt::Error) })
    }
}

impl Connection<Local> for ChannelMockConnection {
    type DisconnectionError = Infallible;
    type SendError = async_channel::SendError<Message>;
    type RecvError = async_channel::RecvError;
    type CallError = core::fmt::Error;

    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn disconnect(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Local::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        Local::from_future(async move { tx.send(message).await })
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        Local::from_future(async move { rx.recv().await })
    }

    fn next_request_id(&self) -> <Local as FutureForm>::Future<'_, RequestId> {
        let peer_id = self.peer_id;
        let counter = self
            .request_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Local::from_future(async move {
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
    ) -> <Local as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        Local::from_future(async { Err(core::fmt::Error) })
    }
}

/// A connection wrapper that fires callbacks during `recv()`, mimicking Wasm behavior.
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
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        // Note: This mimics Wasm behavior where callbacks would fire here,
        // but we can't actually fire callbacks here without access to Subduction.
        // The test will inject a callback-like check separately.
        self.inner.recv()
    }

    fn next_request_id(&self) -> <Sendable as FutureForm>::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> <Sendable as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
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
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &Message,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<Message, Self::RecvError>> {
        self.inner.recv()
    }

    fn next_request_id(&self) -> <Local as FutureForm>::Future<'_, RequestId> {
        self.inner.next_request_id()
    }

    fn call(
        &self,
        req: BatchSyncRequest,
        timeout: Option<Duration>,
    ) -> <Local as FutureForm>::Future<'_, Result<BatchSyncResponse, Self::CallError>> {
        self.inner.call(req, timeout)
    }
}

/// Create a test signer with deterministic key bytes.
#[must_use]
pub fn test_signer() -> MemorySigner {
    MemorySigner::from_bytes(&[42u8; 32])
}

/// A spawner that doesn't actually spawn (for tests that don't need task execution).
#[derive(Debug, Clone, Copy, Default)]
pub struct TestSpawn;

impl Spawn<Sendable> for TestSpawn {
    fn spawn(&self, _fut: BoxFuture<'static, ()>) -> AbortHandle {
        let (handle, _reg) = AbortHandle::new_pair();
        handle
    }
}

impl Spawn<Local> for TestSpawn {
    fn spawn(&self, _fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        let (handle, _reg) = AbortHandle::new_pair();
        handle
    }
}

/// A spawner that uses `tokio::spawn` for tests that need actual task execution.
#[derive(Debug, Clone, Copy)]
pub struct TokioSpawn;

impl Spawn<Sendable> for TokioSpawn {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
        use futures::future::Abortable;
        let (handle, reg) = AbortHandle::new_pair();
        tokio::spawn(Abortable::new(fut, reg));
        handle
    }
}

impl Spawn<Local> for TokioSpawn {
    fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        use futures::future::Abortable;
        let (handle, reg) = AbortHandle::new_pair();
        tokio::task::spawn_local(Abortable::new(fut, reg));
        handle
    }
}

/// Create a new Subduction instance for testing with default settings.
#[allow(clippy::type_complexity)]
pub fn new_test_subduction() -> (
    Arc<
        Subduction<
            'static,
            Sendable,
            MemoryStorage,
            MockConnection,
            OpenPolicy,
            MemorySigner,
            CountLeadingZeroBytes,
        >,
    >,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
) {
    Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
        None,
        test_signer(),
        MemoryStorage::new(),
        OpenPolicy,
        NonceCache::default(),
        CountLeadingZeroBytes,
        ShardedMap::with_key(0, 0),
        TestSpawn,
        DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sedimentree_core::id::SedimentreeId;
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

    #[tokio::test]
    async fn test_channel_mock_connection_send_recv() -> TestResult {
        let peer_id = PeerId::new([1u8; 32]);
        let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);

        let msg = Message::BlobsRequest {
            id: SedimentreeId::new([0u8; 32]),
            digests: vec![],
        };
        Connection::<Sendable>::send(&conn, &msg).await?;
        let received = handle.outbound_rx.recv().await?;
        assert!(matches!(received, Message::BlobsRequest { .. }));

        let inject_msg = Message::BlobsResponse {
            id: SedimentreeId::new([0u8; 32]),
            blobs: vec![],
        };
        handle.inbound_tx.send(inject_msg).await?;
        let received = Connection::<Sendable>::recv(&conn).await?;
        assert!(matches!(received, Message::BlobsResponse { .. }));

        Ok(())
    }
}
