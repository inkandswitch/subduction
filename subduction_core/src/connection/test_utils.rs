//! Test utilities for connection testing.
//!
//! This module provides mock connections and helpers for testing connection-related code.

use alloc::sync::Arc;
use core::{convert::Infallible, time::Duration};

use future_form::{FutureForm, Local, Sendable};
use futures::future::{AbortHandle, BoxFuture, LocalBoxFuture};
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    commit::CountLeadingZeroBytes,
};
use subduction_crypto::signer::memory::MemorySigner;

use super::{Connection, manager::Spawn, message::SyncMessage};
use crate::{
    authenticated::Authenticated,
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
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
        Authenticated::new_for_test(self, self.peer_id)
    }
}

impl Default for MockConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection<Sendable, SyncMessage> for MockConnection {
    type DisconnectionError = core::fmt::Error;
    type SendError = core::fmt::Error;
    type RecvError = core::fmt::Error;

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &SyncMessage,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
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
        Authenticated::new_for_test(self, self.peer_id)
    }
}

impl Default for FailingSendMockConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl Connection<Sendable, SyncMessage> for FailingSendMockConnection {
    type DisconnectionError = core::fmt::Error;
    type SendError = core::fmt::Error;
    type RecvError = core::fmt::Error;

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        _message: &SyncMessage,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
        Sendable::from_future(async { Err(core::fmt::Error) })
    }
}

/// A channel-based mock connection for diagnostic testing.
///
/// This mock uses async channels to simulate real message flow, allowing
/// precise control over when messages arrive and observation of timing.
///
/// Generic over message type `M`, so it works with [`SyncMessage`],
/// [`EphemeralMessage`](subduction_ephemeral::message::EphemeralMessage),
/// or any other wire message type.
#[derive(Debug)]
pub struct ChannelMockConnection<M> {
    peer_id: PeerId,

    /// Sender for outbound messages (from Subduction to "remote")
    outbound_tx: async_channel::Sender<M>,

    /// Receiver for inbound messages (from "remote" to Subduction)
    inbound_rx: async_channel::Receiver<M>,
}

impl<M> Clone for ChannelMockConnection<M> {
    fn clone(&self) -> Self {
        Self {
            peer_id: self.peer_id,
            outbound_tx: self.outbound_tx.clone(),
            inbound_rx: self.inbound_rx.clone(),
        }
    }
}

impl<M> PartialEq for ChannelMockConnection<M> {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id
    }
}

/// Handle for controlling a [`ChannelMockConnection`] from tests.
#[derive(Clone, Debug)]
pub struct ChannelMockConnectionHandle<M> {
    /// Receiver for outbound messages (messages Subduction sends)
    pub outbound_rx: async_channel::Receiver<M>,

    /// Sender for inbound messages (inject messages to Subduction)
    pub inbound_tx: async_channel::Sender<M>,
}

impl<M> ChannelMockConnection<M> {
    /// Create a new channel mock connection with its control handle.
    ///
    /// Returns the connection and a handle that tests can use to:
    /// - Inject messages via `handle.inbound_tx.send(msg)`
    /// - Observe sent messages via `handle.outbound_rx.recv()`
    #[must_use]
    pub fn new_with_handle(peer_id: PeerId) -> (Self, ChannelMockConnectionHandle<M>) {
        let (outbound_tx, outbound_rx) = async_channel::unbounded();
        let (inbound_tx, inbound_rx) = async_channel::unbounded();

        let conn = Self {
            peer_id,
            outbound_tx,
            inbound_rx,
        };

        let handle = ChannelMockConnectionHandle {
            outbound_rx,
            inbound_tx,
        };

        (conn, handle)
    }

    /// Create with default peer ID.
    #[must_use]
    pub fn new_default_with_handle() -> (Self, ChannelMockConnectionHandle<M>) {
        Self::new_with_handle(PeerId::new([0u8; 32]))
    }

    /// Wrap this connection in an `Authenticated` wrapper for testing.
    ///
    /// Uses the connection's peer ID as the authenticated identity.
    #[must_use]
    pub fn authenticated<K: FutureForm>(self) -> Authenticated<Self, K> {
        let peer_id = self.peer_id;
        Authenticated::new_for_test(self, peer_id)
    }
}

impl<M: Clone + Send + Encode + Decode + 'static> Connection<Sendable, M>
    for ChannelMockConnection<M>
{
    type DisconnectionError = Infallible;
    type SendError = async_channel::SendError<M>;
    type RecvError = async_channel::RecvError;

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Sendable::from_future(async { Ok(()) })
    }

    fn send(
        &self,
        message: &M,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        Sendable::from_future(async move { tx.send(message).await })
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<M, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        Sendable::from_future(async move { rx.recv().await })
    }
}

impl<M: Clone + Encode + Decode + 'static> Connection<Local, M> for ChannelMockConnection<M> {
    type DisconnectionError = Infallible;
    type SendError = async_channel::SendError<M>;
    type RecvError = async_channel::RecvError;

    fn disconnect(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        Local::from_future(async { Ok(()) })
    }

    fn send(&self, message: &M) -> <Local as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        let tx = self.outbound_tx.clone();
        let message = message.clone();
        Local::from_future(async move { tx.send(message).await })
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<M, Self::RecvError>> {
        let rx = self.inbound_rx.clone();
        Local::from_future(async move { rx.recv().await })
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

impl<C: Connection<Sendable, SyncMessage> + Send> Connection<Sendable, SyncMessage>
    for CallbackOnRecvConnection<C>
where
    C::RecvError: Send,
{
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;

    fn disconnect(
        &self,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &SyncMessage,
    ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Sendable as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
        // Note: This mimics Wasm behavior where callbacks would fire here,
        // but we can't actually fire callbacks here without access to Subduction.
        // The test will inject a callback-like check separately.
        self.inner.recv()
    }
}

impl<C: Connection<Local, SyncMessage>> Connection<Local, SyncMessage>
    for CallbackOnRecvConnection<C>
{
    type DisconnectionError = C::DisconnectionError;
    type SendError = C::SendError;
    type RecvError = C::RecvError;

    fn disconnect(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::DisconnectionError>> {
        self.inner.disconnect()
    }

    fn send(
        &self,
        message: &SyncMessage,
    ) -> <Local as FutureForm>::Future<'_, Result<(), Self::SendError>> {
        self.inner.send(message)
    }

    fn recv(&self) -> <Local as FutureForm>::Future<'_, Result<SyncMessage, Self::RecvError>> {
        self.inner.recv()
    }
}

// ── Transport mocks ─────────────────────────────────────────────────────

/// A channel-based [`Transport`](crate::transport::Transport) mock for testing.
///
/// Bytes written via `send_bytes` appear on the paired `ChannelTransport`'s
/// `recv_bytes`, and vice versa.
///
/// # Construction
///
/// ```ignore
/// let (a, b) = ChannelTransport::pair();
/// // a.send_bytes(bytes) → b.recv_bytes()
/// // b.send_bytes(bytes) → a.recv_bytes()
/// ```
#[derive(Debug, Clone)]
pub struct ChannelTransport {
    tx: async_channel::Sender<Vec<u8>>,
    rx: async_channel::Receiver<Vec<u8>>,
}

impl ChannelTransport {
    /// Create a bidirectional pair of transports.
    #[must_use]
    pub fn pair() -> (Self, Self) {
        let (tx_a, rx_a) = async_channel::bounded(64);
        let (tx_b, rx_b) = async_channel::bounded(64);
        (Self { tx: tx_a, rx: rx_b }, Self { tx: tx_b, rx: rx_a })
    }
}

impl PartialEq for ChannelTransport {
    fn eq(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx) && self.rx.same_channel(&other.rx)
    }
}

/// Error from a [`ChannelTransport`] operation.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("channel closed")]
pub struct ChannelClosed;

impl crate::transport::Transport<Sendable> for ChannelTransport {
    type SendError = ChannelClosed;
    type RecvError = ChannelClosed;
    type DisconnectionError = core::convert::Infallible;

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let data = bytes.to_vec();
        let tx = self.tx.clone();
        Box::pin(async move { tx.send(data).await.map_err(|_| ChannelClosed) })
    }

    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        let rx = self.rx.clone();
        Box::pin(async move { rx.recv().await.map_err(|_| ChannelClosed) })
    }

    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A [`Timeout`](crate::timeout::Timeout) that never times out.
///
/// Returns the inner future's result directly, ignoring the duration.
/// Useful for deterministic testing where timeouts should not fire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InstantTimeout;

impl crate::timeout::Timeout<Sendable> for InstantTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        _dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, crate::timeout::TimedOut>> {
        Box::pin(async move { Ok(fut.await) })
    }
}

impl crate::timeout::Timeout<Local> for InstantTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        _dur: Duration,
        fut: LocalBoxFuture<'a, T>,
    ) -> LocalBoxFuture<'a, Result<T, crate::timeout::TimedOut>> {
        Box::pin(async move { Ok(fut.await) })
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
            SyncHandler<Sendable, MemoryStorage, MockConnection, OpenPolicy, CountLeadingZeroBytes>,
            OpenPolicy,
            MemorySigner,
            InstantTimeout,
            CountLeadingZeroBytes,
        >,
    >,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
    impl core::future::Future<Output = Result<(), futures::future::Aborted>>,
) {
    let (sd, _handler, listener, manager) = SubductionBuilder::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TestSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, MockConnection>();

    (sd, listener, manager)
}
