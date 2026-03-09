//! Integration tests for [`ComposedHandler`].
//!
//! Verifies that `WireMessage` variants are dispatched to the correct
//! sub-handler and that `on_peer_disconnect` reaches both.
//!
//! # Connection type note
//!
//! `ChannelMockConnection<WireMessage>` only implements
//! `Connection<K, WireMessage>`. The `EphemeralHandler` fan-out path
//! requires `Connection<K, EphemeralMessage>`, so fan-out through the
//! composed handler cannot be tested with this mock. Fan-out is
//! thoroughly tested in `ephemeral_handler.rs` with the correct
//! connection type.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::ref_as_ptr
)]

use std::sync::Arc;

use async_lock::Mutex;
use future_form::{FutureForm, Sendable};
use nonempty::NonEmpty;
use sedimentree_core::{collections::Map, id::SedimentreeId};
use subduction_core::{
    connection::{
        authenticated::Authenticated,
        message::{RemoveSubscriptions, SyncMessage},
        test_utils::ChannelMockConnection,
    },
    handler::Handler,
    peer::id::PeerId,
};
use subduction_ephemeral::{
    composed::ComposedHandler, message::EphemeralMessage, wire::WireMessage,
};
use testresult::TestResult;

// ── Types ───────────────────────────────────────────────────────────────

type WireConn = ChannelMockConnection<WireMessage>;
type WireAuth = Authenticated<WireConn, Sendable>;
type Connections = Arc<Mutex<Map<PeerId, NonEmpty<WireAuth>>>>;

// ── Helpers ─────────────────────────────────────────────────────────────

const fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

const fn topic(n: u8) -> SedimentreeId {
    SedimentreeId::new([n; 32])
}

async fn register_peer(
    connections: &Connections,
    peer_id: PeerId,
) -> (
    WireAuth,
    subduction_core::connection::test_utils::ChannelMockConnectionHandle<WireMessage>,
) {
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    let auth = Authenticated::new_for_test(conn);
    connections
        .lock()
        .await
        .insert(peer_id, NonEmpty::new(auth.clone()));
    (auth, handle)
}

// ── Mock Sync Handler ───────────────────────────────────────────────────

/// Minimal sync handler that records received messages for assertion.
struct MockSyncHandler {
    received: Mutex<Vec<SyncMessage>>,
    disconnected: Mutex<Vec<PeerId>>,
}

impl MockSyncHandler {
    fn new() -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            disconnected: Mutex::new(Vec::new()),
        }
    }
}

impl core::fmt::Debug for MockSyncHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MockSyncHandler").finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct MockSyncError;

impl core::fmt::Display for MockSyncError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "mock sync error")
    }
}

impl core::error::Error for MockSyncError {}

impl Handler<Sendable, WireConn> for MockSyncHandler {
    type Message = SyncMessage;
    type HandlerError = MockSyncError;

    fn handle<'a>(
        &'a self,
        _conn: &'a WireAuth,
        message: SyncMessage,
    ) -> <Sendable as future_form::FutureForm>::Future<'a, Result<(), Self::HandlerError>> {
        Sendable::from_future(async move {
            self.received.lock().await.push(message);
            Ok(())
        })
    }

    fn on_peer_disconnect(
        &self,
        peer: PeerId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, ()> {
        Sendable::from_future(async move {
            self.disconnected.lock().await.push(peer);
        })
    }
}

/// Minimal ephemeral handler that records received messages.
///
/// We use this instead of the real `EphemeralHandler` because the real
/// one's `Handler` impl requires `C: Connection<K, EphemeralMessage>`,
/// which `ChannelMockConnection<WireMessage>` does not satisfy.
struct MockEphemeralHandler {
    received: Mutex<Vec<EphemeralMessage>>,
    disconnected: Mutex<Vec<PeerId>>,
}

impl MockEphemeralHandler {
    fn new() -> Self {
        Self {
            received: Mutex::new(Vec::new()),
            disconnected: Mutex::new(Vec::new()),
        }
    }
}

impl core::fmt::Debug for MockEphemeralHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MockEphemeralHandler")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct MockEphemeralError;

impl core::fmt::Display for MockEphemeralError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "mock ephemeral error")
    }
}

impl core::error::Error for MockEphemeralError {}

impl Handler<Sendable, WireConn> for MockEphemeralHandler {
    type Message = EphemeralMessage;
    type HandlerError = MockEphemeralError;

    fn handle<'a>(
        &'a self,
        _conn: &'a WireAuth,
        message: EphemeralMessage,
    ) -> <Sendable as future_form::FutureForm>::Future<'a, Result<(), Self::HandlerError>> {
        Sendable::from_future(async move {
            self.received.lock().await.push(message);
            Ok(())
        })
    }

    fn on_peer_disconnect(
        &self,
        peer: PeerId,
    ) -> <Sendable as future_form::FutureForm>::Future<'_, ()> {
        Sendable::from_future(async move {
            self.disconnected.lock().await.push(peer);
        })
    }
}

// ── Dispatch routing ────────────────────────────────────────────────────

#[tokio::test]
async fn sync_message_routes_to_sync_handler() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (auth, _handle) = register_peer(&connections, peer(1)).await;

    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    let sync_msg = SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
        ids: vec![topic(0xAA)],
    });
    composed
        .handle(&auth, WireMessage::from(sync_msg.clone()))
        .await?;

    let sync_received = sync_handler.received.lock().await;
    assert_eq!(sync_received.len(), 1);
    assert_eq!(sync_received[0], sync_msg);

    assert!(eph_handler.received.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn ephemeral_message_routes_to_ephemeral_handler() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (auth, _handle) = register_peer(&connections, peer(1)).await;

    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    let eph_msg = EphemeralMessage::Ephemeral {
        id: topic(0xBB),
        payload: vec![1, 2, 3],
    };
    composed
        .handle(&auth, WireMessage::Ephemeral(eph_msg.clone()))
        .await?;

    let eph_received = eph_handler.received.lock().await;
    assert_eq!(eph_received.len(), 1);
    assert_eq!(eph_received[0], eph_msg);

    assert!(sync_handler.received.lock().await.is_empty());
    Ok(())
}

#[tokio::test]
async fn subscribe_routes_to_ephemeral_handler() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (auth, _handle) = register_peer(&connections, peer(1)).await;

    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    let sub_msg = EphemeralMessage::Subscribe {
        ids: vec![topic(0xCC)],
    };
    composed
        .handle(&auth, WireMessage::Ephemeral(sub_msg.clone()))
        .await?;

    let eph_received = eph_handler.received.lock().await;
    assert_eq!(eph_received.len(), 1);
    assert_eq!(eph_received[0], sub_msg);

    assert!(sync_handler.received.lock().await.is_empty());
    Ok(())
}

// ── on_peer_disconnect ──────────────────────────────────────────────────

#[tokio::test]
async fn disconnect_calls_both_handlers() -> TestResult {
    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    Handler::<Sendable, WireConn>::on_peer_disconnect(&composed, peer(1)).await;

    let sync_disc = sync_handler.disconnected.lock().await;
    assert_eq!(sync_disc.len(), 1);
    assert_eq!(sync_disc[0], peer(1));

    let eph_disc = eph_handler.disconnected.lock().await;
    assert_eq!(eph_disc.len(), 1);
    assert_eq!(eph_disc[0], peer(1));

    Ok(())
}

#[tokio::test]
async fn disconnect_multiple_peers() -> TestResult {
    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    for i in 1..=3 {
        Handler::<Sendable, WireConn>::on_peer_disconnect(&composed, peer(i)).await;
    }

    let sync_disc = sync_handler.disconnected.lock().await;
    assert_eq!(sync_disc.len(), 3);

    let eph_disc = eph_handler.disconnected.lock().await;
    assert_eq!(eph_disc.len(), 3);

    Ok(())
}

// ── Mixed traffic ───────────────────────────────────────────────────────

#[tokio::test]
async fn interleaved_sync_and_ephemeral_messages() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (auth, _handle) = register_peer(&connections, peer(1)).await;

    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    let messages: Vec<WireMessage> = vec![
        WireMessage::from(SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
            ids: vec![topic(1)],
        })),
        WireMessage::Ephemeral(EphemeralMessage::Ephemeral {
            id: topic(2),
            payload: vec![10],
        }),
        WireMessage::from(SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
            ids: vec![topic(3)],
        })),
        WireMessage::Ephemeral(EphemeralMessage::Subscribe {
            ids: vec![topic(4)],
        }),
    ];

    for msg in messages {
        composed.handle(&auth, msg).await?;
    }

    assert_eq!(sync_handler.received.lock().await.len(), 2);
    assert_eq!(eph_handler.received.lock().await.len(), 2);

    Ok(())
}

// ── Accessor methods ────────────────────────────────────────────────────

#[test]
fn accessor_returns_correct_sub_handlers() {
    let sync_handler = Arc::new(MockSyncHandler::new());
    let eph_handler = Arc::new(MockEphemeralHandler::new());
    let composed = ComposedHandler::new(sync_handler.clone(), eph_handler.clone());

    // Verify accessors return references to the same objects.
    assert!(std::ptr::eq(
        composed.sync() as *const MockSyncHandler,
        sync_handler.as_ref() as *const MockSyncHandler
    ));
    assert!(std::ptr::eq(
        composed.ephemeral() as *const MockEphemeralHandler,
        eph_handler.as_ref() as *const MockEphemeralHandler
    ));
}
