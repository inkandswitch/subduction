//! Unit tests for [`ComposedHandler`].
//!
//! Uses lightweight mock handlers to verify dispatch, `as_batch_sync_response`
//! extraction, and `on_peer_disconnect` fan-out.

#![allow(clippy::expect_used, clippy::panic, clippy::indexing_slicing)]

use future_form::Sendable;
use sedimentree_core::{
    codec::{
        decode::Decode,
        encode::Encode,
        error::{DecodeError, InvalidSchema},
    },
    id::SedimentreeId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        message::{BatchSyncResponse, RequestId, SyncMessage, SyncResult},
        test_utils::ChannelMockConnection,
    },
    handler::Handler,
    peer::id::PeerId,
    remote_heads::RemoteHeads,
};
use subduction_ephemeral::{
    composed::{ComposedHandler, Dispatched, WireEnvelope},
    message::EphemeralMessage,
};
use testresult::TestResult;

// ── Test wire message ───────────────────────────────────────────────────

/// Minimal wire envelope that wraps sync and ephemeral messages.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TestWireMessage {
    Sync(Box<SyncMessage>),
    Ephemeral(EphemeralMessage),
}

impl From<SyncMessage> for TestWireMessage {
    fn from(msg: SyncMessage) -> Self {
        Self::Sync(Box::new(msg))
    }
}

impl From<EphemeralMessage> for TestWireMessage {
    fn from(msg: EphemeralMessage) -> Self {
        Self::Ephemeral(msg)
    }
}

/// Schema header for test wire messages — reuses the sync schema for the
/// Sync variant and ephemeral schema for the Ephemeral variant.
impl Encode for TestWireMessage {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::Sync(msg) => Encode::encode(msg.as_ref()),
            Self::Ephemeral(msg) => msg.encode(),
        }
    }

    fn encoded_size(&self) -> usize {
        match self {
            Self::Sync(msg) => msg.encoded_size(),
            Self::Ephemeral(msg) => msg.encoded_size(),
        }
    }
}

impl Decode for TestWireMessage {
    const MIN_SIZE: usize = 8;

    fn try_decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < 4 {
            return Err(DecodeError::MessageTooShort {
                type_name: "TestWireMessage schema",
                need: 4,
                have: buf.len(),
            });
        }

        let schema: [u8; 4] = buf[0..4].try_into().expect("4 bytes");

        match schema {
            subduction_core::connection::message::MESSAGE_SCHEMA => {
                SyncMessage::try_decode(buf).map(|m| TestWireMessage::Sync(Box::new(m)))
            }
            subduction_ephemeral::message::EPHEMERAL_SCHEMA => {
                EphemeralMessage::try_decode(buf).map(TestWireMessage::Ephemeral)
            }
            _ => Err(InvalidSchema {
                expected: subduction_core::connection::message::MESSAGE_SCHEMA,
                got: schema,
            }
            .into()),
        }
    }
}

impl WireEnvelope for TestWireMessage {
    fn dispatch(self) -> Dispatched {
        match self {
            Self::Sync(msg) => Dispatched::Sync(msg),
            Self::Ephemeral(msg) => Dispatched::Ephemeral(msg),
        }
    }

    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse> {
        match self {
            Self::Sync(msg) => match msg.as_ref() {
                SyncMessage::BatchSyncResponse(resp) => Some(resp),
                SyncMessage::BatchSyncRequest(_)
                | SyncMessage::BlobsRequest { .. }
                | SyncMessage::BlobsResponse { .. }
                | SyncMessage::DataRequestRejected(_)
                | SyncMessage::Fragment { .. }
                | SyncMessage::LooseCommit { .. }
                | SyncMessage::RemoveSubscriptions(_)
                | SyncMessage::HeadsUpdate { .. } => None,
            },
            Self::Ephemeral(_) => None,
        }
    }
}

// ── Mock handlers ───────────────────────────────────────────────────────

type TestConn = ChannelMockConnection<TestWireMessage>;

/// A handler that tracks dispatched sync messages and disconnect calls.
#[derive(Debug)]
struct TrackingSyncHandler {
    dispatched: async_channel::Sender<SyncMessage>,
    disconnected: async_channel::Sender<PeerId>,
}

impl Handler<Sendable, TestConn> for TrackingSyncHandler {
    type Message = SyncMessage;
    type HandlerError = std::convert::Infallible;

    fn handle<'a>(
        &'a self,
        _conn: &'a Authenticated<TestConn, Sendable>,
        message: SyncMessage,
    ) -> futures::future::BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            self.dispatched
                .send(message)
                .await
                .expect("tracking channel open");
            Ok(())
        })
    }

    fn as_batch_sync_response(msg: &SyncMessage) -> Option<&BatchSyncResponse> {
        match msg {
            SyncMessage::BatchSyncResponse(resp) => Some(resp),
            SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::DataRequestRejected(_)
            | SyncMessage::Fragment { .. }
            | SyncMessage::LooseCommit { .. }
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::HeadsUpdate { .. } => None,
        }
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            self.disconnected
                .send(peer)
                .await
                .expect("tracking channel open");
        })
    }
}

/// A handler that tracks dispatched ephemeral messages and disconnect calls.
#[derive(Debug)]
struct TrackingEphemeralHandler {
    dispatched: async_channel::Sender<EphemeralMessage>,
    disconnected: async_channel::Sender<PeerId>,
}

impl Handler<Sendable, TestConn> for TrackingEphemeralHandler {
    type Message = EphemeralMessage;
    type HandlerError = std::convert::Infallible;

    fn handle<'a>(
        &'a self,
        _conn: &'a Authenticated<TestConn, Sendable>,
        message: EphemeralMessage,
    ) -> futures::future::BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            self.dispatched
                .send(message)
                .await
                .expect("tracking channel open");
            Ok(())
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> futures::future::BoxFuture<'_, ()> {
        Box::pin(async move {
            self.disconnected
                .send(peer)
                .await
                .expect("tracking channel open");
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

const fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

#[allow(clippy::type_complexity)]
fn make_handler() -> (
    ComposedHandler<TrackingSyncHandler, TrackingEphemeralHandler, TestWireMessage>,
    async_channel::Receiver<SyncMessage>,
    async_channel::Receiver<EphemeralMessage>,
    async_channel::Receiver<PeerId>,
    async_channel::Receiver<PeerId>,
) {
    let (sync_tx, sync_rx) = async_channel::unbounded();
    let (sync_disc_tx, sync_disc_rx) = async_channel::unbounded();
    let (eph_tx, eph_rx) = async_channel::unbounded();
    let (eph_disc_tx, eph_disc_rx) = async_channel::unbounded();

    let sync_handler = TrackingSyncHandler {
        dispatched: sync_tx,
        disconnected: sync_disc_tx,
    };
    let eph_handler = TrackingEphemeralHandler {
        dispatched: eph_tx,
        disconnected: eph_disc_tx,
    };

    let composed = ComposedHandler::new(sync_handler, eph_handler);
    (composed, sync_rx, eph_rx, sync_disc_rx, eph_disc_rx)
}

fn make_auth_conn(peer_id: PeerId) -> Authenticated<TestConn, Sendable> {
    let (conn, _handle) = ChannelMockConnection::new_with_handle(peer_id);
    Authenticated::new_for_test(conn, peer_id)
}

const fn test_request_id() -> RequestId {
    RequestId {
        requestor: peer(1),
        nonce: 42,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn dispatch_sync_message_to_sync_handler() -> TestResult {
    let (composed, sync_rx, eph_rx, _, _) = make_handler();
    let auth = make_auth_conn(peer(1));

    let sync_msg = SyncMessage::BlobsRequest {
        id: SedimentreeId::new([0xAA; 32]),
        digests: vec![],
    };
    let wire: TestWireMessage = sync_msg.clone().into();

    Handler::<Sendable, TestConn>::handle(&composed, &auth, wire).await?;

    let received = sync_rx.try_recv().expect("sync handler should receive");
    assert_eq!(received, sync_msg);
    assert!(
        eph_rx.try_recv().is_err(),
        "ephemeral handler should not receive"
    );

    Ok(())
}

#[tokio::test]
async fn dispatch_ephemeral_message_to_ephemeral_handler() -> TestResult {
    let (composed, sync_rx, eph_rx, _, _) = make_handler();
    let auth = make_auth_conn(peer(1));

    let eph_msg = EphemeralMessage::Ephemeral {
        id: SedimentreeId::new([0xBB; 32]),
        payload: vec![1, 2, 3],
    };
    let wire: TestWireMessage = eph_msg.clone().into();

    Handler::<Sendable, TestConn>::handle(&composed, &auth, wire).await?;

    let received = eph_rx.try_recv().expect("ephemeral handler should receive");
    assert_eq!(received, eph_msg);
    assert!(
        sync_rx.try_recv().is_err(),
        "sync handler should not receive"
    );

    Ok(())
}

#[test]
fn as_batch_sync_response_extracts_from_sync() {
    let req_id = test_request_id();
    let resp = BatchSyncResponse {
        req_id,
        id: SedimentreeId::new([1; 32]),
        result: SyncResult::NotFound,
        responder_heads: RemoteHeads::default(),
    };

    let wire = TestWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp.clone())));
    let extracted =
        <ComposedHandler<TrackingSyncHandler, TrackingEphemeralHandler, TestWireMessage> as Handler<
            Sendable,
            TestConn,
        >>::as_batch_sync_response(&wire);

    assert_eq!(extracted, Some(&resp));
}

#[test]
fn as_batch_sync_response_returns_none_for_other_sync() {
    let wire = TestWireMessage::Sync(Box::new(SyncMessage::BlobsRequest {
        id: SedimentreeId::new([0xCC; 32]),
        digests: vec![],
    }));

    let extracted =
        <ComposedHandler<TrackingSyncHandler, TrackingEphemeralHandler, TestWireMessage> as Handler<
            Sendable,
            TestConn,
        >>::as_batch_sync_response(&wire);

    assert_eq!(extracted, None);
}

#[test]
fn as_batch_sync_response_returns_none_for_ephemeral() {
    let wire = TestWireMessage::Ephemeral(EphemeralMessage::Subscribe {
        ids: vec![SedimentreeId::new([0xDD; 32])],
    });

    let extracted =
        <ComposedHandler<TrackingSyncHandler, TrackingEphemeralHandler, TestWireMessage> as Handler<
            Sendable,
            TestConn,
        >>::as_batch_sync_response(&wire);

    assert_eq!(extracted, None);
}

#[tokio::test]
async fn on_peer_disconnect_calls_both_handlers() {
    let (composed, _, _, sync_disc_rx, eph_disc_rx) = make_handler();
    let peer_id = peer(7);

    Handler::<Sendable, TestConn>::on_peer_disconnect(&composed, peer_id).await;

    let sync_peer = sync_disc_rx
        .try_recv()
        .expect("sync handler should receive disconnect");
    let eph_peer = eph_disc_rx
        .try_recv()
        .expect("ephemeral handler should receive disconnect");

    assert_eq!(sync_peer, peer_id);
    assert_eq!(eph_peer, peer_id);
}
