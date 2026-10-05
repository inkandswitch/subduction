//! Failure removal uses the existing borrowed, potentially lazy async disconnect.
//! Slow cleanup must not stop the listener, and explicit cleanup must not be doubled.

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::id::SedimentreeId;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        Connection,
        message::{RemoveSubscriptions, SyncMessage},
        test_utils::{ChannelClosed, ChannelTransport, TokioSpawn, TokioTimeout},
    },
    handler::Handler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder, error::ListenError},
    transport::{Transport, message::MessageTransport},
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

#[derive(Debug, thiserror::Error)]
#[error("cleanup failed")]
struct CleanupFailed;

#[derive(Debug, Clone)]
struct SlowTransport {
    io: ChannelTransport,
    calls: Arc<AtomicUsize>,
    started: async_channel::Sender<()>,
    stopped: async_channel::Receiver<()>,
    finish: async_channel::Receiver<()>,
    finished: Arc<AtomicBool>,
    fail: bool,
}
impl PartialEq for SlowTransport {
    fn eq(&self, other: &Self) -> bool {
        self.io == other.io
    }
}
impl Transport<Sendable> for SlowTransport {
    type SendError = ChannelClosed;
    type RecvError = ChannelClosed;
    type DisconnectionError = CleanupFailed;

    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), ChannelClosed>> {
        self.io.send_bytes(bytes)
    }
    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, ChannelClosed>> {
        Box::pin(async move {
            tokio::select! {
                result = self.io.recv_bytes() => result,
                _closed = self.stopped.recv() => Err(ChannelClosed),
            }
        })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), CleanupFailed>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.close();
            let _closed = self.finish.recv().await;
            self.finished.store(true, Ordering::SeqCst);
            if self.fail {
                Err(CleanupFailed)
            } else {
                Ok(())
            }
        })
    }
}
type Conn = MessageTransport<SlowTransport>;
struct Owner {
    remote: ChannelTransport,
    started: async_channel::Receiver<()>,
    finish: async_channel::Sender<()>,
}
impl Owner {
    fn finish(&self) {
        self.finish.close();
    }
    async fn started(&self) -> TestResult {
        assert!(
            tokio::time::timeout(Duration::from_secs(3), self.started.recv())
                .await?
                .is_err()
        );
        Ok(())
    }
}
fn connection(peer: u8, fail: bool) -> (Authenticated<Conn, Sendable>, Owner) {
    let (io, remote) = ChannelTransport::pair();
    let (started, stopped) = async_channel::bounded(1);
    let (finish_tx, finish) = async_channel::bounded(1);
    let transport = SlowTransport {
        io,
        calls: Arc::new(AtomicUsize::new(0)),
        started,
        stopped: stopped.clone(),
        finish,
        finished: Arc::new(AtomicBool::new(false)),
        fail,
    };
    (
        Authenticated::new_for_test(
            MessageTransport::new(transport),
            PeerId::new([peer; 32]),
            Direction::Dialed,
        ),
        Owner {
            remote,
            started: stopped,
            finish: finish_tx,
        },
    )
}
fn calls(conn: &Authenticated<Conn, Sendable>) -> usize {
    conn.inner().inner().calls.load(Ordering::SeqCst)
}

#[derive(Debug)]
struct RejectMessages;
impl Handler<Sendable, Conn> for RejectMessages {
    type Message = SyncMessage;
    type HandlerError = ListenError<Sendable, MemoryStorage, Conn, SyncMessage>;
    fn handle<'a>(
        &'a self,
        _conn: &'a Authenticated<Conn, Sendable>,
        _message: SyncMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async { Err(ListenError::TrySendError) })
    }
    fn on_peer_disconnect(&self, _peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}
impl RemoteHeadsNotifier for RejectMessages {
    fn notify_remote_heads(&self, _id: SedimentreeId, _peer: PeerId, _heads: RemoteHeads) {}
}
type Node = Subduction<
    'static,
    Sendable,
    MemoryStorage,
    Conn,
    RejectMessages,
    OpenPolicy,
    MemorySigner,
    TokioTimeout,
    TokioSpawn,
>;
struct TestNode {
    core: Arc<Node>,
    listener: tokio::task::JoinHandle<()>,
    manager: tokio::task::JoinHandle<()>,
}
impl TestNode {
    fn new() -> Self {
        let (core, listener, manager) = SubductionBuilder::new()
            .signer(MemorySigner::from_bytes(&[1; 32]))
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TokioSpawn)
            .timer(TokioTimeout)
            .build_with_handler::<Sendable, Conn, _>(Arc::new(RejectMessages));
        Self {
            core,
            listener: tokio::spawn(async move {
                let _result = listener.await;
            }),
            manager: tokio::spawn(async move {
                let _result = manager.await;
            }),
        }
    }
}
impl Drop for TestNode {
    fn drop(&mut self) {
        self.core.shutdown();
        self.listener.abort();
        self.manager.abort();
    }
}
async fn finished(conn: &Authenticated<Conn, Sendable>) -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !conn.inner().inner().finished.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn transport_disconnect_can_still_be_lazy_and_borrowed() -> TestResult {
    let (conn, owner) = connection(2, false);
    let waiter = Connection::<Sendable, SyncMessage>::disconnect(&conn);
    assert!(!owner.started.is_closed());
    drop(waiter);
    assert_eq!(calls(&conn), 0);
    owner.finish();
    Connection::<Sendable, SyncMessage>::disconnect(&conn).await?;
    assert_eq!(calls(&conn), 1);
    Ok(())
}

#[tokio::test]
async fn failure_removal_schedules_once_without_waiting_or_touching_sibling() -> TestResult {
    let node = TestNode::new();
    let (a, owner_a) = connection(2, true);
    let (b, owner_b) = connection(2, false);
    let (unknown, _owner_unknown) = connection(2, false);
    node.core.add_connection(a.clone()).await?;
    node.core.add_connection(b.clone()).await?;
    assert_eq!(node.core.remove_connection_for_test(&a).await, Some(false));
    owner_a.started().await?;
    assert!(!a.inner().inner().finished.load(Ordering::SeqCst));
    assert_eq!(calls(&b), 0);
    assert_eq!(node.core.remove_connection_for_test(&a).await, None);
    assert_eq!(node.core.remove_connection_for_test(&unknown).await, None);
    assert!(!node.core.disconnect(&unknown).await?);
    assert_eq!(calls(&unknown), 0);
    assert_eq!(calls(&a), 1);
    owner_a.finish();
    finished(&a).await?;
    owner_b.finish();
    node.core.disconnect_all().await?;
    assert_eq!(calls(&b), 1);
    Ok(())
}

#[tokio::test]
async fn explicit_disconnect_still_waits_and_returns_error_without_duplicate_task() -> TestResult {
    let node = TestNode::new();
    let (conn, owner) = connection(2, true);
    node.core.add_connection(conn.clone()).await?;
    let mut waiter = Box::pin(node.core.disconnect(&conn));
    assert!(futures::poll!(&mut waiter).is_pending());
    assert!(owner.started.is_closed());
    assert_eq!(calls(&conn), 1);
    owner.finish();
    assert!(waiter.await.is_err());
    assert!(!node.core.disconnect(&conn).await?);
    assert_eq!(calls(&conn), 1);
    Ok(())
}

#[tokio::test]
async fn explicit_bulk_apis_do_not_schedule_duplicate_cleanup() -> TestResult {
    for per_peer in [false, true] {
        let node = TestNode::new();
        let (a, owner_a) = connection(2, false);
        let (b, owner_b) = connection(2, false);
        node.core.add_connection(a.clone()).await?;
        node.core.add_connection(b.clone()).await?;
        owner_a.finish();
        owner_b.finish();
        if per_peer {
            assert!(node.core.disconnect_from_peer(&a.peer_id()).await?);
        } else {
            node.core.disconnect_all().await?;
        }
        assert_eq!(calls(&a), 1);
        assert_eq!(calls(&b), 1);
    }
    Ok(())
}

async fn protocol_failure(bytes: &[u8]) -> TestResult {
    let node = TestNode::new();
    let (failed, owner) = connection(2, false);
    let (sibling, sibling_owner) = connection(2, false);
    let (other, other_owner) = connection(3, false);
    for conn in [&failed, &sibling, &other] {
        node.core.add_connection(conn.clone()).await?;
    }
    // Byte I/O succeeds; decoding or handling above the transport fails.
    owner.remote.send_bytes(bytes).await?;
    owner.started().await?;
    assert_eq!(calls(&failed), 1);
    assert_eq!(calls(&sibling), 0);
    assert_eq!(node.core.connection_count(&failed.peer_id()).await, 1);
    // A blocked cleanup must not prevent the listener from removing another
    // peer's failed connection. Waiting inline in remove_connection deadlocks.
    other_owner.remote.send_bytes(bytes).await?;
    other_owner.started().await?;
    assert_eq!(calls(&other), 1);
    assert!(!failed.inner().inner().finished.load(Ordering::SeqCst));
    owner.finish();
    other_owner.finish();
    sibling_owner.finish();
    finished(&failed).await?;
    finished(&other).await?;
    node.core.disconnect_all().await?;
    Ok(())
}
#[tokio::test]
async fn decode_failure_disconnects_without_stalling_listener() -> TestResult {
    protocol_failure(&[0xff, 0xff, 0xff]).await
}
#[tokio::test]
async fn handler_failure_disconnects_without_stalling_listener() -> TestResult {
    protocol_failure(
        &SyncMessage::RemoveSubscriptions(RemoveSubscriptions {
            ids: vec![SedimentreeId::new([7; 32])],
        })
        .encode(),
    )
    .await
}
