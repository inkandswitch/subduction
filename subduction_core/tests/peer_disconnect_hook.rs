//! `Handler::on_peer_disconnect` fires exactly once per peer departure, on
//! every disconnect path, and not while the peer still has a live connection.

#![allow(clippy::expect_used, clippy::panic)]

use core::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{depth::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    connection::{
        message::SyncMessage,
        test_utils::{ChannelTransport, InstantTimeout, TokioSpawn},
    },
    handler::{Handler, sync::SyncHandler},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{NoRemoteHeadsObserver, RemoteHeads, RemoteHeadsNotifier},
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<ChannelTransport>;

type Inner = SyncHandler<
    Sendable,
    MemoryStorage,
    Conn,
    OpenPolicy,
    CountLeadingZeroBytes,
    TokioSpawn,
    256,
    NoRemoteHeadsObserver,
>;

type Node = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        CountingDisconnects,
        OpenPolicy,
        MemorySigner,
        InstantTimeout,
        TokioSpawn,
    >,
>;

/// Delegates everything, counting disconnect notifications.
struct CountingDisconnects {
    inner: Arc<Inner>,
    disconnects: Arc<AtomicUsize>,
}

impl Handler<Sendable, Conn> for CountingDisconnects {
    type Message = SyncMessage;
    type HandlerError = <Inner as Handler<Sendable, Conn>>::HandlerError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Sendable>,
        message: Self::Message,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move { self.inner.handle(conn, message).await })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.inner.on_peer_disconnect(peer).await })
    }
}

impl RemoteHeadsNotifier<Sendable> for CountingDisconnects {
    fn notify_remote_heads(
        &self,
        id: SedimentreeId,
        peer: PeerId,
        heads: RemoteHeads,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.inner.notify_remote_heads(id, peer, heads).await })
    }

    fn forget_remote_heads(&self, id: SedimentreeId) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.inner.forget_remote_heads(id).await })
    }
}

fn node(seed: u8) -> (Node, Arc<AtomicUsize>) {
    let disconnects = Arc::new(AtomicUsize::new(0));
    let (sd, listener, manager, ()) = SubductionBuilder::new()
        .signer(MemorySigner::from_bytes(&[seed; 32]))
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(InstantTimeout)
        .heads_observer(NoRemoteHeadsObserver)
        .build_composed::<Sendable, Conn, CountingDisconnects, ()>(|sync_handler| {
            (
                Arc::new(CountingDisconnects {
                    inner: sync_handler,
                    disconnects: disconnects.clone(),
                }),
                (),
            )
        });

    tokio::spawn(listener);
    tokio::spawn(manager);
    (sd, disconnects)
}

const fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

/// Adds a connection to `peer`, returning it and the far end that keeps it alive.
async fn connect(
    sd: &Node,
    peer: PeerId,
) -> Result<(Authenticated<Conn, Sendable>, ChannelTransport), Box<dyn std::error::Error>> {
    let (near, far) = ChannelTransport::pair();
    let conn = Authenticated::new_for_test(MessageTransport::new(near), peer, Direction::Accepted);
    sd.add_connection(conn.clone()).await?;
    Ok((conn, far))
}

async fn wait_until(mut predicate: impl FnMut() -> bool, failure: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !predicate() {
        assert!(tokio::time::Instant::now() < deadline, "{failure}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn disconnect_from_peer_fires_once() -> TestResult {
    let (sd, disconnects) = node(1);
    let (_conn, _far) = connect(&sd, peer(2)).await?;

    sd.disconnect_from_peer(&peer(2)).await?;

    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn disconnect_connection_fires_once() -> TestResult {
    let (sd, disconnects) = node(3);
    let (conn, _far) = connect(&sd, peer(4)).await?;

    sd.disconnect(&conn).await?;

    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn disconnect_all_fires_once_per_peer() -> TestResult {
    let (sd, disconnects) = node(5);
    let (_c1, _f1) = connect(&sd, peer(6)).await?;
    let (_c2, _f2) = connect(&sd, peer(6)).await?;
    let (_c3, _f3) = connect(&sd, peer(7)).await?;

    sd.disconnect_all().await?;

    assert_eq!(
        disconnects.load(Ordering::SeqCst),
        2,
        "two peers, three connections"
    );
    Ok(())
}

/// A peer with two connections is still present after losing one.
#[tokio::test]
async fn not_fired_while_another_connection_is_live() -> TestResult {
    let (sd, disconnects) = node(8);
    let (first, _f1) = connect(&sd, peer(9)).await?;
    let (second, _f2) = connect(&sd, peer(9)).await?;

    sd.disconnect(&first).await?;
    assert_eq!(disconnects.load(Ordering::SeqCst), 0);

    sd.disconnect(&second).await?;
    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
    Ok(())
}

/// The listener's reactive removal goes through the same teardown, so a hook
/// in both layers would fire twice.
#[tokio::test]
async fn dropped_connection_fires_once() -> TestResult {
    let (sd, disconnects) = node(10);
    let (_conn, far) = connect(&sd, peer(11)).await?;

    drop(far);
    wait_until(
        || disconnects.load(Ordering::SeqCst) >= 1,
        "listener never noticed the dropped connection",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(disconnects.load(Ordering::SeqCst), 1);
    Ok(())
}
