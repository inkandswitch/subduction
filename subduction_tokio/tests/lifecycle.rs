//! Managed lifecycle tests use distinct concrete backends and deterministic faults.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::{
    blob::{Blob, BlobMeta},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use std::{
    collections::VecDeque,
    future::IntoFuture,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use subduction_core::{
    authenticated::{Authenticated, Direction},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    transport::Transport,
};
use subduction_crypto::signer::memory::MemorySigner;
use subduction_tokio::{
    AttemptScope, AuthenticationContext, CallTimeout, ClientStatus, ConnectionStatus, Connector,
    Error, ErrorKind, EstablishedConnection, ExpectedPeer, ReconnectPolicy, TokioClient,
};
use tokio::time::timeout;

#[path = "lifecycle/failures.rs"]
mod failures;
#[path = "lifecycle/registration.rs"]
mod registration;

const BUDGET: Duration = Duration::from_secs(3);
fn signer(n: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[n; 32])
}
fn peer(n: u8) -> PeerId {
    PeerId::from(signer(n).verifying_key())
}
fn client(n: u8) -> TokioClient {
    TokioClient::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(signer(n))
        .start()
        .unwrap()
}

#[derive(Clone, Debug)]
struct Channel<const BACKEND: u8> {
    tx: async_channel::Sender<Vec<u8>>,
    rx: async_channel::Receiver<Vec<u8>>,
}
impl<const N: u8> PartialEq for Channel<N> {
    fn eq(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }
}
impl<const N: u8> Transport<Sendable> for Channel<N> {
    type SendError = std::io::Error;
    type RecvError = std::io::Error;
    type DisconnectionError = std::io::Error;
    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        let bytes = bytes.to_vec();
        Box::pin(async move { self.tx.send(bytes).await.map_err(std::io::Error::other) })
    }
    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        Box::pin(async move { self.rx.recv().await.map_err(std::io::Error::other) })
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async move {
            self.tx.close();
            self.rx.close();
            Ok(())
        })
    }
}
fn pair<const A: u8, const B: u8>() -> (Channel<A>, Channel<B>) {
    let (tx1, rx1) = async_channel::bounded(16);
    let (tx2, rx2) = async_channel::bounded(16);
    (Channel { tx: tx1, rx: rx2 }, Channel { tx: tx2, rx: rx1 })
}

struct TestConnector<const N: u8> {
    peer: Mutex<PeerId>,
    audiences: Mutex<Vec<ExpectedPeer>>,
    transports: Mutex<VecDeque<Channel<N>>>,
    scopes: Mutex<Vec<AttemptScope>>,
    calls: AtomicUsize,
}
impl<const N: u8> TestConnector<N> {
    fn new(peer: PeerId, transports: Vec<Channel<N>>) -> Arc<Self> {
        Arc::new(Self {
            peer: Mutex::new(peer),
            audiences: Mutex::new(Vec::new()),
            transports: Mutex::new(transports.into()),
            scopes: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        })
    }
}
impl<const N: u8> Connector for TestConnector<N> {
    fn establish(
        &self,
        attempt: AttemptScope,
        authentication: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            self.audiences
                .lock()
                .unwrap()
                .push(authentication.audience());
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.scopes.lock().unwrap().push(attempt.clone());
            let transport = self
                .transports
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| Error::new(ErrorKind::Transport, "no more test transports"))?;
            // Tests bypass crypto deliberately; shared handshake enforcement has
            // separate tests and the runtime must still check this witness's ID.
            attempt.established(Authenticated::new_for_test(
                transport,
                *self.peer.lock().unwrap(),
                Direction::Dialed,
            ))
        })
    }
}
const fn retry() -> ReconnectPolicy {
    ReconnectPolicy {
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
        max_attempts: 3,
        max_elapsed: BUDGET,
        healthy_after: Duration::from_secs(30),
    }
}

#[tokio::test]
async fn offline_storage_and_handles_do_not_keep_runtime_alive() {
    let owner = client(1);
    let handle = owner.handle();
    let id = SedimentreeId::new([1; 32]);
    let blob = Blob::new(b"hello".to_vec());
    let commit = LooseCommit::new(
        id,
        CommitId::new([2; 32]),
        std::collections::BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    handle
        .store_built_batch(id, vec![(commit, blob.clone())], vec![])
        .await
        .unwrap();
    assert_eq!(handle.get_blobs(id).await.unwrap(), vec![blob]);
    let mut status = handle.watch();
    drop(owner);
    timeout(BUDGET, async {
        loop {
            if matches!(*status.borrow_and_update(), ClientStatus::Closed(_)) {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        handle.get_blobs(id).await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn heterogeneous_transports_share_one_node_and_sync() {
    let a = client(1);
    let b = client(2);
    let c = client(3);
    let (ab, ba) = pair::<0, 0>();
    let (ac, ca) = pair::<1, 2>();
    let a_b = a
        .handle()
        .connect(
            TestConnector::new(peer(2), vec![ab]),
            ExpectedPeer::Known(peer(2)),
        )
        .await
        .unwrap();
    let _b_a = b
        .handle()
        .connect(
            TestConnector::new(peer(1), vec![ba]),
            ExpectedPeer::Known(peer(1)),
        )
        .await
        .unwrap();
    let a_c = a
        .handle()
        .connect(
            TestConnector::new(peer(3), vec![ac]),
            ExpectedPeer::Known(peer(3)),
        )
        .await
        .unwrap();
    let _c_a = c
        .handle()
        .connect(
            TestConnector::new(peer(1), vec![ca]),
            ExpectedPeer::Known(peer(1)),
        )
        .await
        .unwrap();
    let id = SedimentreeId::new([10; 32]);
    let blob = Blob::new(b"mixed transports".to_vec());
    let commit = LooseCommit::new(
        id,
        CommitId::new([4; 32]),
        std::collections::BTreeSet::new(),
        BlobMeta::new(&blob),
    );
    a.handle()
        .store_built_batch(id, vec![(commit, blob.clone())], vec![])
        .await
        .unwrap();
    let result = b
        .handle()
        .sync_with_peer(peer(1), id, false, CallTimeout::Default)
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(b.handle().get_blobs(id).await.unwrap(), vec![blob.clone()]);
    a_b.close().await.unwrap();
    assert!(matches!(a_c.status(), ConnectionStatus::Connected { .. }));
    assert!(
        c.handle()
            .sync_with_peer(peer(1), id, false, CallTimeout::Default)
            .await
            .unwrap()
            .success
    );
    assert_eq!(c.handle().get_blobs(id).await.unwrap(), vec![blob]);
    for owner in [a, b, c] {
        let report = timeout(BUDGET, owner.close()).await.unwrap();
        assert!(!report.forced, "{report:?}");
        assert_eq!(report.unfinished_tasks, 0);
    }
}

#[tokio::test]
async fn identity_mismatch_is_fatal_even_for_a_custom_connector() {
    let owner = client(1);
    let (socket, remote) = pair::<0, 0>();
    let connector = TestConnector::new(peer(3), vec![socket]);
    let error = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
        .reconnect(retry())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
    assert!(remote.tx.is_closed());
    assert!(!owner.close().await.forced);
}

#[tokio::test]
async fn replacements_have_fresh_scope_and_stale_work_cannot_remove_them() {
    let owner = client(1);
    let (first, remote1) = pair::<0, 0>();
    let (second, _remote2) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first, second]);
    let connection = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
        .reconnect(retry())
        .await
        .unwrap();
    let id = connection.id();
    remote1.disconnect().await.unwrap();
    let mut states = connection.watch();
    timeout(BUDGET, async {
        loop {
            if matches!(
                *states.borrow_and_update(),
                ConnectionStatus::Connected { generation: 1, .. }
            ) {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(id, connection.id());
    let old = connector.scopes.lock().unwrap().first().unwrap().clone();
    assert!(
        old.spawn("late failure", async {
            Err(Error::new(ErrorKind::Transport, "late"))
        })
        .is_err()
    );
    assert!(matches!(
        connection.status(),
        ConnectionStatus::Connected { generation: 1, .. }
    ));
    owner.handle().disconnect_from_peer(peer(2)).await.unwrap();
    assert!(matches!(
        connection.status(),
        ConnectionStatus::Closed(Ok(()))
    ));
    assert_eq!(connector.calls.load(Ordering::SeqCst), 2);
    assert!(!owner.close().await.forced);
}

struct HangingConnector {
    started: Arc<tokio::sync::Notify>,
    cleaned: Arc<AtomicUsize>,
}
impl Connector for HangingConnector {
    fn establish(
        &self,
        attempt: AttemptScope,
        _: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            let cleaned = self.cleaned.clone();
            attempt.on_cleanup(async move {
                tokio::task::yield_now().await;
                cleaned.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })?;
            attempt.spawn("pre-auth worker", std::future::pending())?;
            self.started.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn abandoning_initial_connect_cleans_pre_auth_resources() {
    let owner = client(1);
    let cleaned = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let future = owner
        .handle()
        .connect(
            HangingConnector {
                started: started.clone(),
                cleaned: cleaned.clone(),
            },
            ExpectedPeer::Known(peer(2)),
        )
        .into_future();
    let task = tokio::spawn(future);
    started.notified().await;
    task.abort();
    let _joined = task.await;
    timeout(BUDGET, async {
        while cleaned.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let report = owner.close().await;
    assert!(!report.forced, "{report:?}");
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn initial_attempt_deadline_cleans_before_reporting_failure() {
    let owner = TokioClient::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(signer(1))
        .connect_timeout(Duration::from_millis(10))
        .start()
        .unwrap();
    let cleaned = Arc::new(AtomicUsize::new(0));
    let connector = HangingConnector {
        started: Arc::new(tokio::sync::Notify::new()),
        cleaned: cleaned.clone(),
    };
    let error = timeout(
        BUDGET,
        owner
            .handle()
            .connect(connector, ExpectedPeer::Known(peer(2)))
            .into_future(),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert!(!owner.close().await.forced);
}

#[tokio::test]
async fn close_waiter_cancellation_does_not_cancel_shutdown() {
    let owner = client(1);
    let (socket, _remote) = pair::<0, 0>();
    let connection = owner
        .handle()
        .connect(
            TestConnector::new(peer(2), vec![socket]),
            ExpectedPeer::Known(peer(2)),
        )
        .await
        .unwrap();
    let mut close = Box::pin(owner.close());
    // Poll once to initiate shutdown, then abandon this particular waiter.
    let _poll = futures::poll!(close.as_mut());
    drop(close);
    let report = timeout(BUDGET, owner.closed()).await.unwrap();
    assert!(!report.forced, "{report:?}");
    assert_eq!(report.unfinished_tasks, 0);
    connection.closed().await.unwrap();
    let again = owner.close().await;
    assert_eq!(again.forced, report.forced);
}

#[test]
fn start_outside_tokio_is_an_error() {
    assert_eq!(
        TokioClient::builder()
            .storage(MemoryStorage::default())
            .policy(OpenPolicy)
            .signer(signer(1))
            .start()
            .unwrap_err()
            .kind(),
        ErrorKind::Configuration
    );
}
