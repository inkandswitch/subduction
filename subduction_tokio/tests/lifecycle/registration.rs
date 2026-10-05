use super::*;
use std::{convert::Infallible, sync::atomic::AtomicBool};
use subduction_core::policy::{connection::ConnectionPolicy, storage::StoragePolicy};
use subduction_crypto::verified_author::VerifiedAuthor;

struct GatePolicy {
    started: Arc<tokio::sync::Notify>,
    block: Arc<AtomicBool>,
    _marker: Arc<()>,
}
impl ConnectionPolicy<Sendable> for GatePolicy {
    type ConnectionDisallowed = Infallible;
    fn authorize_connect(&self, _: PeerId) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async move {
            if self.block.load(Ordering::SeqCst) {
                self.started.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(())
        })
    }
}
impl StoragePolicy<Sendable> for GatePolicy {
    type FetchDisallowed = Infallible;
    type PutDisallowed = Infallible;
    fn authorize_fetch(
        &self,
        _: PeerId,
        _: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
    fn authorize_put(
        &self,
        _: PeerId,
        _: VerifiedAuthor,
        _: SedimentreeId,
    ) -> BoxFuture<'_, Result<(), Infallible>> {
        Box::pin(async { Ok(()) })
    }
    fn filter_authorized_fetch(
        &self,
        _: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> BoxFuture<'_, Vec<SedimentreeId>> {
        Box::pin(async { ids })
    }
}

#[tokio::test]
async fn cancellation_during_registration_releases_transport_and_core() {
    let started = Arc::new(tokio::sync::Notify::new());
    let block = Arc::new(AtomicBool::new(true));
    let marker = Arc::new(());
    let weak = Arc::downgrade(&marker);
    let owner = TokioClient::builder()
        .storage(MemoryStorage::default())
        .signer(signer(1))
        .policy(GatePolicy {
            started: started.clone(),
            block: block.clone(),
            _marker: marker,
        })
        .start()
        .unwrap();
    let handle = owner.handle();
    let (socket, remote) = pair::<0, 0>();
    let connecting = tokio::spawn(
        handle
            .connect(
                TestConnector::new(peer(2), vec![socket]),
                ExpectedPeer::Known(peer(2)),
            )
            .into_future(),
    );
    started.notified().await;
    connecting.abort();
    let _joined = connecting.await;
    timeout(BUDGET, async {
        while !remote.tx.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Cancelled registration must not leave a deduplication entry behind.
    block.store(false, Ordering::SeqCst);
    let (socket, _remote) = pair::<1, 1>();
    let connection = handle
        .connect(
            TestConnector::new(peer(2), vec![socket]),
            ExpectedPeer::Known(peer(2)),
        )
        .await
        .unwrap();
    assert!(matches!(
        connection.status(),
        ConnectionStatus::Connected { .. }
    ));
    let report = timeout(BUDGET, owner.close()).await.unwrap();
    assert!(!report.forced, "{report:?}");
    assert!(
        weak.upgrade().is_none(),
        "retained owner/handle must not pin the typed core after close"
    );
    assert_eq!(
        handle.sedimentree_ids().await.unwrap_err().kind(),
        ErrorKind::Closed
    );
}

#[tokio::test]
async fn event_lag_does_not_erase_retained_terminal_state() {
    let owner = client(1);
    let mut events = owner.handle().events();
    for _ in 0..35 {
        let (socket, _remote) = pair::<0, 0>();
        let connection = owner
            .handle()
            .connect(
                TestConnector::new(peer(2), vec![socket]),
                ExpectedPeer::Known(peer(2)),
            )
            .await
            .unwrap();
        connection.close().await.unwrap();
        assert!(matches!(
            connection.status(),
            ConnectionStatus::Closed(Ok(()))
        ));
    }
    assert!(matches!(
        events.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
    let report = owner.close().await;
    assert!(!report.forced);
    assert!(matches!(owner.status(), ClientStatus::Closed(_)));
}

#[tokio::test]
async fn closing_a_connection_does_not_close_a_shared_endpoint() {
    struct SharedConnector {
        endpoint: Arc<AtomicUsize>,
        transport: Mutex<Option<Channel<0>>>,
    }
    impl Connector for SharedConnector {
        fn establish(
            &self,
            attempt: AttemptScope,
            _: AuthenticationContext,
        ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
            Box::pin(async move {
                let shared = self.endpoint.clone();
                attempt.spawn("endpoint user", async move {
                    std::future::pending::<()>().await;
                    drop(shared);
                    Ok(())
                })?;
                attempt.established(Authenticated::new_for_test(
                    self.transport.lock().unwrap().take().unwrap(),
                    peer(2),
                    Direction::Dialed,
                ))
            })
        }
    }
    let endpoint = Arc::new(AtomicUsize::new(0));
    let owner = client(1);
    let (socket, _remote) = pair::<0, 0>();
    let connection = owner
        .handle()
        .connect(
            SharedConnector {
                endpoint: endpoint.clone(),
                transport: Mutex::new(Some(socket)),
            },
            ExpectedPeer::Known(peer(2)),
        )
        .await
        .unwrap();
    connection.close().await.unwrap();
    // The connector and its worker no longer retain the externally owned endpoint.
    timeout(BUDGET, async {
        while Arc::strong_count(&endpoint) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    endpoint.fetch_add(1, Ordering::SeqCst);
    assert_eq!(endpoint.load(Ordering::SeqCst), 1);
    assert!(!owner.close().await.forced);
}
