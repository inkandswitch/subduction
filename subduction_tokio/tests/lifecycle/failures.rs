use super::*;

#[test]
fn startup_rejects_runtimes_without_timers() {
    for mut builder in [
        tokio::runtime::Builder::new_current_thread(),
        tokio::runtime::Builder::new_multi_thread(),
    ] {
        let runtime = builder.build().unwrap();
        runtime.block_on(async {
            let error = Node::builder()
                .storage(MemoryStorage::default())
                .policy(OpenPolicy)
                .signer(signer(1))
                .start()
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Configuration);
        });
    }
}

#[test]
fn timer_enabled_runtime_can_close_an_offline_node() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let owner = node(1);
        let handle = owner.handle();
        let report = timeout(BUDGET, owner.close()).await.unwrap();
        assert!(!report.forced);
        assert!(report.failure.is_none());
        assert!(matches!(*handle.watch().borrow(), NodeStatus::Closed(_)));
    });
}

#[tokio::test]
async fn decoding_failure_disconnects_and_reconnects_through_existing_async_operation() {
    let owner = node(1);
    let (first, remote1) = pair::<0, 0>();
    let (second, _remote2) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first, second]);
    let connection = owner
        .handle()
        .connect(connector, ExpectedPeer::Known(peer(2)))
        .reconnect(retry())
        .await
        .unwrap();
    // The byte transport succeeds; MessageTransport's decoding fails instead.
    remote1.send_bytes(b"not a Subduction frame").await.unwrap();
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
    assert!(
        remote1.tx.is_closed(),
        "old physical transport must be closed"
    );
    assert!(remote1.rx.is_closed());
    connection.close().await.unwrap();
    assert!(!owner.close().await.forced);
}

#[derive(Clone, Debug)]
struct GatedDisconnect {
    io: Channel<0>,
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    calls: Arc<AtomicUsize>,
    fail: bool,
}
impl PartialEq for GatedDisconnect {
    fn eq(&self, other: &Self) -> bool {
        self.io == other.io
    }
}
impl Transport<Sendable> for GatedDisconnect {
    type SendError = std::io::Error;
    type RecvError = std::io::Error;
    type DisconnectionError = std::io::Error;
    fn send_bytes(&self, bytes: &[u8]) -> BoxFuture<'_, Result<(), Self::SendError>> {
        self.io.send_bytes(bytes)
    }
    fn recv_bytes(&self) -> BoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        self.io.recv_bytes()
    }
    fn disconnect(&self) -> BoxFuture<'_, Result<(), Self::DisconnectionError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            self.release.notified().await;
            self.io.disconnect().await?;
            if self.fail {
                Err(std::io::Error::other("cleanup failure"))
            } else {
                Ok(())
            }
        })
    }
}
impl Connector for GatedDisconnect {
    fn establish(
        &self,
        attempt: AttemptScope,
        _: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            attempt.established(Authenticated::new_for_test(
                self.clone(),
                peer(2),
                Direction::Dialed,
            ))
        })
    }
}

#[tokio::test]
async fn decode_failure_cleanup_is_tracked_shared_and_reported_during_close() {
    for fail in [false, true] {
        let owner = node(1);
        let (io, remote) = pair::<0, 0>();
        let transport = GatedDisconnect {
            io,
            started: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
            calls: Arc::new(AtomicUsize::new(0)),
            fail,
        };
        let _connection = owner
            .handle()
            .connect(transport.clone(), ExpectedPeer::Known(peer(2)))
            .await
            .unwrap();
        remote.send_bytes(b"not a Subduction frame").await.unwrap();
        timeout(BUDGET, transport.started.notified()).await.unwrap();
        // The backend is still open: the decode failure, not byte I/O, must
        // have led core to poll its ordinary lazy disconnect operation.
        assert!(!remote.tx.is_closed());
        let mut close = Box::pin(owner.close());
        assert!(futures::poll!(&mut close).is_pending());
        transport.release.notify_one();
        let report = timeout(BUDGET, close).await.unwrap();
        assert!(!report.forced, "{report:?}");
        assert_eq!(report.unfinished_tasks, 0);
        assert_eq!(report.failure.is_some(), fail);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        assert!(remote.tx.is_closed());
    }
}

#[tokio::test]
async fn discovery_pins_the_first_peer_on_reconnect() {
    let owner = node(1);
    let (first, remote1) = pair::<0, 0>();
    let (second, remote2) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first, second]);
    let connection = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::discover(b"service"))
        .reconnect(retry())
        .await
        .unwrap();
    *connector.peer.lock().unwrap() = peer(3);
    remote1.disconnect().await.unwrap();
    let error = timeout(BUDGET, connection.closed())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Authentication);
    assert_eq!(
        connector.audiences.lock().unwrap().get(1),
        Some(&ExpectedPeer::Known(peer(2)))
    );
    assert!(remote2.tx.is_closed());
    assert!(!owner.close().await.forced);
}

#[tokio::test]
async fn rediscovery_requires_explicit_opt_in() {
    let owner = node(1);
    let (first, remote1) = pair::<0, 0>();
    let (second, _remote2) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first, second]);
    let expected = ExpectedPeer::discover(b"service");
    let connection = owner
        .handle()
        .connect(connector.clone(), expected)
        .reconnect(retry())
        .rediscover_on_reconnect()
        .await
        .unwrap();
    *connector.peer.lock().unwrap() = peer(3);
    remote1.disconnect().await.unwrap();
    let mut states = connection.watch();
    timeout(BUDGET, async {
        loop {
            if matches!(*states.borrow_and_update(), ConnectionStatus::Connected { peer: p, generation: 1 } if p == peer(3)) { break; }
            states.changed().await.unwrap();
        }
    }).await.unwrap();
    assert_eq!(connector.audiences.lock().unwrap().get(1), Some(&expected));
    connection.close().await.unwrap();
    assert!(!owner.close().await.forced);
}

#[tokio::test]
async fn retry_budget_is_bounded_and_close_interrupts_backoff() {
    let owner = node(1);
    let (first, remote) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first]);
    let connection = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
        .reconnect(retry())
        .await
        .unwrap();
    remote.disconnect().await.unwrap();
    let error = timeout(BUDGET, connection.closed())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Transport);
    assert_eq!(connector.calls.load(Ordering::SeqCst), 4); // initial + three replacements
    assert!(!owner.close().await.forced);

    let owner = node(1);
    let (first, remote) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first]);
    let mut policy = retry();
    policy.initial_delay = Duration::from_secs(60);
    policy.max_delay = Duration::from_secs(60);
    policy.max_elapsed = Duration::from_secs(600);
    let connection = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
        .reconnect(policy)
        .await
        .unwrap();
    let mut states = connection.watch();
    remote.disconnect().await.unwrap();
    timeout(BUDGET, async {
        loop {
            if matches!(
                *states.borrow_and_update(),
                ConnectionStatus::Backoff { .. }
            ) {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    timeout(BUDGET, owner.handle().disconnect_from_peer(peer(2)))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        connection.status(),
        ConnectionStatus::Closed(Ok(()))
    ));
    assert!(!owner.close().await.forced);
}

struct CleanupConnector {
    began: Arc<tokio::sync::Notify>,
}
impl Connector for CleanupConnector {
    fn establish(
        &self,
        attempt: AttemptScope,
        _: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            attempt.on_cleanup(std::future::pending())?;
            self.began.notify_one();
            std::future::pending().await
        })
    }
}
#[tokio::test]
async fn shutdown_has_a_shared_deadline_and_abort_fallback() {
    let owner = Node::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(signer(1))
        .shutdown_timeout(Duration::from_millis(10))
        .cleanup_timeout(Duration::from_secs(60))
        .start()
        .unwrap();
    let began = Arc::new(tokio::sync::Notify::new());
    let connecting = tokio::spawn(
        owner
            .handle()
            .connect(
                CleanupConnector {
                    began: began.clone(),
                },
                ExpectedPeer::Known(peer(2)),
            )
            .into_future(),
    );
    began.notified().await;
    let report = timeout(BUDGET, owner.close()).await.unwrap();
    assert!(report.forced);
    assert_eq!(report.unfinished_tasks, 0);
    assert!(connecting.await.unwrap().is_err());
}

#[tokio::test]
async fn cleanup_timeout_stops_retries_and_is_observable() {
    let owner = Node::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(signer(1))
        .connect_timeout(Duration::from_millis(5))
        .cleanup_timeout(Duration::from_millis(5))
        .start()
        .unwrap();
    let error = owner
        .handle()
        .connect(
            CleanupConnector {
                began: Arc::new(tokio::sync::Notify::new()),
            },
            ExpectedPeer::Known(peer(2)),
        )
        .reconnect(retry())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cleanup);
    let report = timeout(BUDGET, owner.closed()).await.unwrap();
    assert_eq!(report.failure.unwrap().kind(), ErrorKind::Cleanup);
}

#[tokio::test(start_paused = true)]
async fn outage_deadline_bounds_cleanup_and_releases_callbacks() {
    // Exercise both limits: the outage can shorten cleanup, but must never
    // extend the configured individual cleanup budget.
    for (outage_budget, cleanup_budget) in [(1, 60), (60, 1)] {
        let owner = Node::builder()
            .storage(MemoryStorage::default())
            .policy(OpenPolicy)
            .signer(signer(1))
            .cleanup_timeout(Duration::from_secs(cleanup_budget))
            .start()
            .unwrap();
        let (first, remote) = pair::<0, 0>();
        let connector = TestConnector::new(peer(2), vec![first]);
        let mut policy = retry();
        policy.max_elapsed = Duration::from_secs(outage_budget);
        let connection = owner
            .handle()
            .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
            .reconnect(policy)
            .await
            .unwrap();

        let resource = Arc::new(());
        let weak = Arc::downgrade(&resource);
        connector
            .scopes
            .lock()
            .unwrap()
            .first()
            .unwrap()
            .on_cleanup(async move {
                std::future::pending::<()>().await;
                drop(resource);
                Ok(())
            })
            .unwrap();
        let start = tokio::time::Instant::now();
        remote.disconnect().await.unwrap();
        let error = timeout(Duration::from_secs(2), connection.closed())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Cleanup);
        assert_eq!(start.elapsed(), Duration::from_secs(1));
        assert!(weak.upgrade().is_none(), "cleanup callback was not dropped");
        assert_eq!(connector.calls.load(Ordering::SeqCst), 1);
        let report = timeout(BUDGET, owner.closed()).await.unwrap();
        assert_eq!(report.failure.unwrap().kind(), ErrorKind::Cleanup);
        assert_eq!(report.unfinished_tasks, 0);
        assert!(!report.forced);
    }
}

struct SlowReplacementCleanup(Arc<TestConnector<0>>);
impl Connector for SlowReplacementCleanup {
    fn establish(
        &self,
        attempt: AttemptScope,
        auth: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async move {
            if attempt.generation() == 0 {
                return self.0.establish(attempt, auth).await;
            }
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            attempt.on_cleanup(std::future::pending())?;
            tokio::time::sleep(Duration::from_millis(750)).await;
            Err(Error::new(ErrorKind::Transport, "replacement failed"))
        })
    }
}

#[tokio::test(start_paused = true)]
async fn replacement_cleanup_uses_remaining_outage_budget() {
    let owner = Node::builder()
        .storage(MemoryStorage::default())
        .policy(OpenPolicy)
        .signer(signer(1))
        .cleanup_timeout(Duration::from_secs(60))
        .start()
        .unwrap();
    let (first, remote) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![first]);
    let mut policy = retry();
    policy.initial_delay = Duration::ZERO;
    policy.max_elapsed = Duration::from_secs(1);
    let connection = owner
        .handle()
        .connect(
            SlowReplacementCleanup(connector.clone()),
            ExpectedPeer::Known(peer(2)),
        )
        .reconnect(policy)
        .await
        .unwrap();

    let start = tokio::time::Instant::now();
    remote.disconnect().await.unwrap();
    let error = timeout(Duration::from_secs(2), connection.closed())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Cleanup);
    // The replacement consumed 750ms, so its cleanup gets only 250ms,
    // not a fresh max_elapsed budget.
    assert_eq!(start.elapsed(), Duration::from_secs(1));
    assert_eq!(connector.calls.load(Ordering::SeqCst), 2);
    let report = timeout(BUDGET, owner.closed()).await.unwrap();
    assert_eq!(report.failure.unwrap().kind(), ErrorKind::Cleanup);
    assert_eq!(report.unfinished_tasks, 0);
}

struct PanickingConnector;
impl Connector for PanickingConnector {
    fn establish(
        &self,
        _: AttemptScope,
        _: AuthenticationContext,
    ) -> BoxFuture<'_, Result<EstablishedConnection, Error>> {
        Box::pin(async { panic!("injected connector panic") })
    }
}
#[tokio::test]
async fn connector_and_driver_panics_are_observable_and_connection_scoped() {
    let owner = node(1);
    let error = owner
        .handle()
        .connect(PanickingConnector, ExpectedPeer::Known(peer(2)))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Background);
    assert!(matches!(owner.status(), NodeStatus::Running));
    let (socket, _remote) = pair::<0, 0>();
    let connector = TestConnector::new(peer(2), vec![socket]);
    let connection = owner
        .handle()
        .connect(connector.clone(), ExpectedPeer::Known(peer(2)))
        .reconnect(retry())
        .await
        .unwrap();
    let scope = connector.scopes.lock().unwrap().first().unwrap().clone();
    scope
        .spawn("panicking driver", async {
            panic!("injected driver panic")
        })
        .unwrap();
    assert_eq!(
        timeout(BUDGET, connection.closed())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        ErrorKind::Background
    );
    assert!(matches!(owner.status(), NodeStatus::Running));
    assert!(!owner.close().await.forced);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_racing_shutdown_leaves_no_work() {
    for _ in 0..20 {
        let owner = node(1);
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (socket, remote) = pair::<0, 0>();
            let connect = owner.handle().connect(
                TestConnector::new(peer(2), vec![socket]),
                ExpectedPeer::Known(peer(2)),
            );
            tasks.push(tokio::spawn(async move {
                let _remote = remote;
                let _result = connect.await;
            }));
        }
        let report = timeout(BUDGET, owner.close()).await.unwrap();
        assert!(!report.forced, "{report:?}");
        assert_eq!(report.unfinished_tasks, 0);
        for task in tasks {
            task.await.unwrap();
        }
    }
}
