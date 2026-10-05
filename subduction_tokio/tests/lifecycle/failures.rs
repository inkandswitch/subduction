use super::*;

#[tokio::test]
async fn decoding_failure_also_notifies_the_logical_owner() {
    let owner = client(1);
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
    connection.close().await.unwrap();
    assert!(!owner.close().await.forced);
}

#[tokio::test]
async fn discovery_pins_the_first_peer_on_reconnect() {
    let owner = client(1);
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
    let owner = client(1);
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
    let owner = client(1);
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

    let owner = client(1);
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
    let owner = TokioClient::builder()
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
    let owner = TokioClient::builder()
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
    let owner = client(1);
    let error = owner
        .handle()
        .connect(PanickingConnector, ExpectedPeer::Known(peer(2)))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Background);
    assert!(matches!(owner.status(), ClientStatus::Running));
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
    assert!(matches!(owner.status(), ClientStatus::Running));
    assert!(!owner.close().await.forced);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_racing_shutdown_leaves_no_work() {
    for _ in 0..20 {
        let owner = client(1);
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
