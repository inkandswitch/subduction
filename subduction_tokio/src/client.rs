//! Owned client, restricted handles, and supervised logical connections.
use crate::{
    Error, ErrorKind,
    connector::{
        AttemptGuard, AttemptScope, AuthenticationContext, Connector, RetryDecision, SharedSigner,
    },
    core::{self, Core, PeerSyncReport},
    reconnect::{ConnectionHandle, ConnectionStatus, Logical, ReconnectPolicy},
    tasks::Tasks,
};
use future_form::Sendable;
use futures::{FutureExt, future::BoxFuture};
use sedimentree_core::{
    blob::Blob, fragment::Fragment, id::SedimentreeId, loose_commit::LooseCommit,
};
use std::{
    collections::BTreeMap,
    fmt,
    future::IntoFuture,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use subduction_core::{
    handshake::audience::Audience,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::Signer;
use tokio::{
    sync::{broadcast, oneshot, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// Retained shutdown outcome. Forced shutdown need not roll back in-flight writes.
#[derive(Debug, Clone)]
pub struct CloseReport {
    /// The graceful deadline expired and abort was requested.
    pub forced: bool,
    /// Tasks still present after the final join budget.
    pub unfinished_tasks: usize,
    /// Retained fatal runtime or cleanup failure, if any.
    pub failure: Option<Error>,
}
/// Observable runtime lifecycle.
#[derive(Debug, Clone)]
pub enum ClientStatus {
    /// Admitting new operations.
    Running,
    /// New work is rejected; teardown continues independently of waiters.
    Closing,
    /// Terminal, cached result.
    Closed(CloseReport),
}
/// Optional bounded event stream. Lag never erases retained handle status.
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// A logical connection changed state.
    Connection {
        /// Logical ID.
        id: u64,
        /// New state.
        status: ConnectionStatus,
    },
    /// The runtime completed teardown.
    Closed(CloseReport),
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)] // These are deliberately all timeout budgets.
struct Config {
    connect_timeout: Duration,
    shutdown_timeout: Duration,
    abort_timeout: Duration,
    cleanup_timeout: Duration,
    roundtrip_timeout: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
            abort_timeout: Duration::from_secs(1),
            cleanup_timeout: Duration::from_secs(2),
            roundtrip_timeout: Duration::from_secs(10),
        }
    }
}

/// Required builder field not yet provided.
#[derive(Debug, Clone, Copy)]
pub struct Unset;
/// Type-state builder: storage, policy, and signer must all be explicitly set.
#[derive(Debug)]
#[must_use]
pub struct ClientBuilder<S = Unset, P = Unset, G = Unset> {
    storage: S,
    policy: P,
    signer: G,
    config: Config,
}
impl<S, P, G> ClientBuilder<S, P, G> {
    /// Choose the local storage backend.
    pub fn storage<T>(self, storage: T) -> ClientBuilder<T, P, G> {
        ClientBuilder {
            storage,
            policy: self.policy,
            signer: self.signer,
            config: self.config,
        }
    }
    /// Choose authorization; no open policy is selected implicitly.
    pub fn policy<T>(self, policy: T) -> ClientBuilder<S, T, G> {
        ClientBuilder {
            storage: self.storage,
            policy,
            signer: self.signer,
            config: self.config,
        }
    }
    /// Choose a persistent or explicitly ephemeral identity.
    pub fn signer<T>(self, signer: T) -> ClientBuilder<S, P, T> {
        ClientBuilder {
            storage: self.storage,
            policy: self.policy,
            signer,
            config: self.config,
        }
    }
    /// Total budget for each dial/authenticate/register attempt.
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }
    /// Shared graceful shutdown budget (not a separate budget per task).
    pub const fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }
    /// Additional join budget after forced task abort.
    pub const fn abort_timeout(mut self, timeout: Duration) -> Self {
        self.config.abort_timeout = timeout;
        self
    }
    /// Bound asynchronous cleanup of an individual attempt.
    pub const fn cleanup_timeout(mut self, timeout: Duration) -> Self {
        self.config.cleanup_timeout = timeout;
        self
    }
    /// Core request-send plus response-wait deadline; not an end-to-end sync deadline.
    pub const fn roundtrip_timeout(mut self, timeout: Duration) -> Self {
        self.config.roundtrip_timeout = timeout;
        self
    }
}

struct Admission {
    core: Option<Arc<dyn Core>>,
    next_id: u64,
    connections: BTreeMap<u64, Arc<Logical>>,
}
struct Inner {
    admission: Mutex<Admission>,
    tasks: Tasks,
    stop: CancellationToken,
    signer: SharedSigner,
    config: Config,
    status: watch::Sender<ClientStatus>,
    events: broadcast::Sender<ClientEvent>,
}
impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("status", &*self.status.borrow())
            .finish_non_exhaustive()
    }
}
impl Inner {
    fn publish(&self, connection: &Logical, status: ConnectionStatus) {
        connection.status.send_replace(status.clone());
        let _receivers = self.events.send(ClientEvent::Connection {
            id: connection.id,
            status,
        });
    }
}

/// Owns the managed service. Dropping it requests shutdown; it is not cloneable.
#[derive(Debug)]
pub struct TokioClient {
    inner: Arc<Inner>,
}
impl TokioClient {
    /// Configure a client with explicit storage, policy, and identity.
    pub fn builder() -> ClientBuilder {
        ClientBuilder {
            storage: Unset,
            policy: Unset,
            signer: Unset,
            config: Config::default(),
        }
    }
    /// Cloneable managed access, without exposing the core or physical transports.
    #[must_use]
    pub fn handle(&self) -> ClientHandle {
        ClientHandle {
            inner: self.inner.clone(),
        }
    }
    /// Request teardown synchronously. Requires a live runtime to finish.
    pub fn shutdown(&self) {
        self.inner.stop.cancel();
    }
    /// Current lifecycle, including the retained shutdown result.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        self.inner.status.borrow().clone()
    }
    /// Await teardown without requesting it.
    pub async fn closed(&self) -> CloseReport {
        wait_closed(self.inner.status.subscribe()).await
    }
    /// Idempotent bounded teardown; cancelling this waiter does not cancel cleanup.
    pub async fn close(&self) -> CloseReport {
        self.shutdown();
        self.closed().await
    }
}
impl Drop for TokioClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl<S, P, G> ClientBuilder<S, P, G>
where
    S: Storage<Sendable> + Send + Sync + fmt::Debug + 'static,
    S::Error: Send + Sync + 'static,
    P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    P::ConnectionDisallowed: Send + Sync + 'static,
    P::FetchDisallowed: Send + Sync + 'static,
    P::PutDisallowed: Send + Sync + 'static,
    G: Signer<Sendable> + Send + Sync + 'static,
{
    /// Start the local node. No network connection or automatic sync is initiated.
    ///
    /// # Errors
    /// Returns an error outside Tokio or for an unrepresentable deadline.
    pub fn start(self) -> Result<TokioClient, Error> {
        tokio::runtime::Handle::try_current().map_err(|e| {
            Error::with_source(ErrorKind::Configuration, "Tokio runtime required", e)
        })?;
        for duration in [
            self.config.connect_timeout,
            self.config.shutdown_timeout,
            self.config.abort_timeout,
            self.config.cleanup_timeout,
            self.config.roundtrip_timeout,
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(Error::new(
                    ErrorKind::Configuration,
                    "deadline out of range",
                ));
            }
        }
        let stop = CancellationToken::new();
        let tasks = Tasks::new(stop.clone());
        let signer = SharedSigner::new(self.signer);
        let core = core::build(
            self.storage,
            self.policy,
            signer.clone(),
            &tasks,
            stop.clone(),
            self.config.roundtrip_timeout,
        );
        let inner = Arc::new(Inner {
            admission: Mutex::new(Admission {
                core: Some(core),
                next_id: 0,
                connections: BTreeMap::new(),
            }),
            tasks,
            stop,
            signer,
            config: self.config,
            status: watch::channel(ClientStatus::Running).0,
            events: broadcast::channel(128).0,
        });
        // The shutdown coordinator intentionally lives outside the set it joins.
        // It owns teardown independently of the owner's or a close waiter's lifetime.
        let supervisor = inner.clone();
        tokio::spawn(async move {
            shutdown(supervisor).await;
        });
        Ok(TokioClient { inner })
    }
}

async fn shutdown(inner: Arc<Inner>) {
    inner.stop.cancelled().await;
    inner.status.send_replace(ClientStatus::Closing);
    {
        let admission = inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(core) = &admission.core {
            core.shutdown();
        }
        for connection in admission.connections.values() {
            connection.cancel.cancel();
        }
    }
    let graceful = async {
        inner.tasks.wait().await;
        inner.tasks.seal();
        inner.tasks.wait().await;
    };
    let forced = tokio::time::timeout(inner.config.shutdown_timeout, graceful)
        .await
        .is_err();
    if forced {
        inner.tasks.abort_all();
        let _joined = tokio::time::timeout(inner.config.abort_timeout, inner.tasks.wait()).await;
    }
    {
        let mut admission = inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        admission.core.take();
        admission.connections.clear();
    }
    let report = CloseReport {
        forced,
        unfinished_tasks: inner.tasks.len(),
        failure: inner.tasks.failure(),
    };
    inner
        .status
        .send_replace(ClientStatus::Closed(report.clone()));
    let _receivers = inner.events.send(ClientEvent::Closed(report));
}
async fn wait_closed(mut status: watch::Receiver<ClientStatus>) -> CloseReport {
    loop {
        if let ClientStatus::Closed(report) = status.borrow_and_update().clone() {
            return report;
        }
        if status.changed().await.is_err() {
            return CloseReport {
                forced: true,
                unfinished_tasks: 0,
                failure: Some(Error::new(
                    ErrorKind::Background,
                    "shutdown coordinator disappeared",
                )),
            };
        }
    }
}

/// Restricted cloneable node access. It cannot keep the service running after owner drop.
#[derive(Debug, Clone)]
pub struct ClientHandle {
    inner: Arc<Inner>,
}
impl ClientHandle {
    /// Start a logical connection through any connector, with retries disabled by default.
    pub fn connect(&self, connector: impl Connector, expected: Audience) -> Connect {
        Connect {
            handle: self.clone(),
            connector: Arc::new(connector),
            expected,
            retry: None,
            rediscover: false,
        }
    }
    /// Subscribe to bounded events. Handle status remains available after event lag.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.inner.events.subscribe()
    }
    /// Watch retained runtime status.
    #[must_use]
    pub fn watch(&self) -> watch::Receiver<ClientStatus> {
        self.inner.status.subscribe()
    }
    /// Persist a mixed batch locally. Cancellation is not rollback.
    ///
    /// # Errors
    /// Returns lifecycle, blob-validation, or storage errors; partial writes may remain.
    pub async fn store_built_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> Result<(), Error> {
        self.operation(move |core| {
            Box::pin(async move { core.store(id, commits, fragments).await })
        })
        .await
    }
    /// Snapshot stored blobs. An empty tree returns an empty vector.
    ///
    /// # Errors
    /// Returns lifecycle or storage errors.
    pub async fn get_blobs(&self, id: SedimentreeId) -> Result<Vec<Blob>, Error> {
        self.operation(move |core| Box::pin(async move { core.blobs(id).await }))
            .await
    }
    /// List locally known trees.
    ///
    /// # Errors
    /// Returns an error if the operation is cancelled or the runtime is closing.
    pub async fn sedimentree_ids(&self) -> Result<Vec<SedimentreeId>, Error> {
        self.operation(move |core| Box::pin(async move { core.ids().await }))
            .await
    }
    /// Sync with the existing core completion semantics. `subscribe` establishes
    /// subscriptions for this connection era; callers must restore them after reconnect.
    ///
    /// # Errors
    /// Returns lifecycle or core ingestion errors. Roundtrip errors are in the report.
    pub async fn sync_with_peer(
        &self,
        peer: PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> Result<PeerSyncReport, Error> {
        self.operation(move |core| {
            Box::pin(async move { core.sync(peer, id, subscribe, timeout).await })
        })
        .await
    }
    /// Close every logical connection bound to this peer, including backoff states.
    ///
    /// # Errors
    /// Returns a retained connection failure or cleanup error after all selected
    /// connections have been requested to stop and their outcomes awaited.
    pub async fn disconnect_from_peer(&self, peer: PeerId) -> Result<(), Error> {
        let connections: Vec<_> = {
            let admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            admission
                .connections
                .values()
                .filter(|c| {
                    *c.peer
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        == Some(peer)
                })
                .cloned()
                .map(ConnectionHandle)
                .collect()
        };
        for connection in &connections {
            connection.shutdown();
        }
        let results =
            futures::future::join_all(connections.iter().map(ConnectionHandle::closed)).await;
        results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map(|_| ())
    }
    async fn operation<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Arc<dyn Core>) -> BoxFuture<'static, Result<T, Error>> + Send + 'static,
    ) -> Result<T, Error> {
        let (tx, rx) = oneshot::channel();
        let cancel = self.inner.stop.child_token();
        let _guard = CancelOnDrop(Some(cancel.clone()));
        {
            let admission = self
                .inner
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.inner.stop.is_cancelled() {
                return Err(Error::new(ErrorKind::Closed, "client closing"));
            }
            let core = admission
                .core
                .clone()
                .ok_or_else(|| Error::new(ErrorKind::Closed, "client closed"))?;
            self.inner.tasks.spawn("operation", async move {
                let result = tokio::select! {
                    biased;
                    () = cancel.cancelled() => Err(Error::new(ErrorKind::Cancelled, "operation cancelled; partial progress possible")),
                    result = operation(core) => result,
                };
                let _delivered = tx.send(result);
            });
        }
        rx.await
            .map_err(|_| Error::new(ErrorKind::Background, "operation aborted"))?
    }
}
struct CancelOnDrop(Option<CancellationToken>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(token) = &self.0 {
            token.cancel();
        }
    }
}

/// Connection options, executed when awaited. The initial attempt does not retry.
#[must_use = "connection establishment starts only when awaited"]
pub struct Connect {
    handle: ClientHandle,
    connector: Arc<dyn Connector>,
    expected: Audience,
    retry: Option<ReconnectPolicy>,
    rediscover: bool,
}
impl fmt::Debug for Connect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connect")
            .field("expected", &self.expected)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}
impl Connect {
    /// Enable automatic replacement after the first successful connection.
    pub const fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.retry = Some(policy);
        self
    }
    /// Explicitly permit discovery to find a different peer on replacement.
    /// Otherwise the first discovered peer is pinned for all later attempts.
    pub const fn rediscover_on_reconnect(mut self) -> Self {
        self.rediscover = true;
        self
    }
}
impl IntoFuture for Connect {
    type Output = Result<ConnectionHandle, Error>;
    type IntoFuture = BoxFuture<'static, Self::Output>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(start_connection(self))
    }
}

async fn start_connection(options: Connect) -> Result<ConnectionHandle, Error> {
    let inner = options.handle.inner.clone();
    if let Some(policy) = &options.retry
        && (Instant::now().checked_add(policy.max_elapsed).is_none()
            || Instant::now().checked_add(policy.max_delay).is_none())
    {
        return Err(Error::new(
            ErrorKind::Configuration,
            "retry deadline out of range",
        ));
    }
    let (tx, rx) = oneshot::channel();
    let logical;
    {
        let mut admission = inner
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.stop.is_cancelled() {
            return Err(Error::new(ErrorKind::Closed, "client closing"));
        }
        let core = admission
            .core
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::Closed, "client closed"))?;
        let id = admission.next_id;
        admission.next_id += 1;
        logical = Arc::new(Logical {
            id,
            cancel: inner.stop.child_token(),
            peer: Mutex::new(match options.expected {
                Audience::Known(peer) => Some(peer),
                Audience::Discover(_) => None,
            }),
            status: watch::channel(ConnectionStatus::Connecting { generation: 0 }).0,
        });
        admission.connections.insert(id, logical.clone());
        let task_inner = inner.clone();
        let task_logical = logical.clone();
        inner.tasks.spawn("logical connection", async move {
            run_connection(task_inner, task_logical, core, options, tx).await;
        });
    }
    let mut guard = CancelOnDrop(Some(logical.cancel.clone()));
    rx.await
        .map_err(|_| Error::new(ErrorKind::Background, "connection supervisor aborted"))??;
    // No await between receiving success and handing ownership to the caller.
    guard.0.take();
    Ok(ConnectionHandle(logical))
}

struct LogicalGuard {
    inner: Weak<Inner>,
    logical: Arc<Logical>,
}
impl Drop for LogicalGuard {
    fn drop(&mut self) {
        if !matches!(*self.logical.status.borrow(), ConnectionStatus::Closed(_)) {
            self.logical
                .status
                .send_replace(ConnectionStatus::Closed(Err(Error::new(
                    ErrorKind::Background,
                    "connection supervisor aborted",
                ))));
        }
        if let Some(inner) = self.inner.upgrade() {
            inner
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .connections
                .remove(&self.logical.id);
        }
    }
}

#[allow(clippy::too_many_lines)] // Keep the owned attempt/retry state machine together.
async fn run_connection(
    inner: Arc<Inner>,
    logical: Arc<Logical>,
    core: Arc<dyn Core>,
    options: Connect,
    tx: oneshot::Sender<Result<(), Error>>,
) {
    let _logical_guard = LogicalGuard {
        inner: Arc::downgrade(&inner),
        logical: logical.clone(),
    };
    let mut initial = Some(tx);
    let mut audience = options.expected;
    let mut generation = 0_u64;
    let mut failures = 0_u32;
    let mut outage = None::<Instant>;
    let terminal = loop {
        if logical.cancel.is_cancelled() {
            break Ok(());
        }
        inner.publish(&logical, ConnectionStatus::Connecting { generation });
        let scope = AttemptScope::new(&inner.tasks, generation, inner.config.cleanup_timeout);
        let _attempt_guard = AttemptGuard(scope.clone());
        let auth = AuthenticationContext {
            signer: inner.signer.clone(),
            audience,
        };
        let attempt = async {
            let established = options.connector.establish(scope.clone(), auth).await?;
            if !scope.same(&established.scope) {
                return Err(Error::new(
                    ErrorKind::Authentication,
                    "connector returned a different attempt",
                ));
            }
            let peer = established.transport.peer_id();
            if let Audience::Known(expected) = audience
                && peer != expected
            {
                return Err(Error::with_source(
                    ErrorKind::Authentication,
                    "connector identity mismatch",
                    crate::error::PeerIdentityMismatch {
                        expected,
                        actual: peer,
                    },
                ));
            }
            let connection = established.transport.map(MessageTransport::new);
            // Register rollback before starting the async core handoff. On
            // cancellation it runs after the handoff future has been dropped.
            let cleanup_core = core.clone();
            let cleanup_connection = connection.clone();
            scope.on_cleanup(async move { cleanup_core.remove(cleanup_connection).await })?;
            core.add(connection).await?;
            Ok(peer)
        };
        let mut deadline = inner.config.connect_timeout;
        if let (Some(start), Some(policy)) = (outage, options.retry.as_ref()) {
            deadline = deadline.min(policy.max_elapsed.saturating_sub(start.elapsed()));
        }
        let result = tokio::select! {
            biased;
            () = logical.cancel.cancelled() => Err(Error::new(ErrorKind::Cancelled, "connection cancelled")),
            error = scope.failed() => Err(error),
            result = tokio::time::timeout(deadline, std::panic::AssertUnwindSafe(attempt).catch_unwind()) => match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err(Error::new(ErrorKind::Background, "connector panicked")),
                Err(_) => Err(Error::new(ErrorKind::Timeout, "connection attempt timed out")),
            },
        };
        let mut connected_at = None;
        let failure = match result {
            Ok(peer) => {
                *logical
                    .peer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(peer);
                if !options.rediscover {
                    audience = Audience::Known(peer);
                }
                inner.publish(&logical, ConnectionStatus::Connected { peer, generation });
                connected_at = Some(Instant::now());
                if let Some(tx) = initial.take()
                    && tx.send(Ok(())).is_err()
                {
                    logical.cancel.cancel();
                }
                tokio::select! {
                    biased;
                    () = logical.cancel.cancelled() => Error::new(ErrorKind::Cancelled, "connection closed"),
                    error = scope.failed() => error,
                }
            }
            Err(error) => error,
        };
        if let Some(policy) = &options.retry {
            if connected_at.is_some_and(|at| at.elapsed() >= policy.healthy_after) {
                failures = 0;
                outage = None;
            }
            if initial.is_none() {
                outage.get_or_insert_with(Instant::now);
            }
        }
        if logical.cancel.is_cancelled() {
            inner.publish(&logical, ConnectionStatus::Closing);
        } else {
            inner.publish(
                &logical,
                ConnectionStatus::CleaningUp {
                    generation,
                    error: failure.clone(),
                },
            );
        }
        let cleanup = scope.cleanup().await;
        if let Err(error) = cleanup {
            inner.tasks.fail(error.clone());
            break Err(error);
        }
        if logical.cancel.is_cancelled() {
            break Ok(());
        }
        if initial.is_some() {
            break Err(failure);
        }
        let Some(policy) = &options.retry else {
            break Err(failure);
        };
        if matches!(
            failure.kind(),
            ErrorKind::Authentication
                | ErrorKind::Policy
                | ErrorKind::Cancelled
                | ErrorKind::Closed
                | ErrorKind::Cleanup
                | ErrorKind::Background
        ) || options.connector.classify_failure(&failure) == RetryDecision::Stop
        {
            break Err(failure);
        }
        let start = *outage.get_or_insert_with(Instant::now);
        if failures >= policy.max_attempts || start.elapsed() >= policy.max_elapsed {
            break Err(failure);
        }
        let delay = policy
            .delay(failures)
            .min(policy.max_elapsed.saturating_sub(start.elapsed()));
        failures += 1;
        generation += 1;
        inner.publish(
            &logical,
            ConnectionStatus::Backoff {
                generation,
                error: failure.clone(),
                delay,
            },
        );
        tokio::select! { biased; () = logical.cancel.cancelled() => break Ok(()), () = tokio::time::sleep(delay) => {} }
        if start.elapsed() >= policy.max_elapsed {
            break Err(failure);
        }
    };
    if let Some(tx) = initial {
        let _delivered = tx.send(terminal.clone().and_then(|()| {
            Err(Error::new(
                ErrorKind::Cancelled,
                "initial connection cancelled",
            ))
        }));
    }
    inner.publish(&logical, ConnectionStatus::Closed(terminal));
}
