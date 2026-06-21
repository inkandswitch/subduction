//! Task-per-connection manager for handling multiple connections.
//!
//! This module provides [`ConnectionManager`] which spawns an independent task
//! for each connection. This provides:
//! - **Isolation**: A panic or failure in one connection doesn't affect others
//! - **Parallelism**: On multi-threaded runtimes, connections can run on different threads
//! - **Active removal**: Connections can be aborted immediately
//!
//! The manager tracks connections using two separate ID types:
//! - [`ConnectionId`]: Logical identifier that survives reconnects (returned to caller)
//! - `TaskId`: Internal identifier for the spawned task (changes on reconnect)

use alloc::{
    sync::{Arc, Weak},
    vec::Vec,
};
use core::sync::atomic::{AtomicUsize, Ordering};

use async_lock::{Mutex, Semaphore, SemaphoreGuardArc};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::stream::AbortHandle;
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    collections::Map,
};

use super::{Connection, id::ConnectionId};
use crate::peer::id::PeerId;

/// Maximum number of inbound handler-dispatch slots kept in flight per peer.
///
/// Each peer gets its own [`Semaphore`] with this many permits, shared across
/// all of that peer's connections. A reader acquires a permit before admitting
/// a request (see [`connection_loop`]) and holds it until the spawned handler
/// completes, capping a peer's concurrent (queued or running) handlers.
///
/// At the cap the reader stops pulling from the connection, so a flooding or
/// slow peer backpressures its own transport rather than consuming a shared
/// dispatch budget.
pub(crate) const MAX_INFLIGHT_DISPATCH_PER_PEER: usize = 512;

/// Get or create the per-peer dispatch [`Semaphore`], shared across all of a
/// peer's connections.
///
/// The registry holds [`Weak`] references: a peer's semaphore stays alive only
/// while one of its `connection_loop`s — or a still-running dispatch task
/// holding a permit — keeps an `Arc`. Once the last holder drops, the `Weak`
/// dangles and is pruned on the next miss, so the registry can't grow unbounded
/// across peer churn. A reconnecting peer whose tasks are still draining
/// re-shares the same semaphore, so its in-flight budget carries across the
/// handoff.
fn slots_for(peer: PeerId, registry: &mut Map<PeerId, Weak<Semaphore>>) -> Arc<Semaphore> {
    if let Some(live) = registry.get(&peer).and_then(Weak::upgrade) {
        return live;
    }

    // Miss: this peer has no live semaphore. Drop any other dangling entries
    // before inserting so peers that departed for good can't accumulate.
    registry.retain(|_, weak| weak.strong_count() > 0);

    let slots = Arc::new(Semaphore::new(MAX_INFLIGHT_DISPATCH_PER_PEER));
    registry.insert(peer, Arc::downgrade(&slots));
    slots
}

/// Internal task identifier for abort handle tracking.
///
/// Changes each time a new task is spawned for a connection.
type TaskId = usize;

/// Commands that can be sent to the [`ConnectionManager`].
#[derive(Debug)]
pub enum Command<Conn> {
    /// Add a new connection to be managed.
    ///
    /// The manager assigns a new [`ConnectionId`] and returns it via the closed channel
    /// when the connection drops.
    Add(Conn, PeerId),

    /// Re-add a reconnected connection, preserving its [`ConnectionId`].
    ///
    /// Used after a successful reconnection to restore the connection to the manager
    /// with the same logical identity.
    ReAdd(ConnectionId, Conn, PeerId),

    /// Remove a connection by its [`ConnectionId`] (aborts its task immediately).
    RemoveById(ConnectionId),

    /// Remove a connection by reference (aborts its task immediately).
    ///
    /// Useful when you have a connection object but not its ID.
    Remove(Conn),
}

/// Trait for spawning connection handler tasks.
///
/// Implement this for your runtime (e.g., tokio, async-std, wasm-bindgen-futures).
pub trait Spawn<Async: FutureForm> {
    /// Spawn a future as a background task.
    ///
    /// The future should be driven to completion. The returned [`AbortHandle`]
    /// can be used to cancel the task.
    fn spawn(&self, fut: Async::Future<'static, ()>) -> AbortHandle;
}

/// Manages connections by spawning an independent task for each one.
///
/// Unlike [`SelectAll`]-based approaches, each connection runs in its own task,
/// providing isolation and (on multi-threaded runtimes) true parallelism.
pub struct ConnectionManager<
    Async: FutureForm,
    Conn,
    WireMsg: Encode + Decode,
    Spawner: Spawn<Async>,
> {
    spawner: Spawner,

    /// Counter for generating internal task IDs.
    next_task_id: AtomicUsize,

    /// Counter for generating logical connection IDs.
    next_connection_id: AtomicUsize,

    /// Active tasks: maps (`ConnectionId`, `TaskId`) to (`AbortHandle`, `Connection`).
    ///
    /// The connection is stored to enable `Remove(Conn)` lookup via `PartialEq`.
    #[allow(clippy::type_complexity)]
    tasks: Arc<Mutex<Vec<(ConnectionId, TaskId, AbortHandle, Conn)>>>,

    /// Inbound commands (add/remove connections).
    commands: async_channel::Receiver<Command<Conn>>,

    /// Outbound (non-response) messages from all connections (bounded —
    /// provides backpressure).
    ///
    /// Each message carries a [`SemaphoreGuardArc`] from its peer's dispatch
    /// semaphore, acquired in [`connection_loop`] and held until the spawned
    /// handler completes, which caps in-flight dispatches per peer.
    messages: async_channel::Sender<(Conn, WireMsg, SemaphoreGuardArc)>,

    /// Fast path for response messages (bounded at high capacity).
    ///
    /// `BatchSyncResponse` messages are routed here instead of through
    /// `messages`, so that responses to our own requests are never blocked
    /// by a full request queue. Bounded at 8192 to cap memory usage if a
    /// peer floods fake responses; legitimate use never exceeds a few
    /// hundred concurrent in-flight requests.
    responses: async_channel::Sender<(Conn, WireMsg)>,

    /// Predicate to identify response messages that should use the fast path.
    is_response: fn(&WireMsg) -> bool,

    /// Notification when a connection closes (either normally or due to error).
    ///
    /// Sends the [`ConnectionId`] and connection object so the caller can
    /// decide whether to attempt reconnection.
    closed: async_channel::Sender<(ConnectionId, Conn)>,

    _marker: core::marker::PhantomData<Async>,
}

impl<Async: FutureForm, Conn, WireMsg: Encode + Decode, Spawner: Spawn<Async>>
    ConnectionManager<Async, Conn, WireMsg, Spawner>
{
    /// Create a new [`ConnectionManager`].
    #[must_use]
    pub fn new(
        spawner: Spawner,
        commands: async_channel::Receiver<Command<Conn>>,
        messages: async_channel::Sender<(Conn, WireMsg, SemaphoreGuardArc)>,
        responses: async_channel::Sender<(Conn, WireMsg)>,
        is_response: fn(&WireMsg) -> bool,
        closed: async_channel::Sender<(ConnectionId, Conn)>,
    ) -> Self {
        Self {
            spawner,
            next_task_id: AtomicUsize::new(0),
            next_connection_id: AtomicUsize::new(0),
            tasks: Arc::new(Mutex::new(Vec::new())),
            commands,
            messages,
            responses,
            is_response,
            closed,
            _marker: core::marker::PhantomData,
        }
    }

    /// Get the number of active connections.
    pub async fn connection_count(&self) -> usize {
        self.tasks.lock().await.len()
    }
}

impl<
    Async: FutureForm,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
    Spawner: Spawn<Async>,
> ConnectionManager<Async, Conn, WireMsg, Spawner>
{
    async fn remove_connection_by_ref(&self, conn: &Conn) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pos) = tasks.iter().position(|(_, _, _, c)| c == conn) {
            let (conn_id, task_id, handle, _) = tasks.swap_remove(pos);
            tracing::debug!(conn = %conn_id, task = %task_id, "removing connection");
            handle.abort();
        } else {
            tracing::debug!("connection not found for removal");
        }
    }

    async fn remove_connection_by_id(&self, conn_id: ConnectionId) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pos) = tasks.iter().position(|(id, _, _, _)| *id == conn_id) {
            let (conn_id, task_id, handle, _) = tasks.swap_remove(pos);
            tracing::debug!(conn = %conn_id, task = %task_id, "removing connection");
            handle.abort();
        } else {
            tracing::debug!(conn = %conn_id, "connection not found for removal");
        }
    }
}

/// Trait for running the connection manager.
///
/// This trait enables generic code to call `run()` on `ConnectionManager<Async, Conn, WireMsg, Spawner>`
/// without knowing whether Async is `Sendable` or `Local`.
pub trait RunManager<Conn, WireMsg: Encode + Decode>: FutureForm + Sized {
    /// Run the manager, processing commands to add/remove connections.
    fn run_manager<Spawner: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, Conn, WireMsg, Spawner>,
    ) -> Self::Future<'static, ()>
    where
        Conn: Connection<Self, WireMsg> + Clone + 'static;
}

impl<
    Async: FutureForm + RunManager<Conn, WireMsg>,
    Conn,
    WireMsg: Encode + Decode,
    Spawner: Spawn<Async> + Send + Sync + 'static,
> ConnectionManager<Async, Conn, WireMsg, Spawner>
{
    /// Run the manager, processing commands to add/remove connections.
    pub fn run(self) -> Async::Future<'static, ()>
    where
        Conn: Connection<Async, WireMsg> + Clone + 'static,
    {
        Async::run_manager(self)
    }
}

// Implementations of RunManager for Sendable and Local
#[future_form(
    Sendable where Conn: Connection<Sendable, WireMsg> + Clone + Send + Sync + 'static, Conn::RecvError: Send, WireMsg: Send + Sync + 'static,
    Local where Conn: Connection<Local, WireMsg> + Clone + 'static, WireMsg: 'static
)]
impl<Async: FutureForm, Conn, WireMsg: Encode + Decode> RunManager<Conn, WireMsg> for Async {
    #[allow(clippy::too_many_lines)]
    fn run_manager<Spawner: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, Conn, WireMsg, Spawner>,
    ) -> Self::Future<'static, ()> {
        Async::from_future(async move {
            // Per-peer dispatch semaphores, shared across a peer's connections.
            // Owned by this single command loop, so no extra synchronization is
            // needed; spawned `connection_loop`s only hold an `Arc` clone.
            let mut dispatch_registry: Map<PeerId, Weak<Semaphore>> = Map::new();

            while let Ok(cmd) = manager.commands.recv().await {
                match cmd {
                    Command::Add(conn, peer_id) => {
                        let conn_id = ConnectionId::new(
                            manager.next_connection_id.fetch_add(1, Ordering::Relaxed),
                        );
                        tracing::debug!(conn = %conn_id, peer = %peer_id, "adding connection");

                        let task_id = manager.next_task_id.fetch_add(1, Ordering::Relaxed);
                        let tasks = manager.tasks.clone();
                        let messages = manager.messages.clone();
                        let responses = manager.responses.clone();
                        let is_response = manager.is_response;
                        let closed = manager.closed.clone();
                        let conn_clone = conn.clone();
                        let dispatch_slots = slots_for(peer_id, &mut dispatch_registry);

                        let fut = Async::from_future(async move {
                            connection_loop(
                                conn_clone.clone(),
                                peer_id,
                                messages,
                                responses,
                                is_response,
                                dispatch_slots,
                            )
                            .await;

                            // Normal completion cleanup - remove from tasks list
                            let mut tasks_guard = tasks.lock().await;
                            let target_id: TaskId = task_id;
                            if let Some(pos) = tasks_guard.iter().position(
                                |(_, id, _, _): &(ConnectionId, TaskId, AbortHandle, Conn)| {
                                    *id == target_id
                                },
                            ) {
                                tasks_guard.swap_remove(pos);
                            }
                            drop(closed.send((conn_id, conn_clone)).await);
                            tracing::debug!(conn = %conn_id, peer = %peer_id, "connection closed normally");
                        });

                        let handle = manager.spawner.spawn(fut);
                        manager
                            .tasks
                            .lock()
                            .await
                            .push((conn_id, task_id, handle, conn));
                    }
                    Command::ReAdd(conn_id, conn, peer_id) => {
                        tracing::debug!(conn = %conn_id, peer = %peer_id, "re-adding connection");

                        let task_id = manager.next_task_id.fetch_add(1, Ordering::Relaxed);
                        let tasks = manager.tasks.clone();
                        let messages = manager.messages.clone();
                        let responses = manager.responses.clone();
                        let is_response = manager.is_response;
                        let closed = manager.closed.clone();
                        let conn_clone = conn.clone();
                        let dispatch_slots = slots_for(peer_id, &mut dispatch_registry);

                        let fut = Async::from_future(async move {
                            connection_loop(
                                conn_clone.clone(),
                                peer_id,
                                messages,
                                responses,
                                is_response,
                                dispatch_slots,
                            )
                            .await;

                            // Normal completion cleanup - remove from tasks list
                            let mut tasks_guard = tasks.lock().await;
                            let target_id: TaskId = task_id;
                            if let Some(pos) = tasks_guard.iter().position(
                                |(_, id, _, _): &(ConnectionId, TaskId, AbortHandle, Conn)| {
                                    *id == target_id
                                },
                            ) {
                                tasks_guard.swap_remove(pos);
                            }
                            drop(closed.send((conn_id, conn_clone)).await);
                            tracing::debug!(conn = %conn_id, peer = %peer_id, "connection closed normally");
                        });

                        let handle = manager.spawner.spawn(fut);
                        manager
                            .tasks
                            .lock()
                            .await
                            .push((conn_id, task_id, handle, conn));
                    }
                    Command::RemoveById(conn_id) => {
                        manager.remove_connection_by_id(conn_id).await;
                    }
                    Command::Remove(conn) => {
                        manager.remove_connection_by_ref(&conn).await;
                    }
                }
            }
            tracing::debug!("command channel closed, shutting down");

            // Abort outstanding `connection_loop`s and drop our `Conn` clones
            // so transports with `Clone`d channel senders (e.g. WebSocket)
            // can close their inbound channels.
            let mut tasks_guard = manager.tasks.lock().await;
            let n = tasks_guard.len();
            if n > 0 {
                tracing::debug!(count = n, "aborting connection_loop tasks");
                for (_, _, handle, _) in tasks_guard.iter() {
                    handle.abort();
                }
                tasks_guard.clear();
            }
        })
    }
}

async fn connection_loop<
    Async: FutureForm,
    Conn: Connection<Async, WireMsg>,
    WireMsg: Encode + Decode,
>(
    conn: Conn,
    peer_id: PeerId,
    messages: async_channel::Sender<(Conn, WireMsg, SemaphoreGuardArc)>,
    responses: async_channel::Sender<(Conn, WireMsg)>,
    is_response: fn(&WireMsg) -> bool,
    dispatch_slots: Arc<Semaphore>,
) {
    loop {
        match conn.recv().await {
            Ok(msg) => {
                tracing::trace!(peer = %peer_id, "connection received message");
                if is_response(&msg) {
                    // Responses use the fast path and are never throttled: they
                    // resolve a pending caller rather than spawning a handler.
                    if responses.send((conn.clone(), msg)).await.is_err() {
                        tracing::warn!(peer = %peer_id, "connection response channel closed");
                        break;
                    }
                } else {
                    // Acquire this peer's dispatch slot before forwarding. At
                    // the cap this awaits, pausing `conn.recv()` for this peer
                    // only; the permit rides the queue and releases when the
                    // handler completes. With metrics, take a try-first fast
                    // path so contention (the rate limiter engaging) is recorded
                    // without touching the uncontended path.
                    #[cfg(feature = "metrics")]
                    let permit = if let Some(permit) = dispatch_slots.try_acquire_arc() {
                        permit
                    } else {
                        let wait_start = std::time::Instant::now();
                        let permit = dispatch_slots.acquire_arc().await;
                        crate::metrics::dispatch_permit_waited(wait_start.elapsed().as_secs_f64());
                        permit
                    };
                    #[cfg(not(feature = "metrics"))]
                    let permit = dispatch_slots.acquire_arc().await;
                    if messages.send((conn.clone(), msg, permit)).await.is_err() {
                        tracing::warn!(peer = %peer_id, "connection message channel closed");
                        break;
                    }
                }
            }
            Err(e) => {
                tracing::debug!(peer = %peer_id, error = %e, "connection recv error");
                break;
            }
        }
    }
}

impl<Async: FutureForm, Conn, WireMsg: Encode + Decode, Spawner: Spawn<Async>> core::fmt::Debug
    for ConnectionManager<Async, Conn, WireMsg, Spawner>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

/// A future representing the running [`ConnectionManager`].
///
/// This allows the caller to monitor and control the lifecycle of the manager.
pub struct ManagerFuture<Async: FutureForm> {
    fut: core::pin::Pin<alloc::boxed::Box<futures::stream::Abortable<Async::Future<'static, ()>>>>,
}

impl<Async: FutureForm> ManagerFuture<Async> {
    /// Create a new manager future from an abortable future.
    pub fn new(fut: futures::stream::Abortable<Async::Future<'static, ()>>) -> Self {
        Self {
            fut: alloc::boxed::Box::pin(fut),
        }
    }

    /// Check if the manager future has been aborted.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.fut.is_aborted()
    }
}

impl<Async: FutureForm> core::future::Future for ManagerFuture<Async> {
    type Output = Result<(), futures::stream::Aborted>;

    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<Async: FutureForm> Unpin for ManagerFuture<Async> {}

impl<Async: FutureForm> core::fmt::Debug for ManagerFuture<Async> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ManagerFuture")
            .field("is_aborted", &self.is_aborted())
            .finish()
    }
}
