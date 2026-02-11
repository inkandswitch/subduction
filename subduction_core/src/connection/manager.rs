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

use alloc::{sync::Arc, vec::Vec};
use core::sync::atomic::{AtomicUsize, Ordering};

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::stream::AbortHandle;

use super::{Connection, id::ConnectionId, message::Message};

/// Internal task identifier for abort handle tracking.
///
/// Changes each time a new task is spawned for a connection.
type TaskId = usize;

/// Commands that can be sent to the [`ConnectionManager`].
#[derive(Debug)]
pub enum Command<C> {
    /// Add a new connection to be managed.
    ///
    /// The manager assigns a new [`ConnectionId`] and returns it via the closed channel
    /// when the connection drops.
    Add(C),

    /// Re-add a reconnected connection, preserving its [`ConnectionId`].
    ///
    /// Used after a successful reconnection to restore the connection to the manager
    /// with the same logical identity.
    ReAdd(ConnectionId, C),

    /// Remove a connection by its [`ConnectionId`] (aborts its task immediately).
    RemoveById(ConnectionId),

    /// Remove a connection by reference (aborts its task immediately).
    ///
    /// Useful when you have a connection object but not its ID.
    Remove(C),
}

/// Trait for spawning connection handler tasks.
///
/// Implement this for your runtime (e.g., tokio, async-std, wasm-bindgen-futures).
pub trait Spawn<K: FutureForm> {
    /// Spawn a future as a background task.
    ///
    /// The future should be driven to completion. The returned [`AbortHandle`]
    /// can be used to cancel the task.
    fn spawn(&self, fut: K::Future<'static, ()>) -> AbortHandle;
}

/// Manages connections by spawning an independent task for each one.
///
/// Unlike [`SelectAll`]-based approaches, each connection runs in its own task,
/// providing isolation and (on multi-threaded runtimes) true parallelism.
pub struct ConnectionManager<K: FutureForm, C, S: Spawn<K>> {
    spawner: S,

    /// Counter for generating internal task IDs.
    next_task_id: AtomicUsize,

    /// Counter for generating logical connection IDs.
    next_connection_id: AtomicUsize,

    /// Active tasks: maps (`ConnectionId`, `TaskId`) to (`AbortHandle`, `Connection`).
    ///
    /// The connection is stored to enable `Remove(C)` lookup via `PartialEq`.
    #[allow(clippy::type_complexity)]
    tasks: Arc<Mutex<Vec<(ConnectionId, TaskId, AbortHandle, C)>>>,

    /// Inbound commands (add/remove connections).
    commands: async_channel::Receiver<Command<C>>,

    /// Outbound messages from all connections.
    messages: async_channel::Sender<(C, Message)>,

    /// Notification when a connection closes (either normally or due to error).
    ///
    /// Sends the [`ConnectionId`] and connection object so the caller can
    /// decide whether to attempt reconnection.
    closed: async_channel::Sender<(ConnectionId, C)>,

    _marker: core::marker::PhantomData<K>,
}

impl<K: FutureForm, C, S: Spawn<K>> ConnectionManager<K, C, S> {
    /// Create a new [`ConnectionManager`].
    #[must_use]
    pub fn new(
        spawner: S,
        commands: async_channel::Receiver<Command<C>>,
        messages: async_channel::Sender<(C, Message)>,
        closed: async_channel::Sender<(ConnectionId, C)>,
    ) -> Self {
        Self {
            spawner,
            next_task_id: AtomicUsize::new(0),
            next_connection_id: AtomicUsize::new(0),
            tasks: Arc::new(Mutex::new(Vec::new())),
            commands,
            messages,
            closed,
            _marker: core::marker::PhantomData,
        }
    }

    /// Get the number of active connections.
    pub async fn connection_count(&self) -> usize {
        self.tasks.lock().await.len()
    }
}

impl<K: FutureForm, C: Connection<K>, S: Spawn<K>> ConnectionManager<K, C, S> {
    async fn remove_connection_by_ref(&self, conn: &C) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pos) = tasks.iter().position(|(_, _, _, c)| c == conn) {
            let (conn_id, task_id, handle, _) = tasks.swap_remove(pos);
            tracing::debug!("ConnectionManager: removing connection {conn_id} (task {task_id})");
            handle.abort();
        } else {
            tracing::debug!("ConnectionManager: connection not found for removal");
        }
    }

    async fn remove_connection_by_id(&self, conn_id: ConnectionId) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pos) = tasks.iter().position(|(id, _, _, _)| *id == conn_id) {
            let (conn_id, task_id, handle, _) = tasks.swap_remove(pos);
            tracing::debug!("ConnectionManager: removing connection {conn_id} (task {task_id})");
            handle.abort();
        } else {
            tracing::debug!("ConnectionManager: connection {conn_id} not found for removal");
        }
    }
}

/// Trait for running the connection manager.
///
/// This trait enables generic code to call `run()` on `ConnectionManager<K, C, S>`
/// without knowing whether K is Sendable or Local.
pub trait RunManager<C>: FutureForm + Sized {
    /// Run the manager, processing commands to add/remove connections.
    fn run_manager<S: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, C, S>,
    ) -> Self::Future<'static, ()>
    where
        C: Connection<Self> + Clone + 'static;
}

impl<K: FutureForm + RunManager<C>, C, S: Spawn<K> + Send + Sync + 'static>
    ConnectionManager<K, C, S>
{
    /// Run the manager, processing commands to add/remove connections.
    pub fn run(self) -> K::Future<'static, ()>
    where
        C: Connection<K> + Clone + 'static,
    {
        K::run_manager(self)
    }
}

// Implementations of RunManager for Sendable and Local
#[future_form(
    Sendable where C: Connection<Sendable> + Clone + Send + Sync + 'static, C::RecvError: Send,
    Local where C: Connection<Local> + Clone + 'static
)]
impl<K: FutureForm, C> RunManager<C> for K {
    fn run_manager<S: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, C, S>,
    ) -> Self::Future<'static, ()> {
        K::from_future(async move {
            while let Ok(cmd) = manager.commands.recv().await {
                match cmd {
                    Command::Add(conn) => {
                        let peer_id = conn.peer_id();
                        let conn_id = ConnectionId::new(
                            manager.next_connection_id.fetch_add(1, Ordering::Relaxed),
                        );
                        tracing::debug!(
                            "ConnectionManager: adding connection {conn_id} for peer {peer_id}"
                        );

                        let task_id = manager.next_task_id.fetch_add(1, Ordering::Relaxed);
                        let tasks = manager.tasks.clone();
                        let messages = manager.messages.clone();
                        let closed = manager.closed.clone();
                        let conn_clone = conn.clone();

                        let fut = K::from_future(async move {
                            connection_loop(conn_clone.clone(), messages).await;

                            // Normal completion cleanup - remove from tasks list
                            let mut tasks_guard = tasks.lock().await;
                            let target_id: TaskId = task_id;
                            if let Some(pos) = tasks_guard.iter().position(
                                |(_, id, _, _): &(ConnectionId, TaskId, AbortHandle, C)| {
                                    *id == target_id
                                },
                            ) {
                                tasks_guard.swap_remove(pos);
                            }
                            drop(closed.send((conn_id, conn_clone)).await);
                            tracing::debug!(
                                "connection {conn_id} for peer {peer_id}: closed normally"
                            );
                        });

                        let handle = manager.spawner.spawn(fut);
                        manager
                            .tasks
                            .lock()
                            .await
                            .push((conn_id, task_id, handle, conn));
                    }
                    Command::ReAdd(conn_id, conn) => {
                        let peer_id = conn.peer_id();
                        tracing::debug!(
                            "ConnectionManager: re-adding connection {conn_id} for peer {peer_id}"
                        );

                        let task_id = manager.next_task_id.fetch_add(1, Ordering::Relaxed);
                        let tasks = manager.tasks.clone();
                        let messages = manager.messages.clone();
                        let closed = manager.closed.clone();
                        let conn_clone = conn.clone();

                        let fut = K::from_future(async move {
                            connection_loop(conn_clone.clone(), messages).await;

                            // Normal completion cleanup - remove from tasks list
                            let mut tasks_guard = tasks.lock().await;
                            let target_id: TaskId = task_id;
                            if let Some(pos) = tasks_guard.iter().position(
                                |(_, id, _, _): &(ConnectionId, TaskId, AbortHandle, C)| {
                                    *id == target_id
                                },
                            ) {
                                tasks_guard.swap_remove(pos);
                            }
                            drop(closed.send((conn_id, conn_clone)).await);
                            tracing::debug!(
                                "connection {conn_id} for peer {peer_id}: closed normally"
                            );
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
            tracing::debug!("ConnectionManager: command channel closed, shutting down");
        })
    }
}

async fn connection_loop<K: FutureForm, C: Connection<K>>(
    conn: C,
    messages: async_channel::Sender<(C, Message)>,
) {
    let peer_id = conn.peer_id();
    loop {
        match conn.recv().await {
            Ok(msg) => {
                tracing::debug!("connection for peer {peer_id}: received message");
                if messages.send((conn.clone(), msg)).await.is_err() {
                    tracing::warn!("connection for peer {peer_id}: message channel closed");
                    break;
                }
            }
            Err(e) => {
                tracing::debug!("connection for peer {peer_id}: recv error: {e}");
                break;
            }
        }
    }
}

impl<K: FutureForm, C, S: Spawn<K>> core::fmt::Debug for ConnectionManager<K, C, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

/// A future representing the running [`ConnectionManager`].
///
/// This allows the caller to monitor and control the lifecycle of the manager.
pub struct ManagerFuture<K: FutureForm> {
    fut: core::pin::Pin<alloc::boxed::Box<futures::stream::Abortable<K::Future<'static, ()>>>>,
}

impl<K: FutureForm> ManagerFuture<K> {
    /// Create a new manager future from an abortable future.
    pub fn new(fut: futures::stream::Abortable<K::Future<'static, ()>>) -> Self {
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

impl<K: FutureForm> core::future::Future for ManagerFuture<K> {
    type Output = Result<(), futures::stream::Aborted>;

    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<K: FutureForm> Unpin for ManagerFuture<K> {}

impl<K: FutureForm> core::fmt::Debug for ManagerFuture<K> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ManagerFuture")
            .field("is_aborted", &self.is_aborted())
            .finish()
    }
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::connection::test_utils::MockConnection;
    use future_form::Sendable;
    use futures::future::BoxFuture;

    /// A spawner that uses `FuturesUnordered` for testing (no actual spawning).
    struct TestSpawn;

    impl Spawn<Sendable> for TestSpawn {
        fn spawn(&self, _fut: BoxFuture<'static, ()>) -> AbortHandle {
            // For testing, we don't actually spawn - just return a dummy handle
            let (handle, _reg) = AbortHandle::new_pair();
            handle
        }
    }

    #[test]
    fn test_manager_creation() {
        let (_cmd_tx, cmd_rx) = async_channel::unbounded();
        let (msg_tx, _msg_rx) = async_channel::unbounded();
        let (closed_tx, _closed_rx) = async_channel::unbounded::<(ConnectionId, MockConnection)>();

        let _manager: ConnectionManager<Sendable, MockConnection, _> =
            ConnectionManager::new(TestSpawn, cmd_rx, msg_tx, closed_tx);
    }
}
