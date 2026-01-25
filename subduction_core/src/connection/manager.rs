//! Task-per-connection manager for handling multiple connections.
//!
//! This module provides [`ConnectionManager`] which spawns an independent task
//! for each connection. This provides:
//! - **Isolation**: A panic or failure in one connection doesn't affect others
//! - **Parallelism**: On multi-threaded runtimes, connections can run on different threads
//! - **Active removal**: Connections can be aborted immediately
//!
//! The manager uses an internal `TaskId` for tracking spawned tasks. This is not
//! exposed externally - callers work directly with connection objects `C`.

use alloc::sync::Arc;
use alloc::vec::Vec;
use core::sync::atomic::{AtomicUsize, Ordering};

use async_lock::Mutex;
use futures::stream::AbortHandle;
use futures_kind::{FutureKind, Local, Sendable};

use super::{Connection, message::Message};

/// Internal task identifier for abort handle tracking.
///
/// Not exposed externally - callers use connection objects directly.
type TaskId = usize;

/// Commands that can be sent to the [`ConnectionManager`].
#[derive(Debug)]
pub enum Command<C> {
    /// Add a new connection to be managed.
    Add(C),
    /// Remove a connection (aborts its task immediately).
    Remove(C),
}

/// Trait for spawning connection handler tasks.
///
/// Implement this for your runtime (e.g., tokio, async-std, wasm-bindgen-futures).
pub trait Spawn<K: FutureKind> {
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
pub struct ConnectionManager<K: FutureKind, C, S: Spawn<K>> {
    spawner: S,

    /// Counter for generating internal task IDs.
    next_task_id: AtomicUsize,

    /// Active tasks: maps internal TaskId to (AbortHandle, Connection).
    ///
    /// The connection is stored to enable `Remove(C)` lookup via `PartialEq`.
    tasks: Arc<Mutex<Vec<(TaskId, AbortHandle, C)>>>,

    /// Inbound commands (add/remove connections).
    commands: async_channel::Receiver<Command<C>>,

    /// Outbound messages from all connections.
    messages: async_channel::Sender<(C, Message)>,

    /// Notification when a connection closes (either normally or due to error).
    closed: async_channel::Sender<C>,

    _marker: core::marker::PhantomData<K>,
}

impl<K: FutureKind, C, S: Spawn<K>> ConnectionManager<K, C, S> {
    /// Create a new [`ConnectionManager`].
    #[must_use]
    pub fn new(
        spawner: S,
        commands: async_channel::Receiver<Command<C>>,
        messages: async_channel::Sender<(C, Message)>,
        closed: async_channel::Sender<C>,
    ) -> Self {
        Self {
            spawner,
            next_task_id: AtomicUsize::new(0),
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

impl<K: FutureKind, C: Connection<K>, S: Spawn<K>> ConnectionManager<K, C, S> {
    async fn remove_connection(&self, conn: &C) {
        let mut tasks = self.tasks.lock().await;
        if let Some(pos) = tasks.iter().position(|(_, _, c)| c == conn) {
            let (task_id, handle, _) = tasks.swap_remove(pos);
            tracing::debug!("ConnectionManager: removing connection (task {task_id})");
            handle.abort();
        } else {
            tracing::debug!("ConnectionManager: connection not found for removal");
        }
    }
}

/// Trait for running the connection manager.
///
/// This trait enables generic code to call `run()` on `ConnectionManager<K, C, S>`
/// without knowing whether K is Sendable or Local.
pub trait RunManager<C>: FutureKind + Sized {
    /// Run the manager, processing commands to add/remove connections.
    fn run_manager<S: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, C, S>,
    ) -> Self::Future<'static, ()>
    where
        C: Connection<Self> + Clone + 'static;
}

impl<K: FutureKind + RunManager<C>, C, S: Spawn<K> + Send + Sync + 'static>
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
#[futures_kind::kinds(
    Sendable where C: Connection<Sendable> + Clone + Send + Sync + 'static, C::RecvError: Send,
    Local where C: Connection<Local> + Clone + 'static
)]
impl<K: FutureKind, C> RunManager<C> for K {
    fn run_manager<S: Spawn<Self> + Send + Sync + 'static>(
        manager: ConnectionManager<Self, C, S>,
    ) -> Self::Future<'static, ()> {
        K::into_kind(async move {
            while let Ok(cmd) = manager.commands.recv().await {
                match cmd {
                    Command::Add(conn) => {
                        let peer_id = conn.peer_id();
                        tracing::debug!("ConnectionManager: adding connection for peer {peer_id}");

                        let task_id = manager.next_task_id.fetch_add(1, Ordering::Relaxed);
                        let tasks = manager.tasks.clone();
                        let messages = manager.messages.clone();
                        let closed = manager.closed.clone();
                        let conn_clone = conn.clone();

                        // Create the connection future inline
                        let fut = K::into_kind(async move {
                            connection_loop(conn_clone.clone(), messages).await;

                            // Normal completion cleanup - remove from tasks list
                            let mut tasks_guard = tasks.lock().await;
                            let target_id: TaskId = task_id;
                            if let Some(pos) = tasks_guard.iter().position(|(id, _, _): &(TaskId, AbortHandle, C)| *id == target_id) {
                                tasks_guard.swap_remove(pos);
                            }
                            let _ = closed.send(conn_clone).await;
                            tracing::debug!("connection for peer {peer_id}: closed normally");
                        });

                        let handle = manager.spawner.spawn(fut);
                        manager.tasks.lock().await.push((task_id, handle, conn));
                    }
                    Command::Remove(conn) => {
                        manager.remove_connection(&conn).await;
                    }
                }
            }
            tracing::debug!("ConnectionManager: command channel closed, shutting down");
        })
    }
}

async fn connection_loop<K: FutureKind, C: Connection<K>>(
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

impl<K: FutureKind, C, S: Spawn<K>> core::fmt::Debug for ConnectionManager<K, C, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectionManager").finish_non_exhaustive()
    }
}

/// A future representing the running [`ConnectionManager`].
///
/// This allows the caller to monitor and control the lifecycle of the manager.
pub struct ManagerFuture<K: FutureKind> {
    fut: core::pin::Pin<alloc::boxed::Box<futures::stream::Abortable<K::Future<'static, ()>>>>,
}

impl<K: FutureKind> ManagerFuture<K> {
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

impl<K: FutureKind> core::future::Future for ManagerFuture<K> {
    type Output = Result<(), futures::stream::Aborted>;

    fn poll(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<K: FutureKind> Unpin for ManagerFuture<K> {}

impl<K: FutureKind> core::fmt::Debug for ManagerFuture<K> {
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
    use futures::future::BoxFuture;
    use futures_kind::Sendable;

    /// A spawner that uses FuturesUnordered for testing (no actual spawning).
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
        let (closed_tx, _closed_rx) = async_channel::unbounded();

        let _manager: ConnectionManager<Sendable, MockConnection, _> =
            ConnectionManager::new(TestSpawn, cmd_rx, msg_tx, closed_tx);
    }
}
