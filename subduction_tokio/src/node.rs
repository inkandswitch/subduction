//! [`TokioSubduction`]: an owned, supervised Subduction node on Tokio.
//!
//! `subduction_core` deliberately has no runtime. Construction returns the
//! node plus two futures — the connection manager and the listener — and
//! leaves driving, supervising, and tearing them down to the caller. Every
//! Tokio consumer ends up writing the same forty lines to do that, and the
//! same subtle bugs: a stale `Arc<Subduction>` in a forgotten task keeps the
//! storage backend's file lock held; a loop that dies unnoticed leaves a node
//! that completes handshakes it can never service; teardown in the wrong
//! order deadlocks `TaskTracker::wait`.
//!
//! [`TokioSubduction`] is that code, once. It is the *owner* of a node:
//!
//! ```text
//!  TokioSubduction::start(|spawner| build(spawner))
//!     │
//!     ├─ spawns listener + manager onto its TaskTracker, supervised
//!     ├─ node.spawn(task)       your tasks, cancelled when the node stops
//!     │
//!     └─ node.stop().await      cancel → Subduction::stop → tracker.wait
//!        drop(node)             last Arc<Subduction> gone → storage released
//! ```
//!
//! It is not `Clone`: there is one owner, and dropping it stops the node.
//! Share the node itself by cloning [`subduction()`](TokioSubduction::subduction)
//! into scoped work, or hand out `Arc::downgrade`d weak references to things
//! you can't scope.

use std::{
    fmt,
    future::Future,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use future_form::Sendable;
use sedimentree_core::depth::{CountLeadingZeroBytes, DepthMetric};
use subduction_core::{
    connection::{Connection, manager::ManagerFuture},
    handler::Handler,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    subduction::{Subduction, SubductionFutureForm, listener_future::ListenerFuture},
    timeout::Timeout,
};
use subduction_crypto::signer::Signer;
use tokio::task::JoinHandle;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{spawn::TrackedTokioSpawn, timeout::TimeoutTokio};

/// The `Subduction` type a [`TokioSubduction`] owns.
pub type Node<
    Store,
    Conn,
    Hdl,
    Auth,
    Sign,
    Timer = TimeoutTokio,
    Metric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> = Subduction<
    'static,
    Sendable,
    Store,
    Conn,
    Hdl,
    Auth,
    Sign,
    Timer,
    TrackedTokioSpawn,
    Metric,
    SHARDS,
>;

/// What a build closure hands back to [`TokioSubduction::start`]: the node
/// and its two loop futures, exactly as `Subduction::new` and the
/// `SubductionBuilder::build*` family return them.
pub type Parts<
    Store,
    Conn,
    Hdl,
    Auth,
    Sign,
    Timer = TimeoutTokio,
    Metric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> = (
    Arc<Node<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>>,
    ListenerFuture<
        'static,
        Sendable,
        Store,
        Conn,
        Hdl,
        Auth,
        Sign,
        Timer,
        TrackedTokioSpawn,
        Metric,
        SHARDS,
    >,
    ManagerFuture<Sendable>,
);

/// An owned, supervised Subduction node running on Tokio.
///
/// See the [module docs](self) for the lifecycle. Dereferences to the
/// underlying [`Subduction`] so sync and storage calls work directly on it.
pub struct TokioSubduction<
    Store,
    Conn,
    Hdl,
    Auth,
    Sign,
    Timer = TimeoutTokio,
    Metric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> where
    Store: Storage<Sendable> + Send + Sync + 'static,
    Conn: Connection<Sendable, Hdl::Message> + PartialEq + Clone + Send + Sync + 'static,
    Hdl: Handler<Sendable, Conn> + Send + Sync + 'static,
    Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    Sign: Signer<Sendable> + Send + Sync + 'static,
    Timer: Timeout<Sendable> + Clone + Send + Sync + 'static,
    Metric: DepthMetric + Send + Sync + 'static,
    Sendable: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
{
    subduction: Arc<Node<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>>,
    /// Every task this node is responsible for: the two loop supervisors,
    /// everything the node's own `TrackedTokioSpawn` fans out (connection
    /// readers, dispatch), and everything passed to [`spawn`](Self::spawn).
    tasks: TaskTracker,
    /// Cancelled on stop. Tasks from [`spawn`](Self::spawn) run under a child
    /// of this token; callers can also observe it directly.
    cancel: CancellationToken,
    /// Set before any teardown begins, by whichever of `request_stop` or a
    /// supervisor gets there first. Gating supervision on a swap of this
    /// flag prevents a spurious ERROR on orderly stop and a double report on
    /// failure.
    stopping: Arc<AtomicBool>,
    /// Set by a supervisor when a loop exited *before* anyone asked it to.
    failed: Arc<AtomicBool>,
}

impl<Store, Conn, Hdl, Auth, Sign, Timer, Metric, const SHARDS: usize>
    TokioSubduction<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Store: Storage<Sendable> + Send + Sync + 'static,
    Conn: Connection<Sendable, Hdl::Message> + PartialEq + Clone + Send + Sync + 'static,
    Hdl: Handler<Sendable, Conn> + Send + Sync + 'static,
    Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    Sign: Signer<Sendable> + Send + Sync + 'static,
    Timer: Timeout<Sendable> + Clone + Send + Sync + 'static,
    Metric: DepthMetric + Send + Sync + 'static,
    Sendable: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
{
    /// Build a node and start it under a fresh [`TaskTracker`] and
    /// [`CancellationToken`].
    ///
    /// The closure receives the [`TrackedTokioSpawn`] the node must be built
    /// with — pass it to `SubductionBuilder::spawner` — so the node's own
    /// fan-out lands on the same tracker `stop` waits on. Return the node and
    /// its two loop futures; they are spawned and supervised immediately.
    ///
    /// Must be called from within a Tokio runtime.
    ///
    /// ```ignore
    /// let node = TokioSubduction::start(|spawner| {
    ///     let (sd, _handler, listener, manager) = SubductionBuilder::new()
    ///         .signer(signer)
    ///         .storage(storage, policy)
    ///         .spawner(spawner)
    ///         .timer(TimeoutTokio)
    ///         .build::<Sendable, MyConn>();
    ///     (sd, listener, manager)
    /// });
    /// ```
    pub fn start<F>(build: F) -> Self
    where
        F: FnOnce(TrackedTokioSpawn) -> Parts<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>,
    {
        Self::start_with(TaskTracker::new(), CancellationToken::new(), build)
    }

    /// Like [`start`](Self::start) but under a caller-supplied tracker and
    /// token.
    ///
    /// Use this to fold the node into a larger lifecycle: a server that also
    /// runs an accept loop on the same tracker, or a process whose root
    /// token should be cancelled if the node's loops die. The token is used
    /// *directly*, not as a child — cancelling the node cancels whatever you
    /// passed in, and vice versa.
    ///
    /// Must be called from within a Tokio runtime.
    pub fn start_with<F>(tasks: TaskTracker, cancel: CancellationToken, build: F) -> Self
    where
        F: FnOnce(TrackedTokioSpawn) -> Parts<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>,
    {
        let (subduction, listener, manager) = build(TrackedTokioSpawn::new(tasks.clone()));

        let node = Self {
            subduction,
            tasks,
            cancel,
            stopping: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
        };

        // Spawned directly, not under `run_until_cancelled`: the loops must
        // exit via their channel-close paths (`request_stop`) so the manager
        // gets to abort its connection readers. Dropping the manager future
        // mid-execution would leave those readers parked and `tasks.wait()`
        // would never return.
        node.supervise("connection manager", manager);
        node.supervise("listener", listener);
        node
    }

    /// Spawn `fut` as a supervisor: when it completes, decide whether that
    /// was the orderly stop or a failure, and in either case make sure the
    /// rest of the node comes down too.
    fn supervise<F>(&self, what: &'static str, fut: F)
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let subduction = Arc::clone(&self.subduction);
        let cancel = self.cancel.clone();
        let stopping = Arc::clone(&self.stopping);
        let failed = Arc::clone(&self.failed);
        let tasks = self.tasks.clone();
        self.tasks.spawn(async move {
            let _outcome = fut.await;
            if stopping.swap(true, Ordering::AcqRel) {
                return;
            }
            // Winning the swap owns teardown unconditionally; only the
            // ERROR is gated on whether anyone asked for it.
            if !cancel.is_cancelled() {
                tracing::error!("Subduction {what} exited outside an orderly stop; stopping node");
                failed.store(true, Ordering::Release);
            }
            subduction.request_stop();
            cancel.cancel();
            tasks.close();
        });
    }

    /// Spawn a task scoped to this node's lifetime.
    ///
    /// The task is tracked (so [`stop`](Self::stop) waits for it) and runs
    /// until it completes or the node stops, whichever comes first. Holding
    /// an `Arc<Subduction>` inside it is fine: the node guarantees the task
    /// is gone before `stop` returns, so the clone cannot outlive the owner.
    ///
    /// Do not `await` [`stop`](Self::stop) from inside a task spawned here —
    /// `stop` waits for this task, which is waiting for `stop`.
    pub fn spawn<F>(&self, fut: F) -> JoinHandle<Option<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let token = self.cancel.child_token();
        self.tasks
            .spawn(async move { token.run_until_cancelled(fut).await })
    }

    /// The node itself. Clone the `Arc` into scoped work; `Arc::downgrade`
    /// it for observers that must not keep the node alive.
    #[must_use]
    pub const fn subduction(
        &self,
    ) -> &Arc<Node<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>> {
        &self.subduction
    }

    /// The tracker every task of this node is registered with.
    ///
    /// Spawn onto it directly if you need a task that `stop` waits for but
    /// that should *not* be cancelled by the node's token.
    #[must_use]
    pub fn tracker(&self) -> TaskTracker {
        self.tasks.clone()
    }

    /// A child of the node's cancellation token, for tasks that want to
    /// `select!` on it themselves.
    #[must_use]
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.child_token()
    }

    /// Signal the node to stop without waiting.
    ///
    /// Order is load-bearing: the node's loops are told to exit *before* the
    /// token is cancelled, so the manager exits via its channel-close path
    /// and aborts its connection readers rather than being dropped around
    /// them. Safe from `Drop` and other non-async contexts. Idempotent.
    pub fn request_stop(&self) {
        // Mark the stop as orderly before waking anything, so the supervisors
        // stay quiet.
        self.stopping.store(true, Ordering::Release);
        self.subduction.request_stop();
        self.cancel.cancel();
        self.tasks.close();
    }

    /// Stop the node and wait until everything it owns has exited.
    ///
    /// [`request_stop`](Self::request_stop), then wait for the loops, then
    /// wait for every tracked task. On return: no loop, connection reader,
    /// dispatch task, or [`spawn`](Self::spawn)ed task is running, and every
    /// write the node made has been committed. Whatever `Arc<Subduction>`
    /// clones remain are yours; drop them to release storage.
    ///
    /// Idempotent. Do not call from a task spawned by this node.
    pub async fn stop(&self) {
        self.request_stop();
        self.subduction.stopped().await;
        self.tasks.wait().await;
    }

    /// Whether the node's loops have exited. Tasks from
    /// [`spawn`](Self::spawn) may still be winding down; use
    /// [`stop`](Self::stop) for full quiescence.
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.subduction.is_stopped()
    }

    /// Whether a loop exited *before* anyone asked it to.
    ///
    /// When this is set the node has already been stopped by its supervisor.
    /// A process-level supervisor (systemd, a Kubernetes liveness probe)
    /// typically wants to exit nonzero in this case so it gets restarted.
    #[must_use]
    pub fn exited_unexpectedly(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }
}

impl<Store, Conn, Hdl, Auth, Sign, Timer, Metric, const SHARDS: usize> Deref
    for TokioSubduction<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Store: Storage<Sendable> + Send + Sync + 'static,
    Conn: Connection<Sendable, Hdl::Message> + PartialEq + Clone + Send + Sync + 'static,
    Hdl: Handler<Sendable, Conn> + Send + Sync + 'static,
    Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    Sign: Signer<Sendable> + Send + Sync + 'static,
    Timer: Timeout<Sendable> + Clone + Send + Sync + 'static,
    Metric: DepthMetric + Send + Sync + 'static,
    Sendable: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
{
    type Target = Node<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>;

    fn deref(&self) -> &Self::Target {
        &self.subduction
    }
}

/// Backstop: an owner that goes away without calling [`stop`] still brings
/// the node down. This is synchronous, so it can only *request* the stop;
/// the loops exit and release their `Arc<Subduction>` shortly after. If you
/// need to know when that has happened — e.g. before reopening a file-locked
/// storage backend — call `stop().await` instead of relying on drop.
///
/// [`stop`]: TokioSubduction::stop
impl<Store, Conn, Hdl, Auth, Sign, Timer, Metric, const SHARDS: usize> Drop
    for TokioSubduction<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Store: Storage<Sendable> + Send + Sync + 'static,
    Conn: Connection<Sendable, Hdl::Message> + PartialEq + Clone + Send + Sync + 'static,
    Hdl: Handler<Sendable, Conn> + Send + Sync + 'static,
    Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    Sign: Signer<Sendable> + Send + Sync + 'static,
    Timer: Timeout<Sendable> + Clone + Send + Sync + 'static,
    Metric: DepthMetric + Send + Sync + 'static,
    Sendable: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
{
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl<Store, Conn, Hdl, Auth, Sign, Timer, Metric, const SHARDS: usize> fmt::Debug
    for TokioSubduction<Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Store: Storage<Sendable> + Send + Sync + 'static,
    Conn: Connection<Sendable, Hdl::Message> + PartialEq + Clone + Send + Sync + 'static,
    Hdl: Handler<Sendable, Conn> + Send + Sync + 'static,
    Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
    Sign: Signer<Sendable> + Send + Sync + 'static,
    Timer: Timeout<Sendable> + Clone + Send + Sync + 'static,
    Metric: DepthMetric + Send + Sync + 'static,
    Sendable: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokioSubduction")
            .field("is_stopped", &self.is_stopped())
            .field("is_cancelled", &self.cancel.is_cancelled())
            .field("exited_unexpectedly", &self.exited_unexpectedly())
            .field("tracked_tasks", &self.tasks.len())
            .finish_non_exhaustive()
    }
}
