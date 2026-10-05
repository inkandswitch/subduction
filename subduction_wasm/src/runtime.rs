//! Owned local tasks, operations, and transport cleanup for a JS node.
//!
//! No lock or `RefCell` borrow is held across an await or call into JavaScript.

use crate::transport::{JsTransport, JsTransportError, longpoll::JsTimeout};
use alloc::{
    collections::BTreeMap,
    rc::{Rc, Weak},
    string::String,
    sync::Arc,
    vec::Vec,
};
use core::{
    cell::{Cell, RefCell},
    future::Future,
    time::Duration,
};
use future_form::Local;
use futures::{
    FutureExt,
    future::{AbortHandle, Abortable, LocalBoxFuture, Shared},
};
use subduction_core::{spawn::Spawn, timeout::Timeout, transport::Transport};
use wasm_bindgen::JsValue;

#[cfg(all(test, target_arch = "wasm32"))]
mod tests;

pub(crate) const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);
type Disconnect = Shared<LocalBoxFuture<'static, Result<(), JsTransportError>>>;

#[derive(Debug, Default)]
struct Tasks {
    next: u64,
    forced: bool,
    handles: BTreeMap<u64, AbortHandle>,
}

/// Node-scoped spawner. All clones share a registry of abortable local tasks.
/// The registry itself is thread-safe; spawned futures need not be `Send`.
#[derive(Debug, Clone)]
pub struct WasmSpawn {
    tasks: Arc<async_lock::Mutex<Tasks>>,
    changed: async_channel::Sender<()>,
    changes: async_channel::Receiver<()>,
}
impl Default for WasmSpawn {
    fn default() -> Self {
        let (sender, receiver) = async_channel::bounded(1);
        Self {
            tasks: Arc::default(),
            changed: sender,
            changes: receiver,
        }
    }
}
impl WasmSpawn {
    // Every critical section is synchronous and invokes no foreign code.
    #[allow(clippy::expect_used)]
    fn tasks(&self) -> async_lock::MutexGuard<'_, Tasks> {
        self.tasks
            .try_lock()
            .expect("task registry is never locked across a yield")
    }
    fn notify(&self) {
        let _ = self.changed.try_send(());
    }
    fn abort_all(&self) {
        let handles = {
            let mut tasks = self.tasks();
            tasks.forced = true;
            tasks.handles.values().cloned().collect::<Vec<_>>()
        };
        for handle in handles {
            handle.abort();
        }
    }
}
struct TaskGuard {
    spawner: WasmSpawn,
    id: u64,
}
impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.spawner.tasks().handles.remove(&self.id);
        self.spawner.notify();
    }
}
impl Spawn<Local> for WasmSpawn {
    fn spawn(&self, future: LocalBoxFuture<'static, ()>) -> AbortHandle {
        let (handle, registration) = AbortHandle::new_pair();
        let id = {
            let mut tasks = self.tasks();
            if tasks.forced {
                handle.abort();
                return handle;
            }
            let id = tasks.next;
            tasks.next += 1;
            tasks.handles.insert(id, handle.clone());
            id
        };
        let guard = TaskGuard {
            spawner: self.clone(),
            id,
        };
        wasm_bindgen_futures::spawn_local(async move {
            let _ = Abortable::new(future, registration).await;
            drop(guard);
        });
        handle
    }
}

#[derive(Debug)]
struct State {
    closing: Cell<bool>,
    next_id: Cell<u64>,
    spawner: WasmSpawn,
    operations: RefCell<BTreeMap<u64, AbortHandle>>,
    transports: RefCell<BTreeMap<u64, ManagedTransport>>,
    errors: RefCell<Vec<String>>,
    finished: async_channel::Sender<()>,
    completion: async_channel::Receiver<()>,
    result: RefCell<Option<Result<(), JsValue>>>,
}
#[derive(Debug, Clone)]
pub(crate) struct Runtime(Rc<State>);
impl Default for Runtime {
    fn default() -> Self {
        let (finished, completion) = async_channel::bounded(1);
        Self(Rc::new(State {
            closing: Cell::new(false),
            next_id: Cell::new(0),
            spawner: WasmSpawn::default(),
            operations: RefCell::default(),
            transports: RefCell::default(),
            errors: RefCell::default(),
            finished,
            completion,
            result: RefCell::default(),
        }))
    }
}
impl State {
    fn id(&self) -> u64 {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        id
    }
}
struct OperationGuard {
    state: Rc<State>,
    id: u64,
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.state.operations.borrow_mut().remove(&self.id);
        self.state.spawner.notify();
    }
}
impl Runtime {
    pub(crate) fn spawner(&self) -> WasmSpawn {
        self.0.spawner.clone()
    }
    pub(crate) fn ensure_open(&self) -> Result<(), JsValue> {
        if self.0.closing.get() {
            Err(closed_error())
        } else {
            Ok(())
        }
    }
    pub(crate) fn run<'a, T: 'a, E: Into<JsValue> + 'a>(
        &'a self,
        future: impl Future<Output = Result<T, E>> + 'a,
    ) -> LocalBoxFuture<'a, Result<T, JsValue>> {
        async move {
            self.ensure_open()?;
            let (handle, registration) = AbortHandle::new_pair();
            let id = self.0.id();
            self.0.operations.borrow_mut().insert(id, handle);
            let _guard = OperationGuard {
                state: self.0.clone(),
                id,
            };
            Abortable::new(future, registration)
                .await
                .map_err(|_| closed_error())?
                .map_err(Into::into)
        }
        .boxed_local()
    }
    pub(crate) fn run_value<'a, T: 'a>(
        &'a self,
        future: impl Future<Output = T> + 'a,
    ) -> LocalBoxFuture<'a, Result<T, JsValue>> {
        self.run(async { Ok::<_, JsValue>(future.await) })
    }
    /// Track a transport and arm cleanup for the attempt introducing it.
    /// An existing transport stays under its current owner's responsibility;
    /// dropping a duplicate attempt's lease must not disconnect it.
    pub(crate) fn own(&self, inner: JsTransport) -> TransportLease {
        if let Some(transport) = self
            .0
            .transports
            .borrow()
            .values()
            .find(|t| t.0.inner == inner)
            .cloned()
        {
            return TransportLease {
                transport,
                disconnect_on_drop: false,
            };
        }
        let id = self.0.id();
        let transport = ManagedTransport(Rc::new(TransportState {
            inner,
            id,
            owner: Rc::downgrade(&self.0),
            disconnect: RefCell::new(None),
        }));
        self.0.transports.borrow_mut().insert(id, transport.clone());
        TransportLease {
            transport,
            disconnect_on_drop: true,
        }
    }
    /// Starts shutdown synchronously; the supervisor outlives every waiter.
    pub(crate) fn close(
        &self,
        shutdown: impl FnOnce() + 'static,
        cleanup: LocalBoxFuture<'static, ()>,
        timeout: Duration,
    ) {
        if self.0.closing.replace(true) {
            return;
        }
        let operations: Vec<_> = self.0.operations.borrow().values().cloned().collect();
        for handle in operations {
            handle.abort();
        }
        shutdown();
        let transports: Vec<_> = self.0.transports.borrow().values().cloned().collect();
        for transport in transports {
            drop(transport.start_disconnect());
        }
        self.0.spawner.spawn(cleanup);
        let runtime = self.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let timed_out = JsTimeout
                .timeout(timeout, runtime.idle().boxed_local())
                .await
                .is_err();
            if timed_out {
                runtime.0.spawner.abort_all();
                // Abortable tasks drop their work on their next executor poll.
                runtime.idle().await;
                let transports = core::mem::take(&mut *runtime.0.transports.borrow_mut());
                for transport in transports.into_values() {
                    transport.0.disconnect.borrow_mut().take();
                }
            }
            // Seal even after graceful drain: late users of a retained spawner
            // must not resurrect work after completion.
            runtime.0.spawner.tasks().forced = true;
            let errors = runtime.0.errors.borrow().join("; ");
            let result = if timed_out || !errors.is_empty() {
                let message = if timed_out {
                    alloc::format!(
                        "close deadline expired; remaining Rust work cancelled. {errors}"
                    )
                } else {
                    errors
                };
                let error = js_sys::Error::new(&message);
                error.set_name("SubductionCloseError");
                Err(error.into())
            } else {
                Ok(())
            };
            *runtime.0.result.borrow_mut() = Some(result);
            runtime.0.finished.close();
        });
    }
    async fn idle(&self) {
        loop {
            if self.0.spawner.tasks().handles.is_empty() && self.0.operations.borrow().is_empty() {
                return;
            }
            let _ = self.0.spawner.changes.recv().await;
        }
    }
    pub(crate) async fn closed(&self) -> Result<(), JsValue> {
        let _ = self.0.completion.recv().await;
        self.0
            .result
            .borrow()
            .clone()
            .unwrap_or_else(|| Err(closed_error()))
    }
}
fn closed_error() -> JsValue {
    let error = js_sys::Error::new("Subduction is closing or closed");
    error.set_name("SubductionClosedError");
    error.into()
}

#[derive(Debug)]
struct TransportState {
    inner: JsTransport,
    id: u64,
    owner: Weak<State>,
    disconnect: RefCell<Option<Disconnect>>,
}
/// A node-owned transport with idempotent, tracked cleanup.
#[derive(Debug, Clone)]
pub(crate) struct ManagedTransport(Rc<TransportState>);
impl PartialEq for ManagedTransport {
    fn eq(&self, other: &Self) -> bool {
        self.0.inner == other.0.inner
    }
}
impl ManagedTransport {
    fn start_disconnect(&self) -> Disconnect {
        if let Some(future) = self.0.disconnect.borrow().as_ref() {
            return future.clone();
        }
        let inner = self.0.inner.clone();
        let owner = self.0.owner.clone();
        let id = self.0.id;
        let future = async move {
            let result = Transport::<Local>::disconnect(&inner).await;
            if let Some(owner) = owner.upgrade() {
                owner.transports.borrow_mut().remove(&id);
                if let Err(ref error) = result {
                    owner
                        .errors
                        .borrow_mut()
                        .push(alloc::format!("transport disconnect failed: {error}"));
                }
                owner.spawner.notify();
            }
            result
        }
        .boxed_local()
        .shared();
        *self.0.disconnect.borrow_mut() = Some(future.clone());
        if let Some(owner) = self.0.owner.upgrade() {
            let drive = future.clone();
            owner.spawner.spawn(
                async move {
                    drop(drive.await);
                }
                .boxed_local(),
            );
        }
        future
    }
}
impl Transport<Local> for ManagedTransport {
    type SendError = JsTransportError;
    type RecvError = JsTransportError;
    type DisconnectionError = JsTransportError;
    fn send_bytes(&self, bytes: &[u8]) -> LocalBoxFuture<'_, Result<(), Self::SendError>> {
        self.0.inner.send_bytes(bytes)
    }
    fn recv_bytes(&self) -> LocalBoxFuture<'_, Result<Vec<u8>, Self::RecvError>> {
        self.0.inner.recv_bytes()
    }
    fn on_unregistered(&self) {
        drop(self.start_disconnect());
    }
    fn disconnect(&self) -> LocalBoxFuture<'_, Result<(), Self::DisconnectionError>> {
        self.start_disconnect().boxed_local()
    }
}
/// One attempt's cleanup guard, held across authentication and registration.
///
/// The caller disarms it only after the whole attempt succeeds. In particular,
/// `link` keeps both guards armed until both sides register successfully.
pub(crate) struct TransportLease {
    transport: ManagedTransport,
    /// Whether dropping this guard must initiate disconnect. This is not the
    /// transport's registration state: a duplicate attempt starts disarmed.
    disconnect_on_drop: bool,
}
impl TransportLease {
    pub(crate) fn transport(&self) -> ManagedTransport {
        self.transport.clone()
    }
    /// Hand cleanup responsibility to the node after successful registration.
    pub(crate) fn disarm_cleanup(mut self) {
        self.disconnect_on_drop = false;
    }
}
impl Drop for TransportLease {
    fn drop(&mut self) {
        if self.disconnect_on_drop {
            drop(self.transport.start_disconnect());
        }
    }
}
