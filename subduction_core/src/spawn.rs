//! Task spawning abstraction.
//!
//! [`Spawn`] keeps the core runtime-agnostic: callers plug in a spawner for
//! their platform (tokio, async-std, wasm-bindgen-futures, …).

use future_form::FutureForm;
use futures::stream::AbortHandle;

/// Spawns background tasks on the caller's runtime.
pub trait Spawn<Async: FutureForm> {
    /// Spawn a future, driving it to completion. The returned [`AbortHandle`]
    /// aborts the task.
    fn spawn(&self, fut: Async::Future<'static, ()>) -> AbortHandle;
}
