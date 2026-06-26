//! Task spawning abstraction.
//!
//! [`Spawn`] keeps the core runtime-agnostic: callers plug in a spawner for
//! their platform (tokio, async-std, wasm-bindgen-futures, …).

use future_form::FutureForm;
use futures::stream::AbortHandle;

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
