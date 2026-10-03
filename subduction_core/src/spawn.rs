//! Task spawning abstraction.
//!
//! [`Spawn`] keeps the core runtime-agnostic: callers plug in a spawner for
//! their platform (tokio, async-std, wasm-bindgen-futures, …).

use future_form::FutureForm;
use futures::stream::AbortHandle;

/// Let deadlines and shutdown be polled even with an immediately ready backend.
pub(crate) async fn yield_once() {
    let mut yielded = false;
    futures::future::poll_fn(move |cx| {
        if core::mem::replace(&mut yielded, true) {
            core::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            core::task::Poll::Pending
        }
    })
    .await;
}

/// Spawns background tasks on the caller's runtime.
pub trait Spawn<Async: FutureForm> {
    /// Spawn a future, driving it to completion. The returned [`AbortHandle`]
    /// aborts the task.
    fn spawn(&self, fut: Async::Future<'static, ()>) -> AbortHandle;
}
