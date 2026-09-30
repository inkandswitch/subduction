//! Synchronous teardown for the benchmarks' multi-thread Tokio runtimes.

use std::future::Future;

use tokio::runtime::Handle;

/// Drain asynchronous cleanup both outside the runtime (normal Criterion
/// teardown) and inside it (including when setup panics). `block_in_place`
/// permits re-entering the runtime and lets other tasks keep making progress.
/// Outside a runtime it simply runs the closure normally.
///
/// This requires a multi-thread runtime, as constructed by the benchmarks.
pub(super) fn drain(rt: &Handle, cleanup: impl Future<Output = ()>) {
    tokio::task::block_in_place(|| rt.block_on(cleanup));
}
