//! Pluggable sleep strategy for connection keepalive.
//!
//! The [`Sleeper`] trait decouples keepalive timing from a specific async
//! runtime or timer crate. The [`websocket`](crate::websocket) module
//! takes a `Sleeper` as a parameter so the standalone crate compiles
//! without pulling in any timer dependency by default; runtimes that
//! need keepalive bring their own implementation (or activate the
//! `futures-timer` feature for the canned [`FuturesTimerSleeper`]).
//!
//! The trait is intentionally minimal — a single `sleep(dur)` method
//! returning a `'static` `Send` future. It does _not_ try to subsume
//! the full [`Timeout`](subduction_core::timeout::Timeout) trait; the
//! two answer different questions (`how long until elapsed?` vs
//! `complete within deadline?`).

use core::time::Duration;

use futures::future::BoxFuture;

#[cfg(feature = "futures-timer")]
use alloc::boxed::Box;

/// Strategy for "sleep for `dur`".
///
/// Implementations are typically zero-sized newtypes around a specific
/// runtime's sleep primitive (`tokio::time::sleep`, `futures_timer::Delay`,
/// `gloo_timers::Sleep`, etc.).
///
/// The returned future must be `'static + Send` so the keepalive task
/// can be safely spawned onto a multi-threaded executor.
pub trait Sleeper: Clone + Send + Sync + 'static {
    /// Return a future that resolves after `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
}

/// [`Sleeper`] backed by [`futures_timer::Delay`].
///
/// Available when the `futures-timer` feature is enabled. Works on any
/// async runtime that polls futures (tokio, async-std, smol, etc.) but
/// _not_ in `no_std` environments.
#[cfg(feature = "futures-timer")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FuturesTimerSleeper;

#[cfg(feature = "futures-timer")]
impl Sleeper for FuturesTimerSleeper {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(futures_timer::Delay::new(dur))
    }
}
