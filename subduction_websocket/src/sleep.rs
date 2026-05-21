//! Pluggable sleep strategy for [`websocket::keepalive_loop`](crate::websocket).
//!
//! Decoupled from a specific timer crate so the standalone build of
//! this crate doesn't pull one in. Two canned implementations:
//! [`TokioSleeper`] (used by the `tokio` module) and
//! [`FuturesTimerSleeper`] (runtime-agnostic, behind the `futures-timer`
//! feature).

use core::time::Duration;

use futures::future::BoxFuture;

#[cfg(any(feature = "futures-timer", feature = "tokio_base"))]
use alloc::boxed::Box;

/// Strategy for "sleep for `dur`".
pub trait Sleeper: Clone + Send + Sync + 'static {
    /// Return a future that resolves after `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
}

/// [`Sleeper`] backed by `tokio::time::sleep`. Integrates with
/// `tokio::time::pause()` for mock-time testing.
#[cfg(feature = "tokio_base")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TokioSleeper;

#[cfg(feature = "tokio_base")]
impl Sleeper for TokioSleeper {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(dur))
    }
}

/// [`Sleeper`] backed by [`futures_timer::Delay`]. Works on any async
/// runtime that polls futures; requires `std`.
#[cfg(feature = "futures-timer")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FuturesTimerSleeper;

#[cfg(feature = "futures-timer")]
impl Sleeper for FuturesTimerSleeper {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(futures_timer::Delay::new(dur))
    }
}
