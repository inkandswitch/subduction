//! Pluggable sleep strategy for [`websocket::keepalive_loop`](crate::websocket).
//!
//! Decoupled from a specific timer crate so the standalone build of
//! this crate doesn't pull one in. The `futures-timer` feature
//! enables the canned [`FuturesTimerSleeper`].

use core::time::Duration;

use futures::future::BoxFuture;

#[cfg(feature = "futures-timer")]
use alloc::boxed::Box;

/// Strategy for "sleep for `dur`".
pub trait Sleeper: Clone + Send + Sync + 'static {
    /// Return a future that resolves after `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
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
