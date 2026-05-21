//! Pluggable sleep strategy for [`websocket::keepalive_loop`](crate::websocket).
//!
//! Decoupled from a specific timer crate so the standalone build of
//! this crate doesn't pull one in. Parameterized by [`FutureForm`] so
//! `Send`-required (`Sendable`) and single-threaded (`Local`) runtimes
//! can each plug in their own sleep primitive.

use core::time::Duration;

use future_form::{FutureForm, Sendable};

#[cfg(any(feature = "futures-timer", feature = "tokio_base"))]
use alloc::boxed::Box;

/// Strategy for "sleep for `dur`", parameterized over [`FutureForm`].
///
/// `Sleeper<Sendable>` produces `Send` futures suitable for `tokio::spawn`;
/// `Sleeper<Local>` produces single-threaded futures for runtimes like
/// `wasm-bindgen-futures`.
pub trait Sleeper<K: FutureForm + ?Sized>: Clone + 'static {
    /// Return a future that resolves after `dur` has elapsed.
    fn sleep(&self, dur: Duration) -> K::Future<'static, ()>;
}

/// [`Sleeper`] backed by `tokio::time::sleep`. Integrates with
/// `tokio::time::pause()` for mock-time testing.
#[cfg(feature = "tokio_base")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct TokioSleeper;

#[cfg(feature = "tokio_base")]
impl Sleeper<Sendable> for TokioSleeper {
    fn sleep(&self, dur: Duration) -> <Sendable as FutureForm>::Future<'static, ()> {
        Box::pin(tokio::time::sleep(dur))
    }
}

/// [`Sleeper`] backed by [`futures_timer::Delay`]. Works on any async
/// runtime that polls futures; requires `std`.
#[cfg(feature = "futures-timer")]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FuturesTimerSleeper;

#[cfg(feature = "futures-timer")]
impl Sleeper<Sendable> for FuturesTimerSleeper {
    fn sleep(&self, dur: Duration) -> <Sendable as FutureForm>::Future<'static, ()> {
        Box::pin(futures_timer::Delay::new(dur))
    }
}
