//! Time capability: the driver's only clock source.
//!
//! The node never tells the time — the driver samples [`Clock::now`] before
//! every `handle` call and arms one timer from `poll_timeout` via
//! [`Clock::sleep`]. Platform crates supply real implementations (e.g.
//! `subduction_tokio`'s `TokioClock`, browser `setTimeout`); tests supply
//! virtual ones.

use core::time::Duration;

use future_form::FutureForm;
use subduction_protocol::timestamp::Now;

/// A monotonic + wall clock with an async sleep.
pub trait Clock<Async: FutureForm> {
    /// The current instant (monotonic milliseconds + wall seconds).
    fn now(&self) -> Now;

    /// Resolve after `duration` has elapsed.
    ///
    /// The driver races this against its event channel; a sleep that
    /// resolves late only delays timer-driven work, never corrupts it
    /// (deadlines are re-checked against [`now`](Self::now) on wake).
    fn sleep(&self, duration: Duration) -> Async::Future<'_, ()>;
}
