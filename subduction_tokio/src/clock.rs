//! Tokio-backed clock.

use core::time::Duration;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use future_form::{future_form, FutureForm, Local, Sendable};
use subduction_protocol::{
    timestamp::{Now, Timestamp},
    wall_clock::TimestampSeconds,
};
use subduction_runtime::clock::Clock;

/// A [`Clock`] backed by [`std::time`] and tokio's timer.
///
/// Monotonic time counts from construction; wall time is the system
/// clock. Requires a tokio runtime with time enabled while sleeping.
#[derive(Debug, Clone, Copy)]
pub struct TokioClock {
    epoch: Instant,
}

impl TokioClock {
    /// A clock whose monotonic zero is now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }
}

#[future_form(Sendable, Local)]
impl<Async: FutureForm> Clock<Async> for TokioClock {
    fn now(&self) -> Now {
        let monotonic =
            Timestamp::from_millis(u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(0));
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Now {
            monotonic,
            wall: TimestampSeconds::new(wall.as_secs()),
        }
    }

    fn sleep(&self, duration: Duration) -> Async::Future<'_, ()> {
        Async::from_future(tokio::time::sleep(duration))
    }
}

impl Default for TokioClock {
    fn default() -> Self {
        Self::new()
    }
}
