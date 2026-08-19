//! Test [`Clock`] with manually-controlled time.

use alloc::sync::Arc;
use core::sync::atomic::{AtomicU64, Ordering};

use subduction_core::timestamp::TimestampSeconds;

use super::Clock;

/// A test [`Clock`] with manually-controlled time.
///
/// # Example
///
/// ```
/// use subduction_core::timestamp::TimestampSeconds;
/// use subduction_ephemeral::clock::Clock;
/// use subduction_ephemeral::clock::fake::FakeClock;
///
/// let clock = FakeClock::new(TimestampSeconds::new(1_000));
/// assert_eq!(clock.now().as_secs(), 1_000);
///
/// clock.advance_secs(500);
/// assert_eq!(clock.now().as_secs(), 1_500);
/// ```
#[derive(Debug, Clone)]
pub struct FakeClock {
    secs: Arc<AtomicU64>,
}

impl FakeClock {
    /// Create a new [`FakeClock`] at the given time.
    #[must_use]
    pub fn new(initial: TimestampSeconds) -> Self {
        Self {
            secs: Arc::new(AtomicU64::new(initial.as_secs())),
        }
    }

    /// Advance the clock by `delta` seconds.
    pub fn advance_secs(&self, delta: u64) {
        self.secs.fetch_add(delta, Ordering::Relaxed);
    }

    /// Set the clock to an exact value.
    pub fn set(&self, ts: TimestampSeconds) {
        self.secs.store(ts.as_secs(), Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn now(&self) -> TimestampSeconds {
        TimestampSeconds::new(self.secs.load(Ordering::Relaxed))
    }
}
