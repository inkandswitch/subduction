//! Test [`Clock`] with manually-controlled time.

use alloc::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::Clock;

/// A test [`Clock`] with manually-controlled time.
///
/// # Example
///
/// ```
/// use subduction_ephemeral::clock::{Clock, FakeClock};
///
/// let clock = FakeClock::new(1_000);
/// assert_eq!(clock.now_utc_ms(), 1_000);
///
/// clock.advance_ms(500);
/// assert_eq!(clock.now_utc_ms(), 1_500);
/// ```
#[derive(Debug, Clone)]
pub struct FakeClock {
    ms: Arc<AtomicU64>,
}

impl FakeClock {
    /// Create a new [`FakeClock`] at the given UTC milliseconds.
    #[must_use]
    pub fn new(initial_ms: u64) -> Self {
        Self {
            ms: Arc::new(AtomicU64::new(initial_ms)),
        }
    }

    /// Advance the clock by `delta` milliseconds.
    pub fn advance_ms(&self, delta: u64) {
        self.ms.fetch_add(delta, Ordering::Relaxed);
    }

    /// Set the clock to an exact UTC millisecond value.
    pub fn set_ms(&self, ms: u64) {
        self.ms.store(ms, Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn now_utc_ms(&self) -> u64 {
        self.ms.load(Ordering::Relaxed)
    }
}
