//! Wall-clock time source for time-bucketed nonce eviction.

#[cfg(feature = "test_utils")]
pub mod fake;
#[cfg(feature = "std")]
pub mod std_clock;

use subduction_core::timestamp::TimestampSeconds;

/// Wall-clock time source for time-bucketed nonce eviction.
///
/// Implementations return the current UTC wall-clock time as
/// [`TimestampSeconds`]. UTC (not monotonic) is the correct choice
/// because nonce buckets must survive process restarts — a monotonic
/// clock resets to zero on restart, which would reset all bucket
/// timestamps and could allow replays of messages observed before
/// the restart.
///
/// Small clock drift or NTP adjustments are harmless; the nonce
/// window is 30 seconds, so sub-second jitter has no practical effect.
pub trait Clock: Clone {
    /// Returns the current UTC time as seconds since the Unix epoch.
    fn now(&self) -> TimestampSeconds;
}
