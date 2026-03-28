//! Wall-clock time source for time-bucketed nonce eviction.

#[cfg(feature = "test_utils")]
pub mod fake;

#[cfg(feature = "std")]
pub mod std_clock;

/// Wall-clock time source for time-bucketed nonce eviction.
///
/// Implementations return UTC milliseconds since the Unix epoch.
/// UTC (not monotonic) is the correct choice because nonce buckets
/// must survive process restarts — a monotonic clock resets to zero
/// on restart, which would reset all bucket timestamps and could
/// allow replays of messages observed before the restart.
///
/// Small clock drift or NTP adjustments are harmless; the nonce
/// window is 30 seconds, so sub-second jitter has no practical effect.
pub trait Clock: Clone {
    /// Returns UTC milliseconds since the Unix epoch (1970-01-01T00:00:00Z).
    fn now_utc_ms(&self) -> u64;
}
