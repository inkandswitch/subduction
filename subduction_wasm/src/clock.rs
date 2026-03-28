//! JavaScript wall-clock for ephemeral nonce eviction.

use subduction_ephemeral::clock::Clock;

/// A [`Clock`] backed by JavaScript's `Date.now()`.
///
/// Returns UTC milliseconds since the Unix epoch, matching
/// the semantics of the [`Clock`] trait.
#[derive(Debug, Clone, Copy)]
pub struct JsClock;

impl Clock for JsClock {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn now_utc_ms(&self) -> u64 {
        // Date.now() returns f64 milliseconds since epoch.
        // Truncation: won't exceed u64 for ~584 million years.
        // Sign loss: Date.now() is always non-negative after 1970.
        js_sys::Date::now() as u64
    }
}
