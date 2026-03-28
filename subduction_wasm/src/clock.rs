//! JavaScript wall-clock for ephemeral nonce eviction.

use subduction_core::timestamp::TimestampSeconds;
use subduction_ephemeral::clock::Clock;

/// A [`Clock`] backed by JavaScript's `Date.now()`.
///
/// Returns the current UTC wall-clock time, matching
/// the semantics of the [`Clock`] trait.
#[derive(Debug, Clone, Copy)]
pub struct JsClock;

impl Clock for JsClock {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn now(&self) -> TimestampSeconds {
        // Date.now() returns f64 milliseconds since epoch.
        // Divide by 1000 for seconds. Truncation safe for ~584M years.
        let secs = (js_sys::Date::now() / 1000.0) as u64;
        TimestampSeconds::new(secs)
    }
}
