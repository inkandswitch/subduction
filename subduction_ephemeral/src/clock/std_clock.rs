//! [`Clock`] implementation backed by [`std::time::SystemTime`].

use super::Clock;

/// A [`Clock`] backed by [`std::time::SystemTime`].
///
/// Returns UTC milliseconds since the Unix epoch via
/// `SystemTime::now().duration_since(UNIX_EPOCH)`.
#[derive(Debug, Clone, Copy)]
pub struct StdClock;

impl Clock for StdClock {
    #[allow(clippy::cast_possible_truncation, clippy::expect_used)]
    fn now_utc_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_millis() as u64
    }
}
