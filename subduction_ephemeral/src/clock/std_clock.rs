//! [`Clock`] implementation backed by [`std::time::SystemTime`].

use subduction_core::timestamp::TimestampSeconds;

use super::Clock;

/// A [`Clock`] backed by [`std::time::SystemTime`].
///
/// Returns the current UTC wall-clock time via
/// `SystemTime::now().duration_since(UNIX_EPOCH)`.
#[derive(Debug, Clone, Copy)]
pub struct StdClock;

impl Clock for StdClock {
    #[allow(clippy::expect_used)]
    fn now(&self) -> TimestampSeconds {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_secs();
        TimestampSeconds::new(secs)
    }
}
