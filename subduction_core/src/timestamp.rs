//! Timestamps

use core::time::Duration;

/// A timestamp represented as non-leap seconds since the Unix epoch.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, minicbor::Encode, minicbor::Decode,
)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cbor(transparent)]
pub struct TimestampSeconds(u64);

impl TimestampSeconds {
    /// Create a new timestamp from seconds since Unix epoch.
    #[must_use]
    pub const fn new(secs: u64) -> Self {
        Self(secs)
    }

    /// Get the current timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the system time is before the Unix epoch.
    #[cfg(feature = "std")]
    #[cfg_attr(docsrs, doc(cfg(feature = "std")))]
    #[allow(clippy::expect_used)]
    #[must_use]
    pub fn now() -> Self {
        let duration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch");
        Self(duration.as_secs())
    }

    /// Get the raw seconds value.
    #[must_use]
    pub const fn as_secs(&self) -> u64 {
        self.0
    }

    /// Compute the absolute difference between two timestamps.
    #[must_use]
    pub const fn abs_diff(&self, other: Self) -> Duration {
        Duration::from_secs(self.0.abs_diff(other.0))
    }

    /// Compute the signed difference (self - other) in seconds.
    #[allow(clippy::cast_possible_truncation, clippy::cast_lossless)]
    #[must_use]
    pub const fn signed_diff(&self, other: Self) -> i64 {
        (self.0 as i128 - other.0 as i128) as i64
    }

    /// Add a signed offset to this timestamp.
    #[allow(clippy::cast_sign_loss)]
    #[must_use]
    pub const fn add_signed(&self, offset_secs: i64) -> Self {
        if offset_secs >= 0 {
            Self(self.0.saturating_add(offset_secs as u64))
        } else {
            Self(self.0.saturating_sub(offset_secs.unsigned_abs()))
        }
    }
}
