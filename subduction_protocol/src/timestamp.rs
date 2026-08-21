//! Opaque, driver-supplied time.
//!
//! The machines never tell the time. Every `handle` call takes a [`Now`]
//! supplied by the driver. The only requirements are:
//!
//! - **Monotonic**: [`Now::monotonic`] never decreases across calls.
//! - **Consistent**: one clock per node instance.

use core::time::Duration;

use crate::wall_clock::TimestampSeconds;

/// The driver's view of "now", supplied with every `handle` call.
///
/// Two clocks because they answer different questions: `monotonic` orders
/// deadlines and never goes backwards; `wall` is Unix time that crosses the
/// wire in handshake freshness checks and may be corrected/skewed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub struct Now {
    /// Monotonic driver time (deadlines).
    pub monotonic: Timestamp,

    /// Wall-clock Unix seconds (handshake freshness, nonce buckets).
    pub wall: TimestampSeconds,
}

/// A monotonic timestamp in milliseconds since an arbitrary epoch.
///
/// Supplied by the driver on every event; the machine only ever compares
/// timestamps and adds [`Duration`]s to compute deadlines.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Timestamp(u64);

impl Timestamp {
    /// The zero timestamp (the epoch itself).
    pub const EPOCH: Self = Self(0);

    /// Create a timestamp from raw milliseconds since the driver's epoch.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Raw milliseconds since the driver's epoch.
    #[must_use]
    pub const fn as_millis(&self) -> u64 {
        self.0
    }

    /// The deadline `duration` after this instant, saturating at the far
    /// future.
    #[must_use]
    pub const fn saturating_add(&self, duration: Duration) -> Self {
        // Duration::as_millis is u128; anything beyond u64::MAX ms
        // (~584 million years) saturates.
        let millis = duration.as_millis();
        if millis > u64::MAX as u128 {
            Self(u64::MAX)
        } else {
            // Guarded by the branch above (`try_from` is not yet const).
            #[allow(clippy::cast_possible_truncation)]
            Self(self.0.saturating_add(millis as u64))
        }
    }

    /// Time elapsed from `earlier` to `self`, or [`Duration::ZERO`] if
    /// `earlier` is in the future (monotonicity violations are clamped,
    /// never negative).
    #[must_use]
    pub const fn saturating_since(&self, earlier: Self) -> Duration {
        Duration::from_millis(self.0.saturating_sub(earlier.0))
    }

    /// Whether a deadline has passed (deadlines are due at exactly `now`).
    #[must_use]
    pub const fn is_due(&self, now: Self) -> bool {
        self.0 <= now.0
    }
}

impl core::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "t+{}ms", self.0)
    }
}

#[cfg(all(test, feature = "std", feature = "bolero"))]
mod proptests {
    use super::*;

    #[test]
    fn prop_saturating_add_is_monotonic() {
        bolero::check!()
            .with_type::<(Timestamp, u64)>()
            .for_each(|(t, millis)| {
                let later = t.saturating_add(Duration::from_millis(*millis));
                assert!(*t <= later);
            });
    }

    #[test]
    fn prop_saturating_since_roundtrips_when_no_overflow() {
        bolero::check!()
            .with_type::<(Timestamp, u32)>()
            .for_each(|(t, millis)| {
                let d = Duration::from_millis(u64::from(*millis));
                let later = t.saturating_add(d);
                if later.as_millis() < u64::MAX {
                    assert_eq!(later.saturating_since(*t), d);
                }
            });
    }

    #[test]
    fn prop_deadline_due_iff_not_after_now() {
        bolero::check!()
            .with_type::<(Timestamp, Timestamp)>()
            .for_each(|(deadline, now)| {
                assert_eq!(deadline.is_due(*now), deadline <= now);
            });
    }
}
