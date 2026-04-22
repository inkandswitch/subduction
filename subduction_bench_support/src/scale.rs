//! Workload size tiers.
//!
//! Benchmarks are categorised into three tiers so the same bench source can serve as a fast
//! CI signal _and_ a realistic performance check, depending on which Cargo features are enabled.
//!
//! | Tier        | Cargo feature         | Intent                                                |
//! |-------------|-----------------------|-------------------------------------------------------|
//! | `Micro`     | (always enabled)      | Run on every PR; sub-second per bench                 |
//! | `Medium`    | `medium_benches`      | Run nightly; seconds-per-bench; finds obvious regressions |
//! | `Realistic` | `realistic_benches`   | Manual / weekly; minutes-per-bench; real workloads    |
//!
//! Each tier exposes a set of canonical sizes. Benches pick the relevant dimension(s):
//!
//! ```ignore
//! use subduction_benches::scale::Scale;
//!
//! for scale in Scale::enabled_tiers() {
//!     let commits = scale.commit_count();
//!     let blob    = scale.blob_size_bytes();
//!     // ...
//! }
//! ```
//!
//! Using the `enabled_tiers()` helper means benches transparently include Medium/Realistic
//! iterations only when the corresponding feature is active.

/// Workload size tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scale {
    /// Fast micro-benchmark size — fits in CPU cache; milliseconds per iteration.
    Micro,

    /// Medium workload — realistic dev-machine size; hundreds of milliseconds.
    Medium,

    /// Realistic user-scale workload; seconds to minutes per iteration.
    Realistic,
}

impl Scale {
    /// All tiers enabled by the currently-compiled feature set.
    ///
    /// - `Micro` is always included.
    /// - `Medium` is included iff the `medium_benches` feature is on.
    /// - `Realistic` is included iff the `realistic_benches` feature is on
    ///   (`realistic_benches` implies `medium_benches`).
    #[must_use]
    pub const fn enabled_tiers() -> &'static [Self] {
        #[cfg(feature = "realistic_benches")]
        {
            &[Self::Micro, Self::Medium, Self::Realistic]
        }
        #[cfg(all(feature = "medium_benches", not(feature = "realistic_benches")))]
        {
            &[Self::Micro, Self::Medium]
        }
        #[cfg(not(feature = "medium_benches"))]
        {
            &[Self::Micro]
        }
    }

    /// Human-readable tag suitable for Criterion `BenchmarkId` parameters.
    #[must_use]
    pub const fn tag(self) -> &'static str {
        match self {
            Self::Micro => "micro",
            Self::Medium => "medium",
            Self::Realistic => "realistic",
        }
    }

    /// Canonical commit count for this tier.
    #[must_use]
    pub const fn commit_count(self) -> usize {
        match self {
            Self::Micro => 32,
            Self::Medium => 1_000,
            Self::Realistic => 100_000,
        }
    }

    /// Canonical fragment count for this tier.
    #[must_use]
    pub const fn fragment_count(self) -> usize {
        match self {
            Self::Micro => 4,
            Self::Medium => 64,
            Self::Realistic => 1_000,
        }
    }

    /// Canonical blob size (bytes) for this tier.
    #[must_use]
    pub const fn blob_size_bytes(self) -> usize {
        match self {
            Self::Micro => 64,
            Self::Medium => 16 * 1024,
            Self::Realistic => 1024 * 1024,
        }
    }

    /// Canonical peer count for fan-in / fan-out scenarios.
    #[must_use]
    pub const fn peer_count(self) -> usize {
        match self {
            Self::Micro => 2,
            Self::Medium => 16,
            Self::Realistic => 128,
        }
    }

    /// Canonical subscriber count for ephemeral broadcast benches.
    #[must_use]
    pub const fn subscriber_count(self) -> usize {
        match self {
            Self::Micro => 4,
            Self::Medium => 64,
            Self::Realistic => 1_024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn micro_is_always_enabled() {
        assert!(Scale::enabled_tiers().contains(&Scale::Micro));
    }

    #[test]
    fn tags_are_unique() {
        let tags: Vec<_> = [Scale::Micro, Scale::Medium, Scale::Realistic]
            .iter()
            .map(|s| s.tag())
            .collect();

        let mut sorted = tags.clone();
        sorted.sort_unstable();
        sorted.dedup();

        assert_eq!(tags.len(), sorted.len(), "tags must be unique");
    }

    #[test]
    fn size_monotonically_increases() {
        // Invariant: Realistic > Medium > Micro for every size dimension.
        let ordered = [Scale::Micro, Scale::Medium, Scale::Realistic];

        for window in ordered.windows(2) {
            if let [small, big] = window {
                assert!(big.commit_count() > small.commit_count());
                assert!(big.fragment_count() > small.fragment_count());
                assert!(big.blob_size_bytes() > small.blob_size_bytes());
                assert!(big.peer_count() > small.peer_count());
                assert!(big.subscriber_count() > small.subscriber_count());
            }
        }
    }
}
