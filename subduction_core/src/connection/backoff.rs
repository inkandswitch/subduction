//! Exponential backoff with jitter for reconnection attempts.
//!
//! This module provides a [`Backoff`] type that calculates delays for retry
//! attempts using exponential backoff with optional jitter to prevent
//! thundering herd problems.
//!
//! # Example
//!
//! ```
//! use core::time::Duration;
//! use subduction_core::connection::backoff::Backoff;
//!
//! let mut backoff = Backoff::default();
//!
//! // First delay: ~100ms (base)
//! let delay1 = backoff.next_delay();
//! assert!(delay1 >= Duration::from_millis(90));
//! assert!(delay1 <= Duration::from_millis(110));
//!
//! // Second delay: ~200ms (base * factor)
//! let delay2 = backoff.next_delay();
//! assert!(delay2 >= Duration::from_millis(180));
//!
//! // Reset after successful operation
//! backoff.reset();
//! let delay_after_reset = backoff.next_delay();
//! assert!(delay_after_reset <= Duration::from_millis(110));
//! ```

use core::time::Duration;

/// Integer power for f64 (`no_std` compatible).
///
/// Computes `base^exp` for non-negative integer exponents.
fn pow_f64(base: f64, exp: usize) -> f64 {
    let mut result = 1.0;
    for _ in 0..exp {
        result *= base;
    }
    result
}

/// Exponential backoff with jitter.
///
/// Calculates retry delays that grow exponentially up to a maximum,
/// with optional jitter to prevent synchronized retry storms.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    factor: f64,
    jitter: f64,
    attempt: usize,
}

impl Backoff {
    /// Create a new backoff with custom parameters.
    ///
    /// # Arguments
    ///
    /// * `base` - Initial delay duration
    /// * `max` - Maximum delay duration (caps exponential growth)
    /// * `factor` - Multiplier for each attempt (typically 2.0)
    /// * `jitter` - Randomization factor (0.0-1.0), applied as ±jitter
    #[must_use]
    pub const fn new(base: Duration, max: Duration, factor: f64, jitter: f64) -> Self {
        Self {
            base,
            max,
            factor,
            jitter,
            attempt: 0,
        }
    }

    /// Get the next delay and increment the attempt counter.
    ///
    /// The delay is calculated as: `min(base * factor^attempt, max) * jitter_factor`
    ///
    /// Where `jitter_factor` is in the range `[1 - jitter, 1 + jitter]`.
    #[must_use]
    pub fn next_delay(&mut self) -> Duration {
        let multiplier = pow_f64(self.factor, self.attempt);
        let delay = self.base.mul_f64(multiplier);
        let delay = delay.min(self.max);
        self.attempt += 1;

        // Apply jitter: delay * (1 - jitter + pseudo_random * 2 * jitter)
        let jitter_factor = 1.0 - self.jitter + (self.pseudo_random() * 2.0 * self.jitter);
        delay.mul_f64(jitter_factor)
    }

    /// Reset the attempt counter to zero.
    ///
    /// Call this after a connection has been healthy for a period of time.
    pub const fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Get the current attempt number.
    #[must_use]
    pub const fn attempt(&self) -> usize {
        self.attempt
    }

    /// Simple deterministic pseudo-random based on attempt number.
    ///
    /// This provides some variation without requiring a random number generator,
    /// making the backoff usable in `no_std` environments. The distribution
    /// isn't uniform but is sufficient for jitter purposes.
    #[allow(clippy::cast_precision_loss)] // Acceptable for jitter calculation
    fn pseudo_random(&self) -> f64 {
        // Linear congruential generator step using POSIX-style LCG constants
        let hash = self.attempt.wrapping_mul(1_103_515_245).wrapping_add(12345);
        (hash % 100) as f64 / 100.0
    }
}

impl Default for Backoff {
    /// Default backoff configuration:
    /// - Base: 100ms
    /// - Max: 30 seconds
    /// - Factor: 2.0 (doubles each attempt)
    /// - Jitter: 0.1 (±10%)
    fn default() -> Self {
        Self::new(
            Duration::from_millis(100),
            Duration::from_secs(30),
            2.0,
            0.1,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delays_increase_exponentially() {
        let mut backoff = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(60),
            2.0,
            0.0, // No jitter for predictable testing
        );

        let d1 = backoff.next_delay();
        let d2 = backoff.next_delay();
        let d3 = backoff.next_delay();
        let d4 = backoff.next_delay();

        // Without jitter: 100ms, 200ms, 400ms, 800ms
        assert_eq!(d1, Duration::from_millis(100));
        assert_eq!(d2, Duration::from_millis(200));
        assert_eq!(d3, Duration::from_millis(400));
        assert_eq!(d4, Duration::from_millis(800));
    }

    #[test]
    fn respects_max_delay() {
        let mut backoff = Backoff::new(
            Duration::from_millis(100),
            Duration::from_millis(500),
            2.0,
            0.0,
        );

        // 100, 200, 400, 500 (capped), 500 (capped)
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        let d4 = backoff.next_delay();
        let d5 = backoff.next_delay();

        assert_eq!(d4, Duration::from_millis(500));
        assert_eq!(d5, Duration::from_millis(500));
    }

    #[test]
    fn reset_clears_attempt_counter() {
        let mut backoff = Backoff::new(
            Duration::from_millis(100),
            Duration::from_secs(30),
            2.0,
            0.0,
        );

        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        let _ = backoff.next_delay();
        assert_eq!(backoff.attempt(), 3);

        backoff.reset();
        assert_eq!(backoff.attempt(), 0);

        let d = backoff.next_delay();
        assert_eq!(d, Duration::from_millis(100));
    }

    #[test]
    fn jitter_varies_delay() {
        let mut backoff = Backoff::new(
            Duration::from_millis(1000),
            Duration::from_secs(30),
            2.0,
            0.2, // ±20% jitter
        );

        let d1 = backoff.next_delay();

        // With ±20% jitter on 1000ms base, delay should be in [800, 1200]
        assert!(d1 >= Duration::from_millis(800));
        assert!(d1 <= Duration::from_millis(1200));
    }

    #[cfg(all(feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        #[test]
        fn delays_never_exceed_max() {
            bolero::check!()
                .with_arbitrary::<(u16, u16, u8)>()
                .for_each(|(base_ms, max_ms, attempts)| {
                    // Ensure base and max are positive (at least 1ms)
                    let base_ms = u64::from((*base_ms).max(1));
                    let max_ms = u64::from((*max_ms).max(1)).max(base_ms);
                    // Limit attempts to avoid exponential overflow
                    let attempts = (*attempts).min(30);

                    let mut backoff = Backoff::new(
                        Duration::from_millis(base_ms),
                        Duration::from_millis(max_ms),
                        2.0,
                        0.0, // No jitter for precise bounds
                    );

                    for _ in 0..attempts {
                        let delay = backoff.next_delay();
                        assert!(
                            delay <= Duration::from_millis(max_ms),
                            "delay {delay:?} exceeded max {max_ms}ms"
                        );
                    }
                });
        }

        #[test]
        fn delays_monotonically_increase_without_jitter() {
            bolero::check!()
                .with_arbitrary::<(u16, u8)>()
                .for_each(|(base_ms, attempts)| {
                    let base_ms = (*base_ms).max(1);
                    let attempts = (*attempts).min(20); // Limit to avoid overflow

                    let mut backoff = Backoff::new(
                        Duration::from_millis(u64::from(base_ms)),
                        Duration::from_secs(3600), // Large max
                        2.0,
                        0.0, // No jitter
                    );

                    let mut prev = Duration::ZERO;
                    for _ in 0..attempts {
                        let delay = backoff.next_delay();
                        assert!(
                            delay >= prev,
                            "delay {delay:?} decreased from previous {prev:?}"
                        );
                        prev = delay;
                    }
                });
        }
    }
}
