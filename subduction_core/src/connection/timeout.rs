//! Generic timeout strategies for async operations.
//!
//! This trait allows connections and transports to apply timeouts
//! without depending on a specific async runtime.

use core::time::Duration;

use future_form::FutureForm;
use thiserror::Error;

/// A trait for time-limiting futures.
///
/// Implementations wrap a future with a deadline, returning [`TimedOut`]
/// if the inner future does not complete within the given duration.
///
/// # Example (conceptual)
///
/// ```text
/// let result = timeout.timeout(Duration::from_secs(5), some_future).await;
/// match result {
///     Ok(value) => { /* completed in time */ }
///     Err(TimedOut) => { /* deadline exceeded */ }
/// }
/// ```
pub trait Timeout<K: FutureForm + ?Sized>: Clone {
    /// Wrap a future with a timeout.
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: K::Future<'a, T>,
    ) -> K::Future<'a, Result<T, TimedOut>>;
}

/// An error indicating that an operation has timed out.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq, Hash)]
#[error("Operation timed out")]
pub struct TimedOut;
