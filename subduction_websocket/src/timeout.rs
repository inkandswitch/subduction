//! Generic timeout strategies.

use core::time::Duration;

use futures_kind::FutureKind;

#[cfg(feature = "futures-timer")]
use futures_kind::{Local, Sendable};
use thiserror::Error;

#[cfg(feature = "futures-timer")]
use futures::{
    FutureExt,
    future::{BoxFuture, Either, LocalBoxFuture, select},
};

#[cfg(feature = "futures-timer")]
use futures_timer::Delay;

/// A trait for time-limiting futures.
pub trait Timeout<K: FutureKind + ?Sized> {
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

#[cfg(feature = "futures-timer")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// A timeout strategy using the [`futures-timer`] crate.
pub struct FuturesTimerTimeout;

#[cfg(feature = "futures-timer")]
impl Timeout<Local> for FuturesTimerTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: LocalBoxFuture<'a, T>,
    ) -> LocalBoxFuture<'a, Result<T, TimedOut>> {
        async move {
            match select(fut, Delay::new(dur)).await {
                Either::Left((val, _delay)) => Ok(val),
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed_local()
    }
}

#[cfg(feature = "futures-timer")]
impl Timeout<Sendable> for FuturesTimerTimeout {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, TimedOut>> {
        async move {
            match select(fut, Delay::new(dur)).await {
                Either::Left((val, _delay)) => Ok(val),
                Either::Right(_) => Err(TimedOut),
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod timed_out_error {
        use super::*;

        #[test]
        fn test_display() {
            let err = TimedOut;
            assert_eq!(format!("{err}"), "Operation timed out");
        }

        #[test]
        fn test_is_clone() {
            let err1 = TimedOut;
            let err2 = err1;
            assert_eq!(format!("{err2}"), "Operation timed out");
        }

        #[test]
        fn test_is_copy() {
            let err1 = TimedOut;
            let err2 = err1;
            // Both should still be usable
            assert_eq!(format!("{err1}"), "Operation timed out");
            assert_eq!(format!("{err2}"), "Operation timed out");
        }

        #[test]
        fn test_equality() {
            let err1 = TimedOut;
            let err2 = TimedOut;
            assert_eq!(err1, err2);
        }

        #[test]
        fn test_equality_reflexive() {
            let err = TimedOut;
            assert_eq!(err, err);
        }

        #[test]
        fn test_debug_output() {
            let err = TimedOut;
            let debug = format!("{err:?}");
            assert!(debug.contains("TimedOut"));
        }

        #[test]
        fn test_hash() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let err1 = TimedOut;
            let err2 = TimedOut;

            let mut hasher1 = DefaultHasher::new();
            let mut hasher2 = DefaultHasher::new();

            err1.hash(&mut hasher1);
            err2.hash(&mut hasher2);

            assert_eq!(hasher1.finish(), hasher2.finish());
        }
    }

    #[cfg(feature = "futures-timer")]
    mod futures_timer_timeout {
        use super::*;

        #[test]
        fn test_is_clone() {
            let timeout1 = FuturesTimerTimeout;
            let timeout2 = timeout1;
            // Both should be usable
            let _ = timeout1;
            let _ = timeout2;
        }

        #[test]
        fn test_is_copy() {
            let timeout1 = FuturesTimerTimeout;
            let timeout2 = timeout1;
            // Both should still be usable
            let _ = timeout1;
            let _ = timeout2;
        }

        #[test]
        fn test_equality() {
            let timeout1 = FuturesTimerTimeout;
            let timeout2 = FuturesTimerTimeout;
            assert_eq!(timeout1, timeout2);
        }

        #[test]
        fn test_debug_output() {
            let timeout = FuturesTimerTimeout;
            let debug = format!("{timeout:?}");
            assert!(debug.contains("FuturesTimerTimeout"));
        }
    }

    #[cfg(all(test, feature = "std"))]
    mod proptests {
        use super::*;

        #[test]
        fn prop_timed_out_equality_is_reflexive() {
            let err = TimedOut;
            assert_eq!(err, err);
        }

        #[test]
        fn prop_timed_out_equality_is_symmetric() {
            let err1 = TimedOut;
            let err2 = TimedOut;
            assert_eq!(err1, err2);
            assert_eq!(err2, err1);
        }

        #[test]
        fn prop_timed_out_hash_consistency() {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let err = TimedOut;

            let mut hasher1 = DefaultHasher::new();
            let mut hasher2 = DefaultHasher::new();

            err.hash(&mut hasher1);
            err.hash(&mut hasher2);

            assert_eq!(hasher1.finish(), hasher2.finish());
        }
    }
}
