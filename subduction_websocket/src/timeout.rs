//! Generic timeout strategies.

use core::time::Duration;

use sedimentree_core::future::FutureKind;

#[cfg(feature = "futures-timer")]
use sedimentree_core::future::{Local, Sendable};
use thiserror::Error;

#[cfg(feature = "futures-timer")]
use futures::{
    future::{select, BoxFuture, Either, LocalBoxFuture},
    FutureExt,
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
