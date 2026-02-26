//! Generic timeout strategies.
//!
//! The core [`Timeout`] trait and [`TimedOut`] error are re-exported from
//! `subduction_core`. This module provides concrete implementations.

use core::time::Duration;

pub use subduction_core::connection::timeout::{TimedOut, Timeout};

#[cfg(feature = "futures-timer")]
use future_form::{Local, Sendable};

#[cfg(feature = "futures-timer")]
use futures::{
    FutureExt,
    future::{BoxFuture, Either, LocalBoxFuture, select},
};

#[cfg(feature = "futures-timer")]
use futures_timer::Delay;

/// A timeout strategy using the [`futures-timer`] crate.
///
/// Works with both `Sendable` and `Local` future forms, making it
/// suitable for native and Wasm environments.
#[cfg(feature = "futures-timer")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
