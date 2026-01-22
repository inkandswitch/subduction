//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

use core::time::Duration;

use futures::{future::BoxFuture, FutureExt};
use futures_kind::Sendable;

use crate::timeout::{TimedOut, Timeout};

#[cfg(feature = "tokio_client_any")]
pub mod client;

#[cfg(feature = "tokio_server_any")]
pub mod server;

#[cfg(feature = "tokio_server_any")]
pub mod unified;

/// Tokio-backed timeout wrapper.
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeoutTokio;

impl Timeout<Sendable> for TimeoutTokio {
    fn timeout<'a, T: 'a>(
        &'a self,
        dur: Duration,
        fut: BoxFuture<'a, T>,
    ) -> BoxFuture<'a, Result<T, TimedOut>> {
        async move {
            match tokio::time::timeout(dur, fut).await {
                Ok(v) => Ok(v),
                Err(_elapsed) => Err(TimedOut),
            }
        }
        .boxed()
    }
}
