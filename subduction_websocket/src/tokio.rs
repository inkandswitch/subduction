//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

use core::time::Duration;

use futures::{future::BoxFuture, FutureExt};
use futures_kind::Sendable;

use crate::timeout::{TimedOut, Timeout};

#[cfg(any(feature = "tokio_client", feature = "tokio_client_rustls"))]
pub mod client;

#[cfg(any(feature = "tokio_server", feature = "tokio_server_rustls"))]
pub mod server;

#[cfg(any(
    feature = "tokio_client",
    feature = "tokio_client_rustls",
    feature = "tokio_server",
    feature = "tokio_server_rustls"
))]
pub mod stream;

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
