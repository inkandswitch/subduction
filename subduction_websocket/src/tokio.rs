//! Tokio implementations for [`WebSocket`][crate::websocket::WebSocket]s.

use core::time::Duration;

use futures::{
    future::BoxFuture,
    stream::{AbortHandle, Abortable},
    FutureExt,
};
use futures_kind::Sendable;
use subduction_core::connection::manager::Spawner;

use crate::timeout::{TimedOut, Timeout};

#[cfg(feature = "tokio_client_any")]
pub mod client;

#[cfg(feature = "tokio_server_any")]
pub mod server;

#[cfg(feature = "tokio_server_any")]
pub mod unified;

/// A spawner that uses tokio to spawn tasks.
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSpawner;

impl Spawner<Sendable> for TokioSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
        let (handle, reg) = AbortHandle::new_pair();
        tokio::spawn(async move {
            let _ = Abortable::new(fut, reg).await;
        });
        handle
    }
}

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
