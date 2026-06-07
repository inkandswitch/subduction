//! The future returned for the Subduction listener task.
//!
//! [`ListenerFuture`] wraps the abortable listener future so a caller can manage
//! its lifecycle (e.g. abort it) independently of the [`Subduction`] instance.
//!
//! [`Subduction`]: super::Subduction

use alloc::boxed::Box;
use core::{
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

use futures::stream::{Abortable, Aborted};
use sedimentree_core::depth::{CountLeadingZeroBytes, DepthMetric};
use subduction_crypto::signer::Signer;

use super::SubductionFutureForm;
use crate::{
    connection::{Connection, manager::Spawn},
    handler::Handler,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    storage::traits::Storage,
    timeout::Timeout,
};

/// A future representing the listener task for Subduction.
///
/// This lets the caller decide how they want to manage the listener's lifecycle,
/// including the ability to abort it when needed.
#[derive(Debug)]
pub struct ListenerFuture<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> {
    fut: Pin<Box<Abortable<Async::Future<'a, ()>>>>,
    #[allow(clippy::type_complexity)]
    _phantom: PhantomData<(Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric)>,
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    /// Create a new [`ListenerFuture`] wrapping the given abortable future.
    pub(crate) fn new(fut: Abortable<Async::Future<'a, ()>>) -> Self {
        Self {
            fut: Box::pin(fut),
            _phantom: PhantomData,
        }
    }

    /// Check if the listener future has been aborted.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.fut.is_aborted()
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Deref for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    type Target = Abortable<Async::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Future for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Unpin for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
}
