//! The future returned for the Subduction listener task.
//!
//! [`ListenerFuture`] wraps the abortable listener future so a caller can manage
//! its lifecycle (e.g. abort it) independently of the [`Subduction`] instance.
//!
//! [`Subduction`]: super::Subduction

use alloc::boxed::Box;
use core::{
    future::Future,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

use future_form::FutureForm;
use futures::stream::{Abortable, Aborted};

/// A future representing the listener task for Subduction.
///
/// This lets the caller decide how they want to manage the listener's lifecycle,
/// including the ability to abort it when needed.
#[derive(Debug)]
pub struct ListenerFuture<'a, Async: FutureForm> {
    fut: Pin<Box<Abortable<Async::Future<'a, ()>>>>,
}

impl<'a, Async: FutureForm> ListenerFuture<'a, Async> {
    /// Create a new [`ListenerFuture`] wrapping the given abortable future.
    pub(crate) fn new(fut: Abortable<Async::Future<'a, ()>>) -> Self {
        Self { fut: Box::pin(fut) }
    }

    /// Check if the listener future has been aborted.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.fut.is_aborted()
    }
}

impl<'a, Async: FutureForm> Deref for ListenerFuture<'a, Async> {
    type Target = Abortable<Async::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<Async: FutureForm> Future for ListenerFuture<'_, Async> {
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}
