//! Actor for handling connections and messages.

use alloc::boxed::Box;
use core::{
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    stream::{AbortRegistration, Abortable, Aborted, FuturesUnordered},
    FutureExt, StreamExt,
};
use sedimentree_core::future::{FutureKind, Local, Sendable};

use super::{id::ConnectionId, message::Message, recv_once::RecvOnce, Connection};

/// An actor that listens for incoming connections and processes messages.
#[derive(Debug)]
pub struct ConnectionActor<'a, F: FutureKind, C: Connection<F>> {
    inbox: async_channel::Receiver<(ConnectionId, C)>,
    outbox: async_channel::Sender<(ConnectionId, C, Message)>,
    queue: FuturesUnordered<F::Future<'a, ()>>, // TODO cancel on disconnection
}

impl<'a, F: RecvOnce<'a, C>, C: Connection<F>> ConnectionActor<'a, F, C> {
    /// Create a new [`ConnectionActor`].
    #[must_use]
    pub fn new(
        inbox: async_channel::Receiver<(ConnectionId, C)>,
        outbox: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self {
        ConnectionActor {
            inbox,
            outbox,
            queue: FuturesUnordered::new(),
        }
    }

    /// Listen for incoming connections and process messages.
    pub async fn listen(&mut self) {
        loop {
            tracing::debug!(
                "ConnectionActor: waiting (queue size: {})",
                self.queue.len()
            );

            if self.queue.is_empty() {
                match self.inbox.recv().await {
                    Ok((conn_id, conn)) => {
                        tracing::debug!("ConnectionActor: new connection {:?}", conn_id);
                        self.queue
                            .push(F::recv_once(conn_id, conn, self.outbox.clone()));
                    }
                    Err(e) => {
                        tracing::warn!("ConnectionActor: inbox closed: {e:?}; draining tasks");
                        while let Some(()) = self.queue.next().await {}
                        break;
                    }
                }
                continue;
            }

            futures::select! {
                maybe = self.inbox.recv().fuse() => {
                    match maybe {
                        Ok((conn_id, conn)) => {
                            tracing::debug!("ConnectionActor: new connection {:?}", conn_id);
                            self.queue.push(F::recv_once(conn_id, conn, self.outbox.clone()));
                        }
                        Err(e) => {
                            tracing::warn!("ConnectionActor: inbox closed: {e:?}; draining tasks");
                            while let Some(()) = self.queue.next().await {}
                            break;
                        }
                    }
                }
                done = self.queue.next() => {
                    if let Some(()) = done {
                        tracing::debug!("ConnectionActor: queued request processed");
                    }
                    // else: nothing in the queue -- totally normal, start the next loop
                }
            }
        }
    }
}

/// Trait for starting a [`ConnectionActor`] as an abortable future.
///
/// This trait is implemented for both `Send` and `!Send` futures,
pub trait StartConnectionActor<'a, C: Connection<Self>>: FutureKind + Sized {
    /// Make a future for the actor's run loop.
    fn start_actor(
        actor: ConnectionActor<'a, Self, C>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>;
}

impl<'a, C: Connection<Sendable> + Send + 'a> StartConnectionActor<'a, C> for Sendable {
    fn start_actor(
        actor: ConnectionActor<'a, Self, C>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            async move {
                let mut inner = actor;
                ConnectionActor::listen(&mut inner).await;
            }
            .boxed(),
            abort_reg,
        )
    }
}

impl<'a, C: Connection<Local> + 'a> StartConnectionActor<'a, C> for Local {
    fn start_actor(
        actor: ConnectionActor<'a, Self, C>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            async move {
                let mut inner = actor;
                ConnectionActor::listen(&mut inner).await;
            }
            .boxed_local(),
            abort_reg,
        )
    }
}

/// A future representing the running [`ConnectionActor`].
///
/// This allows the caller to monitor and control the lifecycle of the [`ConnectionActor`].
#[derive(Debug)]
pub struct ConnectionActorFuture<'a, F: StartConnectionActor<'a, C>, C: Connection<F>> {
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<C>,
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F>> ConnectionActorFuture<'a, F, C> {
    pub(crate) fn new(fut: Abortable<F::Future<'a, ()>>) -> Self {
        Self {
            fut: Box::pin(fut),
            _phantom: PhantomData,
        }
    }

    /// Check if the actor future has been aborted.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.fut.is_aborted()
    }
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq> Future
    for ConnectionActorFuture<'a, F, C>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq> Unpin
    for ConnectionActorFuture<'a, F, C>
{
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq> Deref
    for ConnectionActorFuture<'a, F, C>
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}
