//! Actor for handling connections and messages.
//!
//! The [`ConnectionActor`] manages connection streams and multiplexes incoming messages.
//! Each connection is wrapped as an async stream that continuously yields messages.
//! When a stream ends (connection dies), the actor notifies via the `connection_closed` channel.

use alloc::boxed::Box;
use core::{
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::{pin, Pin},
    task::{Context, Poll},
};

use futures::{
    future::{select, Either},
    stream::{AbortRegistration, Abortable, Aborted, SelectAll, StreamExt},
    FutureExt,
};
use futures_kind::{kinds, FutureKind, Local, Sendable};

use super::{id::ConnectionId, message::Message, stream::IntoConnectionStream, Connection};

/// Commands that can be sent to the [`ConnectionActor`].
#[derive(Debug)]
pub enum ActorCommand<C> {
    /// Add a new connection to be managed.
    AddConnection(ConnectionId, C),
    /// Remove a connection (stop listening for messages from it).
    RemoveConnection(ConnectionId),
}

/// An actor that manages connection streams and multiplexes incoming messages.
///
/// The actor holds all active connection streams and forwards received messages
/// to the outbox channel. When a connection stream ends (due to error or disconnect),
/// the actor notifies via the `connection_closed` channel.
pub struct ConnectionActor<'a, F: IntoConnectionStream<'a, C>, C: Connection<F>> {
    /// Incoming commands (add/remove connections).
    commands: async_channel::Receiver<ActorCommand<C>>,

    /// Outbound messages: (`connection_id`, message).
    outbox: async_channel::Sender<(ConnectionId, Message)>,

    /// Notification channel for when connections die.
    connection_closed: async_channel::Sender<ConnectionId>,

    /// All active connection streams merged together.
    streams: SelectAll<F::Stream>,

    _phantom: PhantomData<&'a ()>,
}

impl<'a, F: IntoConnectionStream<'a, C>, C: Connection<F> + 'a> ConnectionActor<'a, F, C> {
    /// Create a new [`ConnectionActor`].
    #[must_use]
    pub fn new(
        commands: async_channel::Receiver<ActorCommand<C>>,
        outbox: async_channel::Sender<(ConnectionId, Message)>,
        connection_closed: async_channel::Sender<ConnectionId>,
    ) -> Self {
        ConnectionActor {
            commands,
            outbox,
            connection_closed,
            streams: SelectAll::new(),
            _phantom: PhantomData,
        }
    }

    /// Listen for commands and stream messages.
    ///
    /// This method runs indefinitely, processing:
    /// - Commands to add/remove connections
    /// - Messages from all active connection streams
    ///
    /// When a connection stream ends, it notifies via `connection_closed`.
    pub async fn listen(&mut self) {
        loop {
            tracing::debug!("ConnectionActor: waiting (streams: {})", self.streams.len());

            if self.streams.is_empty() {
                // No active streams, just wait for commands
                match self.commands.recv().await {
                    Ok(cmd) => self.handle_command(cmd),
                    Err(e) => {
                        tracing::warn!("ConnectionActor: command channel closed: {e:?}");
                        break;
                    }
                }
                continue;
            }

            // Race between commands and stream messages
            let cmd_fut = pin!(self.commands.recv().fuse());
            let stream_fut = pin!(self.streams.next().fuse());

            match select(cmd_fut, stream_fut).await {
                Either::Left((cmd_result, _)) => match cmd_result {
                    Ok(cmd) => self.handle_command(cmd),
                    Err(e) => {
                        tracing::warn!("ConnectionActor: command channel closed: {e:?}");
                        break;
                    }
                },
                Either::Right((stream_result, _)) => {
                    self.handle_stream_item(stream_result).await;
                }
            }
        }
    }

    fn handle_command(&mut self, cmd: ActorCommand<C>) {
        match cmd {
            ActorCommand::AddConnection(conn_id, conn) => {
                tracing::debug!("ConnectionActor: adding connection {conn_id}");
                let stream = F::into_stream(conn_id, conn);
                self.streams.push(stream);
            }
            ActorCommand::RemoveConnection(conn_id) => {
                tracing::debug!("ConnectionActor: remove request for {conn_id} (will be filtered)");
                // Note: SelectAll doesn't support removal by ID.
                // The stream will naturally end when it errors or disconnects.
            }
        }
    }

    async fn handle_stream_item(
        &mut self,
        item: Option<(ConnectionId, Result<Message, C::RecvError>)>,
    ) {
        match item {
            Some((conn_id, Ok(msg))) => {
                // Successfully received a message - send to outbox
                if let Err(e) = self.outbox.send((conn_id, msg)).await {
                    tracing::error!("ConnectionActor: outbox closed: {e:?}");
                }
            }
            Some((conn_id, Err(e))) => {
                // Stream yielded an error - connection is broken
                tracing::debug!("ConnectionActor: connection {conn_id} error: {e:?}");
                let _ = self.connection_closed.send(conn_id).await;
            }
            None => {
                // All streams ended
                tracing::debug!("ConnectionActor: all streams ended");
            }
        }
    }
}

// Manual Debug impl since we can't derive it with the stream type
impl<'a, F: IntoConnectionStream<'a, C>, C: Connection<F> + 'a> core::fmt::Debug
    for ConnectionActor<'a, F, C>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ConnectionActor")
            .field("streams_count", &self.streams.len())
            .finish_non_exhaustive()
    }
}

/// Trait for starting a [`ConnectionActor`] as an abortable future.
///
/// This trait is implemented for both `Send` and `!Send` futures.
pub trait StartConnectionActor<'a, C: Connection<Self> + 'a>:
    IntoConnectionStream<'a, C> + Sized
{
    /// Make a future for the actor's run loop.
    fn start_actor(
        actor: ConnectionActor<'a, Self, C>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>;
}

#[kinds(Sendable where C: Connection<Sendable> + Send + 'static, C::RecvError: Send, Local where C: Connection<Local> + 'a)]
impl<'a, K: FutureKind, C> StartConnectionActor<'a, C> for K
where
    K: IntoConnectionStream<'a, C>,
{
    fn start_actor(
        actor: ConnectionActor<'a, Self, C>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            K::into_kind(async move {
                let mut inner = actor;
                ConnectionActor::listen(&mut inner).await;
            }),
            abort_reg,
        )
    }
}

/// A future representing the running [`ConnectionActor`].
///
/// This allows the caller to monitor and control the lifecycle of the [`ConnectionActor`].
#[derive(Debug)]
pub struct ConnectionActorFuture<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + 'a> {
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<C>,
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + 'a> ConnectionActorFuture<'a, F, C> {
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

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq + 'a> Future
    for ConnectionActorFuture<'a, F, C>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq + 'a> Unpin
    for ConnectionActorFuture<'a, F, C>
{
}

impl<'a, F: StartConnectionActor<'a, C>, C: Connection<F> + PartialEq + 'a> Deref
    for ConnectionActorFuture<'a, F, C>
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::connection::test_utils::MockConnection;

    mod connection_actor {
        use super::*;

        #[test]
        fn test_new_creates_empty_streams() {
            let (_cmd_tx, cmd_rx) = async_channel::unbounded();
            let (outbox_tx, _outbox_rx) = async_channel::unbounded();
            let (closed_tx, _closed_rx) = async_channel::unbounded();

            let actor: ConnectionActor<'_, Sendable, MockConnection> =
                ConnectionActor::new(cmd_rx, outbox_tx, closed_tx);

            assert_eq!(actor.streams.len(), 0);
        }
    }

    mod connection_actor_future {
        use super::*;

        #[test]
        fn test_is_aborted_initially_false() {
            let (abort_handle, abort_reg) = futures::stream::AbortHandle::new_pair();
            let abortable = Abortable::new(
                Box::pin(async {}) as Pin<Box<dyn Future<Output = ()> + Send>>,
                abort_reg,
            );

            let fut: ConnectionActorFuture<'_, Sendable, MockConnection> =
                ConnectionActorFuture::new(abortable);

            assert!(!fut.is_aborted());

            drop(abort_handle);
        }

        #[test]
        fn test_is_aborted_after_abort() {
            let (abort_handle, abort_reg) = futures::stream::AbortHandle::new_pair();
            let abortable = Abortable::new(
                Box::pin(async {}) as Pin<Box<dyn Future<Output = ()> + Send>>,
                abort_reg,
            );

            let fut: ConnectionActorFuture<'_, Sendable, MockConnection> =
                ConnectionActorFuture::new(abortable);

            abort_handle.abort();

            assert!(fut.is_aborted());
        }
    }
}
