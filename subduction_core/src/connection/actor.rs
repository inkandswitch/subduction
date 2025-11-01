//! Actor for handling connections and messages.

use futures::{
    channel::mpsc::UnboundedReceiver,
    stream::{FusedStream, FuturesUnordered},
    FutureExt, StreamExt,
};
use sedimentree_core::future::FutureKind;

use super::{id::ConnectionId, message::Message, recv_once::RecvOnce, Connection};

/// An actor that listens for incoming connections and processes messages.
#[derive(Debug)]
pub struct ConnectionActor<'a, F: FutureKind, C: Connection<F>> {
    inbox: UnboundedReceiver<(ConnectionId, C)>,
    outbox: async_channel::Sender<(ConnectionId, C, Message)>,
    queue: FuturesUnordered<F::Future<'a, ()>>,
}

impl<'a, F: RecvOnce<'a, C>, C: Connection<F>> ConnectionActor<'a, F, C> {
    /// Create a new [`ConnectionActor`].
    pub fn new(
        inbox: UnboundedReceiver<(ConnectionId, C)>,
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
        let mut inbox = self.inbox.by_ref().fuse();

        loop {
            if inbox.is_terminated() && self.queue.is_empty() {
                break;
            }

            futures::select! {
                maybe = inbox.next() => {
                    if let Some((conn_id, conn)) = maybe {
                        self.queue.push(F::recv_once(conn_id, conn, self.outbox.clone()));
                    }
                }

                _maybe = self.queue.next().fuse() => {
                    //  Just drain
                }
            }
        }
    }
}
