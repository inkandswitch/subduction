use futures::FutureExt;
use futures_kind::{FutureKind, Local, Sendable};

use super::{id::ConnectionId, message::Message, Connection};

pub trait RecvOnce<'a, C: Connection<Self>>: FutureKind {
    fn recv_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()>;
}

impl<'a, C: 'a + Connection<Sendable> + Send> RecvOnce<'a, C> for Sendable {
    fn recv_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()> {
        async move {
            tracing::info!("recv_once (Sendable): BEFORE conn.recv() for {conn_id}");
            let msg = match conn.recv().await {
                Ok(msg) => {
                    tracing::debug!("received message from {conn_id}: {msg:?}");
                    msg
                }
                Err(e) => {
                    // NOTE: This early return is how dead connections get fully cleaned up.
                    // By not sending to `sender`, the connection is never re-registered
                    // to the actor_channel, so it gets dropped here.
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                    return;
                }
            };

            tracing::debug!("recv_once: received message from {conn_id}: {msg:?}");

            // Send to msg_queue for dispatch. The listener will re-register
            // this connection to actor_channel after processing.
            if let Err(e) = sender.send((conn_id, conn, msg)).await {
                tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
            }
            tracing::info!("recv_once (Sendable): AFTER sending msg for {conn_id}");
        }
        .boxed()
    }
}

impl<'a, C: 'a + Connection<Local>> RecvOnce<'a, C> for Local {
    fn recv_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()> {
        async move {
            tracing::debug!("waiting to receive message from {conn_id}");
            let msg = match conn.recv().await {
                Ok(msg) => {
                    tracing::debug!("received message from {conn_id}: {msg:?}");
                    msg
                }
                Err(e) => {
                    // NOTE: This early return is how dead connections get fully cleaned up.
                    // By not sending to `sender`, the connection is never re-registered
                    // to the actor_channel, so it gets dropped here.
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                    return;
                }
            };

            tracing::debug!("recv_once: received message from {conn_id}: {msg:?}");

            // Send to msg_queue for dispatch. The listener will re-register
            // this connection to actor_channel after processing.
            if let Err(e) = sender.send((conn_id, conn, msg)).await {
                tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
            }
        }
        .boxed_local()
    }
}
