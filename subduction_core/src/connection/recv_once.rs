use futures::FutureExt;
use sedimentree_core::future::{FutureKind, Local, Sendable};

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
            let msg = match conn.recv().await {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                    return;
                }
            };

            tracing::debug!("received message from {conn_id}: {msg:?}");

            if let Err(e) = sender.send((conn_id, conn, msg)).await {
                tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
            }
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
                Ok(msg) => msg,
                Err(e) => {
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                    return;
                }
            };

            tracing::debug!("received message from {conn_id}: {msg:?}");

            if let Err(e) = sender.send((conn_id, conn, msg)).await {
                tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
            }
        }
        .boxed_local()
    }
}
