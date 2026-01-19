//! Stream wrapper for connections.
//!
//! This module provides the [`IntoConnectionStream`] trait which converts a [`Connection`]
//! into an async [`Stream`] of messages. Each connection continuously yields messages
//! until it errors, at which point the stream terminates and the connection is
//! automatically cleaned up.

use futures::stream::{BoxStream, LocalBoxStream, Stream, StreamExt};
use futures_kind::{FutureKind, Local, Sendable};

use super::{id::ConnectionId, message::Message, Connection};

/// Trait for creating message streams from connections.
///
/// This trait is generic over [`FutureKind`] to support both `Send` and `!Send` futures,
/// allowing the same code to work in both threaded (native) and single-threaded (WASM) environments.
///
/// When a connection's `recv()` returns an error, the stream terminates.
/// This is how dead connections get cleaned up - they simply stop yielding messages.
pub trait IntoConnectionStream<'a, C: Connection<Self> + 'a>: FutureKind {
    /// The stream type produced.
    type Stream: Stream<Item = (ConnectionId, Result<Message, C::RecvError>)> + Unpin + 'a;

    /// Convert a connection into a stream of messages.
    ///
    /// The returned stream will continuously call `conn.recv()` and yield messages.
    /// When `recv()` returns an error, the stream terminates.
    fn into_stream(conn_id: ConnectionId, conn: C) -> Self::Stream;
}

impl<'a, C: Connection<Sendable> + Send + 'a> IntoConnectionStream<'a, C> for Sendable
where
    C::RecvError: Send,
{
    type Stream = BoxStream<'a, (ConnectionId, Result<Message, C::RecvError>)>;

    fn into_stream(conn_id: ConnectionId, conn: C) -> Self::Stream {
        futures::stream::unfold(conn, move |conn| async move {
            let result = conn.recv().await;
            if result.is_ok() {
                tracing::debug!("connection {conn_id}: received message");
                Some(((conn_id, result), conn))
            } else {
                // Stream terminates on error - this is how dead connections get cleaned up.
                // The actor will be notified when the stream ends.
                tracing::debug!("connection {conn_id}: stream ending due to recv error");
                None
            }
        })
        .boxed()
    }
}

impl<'a, C: Connection<Local> + 'a> IntoConnectionStream<'a, C> for Local {
    type Stream = LocalBoxStream<'a, (ConnectionId, Result<Message, C::RecvError>)>;

    fn into_stream(conn_id: ConnectionId, conn: C) -> Self::Stream {
        futures::stream::unfold(conn, move |conn| async move {
            let result = conn.recv().await;
            if result.is_ok() {
                tracing::debug!("connection {conn_id}: received message");
                Some(((conn_id, result), conn))
            } else {
                // Stream terminates on error - this is how dead connections get cleaned up.
                // The actor will be notified when the stream ends.
                tracing::debug!("connection {conn_id}: stream ending due to recv error");
                None
            }
        })
        .boxed_local()
    }
}
