//! Composed handler that dispatches a wire envelope to sub-handlers.
//!
//! [`ComposedHandler`] takes a sync handler and an ephemeral handler,
//! and dispatches based on the wire message variant via [`WireEnvelope`].

use alloc::boxed::Box;
use core::fmt::Debug;

use future_form::{FutureForm, Local, Sendable, future_form};
use sedimentree_core::{
    codec::{decode::Decode, encode::Encode},
    id::SedimentreeId,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        Connection,
        message::{BatchSyncResponse, SyncMessage},
    },
    handler::Handler,
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::traits::Storage,
    subduction::error::{IoError, ListenError},
};
use thiserror::Error;

use crate::message::EphemeralMessage;

/// Result of dispatching a wire envelope message.
#[derive(Debug)]
pub enum Dispatched {
    /// The message was a sync-protocol message.
    Sync(Box<SyncMessage>),

    /// The message was an ephemeral-protocol message.
    Ephemeral(EphemeralMessage),
}

/// Trait for wire envelope types that can be dispatched to sub-handlers.
///
/// Implement this on your wire message enum (e.g., `CliWireMessage`,
/// `WireMessage`) to enable [`ComposedHandler`] dispatch.
pub trait WireEnvelope:
    sedimentree_core::codec::encode::Encode
    + sedimentree_core::codec::decode::Decode
    + From<SyncMessage>
    + Clone
    + Send
    + Debug
    + 'static
{
    /// Dispatch this message into its sub-handler variant.
    fn dispatch(self) -> Dispatched;

    /// Extract a [`BatchSyncResponse`] reference for roundtrip routing.
    fn as_batch_sync_response(&self) -> Option<&BatchSyncResponse>;
}

/// Composed handler that dispatches to sync and ephemeral sub-handlers.
pub struct ComposedHandler<SyncH, EphH, W> {
    sync: SyncH,
    ephemeral: EphH,
    _wire: core::marker::PhantomData<fn() -> W>,
}

impl<SyncH: Debug, EphH: Debug, W> Debug for ComposedHandler<SyncH, EphH, W> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ComposedHandler")
            .field("sync", &self.sync)
            .field("ephemeral", &self.ephemeral)
            .finish()
    }
}

impl<SyncH, EphH, W> ComposedHandler<SyncH, EphH, W> {
    /// Create a new composed handler from sync and ephemeral sub-handlers.
    #[must_use]
    pub const fn new(sync: SyncH, ephemeral: EphH) -> Self {
        Self {
            sync,
            ephemeral,
            _wire: core::marker::PhantomData,
        }
    }
}

impl<SyncH: RemoteHeadsNotifier, EphH, W> RemoteHeadsNotifier for ComposedHandler<SyncH, EphH, W> {
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.sync.notify_remote_heads(id, peer, heads);
    }
}

impl<SyncH, EphH, W> ComposedHandler<SyncH, EphH, W> {
    /// Access the sync sub-handler.
    #[must_use]
    pub const fn sync(&self) -> &SyncH {
        &self.sync
    }

    /// Access the ephemeral sub-handler.
    #[must_use]
    pub const fn ephemeral(&self) -> &EphH {
        &self.ephemeral
    }
}

/// Error from a [`ComposedHandler`].
#[derive(Debug, Error)]
pub enum ComposedHandlerError<S: core::error::Error, E: core::error::Error> {
    /// Error from the sync sub-handler.
    #[error("sync: {0}")]
    Sync(S),

    /// Error from the ephemeral sub-handler.
    #[error("ephemeral: {0}")]
    Ephemeral(E),
}

/// Convert a [`ComposedHandlerError`] into a [`ListenError`] parameterised
/// over the wire-envelope message type `W`.
///
/// This works because [`MessageTransport`]-based connections have error
/// types that are independent of the message generic `M`, so
/// `ListenError<Async,Store,Conn,SyncMessage>` can be retyped to
/// `ListenError<Async,Store,Conn,WireMsg>` without loss of information.
impl<Async, Store, Conn, WireMsg, EphErr>
    From<ComposedHandlerError<ListenError<Async, Store, Conn, SyncMessage>, EphErr>>
    for ListenError<Async, Store, Conn, WireMsg>
where
    Async: FutureForm + Debug,
    Store: Storage<Async> + Debug,
    Conn: Connection<Async, SyncMessage> + Connection<Async, WireMsg> + Debug,
    WireMsg: Encode + Decode,
    Store::Error: Debug,
    <Conn as Connection<Async, SyncMessage>>::SendError:
        Debug + Into<<Conn as Connection<Async, WireMsg>>::SendError>,
    <Conn as Connection<Async, SyncMessage>>::RecvError:
        Debug + Into<<Conn as Connection<Async, WireMsg>>::RecvError>,
    <Conn as Connection<Async, WireMsg>>::SendError: Debug,
    <Conn as Connection<Async, WireMsg>>::RecvError: Debug,
    EphErr: core::error::Error,
{
    fn from(
        err: ComposedHandlerError<ListenError<Async, Store, Conn, SyncMessage>, EphErr>,
    ) -> Self {
        match err {
            ComposedHandlerError::Sync(listen_err) => match listen_err {
                ListenError::IoError(io_err) => ListenError::IoError(match io_err {
                    IoError::Storage(e) => IoError::Storage(e),
                    IoError::ConnSend(e) => IoError::ConnSend(e.into()),
                    IoError::ConnRecv(e) => IoError::ConnRecv(e.into()),
                    IoError::ConnCall(e) => IoError::ConnCall(e.map_send(Into::into)),
                    IoError::BlobMismatch(e) => IoError::BlobMismatch(e),
                }),
                ListenError::TrySendError => ListenError::TrySendError,
            },
            ComposedHandlerError::Ephemeral(_eph_err) => {
                // Ephemeral handler errors are non-fatal by design —
                // map to TrySendError as a generic "handler failed" signal.
                ListenError::TrySendError
            }
        }
    }
}

#[future_form(
    Sendable where
        C: Clone + Send + Sync + 'static,
        SyncH: Handler<Sendable, C, Message = SyncMessage> + Send + Sync,
        SyncH::HandlerError: Send + 'static,
        EphH: Handler<Sendable, C, Message = EphemeralMessage> + Send + Sync,
        EphH::HandlerError: Send + 'static,
    Local where
        C: Clone + 'static,
        SyncH: Handler<Local, C, Message = SyncMessage>,
        EphH: Handler<Local, C, Message = EphemeralMessage>,
)]
impl<Async: FutureForm, C, SyncH, EphH, W> Handler<Async, C> for ComposedHandler<SyncH, EphH, W>
where
    SyncH: Handler<Async, C, Message = SyncMessage>,
    EphH: Handler<Async, C, Message = EphemeralMessage>,
    W: WireEnvelope,
{
    type Message = W;
    type HandlerError = ComposedHandlerError<SyncH::HandlerError, EphH::HandlerError>;

    fn as_batch_sync_response(msg: &W) -> Option<&BatchSyncResponse> {
        msg.as_batch_sync_response()
    }

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, Async>,
        message: W,
    ) -> Async::Future<'a, Result<(), Self::HandlerError>> {
        Async::from_future(async move {
            match message.dispatch() {
                Dispatched::Sync(msg) => self
                    .sync
                    .handle(conn, *msg)
                    .await
                    .map_err(ComposedHandlerError::Sync),
                Dispatched::Ephemeral(msg) => self
                    .ephemeral
                    .handle(conn, msg)
                    .await
                    .map_err(ComposedHandlerError::Ephemeral),
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> Async::Future<'_, ()> {
        Async::from_future(async move {
            self.sync.on_peer_disconnect(peer).await;
            self.ephemeral.on_peer_disconnect(peer).await;
        })
    }
}
