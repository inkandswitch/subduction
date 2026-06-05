//! Composed message handler for the CLI server.
//!
//! Dispatches [`CliWireMessage`] variants to the appropriate sub-handler:
//!
//! | Variant      | Handler                    |
//! |--------------|----------------------------|
//! | `Sync`       | [`SyncHandler`]            |
//! | `Ephemeral`  | [`EphemeralHandler`]       |
//! | `Keyhive`    | [`SendableKeyhiveHandler`] |

use std::sync::Arc;

use crate::{
    keyhive::{CliConnKeyhiveAdapter, FsKeyhiveStorage},
    policy::CliKeyhivePolicyHandle,
    transport::UnifiedTransport,
    wire::CliWireMessage,
};
use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::commit::CountLeadingZeroBytes;
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    authenticated::Authenticated,
    connection::message::SyncMessage,
    handler::{Handler, sync::SyncHandler},
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::metrics::MetricsStorage,
    subduction::error::{IoError, ListenError},
    transport::message::MessageTransport,
};
use subduction_ephemeral::{
    clock::std_clock::StdClock, handler::EphemeralHandler, policy::OpenEphemeralPolicy,
};
use subduction_keyhive::handler::{SendableKeyhiveHandler, SendableRuntimeProtocol};

/// The concrete connection type used by the CLI server.
pub(crate) type CliConn = MessageTransport<UnifiedTransport>;

/// The concrete ephemeral handler type for the CLI server.
pub(crate) type CliEphemeralHandler =
    EphemeralHandler<Sendable, CliConn, OpenEphemeralPolicy, StdClock>;

/// The concrete keyhive protocol type for the CLI server.
pub(crate) type CliKeyhiveProtocol =
    Arc<SendableRuntimeProtocol<CliConnKeyhiveAdapter, FsKeyhiveStorage>>;

/// The concrete keyhive handler type for the CLI server.
pub(crate) type CliKeyhiveHandler = SendableKeyhiveHandler<
    CliConnKeyhiveAdapter,
    FsKeyhiveStorage,
    CliConn,
    fn(Authenticated<CliConn, Sendable>) -> CliConnKeyhiveAdapter,
>;

/// Concrete `ListenError` for the CLI handler.
type CliListenError = ListenError<Sendable, MetricsStorage<FsStorage>, CliConn, CliWireMessage>;

/// Bundles the bounds the server's connection plumbing requires of a CLI
/// handler, so [`CliHandler`] and [`CliHandlerNoKeyhive`] can be used
/// interchangeably as the `H` of [`CliSubduction`](crate::server) without
/// repeating the full bound at every site.
pub(crate) trait CliWireHandler:
    Handler<Sendable, CliConn, Message = CliWireMessage, HandlerError = CliListenError>
    + RemoteHeadsNotifier
    + Send
    + Sync
    + 'static
{
}

impl<H> CliWireHandler for H where
    H: Handler<Sendable, CliConn, Message = CliWireMessage, HandlerError = CliListenError>
        + RemoteHeadsNotifier
        + Send
        + Sync
        + 'static
{
}

/// The concrete sync handler type for the CLI server.
pub(crate) type CliSyncHandler = Arc<
    SyncHandler<
        Sendable,
        MetricsStorage<FsStorage>,
        CliConn,
        CliKeyhivePolicyHandle,
        CountLeadingZeroBytes,
    >,
>;

/// Server handler with keyhive disabled: keyhive (SUK) wire messages are
/// dropped and disconnects are not forwarded to keyhive. Used by `--auth open`
/// so a keyhive-free server never touches the keyhive runtime path.
///
/// This is also the shared sync + ephemeral core that the keyhive-enabled
/// [`CliHandler`] embeds and delegates its non-keyhive arms to.
pub(crate) struct CliHandlerNoKeyhive {
    sync: CliSyncHandler,
    ephemeral: CliEphemeralHandler,
}

impl CliHandlerNoKeyhive {
    pub(crate) const fn new(sync: CliSyncHandler, ephemeral: CliEphemeralHandler) -> Self {
        Self { sync, ephemeral }
    }

    /// Dispatch a `Sync` wire message.
    async fn handle_sync(
        &self,
        conn: &Authenticated<CliConn, Sendable>,
        sync_msg: SyncMessage,
    ) -> Result<(), CliListenError> {
        Handler::<Sendable, CliConn>::handle(self.sync.as_ref(), conn, sync_msg)
            .await
            .map_err(convert_sync_listen_error)
    }

    /// Dispatch an `Ephemeral` wire message (errors are logged, not fatal).
    async fn handle_ephemeral(
        &self,
        conn: &Authenticated<CliConn, Sendable>,
        eph_msg: subduction_ephemeral::message::EphemeralMessage,
    ) {
        if let Err(e) = Handler::<Sendable, CliConn>::handle(&self.ephemeral, conn, eph_msg).await {
            tracing::error!(error = %e, "ephemeral handler error (non-fatal)");
        }
    }

    /// Forward a peer disconnect to the sync and ephemeral handlers.
    async fn disconnect_core(&self, peer: PeerId) {
        Handler::<Sendable, CliConn>::on_peer_disconnect(self.sync.as_ref(), peer).await;
        Handler::<Sendable, CliConn>::on_peer_disconnect(&self.ephemeral, peer).await;
    }
}

/// Server handler with keyhive enabled: keyhive (SUK) wire messages are
/// delegated to [`CliKeyhiveHandler`] and disconnects drive its bookkeeping.
pub(crate) struct CliHandler {
    core: CliHandlerNoKeyhive,
    keyhive: CliKeyhiveHandler,
}

impl CliHandler {
    pub(crate) const fn new(
        sync: CliSyncHandler,
        ephemeral: CliEphemeralHandler,
        keyhive: CliKeyhiveHandler,
    ) -> Self {
        Self {
            core: CliHandlerNoKeyhive::new(sync, ephemeral),
            keyhive,
        }
    }
}

impl core::fmt::Debug for CliHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CliHandler").finish_non_exhaustive()
    }
}

impl core::fmt::Debug for CliHandlerNoKeyhive {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CliHandlerNoKeyhive")
            .finish_non_exhaustive()
    }
}

impl RemoteHeadsNotifier for CliHandler {
    fn notify_remote_heads(
        &self,
        id: sedimentree_core::id::SedimentreeId,
        peer: PeerId,
        heads: RemoteHeads,
    ) {
        self.core.notify_remote_heads(id, peer, heads);
    }
}

impl RemoteHeadsNotifier for CliHandlerNoKeyhive {
    fn notify_remote_heads(
        &self,
        id: sedimentree_core::id::SedimentreeId,
        peer: PeerId,
        heads: RemoteHeads,
    ) {
        RemoteHeadsNotifier::notify_remote_heads(self.sync.as_ref(), id, peer, heads);
    }
}

impl Handler<Sendable, CliConn> for CliHandler {
    type Message = CliWireMessage;
    type HandlerError = CliListenError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<CliConn, Sendable>,
        message: CliWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                CliWireMessage::Sync(sync_msg) => self.core.handle_sync(conn, *sync_msg).await,
                CliWireMessage::Ephemeral(eph_msg) => {
                    self.core.handle_ephemeral(conn, eph_msg).await;
                    Ok(())
                }
                CliWireMessage::Keyhive(keyhive_msg) => {
                    if let Err(e) =
                        Handler::<Sendable, CliConn>::handle(&self.keyhive, conn, keyhive_msg).await
                    {
                        tracing::error!(error = %e, "keyhive handler error (non-fatal)");
                    }
                    Ok(())
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.core.disconnect_core(peer).await;
            Handler::<Sendable, CliConn>::on_peer_disconnect(&self.keyhive, peer).await;
        })
    }
}

impl Handler<Sendable, CliConn> for CliHandlerNoKeyhive {
    type Message = CliWireMessage;
    type HandlerError = CliListenError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<CliConn, Sendable>,
        message: CliWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                CliWireMessage::Sync(sync_msg) => self.handle_sync(conn, *sync_msg).await,
                CliWireMessage::Ephemeral(eph_msg) => {
                    self.handle_ephemeral(conn, eph_msg).await;
                    Ok(())
                }
                // Keyhive disabled: drop inbound SUK messages.
                CliWireMessage::Keyhive(_keyhive_msg) => Ok(()),
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.disconnect_core(peer).await;
        })
    }
}

/// Convert a sync `ListenError<..., SyncMessage>` into `CliListenError`.
fn convert_sync_listen_error(
    err: ListenError<
        Sendable,
        MetricsStorage<FsStorage>,
        CliConn,
        subduction_core::connection::message::SyncMessage,
    >,
) -> CliListenError {
    match err {
        ListenError::IoError(io_err) => ListenError::IoError(match io_err {
            IoError::Storage(e) => IoError::Storage(e),
            IoError::ConnSend(e) => IoError::ConnSend(e),
            IoError::ConnRecv(e) => IoError::ConnRecv(e),
            IoError::ConnCall(e) => IoError::ConnCall(e),
            IoError::BlobMismatch(e) => IoError::BlobMismatch(e),
        }),
        ListenError::TrySendError => ListenError::TrySendError,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use future_form::Sendable;
    use sedimentree_core::id::SedimentreeId;
    use subduction_core::{
        connection::message::{
            BatchSyncResponse, RemoveSubscriptions, RequestId, SyncMessage, SyncResult,
            TryAsBatchSyncResponse,
        },
        peer::id::PeerId,
        remote_heads::RemoteHeads,
        timestamp::TimestampSeconds,
    };
    use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
    use subduction_ephemeral::{
        message::{EphemeralMessage, EphemeralPayload},
        topic::Topic,
    };
    use subduction_keyhive::KeyhiveMessage;

    use crate::wire::CliWireMessage;

    fn test_peer_id() -> PeerId {
        PeerId::new([42u8; 32])
    }

    fn test_request_id() -> RequestId {
        RequestId {
            requestor: test_peer_id(),
            nonce: 99,
        }
    }

    #[test]
    fn try_as_batch_sync_response_extracts_from_sync() {
        let resp = BatchSyncResponse {
            req_id: test_request_id(),
            id: SedimentreeId::new([0xAA; 32]),
            result: SyncResult::NotFound,
            responder_heads: RemoteHeads::default(),
        };
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp.clone())));

        assert_eq!(msg.try_as_batch_sync_response(), Some(&resp));
    }

    #[test]
    fn try_as_batch_sync_response_none_for_other_sync() {
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::RemoveSubscriptions(
            RemoveSubscriptions {
                ids: vec![SedimentreeId::new([0xBB; 32])],
            },
        )));

        assert_eq!(msg.try_as_batch_sync_response(), None);
    }

    #[tokio::test]
    async fn try_as_batch_sync_response_none_for_ephemeral() {
        let signer = MemorySigner::generate();
        let ep = EphemeralPayload {
            id: Topic::new([0xCC; 32]),
            nonce: 42,
            timestamp: TimestampSeconds::new(1_700_000_000),
            payload: vec![1, 2, 3],
        };
        let verified = Signed::seal::<Sendable, _>(&signer, ep).await;
        let msg = CliWireMessage::Ephemeral(EphemeralMessage::Ephemeral(Box::new(
            verified.into_signed(),
        )));

        assert_eq!(msg.try_as_batch_sync_response(), None);
    }

    #[test]
    fn try_as_batch_sync_response_none_for_keyhive() {
        let msg = CliWireMessage::Keyhive(KeyhiveMessage::new(vec![0xDE, 0xAD]));

        assert_eq!(msg.try_as_batch_sync_response(), None);
    }
}
