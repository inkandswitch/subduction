//! Composed message handler for the CLI server.
//!
//! Dispatches [`CliWireMessage`] variants to the appropriate sub-handler:
//!
//! | Variant      | Handler                   |
//! |--------------|---------------------------|
//! | `Sync`       | [`SyncHandler`]           |
//! | `Ephemeral`  | Logged and dropped        |
//! | `Keyhive`    | [`KeyhiveProtocolHandle`] |

use std::sync::Arc;

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::commit::CountLeadingZeroBytes;
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    authenticated::Authenticated,
    handler::Handler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    storage::metrics::MetricsStorage,
    subduction::error::{IoError, ListenError},
    transport::message::MessageTransport,
};
use subduction_keyhive_policy::handler::KeyhiveProtocolHandle;

use crate::{transport::UnifiedTransport, wire::CliWireMessage};

/// The concrete connection type used by the CLI server.
pub(crate) type CliConn = MessageTransport<UnifiedTransport>;

/// Concrete `ListenError` for the CLI handler.
type CliListenError = ListenError<Sendable, MetricsStorage<FsStorage>, CliConn, CliWireMessage>;

/// Handler that dispatches [`CliWireMessage`] variants to sub-handlers.
pub(crate) struct CliHandler {
    pub(crate) sync: Arc<
        subduction_core::handler::sync::SyncHandler<
            Sendable,
            MetricsStorage<FsStorage>,
            CliConn,
            OpenPolicy,
            CountLeadingZeroBytes,
        >,
    >,
    pub(crate) keyhive: KeyhiveProtocolHandle,
}

impl core::fmt::Debug for CliHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CliHandler").finish_non_exhaustive()
    }
}

impl RemoteHeadsNotifier for CliHandler {
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

    fn as_batch_sync_response(
        msg: &CliWireMessage,
    ) -> Option<&subduction_core::connection::message::BatchSyncResponse> {
        match msg {
            CliWireMessage::Sync(sync_msg) => match sync_msg.as_ref() {
                subduction_core::connection::message::SyncMessage::BatchSyncResponse(resp) => {
                    Some(resp)
                }
                subduction_core::connection::message::SyncMessage::BatchSyncRequest(_)
                | subduction_core::connection::message::SyncMessage::BlobsRequest { .. }
                | subduction_core::connection::message::SyncMessage::BlobsResponse { .. }
                | subduction_core::connection::message::SyncMessage::DataRequestRejected(_)
                | subduction_core::connection::message::SyncMessage::Fragment { .. }
                | subduction_core::connection::message::SyncMessage::LooseCommit { .. }
                | subduction_core::connection::message::SyncMessage::RemoveSubscriptions(_)
                | subduction_core::connection::message::SyncMessage::HeadsUpdate { .. } => None,
            },
            CliWireMessage::Ephemeral(_) | CliWireMessage::Keyhive(_) => None,
        }
    }

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<CliConn, Sendable>,
        message: CliWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                CliWireMessage::Sync(sync_msg) => {
                    Handler::<Sendable, CliConn>::handle(self.sync.as_ref(), conn, *sync_msg)
                        .await
                        .map_err(convert_sync_listen_error)
                }

                CliWireMessage::Ephemeral(_) => {
                    tracing::debug!(
                        "received ephemeral message but ephemeral handler not configured"
                    );
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
            Handler::<Sendable, CliConn>::on_peer_disconnect(self.sync.as_ref(), peer).await;
            Handler::<Sendable, CliConn>::on_peer_disconnect(&self.keyhive, peer).await;
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
            BatchSyncResponse, RemoteHeads, RemoveSubscriptions, RequestId, SyncMessage, SyncResult,
        },
        handler::Handler,
        peer::id::PeerId,
    };
    use subduction_ephemeral::message::EphemeralMessage;
    use subduction_keyhive::KeyhiveMessage;

    use super::{CliConn, CliHandler};
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
    fn as_batch_sync_response_extracts_from_sync() {
        let resp = BatchSyncResponse {
            req_id: test_request_id(),
            id: SedimentreeId::new([0xAA; 32]),
            result: SyncResult::NotFound,
            responder_heads: RemoteHeads::default(),
        };
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::BatchSyncResponse(resp.clone())));

        let extracted = <CliHandler as Handler<Sendable, CliConn>>::as_batch_sync_response(&msg);
        assert_eq!(extracted, Some(&resp));
    }

    #[test]
    fn as_batch_sync_response_none_for_other_sync() {
        let msg = CliWireMessage::Sync(Box::new(SyncMessage::RemoveSubscriptions(
            RemoveSubscriptions {
                ids: vec![SedimentreeId::new([0xBB; 32])],
            },
        )));

        let extracted = <CliHandler as Handler<Sendable, CliConn>>::as_batch_sync_response(&msg);
        assert_eq!(extracted, None);
    }

    #[test]
    fn as_batch_sync_response_none_for_ephemeral() {
        let msg = CliWireMessage::Ephemeral(EphemeralMessage::Ephemeral {
            id: SedimentreeId::new([0xCC; 32]),
            payload: vec![1, 2, 3],
        });

        let extracted = <CliHandler as Handler<Sendable, CliConn>>::as_batch_sync_response(&msg);
        assert_eq!(extracted, None);
    }

    #[test]
    fn as_batch_sync_response_none_for_keyhive() {
        let msg = CliWireMessage::Keyhive(KeyhiveMessage::new(vec![0xDE, 0xAD]));

        let extracted = <CliHandler as Handler<Sendable, CliConn>>::as_batch_sync_response(&msg);
        assert_eq!(extracted, None);
    }
}
