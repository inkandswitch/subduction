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
                | subduction_core::connection::message::SyncMessage::RemoveSubscriptions(_) => None,
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
                    tracing::warn!(
                        "received ephemeral message but ephemeral handler not configured"
                    );
                    Ok(())
                }

                CliWireMessage::Keyhive(keyhive_msg) => {
                    Handler::<Sendable, CliConn>::handle(&self.keyhive, conn, keyhive_msg)
                        .await
                        .map_err(|e| {
                            tracing::error!(error = %e, "keyhive handler error");
                            ListenError::TrySendError
                        })
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
