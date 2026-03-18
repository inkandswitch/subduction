//! Composed message handler for the CLI server.
//!
//! Dispatches [`CliWireMessage`] variants to the appropriate sub-handler:
//!
//! | Variant      | Handler                                       |
//! |--------------|-----------------------------------------------|
//! | `Sync`       | [`SyncHandler`]                               |
//! | `Ephemeral`  | Logs warning (not yet wired)                  |
//! | `Keyhive`    | [`KeyhiveProtocolHandle`]                     |

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

/// Type alias for the concrete `SyncHandler` used in the CLI.
pub(crate) type CliSyncHandler = subduction_core::handler::sync::SyncHandler<
    Sendable,
    MetricsStorage<FsStorage>,
    CliConn,
    OpenPolicy,
    CountLeadingZeroBytes,
>;

/// Concrete `ListenError` for the CLI composed handler.
type CliListenError = ListenError<Sendable, MetricsStorage<FsStorage>, CliConn, CliWireMessage>;

/// Composed handler that dispatches [`CliWireMessage`] variants.
pub(crate) struct CliComposedHandler {
    pub(crate) sync: Arc<CliSyncHandler>,
    pub(crate) keyhive: KeyhiveProtocolHandle,
}

impl core::fmt::Debug for CliComposedHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CliComposedHandler").finish_non_exhaustive()
    }
}

impl Handler<Sendable, CliConn> for CliComposedHandler {
    type Message = CliWireMessage;
    type HandlerError = CliListenError;

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
