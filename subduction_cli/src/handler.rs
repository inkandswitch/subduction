//! Composed message handler for the CLI server.
//!
//! Dispatches [`CliWireMessage`] variants to the appropriate sub-handler:
//!
//! | Variant      | Handler                                         |
//! |--------------|-------------------------------------------------|
//! | `Sync`       | [`SyncHandler`]                                 |
//! | `Ephemeral`  | Logs warning (ephemeral not yet wired in CLI)   |
//! | `Keyhive`    | [`KeyhiveHandlerRelay`] → outbound via conn     |

use std::sync::Arc;

use future_form::Sendable;
use futures::future::BoxFuture;
use sedimentree_core::commit::CountLeadingZeroBytes;
use sedimentree_fs_storage::FsStorage;
use subduction_core::{
    connection::{Connection, authenticated::Authenticated, message::SyncMessage},
    handler::Handler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::metrics::MetricsStorage,
    subduction::error::{IoError, ListenError},
};
use subduction_keyhive_policy::relay::KeyhiveHandlerRelay;
use subduction_websocket::timeout::FuturesTimerTimeout;

use crate::{transport::UnifiedTransport, wire::CliWireMessage};

/// Type alias for the concrete `SyncHandler` used in the CLI.
type CliSyncHandler = subduction_core::handler::sync::SyncHandler<
    Sendable,
    MetricsStorage<FsStorage>,
    UnifiedTransport<FuturesTimerTimeout>,
    OpenPolicy,
    CountLeadingZeroBytes,
>;

/// Concrete `ListenError` for the CLI composed handler.
type CliListenError = ListenError<
    Sendable,
    MetricsStorage<FsStorage>,
    UnifiedTransport<FuturesTimerTimeout>,
    CliWireMessage,
>;

/// Composed handler that dispatches [`CliWireMessage`] variants to
/// sync and keyhive sub-handlers.
pub(crate) struct CliComposedHandler {
    pub(crate) sync: Arc<CliSyncHandler>,
    pub(crate) keyhive_relay: KeyhiveHandlerRelay,
}

impl core::fmt::Debug for CliComposedHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CliComposedHandler").finish_non_exhaustive()
    }
}

impl Handler<Sendable, UnifiedTransport<FuturesTimerTimeout>> for CliComposedHandler {
    type Message = CliWireMessage;
    type HandlerError = CliListenError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<UnifiedTransport<FuturesTimerTimeout>, Sendable>,
        message: CliWireMessage,
    ) -> BoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                CliWireMessage::Sync(sync_msg) => Handler::<
                    Sendable,
                    UnifiedTransport<FuturesTimerTimeout>,
                >::handle(
                    self.sync.as_ref(), conn, *sync_msg
                )
                .await
                .map_err(convert_sync_listen_error),

                CliWireMessage::Ephemeral(_) => {
                    tracing::warn!(
                        "received ephemeral message but ephemeral handler not configured in CLI"
                    );
                    Ok(())
                }

                CliWireMessage::Keyhive(wire_bytes) => {
                    let peer_id = conn.peer_id();
                    let outbound = self
                        .keyhive_relay
                        .handle_inbound(peer_id, wire_bytes)
                        .await
                        .map_err(|e| {
                            tracing::error!(error = %e, "keyhive relay error");
                            ListenError::TrySendError
                        })?;

                    for response_bytes in outbound {
                        let msg = CliWireMessage::Keyhive(response_bytes);
                        Connection::<Sendable, CliWireMessage>::send(conn.inner(), &msg)
                            .await
                            .map_err(IoError::ConnSend)
                            .map_err(ListenError::IoError)?;
                    }

                    Ok(())
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Sendable, UnifiedTransport<FuturesTimerTimeout>>::on_peer_disconnect(
                self.sync.as_ref(),
                peer,
            )
            .await;

            self.keyhive_relay.on_peer_disconnect(peer).await;
        })
    }
}

/// Convert a sync `ListenError<..., SyncMessage>` into `ListenError<..., CliWireMessage>`.
///
/// The concrete error variants are identical because `UnifiedTransport`
/// uses the same associated error types for both message parameterizations.
fn convert_sync_listen_error(
    err: ListenError<
        Sendable,
        MetricsStorage<FsStorage>,
        UnifiedTransport<FuturesTimerTimeout>,
        SyncMessage,
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
