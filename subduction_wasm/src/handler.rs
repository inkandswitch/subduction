//! Custom composed handler that dispatches wire messages to sub-handlers,
//! forwarding unknown-protocol frames to a registered JS callback.

use alloc::{boxed::Box, sync::Arc};

use async_lock::Mutex;
use future_form::Local;
use futures::future::LocalBoxFuture;
use js_sys::Uint8Array;
use sedimentree_core::id::SedimentreeId;
use subduction_core::{
    authenticated::Authenticated,
    connection::message::{BatchSyncResponse, SyncMessage},
    handler::{Handler, sync::SyncHandler},
    peer::id::PeerId,
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    subduction::error::{IoError, ListenError},
    transport::message::MessageTransport,
};
use subduction_ephemeral::handler::EphemeralHandler;
use wasm_bindgen::JsValue;

use crate::{
    clock::JsClock,
    peer_id::WasmPeerId,
    policy::{JsPolicy, ephemeral::JsEphemeralPolicy},
    remote_heads::JsRemoteHeadsObserver,
    subduction::{WASM_SHARD_COUNT, WasmHashMetric},
    transport::JsTransport,
    wire::WireMessage,
};
use sedimentree_wasm::storage::JsStorage;

type WasmConn = MessageTransport<JsTransport>;

type WasmSyncHandler = SyncHandler<
    Local,
    JsStorage,
    WasmConn,
    JsPolicy,
    WasmHashMetric,
    { WASM_SHARD_COUNT },
    JsRemoteHeadsObserver,
>;

type WasmEphemeralHandler = EphemeralHandler<Local, WasmConn, JsEphemeralPolicy, JsClock>;

pub(crate) type WasmListenError = ListenError<Local, JsStorage, WasmConn, WireMessage>;

pub(crate) struct WasmComposedHandler {
    sync: WasmSyncHandler,
    ephemeral: WasmEphemeralHandler,
    unknown_frame_callback: Arc<Mutex<Option<js_sys::Function>>>,
}

impl core::fmt::Debug for WasmComposedHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmComposedHandler")
            .finish_non_exhaustive()
    }
}

impl WasmComposedHandler {
    pub(crate) fn new(
        sync: WasmSyncHandler,
        ephemeral: WasmEphemeralHandler,
        unknown_frame_callback: Arc<Mutex<Option<js_sys::Function>>>,
    ) -> Self {
        Self {
            sync,
            ephemeral,
            unknown_frame_callback,
        }
    }
}

impl RemoteHeadsNotifier for WasmComposedHandler {
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.sync.notify_remote_heads(id, peer, heads);
    }
}

impl Handler<Local, WasmConn> for WasmComposedHandler {
    type Message = WireMessage;
    type HandlerError = WasmListenError;

    fn as_batch_sync_response(msg: &WireMessage) -> Option<&BatchSyncResponse> {
        match msg {
            WireMessage::Sync(sync_msg) => {
                if let SyncMessage::BatchSyncResponse(resp) = sync_msg.as_ref() {
                    Some(resp)
                } else {
                    None
                }
            }
            WireMessage::Ephemeral(_) | WireMessage::Unknown(_) => None,
        }
    }

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<WasmConn, Local>,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            match message {
                WireMessage::Sync(sync_msg) => {
                    Handler::<Local, WasmConn>::handle(&self.sync, conn, *sync_msg)
                        .await
                        .map_err(convert_sync_listen_error)
                }
                WireMessage::Ephemeral(eph_msg) => {
                    if let Err(e) =
                        Handler::<Local, WasmConn>::handle(&self.ephemeral, conn, eph_msg).await
                    {
                        tracing::error!(error = %e, "ephemeral handler error (non-fatal)");
                    }
                    Ok(())
                }
                WireMessage::Unknown(bytes) => {
                    let cb = self.unknown_frame_callback.lock().await.clone();
                    if let Some(callback) = cb.as_ref() {
                        let js_bytes = Uint8Array::from(bytes.as_slice());
                        let js_peer_id: JsValue = WasmPeerId::from(conn.peer_id()).into();
                        if let Err(e) = callback.call2(&JsValue::NULL, &js_bytes, &js_peer_id) {
                            tracing::error!("unknown frame callback error: {:?}", e);
                        }
                    }
                    Ok(())
                }
            }
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> LocalBoxFuture<'_, ()> {
        Box::pin(async move {
            Handler::<Local, WasmConn>::on_peer_disconnect(&self.sync, peer).await;
            Handler::<Local, WasmConn>::on_peer_disconnect(&self.ephemeral, peer).await;
        })
    }
}

fn convert_sync_listen_error(
    err: ListenError<Local, JsStorage, WasmConn, SyncMessage>,
) -> WasmListenError {
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
