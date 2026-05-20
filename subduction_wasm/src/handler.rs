//! Composed handler that dispatches wire messages to sub-handlers,
//! forwarding keyhive-protocol frames to a registered JS handler.

use alloc::{boxed::Box, format, string::String, sync::Arc};

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
use subduction_keyhive::KeyhiveMessage;
use wasm_bindgen::prelude::*;

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

// ── JS FrameHandler interface ────────────────────────────────────────

#[wasm_bindgen(typescript_custom_section)]
const TS_FRAME_HANDLER: &str = r#"
/**
 * Handler for keyhive (SUK) protocol frames.
 *
 * Register via `Subduction.registerFrameHandler()`. The two methods
 * mirror the Rust `Handler` trait's `handle` / `on_peer_disconnect`.
 */
export interface FrameHandler {
    /** Called with the CBOR payload (no SUK envelope) for each inbound keyhive frame. */
    onMessage(payload: Uint8Array, peerId: PeerId): void;
    /** Called when a peer's last connection drops. */
    onPeerDisconnect(peerId: PeerId): void;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// A JS object implementing the `FrameHandler` interface.
    #[wasm_bindgen(js_name = FrameHandler, typescript_type = "FrameHandler")]
    pub type JsFrameHandler;

    #[wasm_bindgen(method, catch, js_name = onMessage)]
    fn js_on_message(
        this: &JsFrameHandler,
        payload: Uint8Array,
        peer_id: WasmPeerId,
    ) -> Result<(), JsValue>;

    #[wasm_bindgen(method, catch, js_name = onPeerDisconnect)]
    fn js_on_peer_disconnect(this: &JsFrameHandler, peer_id: WasmPeerId) -> Result<(), JsValue>;
}

impl core::fmt::Debug for JsFrameHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("JsFrameHandler").finish()
    }
}

impl Clone for JsFrameHandler {
    fn clone(&self) -> Self {
        JsCast::unchecked_into(JsValue::from(self).clone())
    }
}

// ── WasmKeyhiveHandler ───────────────────────────────────────────────

type WasmConn = MessageTransport<JsTransport>;

#[derive(Debug, thiserror::Error)]
#[error("keyhive frame handler error: {0}")]
pub(crate) struct WasmKeyhiveHandlerError(String);

/// Keyhive frame handler that forwards decoded SUK payloads to a
/// registered JS [`FrameHandler`](JsFrameHandler).
pub(crate) struct WasmKeyhiveHandler {
    handler: Arc<Mutex<Option<JsFrameHandler>>>,
}

impl core::fmt::Debug for WasmKeyhiveHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmKeyhiveHandler").finish_non_exhaustive()
    }
}

impl WasmKeyhiveHandler {
    pub(crate) fn new(handler: Arc<Mutex<Option<JsFrameHandler>>>) -> Self {
        Self { handler }
    }
}

impl Handler<Local, WasmConn> for WasmKeyhiveHandler {
    type Message = KeyhiveMessage;
    type HandlerError = WasmKeyhiveHandlerError;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<WasmConn, Local>,
        message: KeyhiveMessage,
    ) -> LocalBoxFuture<'a, Result<(), Self::HandlerError>> {
        Box::pin(async move {
            let guard = self.handler.lock().await;
            if let Some(ref handler) = *guard {
                let payload = Uint8Array::from(message.payload());
                let peer_id = WasmPeerId::from(conn.peer_id());
                handler
                    .js_on_message(payload, peer_id)
                    .map_err(|e| WasmKeyhiveHandlerError(format!("{e:?}")))?;
            }
            Ok(())
        })
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> LocalBoxFuture<'_, ()> {
        Box::pin(async move {
            let guard = self.handler.lock().await;
            if let Some(ref handler) = *guard {
                let peer_id = WasmPeerId::from(peer);
                if let Err(e) = handler.js_on_peer_disconnect(peer_id) {
                    tracing::error!("keyhive frame handler onPeerDisconnect error: {:?}", e);
                }
            }
        })
    }
}

// ── WasmComposedHandler ──────────────────────────────────────────────

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
    keyhive: WasmKeyhiveHandler,
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
        keyhive: WasmKeyhiveHandler,
    ) -> Self {
        Self {
            sync,
            ephemeral,
            keyhive,
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
            WireMessage::Ephemeral(_) | WireMessage::Keyhive(_) => None,
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
                WireMessage::Keyhive(msg) => {
                    if let Err(e) =
                        Handler::<Local, WasmConn>::handle(&self.keyhive, conn, msg).await
                    {
                        tracing::error!(error = %e, "keyhive handler error (non-fatal)");
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
            Handler::<Local, WasmConn>::on_peer_disconnect(&self.keyhive, peer).await;
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
