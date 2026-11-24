//! Subduction's keyhive sync protocol, run for the optional keyhive
//! passed into the `keyhive` constructor.

pub mod access;

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::fmt::Display;

use async_lock::{Mutex, OnceCell};
use future_form::{FutureForm, Local};
use futures::FutureExt;
use subduction_core::{authenticated::Authenticated, handler::Handler, peer::id::PeerId};
use subduction_keyhive::{
    KeyhiveMessage, KeyhivePeerId, KeyhiveProtocol, KeyhiveStorage, MessageOutcome, StorageHash,
    SyncMode,
    handler::{Connections, KeyhiveHandler, OutcomeObserver},
};
use wasm_bindgen::JsValue;

use crate::subduction::NodeConnection;
use access::{JsKeyhiveAccess, WasmKeyhive};

/// An error from the keyhive protocol or the connection it sends on, as a
/// message.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct KeyhiveError(String);

impl KeyhiveError {
    fn from_display(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

/// Stores and loads nothing.
///
/// The keyhive's event listener already reports every event it applies and
/// the JavaScript side persists from that listener.
#[derive(Debug, Clone, Copy)]
struct ListenerPersists;

impl KeyhiveStorage<Local> for ListenerPersists {
    type Error = KeyhiveError;

    fn save_archive(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_archives(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, KeyhiveError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_archive(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn save_event(
        &self,
        _hash: StorageHash,
        _data: Vec<u8>,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }

    fn load_events(
        &self,
    ) -> <Local as FutureForm>::Future<'_, Result<Vec<(StorageHash, Vec<u8>)>, KeyhiveError>> {
        async { Ok(Vec::new()) }.boxed_local()
    }

    fn delete_event(
        &self,
        _hash: StorageHash,
    ) -> <Local as FutureForm>::Future<'_, Result<(), KeyhiveError>> {
        async { Ok(()) }.boxed_local()
    }
}

/// Reports each frame's outcome to the `onKeyhiveOutcome` callback.
struct OutcomeCallback(Option<js_sys::Function>);

impl OutcomeObserver for OutcomeCallback {
    fn on_outcome(&self, peer: PeerId, outcome: MessageOutcome) {
        let Some(callback) = &self.0 else {
            return;
        };
        let peer = JsValue::from(sedimentree_wasm::pojo::hex(peer.as_bytes()));
        if let Err(e) = callback.call3(
            &JsValue::NULL,
            &peer,
            &JsValue::from_bool(outcome.ingested),
            &JsValue::from_bool(outcome.confirmed),
        ) {
            tracing::warn!("onKeyhiveOutcome callback error: {e:?}");
        }
    }
}

type BuiltinHandler =
    KeyhiveHandler<Local, WasmKeyhive, ListenerPersists, NodeConnection, OutcomeCallback>;

/// The keyhive sync protocol for a keyhive that JavaScript owns.
///
/// The handler and its protocol are created on first use since creating the
/// protocol reads the keyhive's contact card, which is asynchronous.
///
/// Nothing serializes calls JavaScript makes on the keyhive against calls
/// the protocol makes, so the two must not overlap.
pub(crate) struct KeyhiveSync {
    keyhive: WasmKeyhive,
    on_outcome: Option<js_sys::Function>,
    connections: Connections<NodeConnection, Local>,
    handler: OnceCell<BuiltinHandler>,
}

impl core::fmt::Debug for KeyhiveSync {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyhiveSync").finish_non_exhaustive()
    }
}

impl KeyhiveSync {
    pub(crate) fn new(
        access: JsKeyhiveAccess,
        on_outcome: Option<js_sys::Function>,
        connections: Connections<NodeConnection, Local>,
    ) -> Self {
        Self {
            keyhive: WasmKeyhive::from(access),
            on_outcome,
            connections,
            handler: OnceCell::new(),
        }
    }

    // The handler takes its protocol in an `Arc`, and wasm is single-threaded.
    #[allow(clippy::arc_with_non_send_sync)]
    async fn handler(&self) -> Result<&BuiltinHandler, KeyhiveError> {
        self.handler
            .get_or_try_init(|| async {
                let peer = KeyhivePeerId::from_bytes(
                    self.keyhive.id().map_err(KeyhiveError::from_display)?,
                );
                let contact_card = self
                    .keyhive
                    .contact_card()
                    .await
                    .map_err(KeyhiveError::from_display)?;
                let protocol = KeyhiveProtocol::new(
                    Arc::new(Mutex::new(self.keyhive.clone())),
                    ListenerPersists,
                    peer,
                    contact_card,
                );
                let handler = KeyhiveHandler::new(
                    Arc::new(protocol),
                    self.connections.clone(),
                    OutcomeCallback(self.on_outcome.clone()),
                );
                // Peers that connected earlier, when `add_peer` could not
                // create the handler.
                let connected: Vec<PeerId> =
                    self.connections.lock().await.keys().copied().collect();
                for peer in connected {
                    handler.add_peer(peer).await;
                }
                Ok(handler)
            })
            .await
    }

    /// Register a newly connected peer, creating the handler if this is the
    /// first.
    pub(crate) async fn add_peer(&self, peer: PeerId) -> Result<(), KeyhiveError> {
        self.handler().await?.add_peer(peer).await;
        Ok(())
    }

    /// Handle a keyhive frame and report its outcome to the
    /// `onKeyhiveOutcome` callback. Events can already be ingested when an
    /// error is returned, in which case the outcome is not reported.
    pub(crate) async fn handle_frame(
        &self,
        conn: &Authenticated<NodeConnection, Local>,
        message: KeyhiveMessage,
    ) -> Result<(), KeyhiveError> {
        Handler::<Local, NodeConnection>::handle(self.handler().await?, conn, message)
            .await
            .map_err(KeyhiveError::from_display)
    }

    /// Start a round with `peer`, as `mode` says.
    pub(crate) async fn sync_with_peer(
        &self,
        peer: PeerId,
        mode: SyncMode,
    ) -> Result<(), KeyhiveError> {
        self.handler()
            .await?
            .protocol()
            .sync_with_peer(&KeyhivePeerId::from_bytes(*peer.as_bytes()), mode)
            .await
            .map_err(KeyhiveError::from_display)
    }

    /// Rebuild the event cache if keyhive's op count has changed.
    pub(crate) async fn refresh_cache(&self) -> Result<(), KeyhiveError> {
        self.handler()
            .await?
            .protocol()
            .refresh_cache()
            .await
            .map_err(KeyhiveError::from_display)
    }

    /// Forget a disconnected peer, unless it has connected again since.
    pub(crate) async fn remove_peer(&self, peer: PeerId) {
        if let Some(handler) = self.handler.get() {
            handler.remove_peer(peer).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_errors_are_unwind_safe() {
        fn unwind_safe<T: core::panic::UnwindSafe + core::panic::RefUnwindSafe>() {}
        unwind_safe::<
            subduction_keyhive::KeyhiveProtocolError<
                WasmKeyhive,
                subduction_keyhive::handler::PeerConnections<NodeConnection, Local>,
                Local,
            >,
        >();
    }
}
