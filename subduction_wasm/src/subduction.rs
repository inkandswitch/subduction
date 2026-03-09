//! Subduction node.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::{fmt::Debug, time::Duration};
use sedimentree_core::collections::{Map, Set};

use from_js_ref::FromJsRef;
use future_form::Local;
use futures::{
    FutureExt,
    future::{Either, select},
    stream::Aborted,
};
use js_sys::Uint8Array;
use sedimentree_core::{
    blob::Blob,
    commit::CountLeadingZeroBytes,
    crypto::digest::Digest,
    depth::{Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::Sedimentree,
};
#[cfg(not(feature = "ephemeral"))]
use subduction_core::subduction::builder::SubductionBuilder;
use subduction_core::{
    connection::{handshake::DiscoveryId, manager::Spawn, message::SyncMessage},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    subduction::{
        Subduction, error::HydrationError, pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    },
};
use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;

use crate::{
    connection::{
        JsConnection,
        longpoll::{WasmLongPoll, WasmLongPollConn},
        transport::{TransportCallError, WasmUnifiedTransport},
        websocket::WasmWebSocket,
    },
    error::{
        WasmAttachError, WasmConnectError, WasmDisconnectionError, WasmHydrationError, WasmIoError,
        WasmLongPollConnectError, WasmWriteError,
    },
    fragment::WasmFragmentRequested,
    peer_id::WasmPeerId,
    signer::JsSigner,
    sync_stats::WasmSyncStats,
};
use sedimentree_wasm::{
    depth::{JsToDepth, WasmDepth},
    digest::{JsDigest, WasmDigest},
    fragment::WasmFragment,
    loose_commit::WasmLooseCommit,
    sedimentree::WasmSedimentree,
    sedimentree_id::WasmSedimentreeId,
    storage::{JsStorage, JsStorageError},
};

use futures::{
    future::LocalBoxFuture,
    stream::{AbortHandle, Abortable},
};

#[cfg(feature = "ephemeral")]
use subduction_ephemeral::{
    composed::{ComposedError, ComposedHandler},
    config::{EphemeralConfig, EphemeralEvent},
    handler::{EphemeralHandler, EphemeralHandlerError},
    policy::OpenEphemeralPolicy,
    wire::WireMessage,
};

/// Number of shards for the sedimentree map in Wasm (smaller for client-side).
const WASM_SHARD_COUNT: usize = 4;

/// A spawner that uses wasm-bindgen-futures to spawn local tasks.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasmSpawn;

impl Spawn<Local> for WasmSpawn {
    fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
        let (handle, reg) = AbortHandle::new_pair();
        wasm_bindgen_futures::spawn_local(async move {
            let _ = Abortable::new(fut, reg).await;
        });
        handle
    }
}

// ---------------------------------------------------------------------------
// Wire message type alias — switches with ephemeral feature
// ---------------------------------------------------------------------------

/// The wire message type used by this node.
///
/// When the `ephemeral` feature is enabled, both sync and ephemeral
/// traffic are multiplexed through [`WireMessage`]. Otherwise,
/// only [`SyncMessage`] flows through the connection.
#[cfg(feature = "ephemeral")]
pub(crate) type WasmWireMessage = WireMessage;

#[cfg(not(feature = "ephemeral"))]
pub(crate) type WasmWireMessage = SyncMessage;

// ---------------------------------------------------------------------------
// Ephemeral handler wrapper (orphan-rule workaround)
// ---------------------------------------------------------------------------

/// Type alias for the concrete `SyncHandler` used in wasm.
#[cfg(feature = "ephemeral")]
type WasmSyncHandler = subduction_core::handler::sync::SyncHandler<
    Local,
    JsStorage,
    WasmUnifiedTransport,
    OpenPolicy,
    WasmHashMetric,
    WASM_SHARD_COUNT,
>;

/// Type alias for the concrete `EphemeralHandler` used in wasm.
#[cfg(feature = "ephemeral")]
type WasmEphemeralHandler = EphemeralHandler<Local, WasmUnifiedTransport, OpenEphemeralPolicy>;

/// Thin handler wrapper that delegates to [`ComposedHandler`] and
/// converts its [`ComposedError`] into [`ListenError<..., WireMessage>`].
///
/// This is necessary because of the orphan rule: we cannot implement
/// `Into<ListenError<..., WireMessage>>` for `ComposedError<..., ...>`
/// from either `subduction_core` or `subduction_ephemeral`, so we
/// produce the correct `HandlerError` type directly.
#[cfg(feature = "ephemeral")]
struct WasmComposedHandler {
    inner: ComposedHandler<WasmSyncHandler, WasmEphemeralHandler>,
}

#[cfg(feature = "ephemeral")]
impl core::fmt::Debug for WasmComposedHandler {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmComposedHandler")
            .finish_non_exhaustive()
    }
}

/// Concrete `ListenError` for the wasm ephemeral path.
#[cfg(feature = "ephemeral")]
type WasmListenError = subduction_core::subduction::error::ListenError<
    Local,
    JsStorage,
    WasmUnifiedTransport,
    WireMessage,
>;

#[cfg(feature = "ephemeral")]
impl subduction_core::handler::Handler<Local, WasmUnifiedTransport> for WasmComposedHandler {
    type Message = WireMessage;
    type HandlerError = WasmListenError;

    fn handle<'a>(
        &'a self,
        conn: &'a subduction_core::connection::authenticated::Authenticated<
            WasmUnifiedTransport,
            Local,
        >,
        message: WireMessage,
    ) -> LocalBoxFuture<'a, Result<(), Self::HandlerError>> {
        let fut = subduction_core::handler::Handler::<Local, WasmUnifiedTransport>::handle(
            &self.inner,
            conn,
            message,
        );
        async move { fut.await.map_err(convert_composed_error) }.boxed_local()
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> LocalBoxFuture<'_, ()> {
        subduction_core::handler::Handler::<Local, WasmUnifiedTransport>::on_peer_disconnect(
            &self.inner,
            peer,
        )
    }
}

/// Convert a [`ComposedError`] into a [`ListenError<..., WireMessage>`].
///
/// This is possible because all three `Connection<Local, *>` impls on
/// [`WasmUnifiedTransport`] share the same concrete error types.
#[cfg(feature = "ephemeral")]
#[allow(clippy::type_complexity)]
fn convert_composed_error(
    err: ComposedError<
        subduction_core::subduction::error::ListenError<
            Local,
            JsStorage,
            WasmUnifiedTransport,
            SyncMessage,
        >,
        EphemeralHandlerError<
            <WasmUnifiedTransport as subduction_core::connection::Connection<
                Local,
                subduction_ephemeral::message::EphemeralMessage,
            >>::SendError,
        >,
    >,
) -> WasmListenError {
    match err {
        ComposedError::Sync(listen_err) => convert_sync_listen_error(listen_err),
        ComposedError::Ephemeral(eph_err) => convert_ephemeral_error(eph_err),
    }
}

/// Convert a sync `ListenError<..., SyncMessage>` into `ListenError<..., WireMessage>`.
///
/// The concrete error variants are identical because `WasmUnifiedTransport`
/// uses the same associated error types for all message parameterizations.
#[cfg(feature = "ephemeral")]
fn convert_sync_listen_error(
    err: subduction_core::subduction::error::ListenError<
        Local,
        JsStorage,
        WasmUnifiedTransport,
        SyncMessage,
    >,
) -> WasmListenError {
    use subduction_core::subduction::error::{IoError, ListenError};

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

/// Convert an [`EphemeralHandlerError`] into `ListenError<..., WireMessage>`.
#[cfg(feature = "ephemeral")]
fn convert_ephemeral_error(
    err: EphemeralHandlerError<
        <WasmUnifiedTransport as subduction_core::connection::Connection<
            Local,
            subduction_ephemeral::message::EphemeralMessage,
        >>::SendError,
    >,
) -> WasmListenError {
    use subduction_core::subduction::error::{IoError, ListenError};

    match err {
        EphemeralHandlerError::Send(send_err) => ListenError::IoError(IoError::ConnSend(send_err)),
    }
}

// ---------------------------------------------------------------------------
// WasmSubduction
// ---------------------------------------------------------------------------

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
pub struct WasmSubduction {
    core: Arc<
        Subduction<
            'static,
            Local,
            JsStorage,
            WasmUnifiedTransport,
            WasmWireMessage,
            OpenPolicy,
            JsSigner,
            WasmHashMetric,
            WASM_SHARD_COUNT,
        >,
    >,

    /// Reference to the JS storage object for callback registration.
    js_storage: JsValue,

    /// The ephemeral handler, for calling `publish()`.
    #[cfg(feature = "ephemeral")]
    ephemeral_handler: Arc<WasmEphemeralHandler>,

    /// Receiver for inbound ephemeral events from peers.
    #[cfg(feature = "ephemeral")]
    ephemeral_rx: async_channel::Receiver<EphemeralEvent>,
}

impl core::fmt::Debug for WasmSubduction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut d = f.debug_struct("WasmSubduction");
        d.field("js_storage", &self.js_storage);
        #[cfg(feature = "ephemeral")]
        d.field("ephemeral", &true);
        d.finish_non_exhaustive()
    }
}

#[wasm_bindgen(js_class = Subduction)]
impl WasmSubduction {
    /// Create a new [`Subduction`] instance.
    ///
    /// # Arguments
    ///
    /// * `signer` - The cryptographic signer for this node's identity
    /// * `storage` - Storage backend for persisting data
    /// * `service_name` - Optional service identifier for discovery mode (e.g., `sync.example.com`).
    ///   When set, clients can connect without knowing the server's peer ID.
    /// * `hash_metric_override` - Optional custom depth metric function
    /// * `max_pending_blob_requests` - Optional maximum number of pending blob requests (default: 10,000)
    ///
    /// # Panics
    ///
    /// Panics if `hash_metric_override` is `Some` but the underlying JS value
    /// cannot be cast to a `Function`.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(
        signer: JsSigner,
        storage: JsStorage,
        service_name: Option<String>,
        hash_metric_override: Option<JsToDepth>,
        max_pending_blob_requests: Option<usize>,
    ) -> Self {
        tracing::debug!("new Subduction node");
        let js_storage = <JsStorage as AsRef<JsValue>>::as_ref(&storage).clone();
        #[allow(clippy::expect_used)]
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(|h| {
            JsValue::from(h)
                .dyn_into()
                .expect("hash_metric_override is not a Function")
        });
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));
        let depth_metric = WasmHashMetric(raw_fn);
        let max_pending = max_pending_blob_requests.unwrap_or(DEFAULT_MAX_PENDING_BLOB_REQUESTS);

        #[cfg(not(feature = "ephemeral"))]
        {
            let mut builder = SubductionBuilder::<_, _, _, _, WASM_SHARD_COUNT>::new()
                .signer(signer)
                .storage(storage, Arc::new(OpenPolicy))
                .spawner(WasmSpawn)
                .depth_metric(depth_metric)
                .max_pending_blob_requests(max_pending);

            if let Some(id) = discovery_id {
                builder = builder.discovery_id(id);
            }

            let (core, _handler, listener_fut, manager_fut) = builder.build();

            spawn_subduction_tasks(listener_fut, manager_fut);

            Self { core, js_storage }
        }

        #[cfg(feature = "ephemeral")]
        {
            let (core, ephemeral_handler, ephemeral_rx, listener_fut, manager_fut) =
                build_ephemeral(
                    signer,
                    storage,
                    discovery_id,
                    depth_metric,
                    max_pending,
                    None,
                );

            spawn_subduction_tasks(listener_fut, manager_fut);

            Self {
                core,
                js_storage,
                ephemeral_handler,
                ephemeral_rx,
            }
        }
    }

    /// Hydrate a [`Subduction`] instance from external storage.
    ///
    /// Loads all sedimentree data from storage and reconstructs the in-memory
    /// state before initializing the sync engine.
    ///
    /// # Arguments
    ///
    /// * `signer` - The cryptographic signer for this node's identity
    /// * `storage` - Storage backend for persisting data
    /// * `service_name` - Optional service identifier for discovery mode (e.g., `sync.example.com`).
    ///   When set, clients can connect without knowing the server's peer ID.
    /// * `hash_metric_override` - Optional custom depth metric function
    /// * `max_pending_blob_requests` - Optional maximum number of pending blob requests (default: 10,000)
    ///
    /// # Panics
    ///
    /// Panics if `hash_metric_override` is `Some` but the underlying JS value
    /// cannot be cast to a `Function`.
    ///
    /// # Errors
    ///
    /// Returns [`WasmHydrationError`] if hydration fails.
    #[wasm_bindgen]
    pub async fn hydrate(
        signer: JsSigner,
        storage: JsStorage,
        service_name: Option<String>,
        hash_metric_override: Option<JsToDepth>,
        max_pending_blob_requests: Option<usize>,
    ) -> Result<Self, WasmHydrationError> {
        use subduction_core::storage::traits::Storage as _;

        tracing::debug!("hydrating new Subduction node");
        let js_storage = <JsStorage as AsRef<JsValue>>::as_ref(&storage).clone();
        #[allow(clippy::expect_used)]
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(|h| {
            JsValue::from(h)
                .dyn_into()
                .expect("hash_metric_override is not a Function")
        });
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));
        let depth_metric = WasmHashMetric(raw_fn);
        let max_pending = max_pending_blob_requests.unwrap_or(DEFAULT_MAX_PENDING_BLOB_REQUESTS);

        // Load sedimentree IDs from raw storage before wrapping in powerbox
        let ids: Set<SedimentreeId> = storage
            .load_all_sedimentree_ids()
            .await
            .map_err(HydrationError::LoadAllIdsError)?;

        // Hydrate sedimentrees from storage
        let sedimentrees = Arc::new(ShardedMap::new());
        for id in ids {
            let commits = storage
                .load_loose_commits(id)
                .await
                .map_err(HydrationError::LoadLooseCommitsError)?;
            let fragments = storage
                .load_fragments(id)
                .await
                .map_err(HydrationError::LoadFragmentsError)?;

            let loose_commits: Vec<_> = commits.into_iter().map(|v| v.payload().clone()).collect();
            let fragments: Vec<_> = fragments.into_iter().map(|v| v.payload().clone()).collect();

            let sedimentree = Sedimentree::new(fragments, loose_commits);
            sedimentrees
                .with_entry_or_default(id, |tree: &mut Sedimentree| tree.merge(sedimentree))
                .await;
            sedimentrees
                .with_entry(&id, |tree| {
                    *tree = tree.minimize(&depth_metric);
                })
                .await;
        }

        #[cfg(not(feature = "ephemeral"))]
        {
            let mut builder = SubductionBuilder::<_, _, _, _, WASM_SHARD_COUNT>::new()
                .signer(signer)
                .storage(storage, Arc::new(OpenPolicy))
                .spawner(WasmSpawn)
                .depth_metric(depth_metric)
                .max_pending_blob_requests(max_pending)
                .sedimentrees(sedimentrees);

            if let Some(id) = discovery_id {
                builder = builder.discovery_id(id);
            }

            let (core, _handler, listener_fut, manager_fut) = builder.build();

            spawn_subduction_tasks(listener_fut, manager_fut);

            Ok(Self { core, js_storage })
        }

        #[cfg(feature = "ephemeral")]
        {
            let (core, ephemeral_handler, ephemeral_rx, listener_fut, manager_fut) =
                build_ephemeral(
                    signer,
                    storage,
                    discovery_id,
                    depth_metric,
                    max_pending,
                    Some(sedimentrees),
                );

            spawn_subduction_tasks(listener_fut, manager_fut);

            Ok(Self {
                core,
                js_storage,
                ephemeral_handler,
                ephemeral_rx,
            })
        }
    }

    /// Add a Sedimentree.
    ///
    /// # Errors
    ///
    /// Returns [`WasmWriteError`] if there is a problem with storage, networking, or policy.
    #[wasm_bindgen(js_name = addSedimentree)]
    pub async fn add_sedimentree(
        &self,
        id: &WasmSedimentreeId,
        sedimentree: &WasmSedimentree,
        blobs: Vec<Uint8Array>,
    ) -> Result<(), WasmWriteError> {
        self.core
            .add_sedimentree(
                id.clone().into(),
                sedimentree.clone().into(),
                blobs
                    .into_iter()
                    .map(|bytes| bytes.to_vec().into())
                    .collect(),
            )
            .await?;
        Ok(())
    }

    /// Remove a Sedimentree and all associated data.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = removeSedimentree)]
    pub async fn remove_sedimentree(&self, id: &WasmSedimentreeId) -> Result<(), WasmIoError> {
        self.core.remove_sedimentree(id.clone().into()).await?;
        Ok(())
    }

    /// Connect to a peer via WebSocket and register the connection.
    ///
    /// This performs the cryptographic handshake, verifies the server's identity,
    /// and registers the authenticated connection for syncing.
    ///
    /// Returns the verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `signer` - The client's signer for authentication  
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or registration fails.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect(
        &self,
        address: &web_sys::Url,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: u32,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated = WasmWebSocket::connect_authenticated(
            address,
            signer,
            expected_peer_id,
            timeout_milliseconds,
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .register(authenticated.map(WasmUnifiedTransport::WebSocket))
            .await?;
        Ok(peer_id.into())
    }

    /// Connect to a peer via WebSocket using discovery mode and register the connection.
    ///
    /// Returns the discovered and verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `signer` - The client's signer for authentication
    /// * `timeout_milliseconds` - Request timeout in milliseconds (defaults to 30000)
    /// * `service_name` - The service name for discovery (defaults to URL host)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or registration fails.
    #[wasm_bindgen(js_name = connectDiscover)]
    pub async fn connect_discover(
        &self,
        address: &web_sys::Url,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated = WasmWebSocket::connect_discover_authenticated(
            address,
            signer,
            timeout_milliseconds,
            service_name,
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .register(authenticated.map(WasmUnifiedTransport::WebSocket))
            .await?;
        Ok(peer_id.into())
    }

    /// Connect to a peer via HTTP long-poll and register the connection.
    ///
    /// Returns the verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `signer` - The client's signer for authentication
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or registration fails.
    #[wasm_bindgen(js_name = connectLongPoll)]
    pub async fn connect_long_poll(
        &self,
        base_url: &str,
        signer: &JsSigner,
        expected_peer_id: &WasmPeerId,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerId, WasmLongPollConnectError> {
        let (authenticated, _session_id) = WasmLongPoll::connect_authenticated(
            base_url,
            signer,
            expected_peer_id,
            timeout_milliseconds.unwrap_or(30_000),
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .register(authenticated.map(WasmUnifiedTransport::LongPoll))
            .await?;
        Ok(peer_id.into())
    }

    /// Connect to a peer via HTTP long-poll using discovery mode.
    ///
    /// Returns the discovered and verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `signer` - The client's signer for authentication
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    /// * `service_name` - The service name for discovery (defaults to `base_url`)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or registration fails.
    #[wasm_bindgen(js_name = connectDiscoverLongPoll)]
    pub async fn connect_discover_long_poll(
        &self,
        base_url: &str,
        signer: &JsSigner,
        timeout_milliseconds: Option<u32>,
        service_name: Option<String>,
    ) -> Result<WasmPeerId, WasmLongPollConnectError> {
        let (authenticated, _session_id) = WasmLongPoll::connect_discover_authenticated(
            base_url,
            signer,
            timeout_milliseconds,
            service_name,
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .register(authenticated.map(WasmUnifiedTransport::LongPoll))
            .await?;
        Ok(peer_id.into())
    }

    /// Disconnect from all peers.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDisconnectionError`] if disconnection was not graceful.
    #[wasm_bindgen(js_name = disconnectAll)]
    pub async fn disconnect_all(&self) -> Result<(), WasmDisconnectionError> {
        Ok(self.core.disconnect_all().await?)
    }

    /// Disconnect from a peer by its ID.
    ///
    /// # Errors
    ///
    /// Returns a `WasmDisconnectionError` if disconnection fails.
    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub async fn disconnect_from_peer(
        &self,
        peer_id: &WasmPeerId,
    ) -> Result<bool, WasmDisconnectionError> {
        Ok(self
            .core
            .disconnect_from_peer(&peer_id.clone().into())
            .await?)
    }

    /// Attach an authenticated WebSocket connection and sync all sedimentrees.
    ///
    /// The connection must have been authenticated via [`SubductionWebSocket::setup`],
    /// [`SubductionWebSocket::tryConnect`], or [`SubductionWebSocket::tryDiscover`].
    ///
    /// Returns `true` if this is a new peer, `false` if already connected.
    ///
    /// # Errors
    ///
    /// Returns an error if registration or sync fails.
    #[wasm_bindgen]
    pub async fn attach(
        &self,
        conn: &crate::connection::websocket::WasmAuthenticatedWebSocket,
    ) -> Result<bool, WasmAttachError> {
        self.core
            .attach(conn.inner().clone().map(WasmUnifiedTransport::WebSocket))
            .await
            .map_err(Into::into)
    }

    /// Get a local blob by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`JsStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getBlob)]
    pub async fn get_blob(
        &self,
        id: &WasmSedimentreeId,
        digest: &WasmDigest,
    ) -> Result<Option<Uint8Array>, JsStorageError> {
        Ok(self
            .core
            .get_blob(id.clone().into(), digest.clone().into())
            .await?
            .map(|blob| Uint8Array::from(blob.as_slice())))
    }

    /// Get all local blobs for a given Sedimentree ID.
    ///
    /// # Errors
    ///
    /// Returns a [`JsStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getBlobs)]
    pub async fn get_blobs(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<Vec<Uint8Array>, JsStorageError> {
        #[allow(clippy::expect_used)]
        if let Some(blobs) = self.core.get_blobs(id.clone().into()).await? {
            Ok(blobs
                .into_iter()
                .map(|blob| Uint8Array::from(blob.as_slice()))
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Fetch blobs by their digests, with an optional timeout in milliseconds.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = fetchBlobs)]
    pub async fn fetch_blobs(
        &self,
        id: &WasmSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<Option<Vec<Uint8Array>>, WasmIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        if let Some(blobs) = self
            .core
            .fetch_blobs(id.clone().into(), timeout)
            .await
            .map_err(WasmIoError::from)?
        {
            Ok(Some(
                blobs
                    .into_iter()
                    .map(|blob| Uint8Array::from(blob.as_slice()))
                    .collect(),
            ))
        } else {
            Ok(None)
        }
    }

    /// Add a commit with its associated blob to the storage.
    ///
    /// The commit metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if storage, networking, or policy fail.
    #[wasm_bindgen(js_name = addCommit)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn add_commit(
        &self,
        id: &WasmSedimentreeId,
        parents: Vec<JsDigest>,
        blob: &Uint8Array,
    ) -> Result<Option<WasmFragmentRequested>, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_parents: BTreeSet<Digest<LooseCommit>> =
            parents.iter().map(|d| WasmDigest::from(d).into()).collect();
        let blob: Blob = blob.clone().to_vec().into();

        let maybe_fragment_requested = self.core.add_commit(core_id, core_parents, blob).await?;

        Ok(maybe_fragment_requested.map(WasmFragmentRequested::from))
    }

    /// Add a fragment with its associated blob to the storage.
    ///
    /// The fragment metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if storage, networking, or policy fail.
    #[wasm_bindgen(js_name = addFragment)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn add_fragment(
        &self,
        id: &WasmSedimentreeId,
        head: &WasmDigest,
        boundary: Vec<JsDigest>,
        checkpoints: Vec<JsDigest>,
        blob: &Uint8Array,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_head: Digest<LooseCommit> = head.clone().into();
        let core_boundary = boundary
            .iter()
            .map(|d| WasmDigest::from(d).into())
            .collect();
        let core_checkpoints: Vec<Digest<LooseCommit>> = checkpoints
            .iter()
            .map(|d| WasmDigest::from(d).into())
            .collect();
        let blob: Blob = blob.clone().to_vec().into();

        self.core
            .add_fragment(core_id, core_head, core_boundary, &core_checkpoints, blob)
            .await?;

        Ok(())
    }

    /// Request blobs by their digests from connected peers for a specific sedimentree.
    #[wasm_bindgen(js_name = requestBlobs)]
    pub async fn request_blobs(&self, id: &WasmSedimentreeId, digests: Vec<JsDigest>) {
        let digests: Vec<_> = digests
            .iter()
            .map(|js_digest| WasmDigest::from(js_digest).into())
            .collect();
        self.core.request_blobs(id.clone().into(), digests).await;
    }

    /// Request batch sync for a given Sedimentree ID from a specific peer.
    ///
    /// # Arguments
    ///
    /// * `to_ask` - The peer ID to sync with
    /// * `id` - The sedimentree ID to sync
    /// * `subscribe` - Whether to subscribe for incremental updates
    /// * `timeout_milliseconds` - Optional timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = syncWithPeer)]
    pub async fn sync_with_peer(
        &self,
        to_ask: &WasmPeerId,
        id: &WasmSedimentreeId,
        subscribe: bool,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerBatchSyncResult, WasmIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, stats, conn_errors) = self
            .core
            .sync_with_peer(
                &to_ask.clone().into(),
                id.clone().into(),
                subscribe,
                timeout,
            )
            .await
            .map_err(WasmIoError::from)?;

        Ok(PeerBatchSyncResult {
            success,
            stats: stats.into(),
            conn_errors: conn_errors
                .into_iter()
                .map(|(conn, err)| ConnErrPair {
                    conn: to_js_connection(conn.into_inner()),
                    err: WasmCallError::from(err),
                })
                .collect(),
        })
    }

    /// Request batch sync for a given Sedimentree ID from all connected peers.
    ///
    /// # Arguments
    ///
    /// * `id` - The sedimentree ID to sync
    /// * `subscribe` - Whether to subscribe for incremental updates
    /// * `timeout_milliseconds` - Optional timeout in milliseconds
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = syncAll)]
    pub async fn sync_all(
        &self,
        id: &WasmSedimentreeId,
        subscribe: bool,
        timeout_milliseconds: Option<u64>,
    ) -> Result<WasmPeerResultMap, WasmIoError> {
        tracing::debug!("WasmSubduction::sync_all");
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let peer_map = self
            .core
            .sync_all(id.clone().into(), subscribe, timeout)
            .await?;
        tracing::debug!("WasmSubduction::sync_all - done");
        Ok(WasmPeerResultMap(
            peer_map
                .into_iter()
                .map(|(peer_id, (success, stats, conn_errs))| {
                    (
                        peer_id,
                        (
                            success,
                            stats.into(),
                            conn_errs
                                .into_iter()
                                .map(|(conn, err)| {
                                    (
                                        to_js_connection(conn.into_inner()),
                                        WasmCallError::from(err),
                                    )
                                })
                                .collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect(),
        ))
    }

    /// Request batch sync for all known Sedimentree IDs from all connected peers.
    #[wasm_bindgen(js_name = fullSync)]
    pub async fn full_sync(&self, timeout_milliseconds: Option<u64>) -> PeerBatchSyncResult {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, stats, conn_errs, io_errs) = self.core.full_sync(timeout).await;

        for (id, err) in &io_errs {
            tracing::error!("full_sync I/O error for sedimentree {:?}: {}", id, err);
        }

        PeerBatchSyncResult {
            success,
            stats: stats.into(),
            conn_errors: conn_errs
                .into_iter()
                .map(|(conn, err)| ConnErrPair {
                    conn: to_js_connection(conn.into_inner()),
                    err: WasmCallError::from(err),
                })
                .collect(),
        }
    }

    /// Get all known Sedimentree IDs
    #[wasm_bindgen(js_name = sedimentreeIds)]
    pub async fn sedimentree_ids(&self) -> Vec<WasmSedimentreeId> {
        self.core
            .sedimentree_ids()
            .await
            .into_iter()
            .map(WasmSedimentreeId::from)
            .collect()
    }

    /// Get all commits for a given Sedimentree ID
    #[must_use]
    #[wasm_bindgen(js_name = getCommits)]
    pub async fn get_commits(&self, id: &WasmSedimentreeId) -> Option<Vec<WasmLooseCommit>> {
        self.core
            .get_commits(id.clone().into())
            .await
            .map(|commits| commits.into_iter().map(WasmLooseCommit::from).collect())
    }

    /// Get all fragments for a given Sedimentree ID
    #[must_use]
    #[wasm_bindgen(js_name = getFragments)]
    pub async fn get_fragments(&self, id: &WasmSedimentreeId) -> Option<Vec<WasmFragment>> {
        self.core
            .get_fragments(id.clone().into())
            .await
            .map(|fragments| fragments.into_iter().map(WasmFragment::from).collect())
    }

    /// Get the peer IDs of all connected peers.
    #[wasm_bindgen(js_name = getConnectedPeerIds)]
    pub async fn connected_peer_ids(&self) -> Vec<WasmPeerId> {
        self.core
            .connected_peer_ids()
            .await
            .into_iter()
            .map(WasmPeerId::from)
            .collect()
    }

    /// Get the backing storage.
    #[must_use]
    #[wasm_bindgen(getter, js_name = storage)]
    pub fn storage(&self) -> JsValue {
        self.js_storage.clone()
    }

    /// Register a callback for incoming ephemeral messages.
    ///
    /// The callback receives `(sedimentreeId: Uint8Array, senderPeerId: Uint8Array, payload: Uint8Array)`
    /// for every ephemeral message delivered by connected peers.
    ///
    /// Only one callback can be active at a time. Calling `onEphemeral`
    /// again replaces the previous drain loop.
    ///
    /// Requires the `ephemeral` feature (enabled by default).
    #[cfg(feature = "ephemeral")]
    #[wasm_bindgen(js_name = onEphemeral)]
    pub fn on_ephemeral(&self, callback: js_sys::Function) {
        let rx = self.ephemeral_rx.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Ok(event) = rx.recv().await {
                let id_bytes = Uint8Array::from(event.id.as_bytes().as_slice());
                let sender_bytes = Uint8Array::from(event.sender.as_bytes().as_slice());
                let payload_bytes = Uint8Array::from(event.payload.as_slice());

                if let Err(e) = callback.call3(
                    &JsValue::NULL,
                    &id_bytes.into(),
                    &sender_bytes.into(),
                    &payload_bytes.into(),
                ) {
                    tracing::warn!("onEphemeral callback threw: {:?}", e);
                }
            }
            tracing::debug!("onEphemeral drain loop ended (channel closed)");
        });
    }

    /// Publish an ephemeral message to all subscribers of a sedimentree.
    ///
    /// The message is _not_ persisted — it is delivered to connected
    /// peers that have subscribed to `id` via the ephemeral protocol.
    ///
    /// Requires the `ephemeral` feature (enabled by default).
    ///
    /// # Arguments
    ///
    /// * `id` - The sedimentree ID / topic to publish to
    /// * `payload` - The opaque payload bytes
    #[cfg(feature = "ephemeral")]
    #[wasm_bindgen(js_name = publishEphemeral)]
    pub async fn publish_ephemeral(&self, id: &WasmSedimentreeId, payload: &Uint8Array) {
        self.ephemeral_handler
            .publish(id.clone().into(), payload.to_vec())
            .await;
    }
}

// ---------------------------------------------------------------------------
// Spawn helper (shared by both cfg paths)
// ---------------------------------------------------------------------------

/// Type alias for the `Subduction` core when the ephemeral feature is on.
#[cfg(feature = "ephemeral")]
type WasmSubductionCore = Subduction<
    'static,
    Local,
    JsStorage,
    WasmUnifiedTransport,
    WireMessage,
    OpenPolicy,
    JsSigner,
    WasmHashMetric,
    WASM_SHARD_COUNT,
>;

/// Spawn the listener and connection manager futures.
fn spawn_subduction_tasks<L, M>(listener_fut: L, manager_fut: M)
where
    L: core::future::Future<Output = Result<(), Aborted>> + Unpin + 'static,
    M: core::future::Future<Output = Result<(), Aborted>> + Unpin + 'static,
{
    wasm_bindgen_futures::spawn_local(async move {
        let manager = manager_fut.fuse();
        let listener = listener_fut.fuse();

        match select(manager, listener).await {
            Either::Left((manager_result, _pin)) => {
                if let Err(Aborted) = manager_result {
                    tracing::error!("Subduction manager aborted");
                }
            }
            Either::Right((listener_result, _pin)) => {
                if let Err(Aborted) = listener_result {
                    tracing::error!("Subduction listener aborted");
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Ephemeral builder (manual Subduction::new with ComposedHandler)
// ---------------------------------------------------------------------------

/// Build a [`Subduction`] instance wired up with a [`ComposedHandler`]
/// that routes sync and ephemeral traffic.
///
/// Returns the core, ephemeral handler, ephemeral event receiver, and the
/// two background task futures.
#[cfg(feature = "ephemeral")]
#[allow(clippy::type_complexity)]
fn build_ephemeral(
    signer: JsSigner,
    storage: JsStorage,
    discovery_id: Option<DiscoveryId>,
    depth_metric: WasmHashMetric,
    max_pending: usize,
    sedimentrees: Option<Arc<ShardedMap<SedimentreeId, Sedimentree, WASM_SHARD_COUNT>>>,
) -> (
    Arc<WasmSubductionCore>,
    Arc<WasmEphemeralHandler>,
    async_channel::Receiver<EphemeralEvent>,
    subduction_core::subduction::ListenerFuture<
        'static,
        Local,
        JsStorage,
        WasmUnifiedTransport,
        WireMessage,
        OpenPolicy,
        JsSigner,
        WasmHashMetric,
        WASM_SHARD_COUNT,
    >,
    subduction_core::connection::manager::ManagerFuture<Local>,
) {
    use async_lock::Mutex;
    use nonempty::NonEmpty;
    use subduction_core::{
        connection::{authenticated::Authenticated, nonce_cache::NonceCache},
        handler::sync::SyncHandler,
        storage::powerbox::StoragePowerbox,
        subduction::pending_blob_requests::PendingBlobRequests,
    };

    let sedimentrees = sedimentrees.unwrap_or_else(|| Arc::new(ShardedMap::new()));

    let connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<WasmUnifiedTransport, Local>>>>> =
        Arc::new(Mutex::new(Map::new()));
    let subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>> =
        Arc::new(Mutex::new(Map::new()));
    let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(max_pending)));
    let nonce_cache = NonceCache::default();

    let powerbox = StoragePowerbox::new(storage, Arc::new(OpenPolicy));

    // Build sync sub-handler
    let sync_handler = Arc::new(SyncHandler::new(
        sedimentrees.clone(),
        connections.clone(),
        subscriptions.clone(),
        powerbox.clone(),
        pending_blob_requests.clone(),
        depth_metric.clone(),
    ));

    // Build ephemeral sub-handler
    let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
        connections.clone(),
        OpenEphemeralPolicy,
        EphemeralConfig::default(),
    );
    let ephemeral_handler = Arc::new(ephemeral_handler);

    // Compose into a single handler
    let composed = ComposedHandler::new(sync_handler, ephemeral_handler.clone());
    let handler = Arc::new(WasmComposedHandler { inner: composed });

    // Build Subduction manually (same as build_with_handler, but we
    // constructed the connections map ourselves so both handlers share it).
    let (core, listener_fut, manager_fut) = Subduction::new(
        handler,
        discovery_id,
        signer,
        sedimentrees,
        connections,
        subscriptions,
        powerbox,
        pending_blob_requests,
        nonce_cache,
        depth_metric,
        WasmSpawn,
    );

    (
        core,
        ephemeral_handler,
        ephemeral_rx,
        listener_fut,
        manager_fut,
    )
}

/// Result of a peer batch sync request.
#[wasm_bindgen(js_name = PeerBatchSyncResult)]
#[derive(Debug)]
pub struct PeerBatchSyncResult {
    success: bool,
    stats: WasmSyncStats,
    conn_errors: Vec<ConnErrPair>,
}

#[wasm_bindgen(js_class = PeerBatchSyncResult)]
impl PeerBatchSyncResult {
    /// Whether the batch sync was successful with at least one connection.
    #[must_use]
    #[wasm_bindgen(getter)]
    #[allow(clippy::missing_const_for_fn)]
    pub fn success(&self) -> bool {
        self.success
    }

    /// Statistics about the sync operation.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn stats(&self) -> WasmSyncStats {
        self.stats
    }

    /// List of connection errors that occurred during the batch sync.
    #[must_use]
    #[wasm_bindgen(getter, js_name = connErrors)]
    pub fn conn_errors(&self) -> Vec<ConnErrPair> {
        self.conn_errors.clone()
    }
}

/// Convert a [`WasmUnifiedTransport`] to a [`JsConnection`] for the JS boundary.
///
/// Both `SubductionWebSocket` and `SubductionLongPollConnection` satisfy the
/// `Connection` TypeScript interface, so the cast is safe by construction.
fn to_js_connection(transport: WasmUnifiedTransport) -> JsConnection {
    match transport {
        WasmUnifiedTransport::WebSocket(ws) => JsValue::from(ws).unchecked_into(),
        WasmUnifiedTransport::LongPoll(lp) => {
            JsValue::from(WasmLongPollConn::new(lp)).unchecked_into()
        }
    }
}

/// A pair of a connection and an error that occurred during a call.
#[wasm_bindgen(js_name = ConnErrorPair)]
#[derive(Debug, Clone)]
pub struct ConnErrPair {
    conn: JsConnection,
    err: WasmCallError,
}

#[wasm_bindgen(js_class = ConnErrorPair)]
impl ConnErrPair {
    /// The connection that encountered the error.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn conn(&self) -> JsConnection {
        self.conn.clone()
    }

    /// The error that occurred during the call.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn err(&self) -> js_sys::Error {
        self.err.clone().into()
    }
}

/// Map of peer IDs to their batch sync results.
#[wasm_bindgen(js_name = PeerResultMap)]
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct WasmPeerResultMap(
    Map<PeerId, (bool, WasmSyncStats, Vec<(JsConnection, WasmCallError)>)>,
);

#[wasm_bindgen(js_class = PeerResultMap)]
impl WasmPeerResultMap {
    /// Get the result for a specific peer ID.
    #[must_use]
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self, peer_id: &WasmPeerId) -> Option<PeerBatchSyncResult> {
        self.0
            .get(&peer_id.clone().into())
            .map(|(success, stats, conn_errs)| PeerBatchSyncResult {
                success: *success,
                stats: *stats,
                conn_errors: conn_errs
                    .iter()
                    .map(|(conn, err)| ConnErrPair {
                        conn: conn.clone(),
                        err: err.clone(),
                    })
                    .collect(),
            })
    }

    /// Get all entries in the peer result map.
    #[must_use]
    pub fn entries(&self) -> Vec<PeerBatchSyncResult> {
        let mut results = Vec::with_capacity(self.0.len());
        for (success, stats, conn_errs) in self.0.values() {
            results.push(PeerBatchSyncResult {
                success: *success,
                stats: *stats,
                conn_errors: conn_errs
                    .iter()
                    .map(|(conn, err)| ConnErrPair {
                        conn: conn.clone(),
                        err: err.clone(),
                    })
                    .collect(),
            });
        }
        results
    }
}

/// An overridable hash metric.
#[derive(Debug, Clone)]
#[wasm_bindgen(js_name = HashMetric)]
pub struct WasmHashMetric(Option<js_sys::Function>);

#[wasm_bindgen(js_class = HashMetric)]
impl WasmHashMetric {
    /// Create a new `WasmHashMetric` with an optional JavaScript function.
    ///
    /// Defaults to counting leading zero bytes if no function is provided.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    #[wasm_bindgen(constructor)]
    pub fn new(func: Option<js_sys::Function>) -> Self {
        Self(func)
    }
}

impl DepthMetric for WasmHashMetric {
    fn to_depth(&self, digest: Digest<LooseCommit>) -> Depth {
        if let Some(func) = &self.0 {
            let wasm_digest = WasmDigest::from(digest);

            #[allow(clippy::expect_used)]
            let js_value = func
                .call1(&JsValue::NULL, &JsValue::from(wasm_digest))
                .expect("callback failed");

            #[allow(clippy::expect_used)]
            WasmDepth::try_from_js_value(&js_value)
                .expect("invalid Depth returned from callback")
                .into()
        } else {
            CountLeadingZeroBytes.to_depth(digest)
        }
    }
}

/// Wasm wrapper for call errors from the unified transport.
#[wasm_bindgen(js_name = CallError)]
#[derive(Debug, Clone, thiserror::Error)]
#[error(transparent)]
pub struct WasmCallError(#[from] TransportCallError);

impl From<WasmCallError> for js_sys::Error {
    fn from(err: WasmCallError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name("CallError");
        js_err
    }
}
