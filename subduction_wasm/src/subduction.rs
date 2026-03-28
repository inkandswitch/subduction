//! Subduction node.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use async_lock::Mutex;
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
use subduction_core::{
    connection::manager::Spawn,
    handler::sync::SyncHandler,
    handshake::audience::DiscoveryId,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    sharded_map::ShardedMap,
    storage::powerbox::StoragePowerbox,
    subduction::{
        Subduction,
        error::HydrationError,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
    },
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_ephemeral::{
    composed::ComposedHandler, config::EphemeralConfig, handler::EphemeralHandler,
    message::EphemeralMessage, policy::OpenEphemeralPolicy, topic::Topic,
};
use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;

use crate::{
    error::{
        WasmAddConnectionError, WasmConnectError, WasmDisconnectionError, WasmHandshakeError,
        WasmHydrationError, WasmIoError, WasmLongPollConnectError, WasmWriteError,
    },
    fragment::WasmFragmentRequested,
    peer_id::WasmPeerId,
    signer::JsSigner,
    sync_stats::WasmSyncStats,
    topic::WasmTopic,
    transport::{
        DEFAULT_LOCAL_SERVICE_NAME, JsTransport, WasmAuthenticatedTransport,
        longpoll::{JsTimeout, WasmHttpLongPoll, WasmLongPoll},
        websocket::WasmWebSocket,
    },
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

use crate::{
    clock::JsClock,
    policy::{JsPolicy, make_open_policy},
};

type WasmConn = MessageTransport<JsTransport>;

type WasmHandler = ComposedHandler<
    SyncHandler<
        Local,
        JsStorage,
        WasmConn,
        JsPolicy,
        WasmHashMetric,
        WASM_SHARD_COUNT,
        crate::remote_heads::JsRemoteHeadsObserver,
    >,
    EphemeralHandler<Local, WasmConn, OpenEphemeralPolicy, JsClock>,
    crate::wire::WireMessage,
>;

type WasmEphemeralHandler = EphemeralHandler<Local, WasmConn, OpenEphemeralPolicy, JsClock>;

type WasmSubductionCore = Subduction<
    'static,
    Local,
    JsStorage,
    WasmConn,
    WasmHandler,
    JsPolicy,
    JsSigner,
    JsTimeout,
    WasmHashMetric,
    WASM_SHARD_COUNT,
>;

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
pub struct WasmSubduction {
    core: Arc<WasmSubductionCore>,
    js_storage: JsValue, // helpful for implementations to registering callbacks on the original object
    ephemeral_handler: WasmEphemeralHandler,
}

impl core::fmt::Debug for WasmSubduction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmSubduction")
            .field("js_storage", &self.js_storage)
            .finish_non_exhaustive()
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
    /// * `policy` - Optional JS object implementing authorization.
    ///   Must have `authorizeConnect(...)`, `authorizeFetch(...)`, `authorizePut(...)`,
    ///   `filterAuthorizedFetch(...)`. Defaults to allow-all.
    ///
    /// # Panics
    ///
    /// Panics if `hash_metric_override` is `Some` but the underlying JS value
    /// cannot be cast to a `Function`.
    #[must_use]
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        signer: JsSigner,
        storage: JsStorage,
        service_name: Option<String>,
        hash_metric_override: Option<JsToDepth>,
        max_pending_blob_requests: Option<usize>,
        policy: Option<JsPolicy>,
        on_remote_heads: Option<js_sys::Function>,
        on_ephemeral: Option<js_sys::Function>,
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

        let policy = policy.unwrap_or_else(make_open_policy);

        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let sedimentrees = Arc::new(ShardedMap::new());
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(max_pending)));
        let powerbox = StoragePowerbox::new(storage, Arc::new(policy));

        let observer = match on_remote_heads {
            Some(f) => crate::remote_heads::JsRemoteHeadsObserver::with_callback(f),
            None => crate::remote_heads::JsRemoteHeadsObserver::new(),
        };
        let sync_handler = SyncHandler::with_remote_heads_observer(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            powerbox.clone(),
            pending_blob_requests.clone(),
            depth_metric.clone(),
            observer.clone(),
        );

        let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
            connections.clone(),
            OpenEphemeralPolicy,
            EphemeralConfig::default(),
            JsClock,
        );
        let ephemeral_for_wasm = ephemeral_handler.clone();

        let send_counter = sync_handler.send_counter().clone();
        let handler = Arc::new(ComposedHandler::new(sync_handler, ephemeral_handler));

        let (core, listener_fut, manager_fut) = Subduction::new(
            handler,
            discovery_id,
            signer,
            sedimentrees,
            connections,
            subscriptions,
            powerbox,
            pending_blob_requests,
            send_counter,
            NonceCache::default(),
            JsTimeout,
            Duration::from_secs(30),
            depth_metric,
            WasmSpawn,
        );

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

        // Always drain the ephemeral channel to prevent "channel full" warnings
        // in EphemeralHandler when no JS callback is registered.
        let observer = on_ephemeral.map(crate::ephemeral::JsEphemeralObserver::new);
        wasm_bindgen_futures::spawn_local(async move {
            while let Ok(event) = ephemeral_rx.recv().await {
                if let Some(ref obs) = observer {
                    obs.on_event(event.id, event.sender, &event.payload);
                }
            }
        });

        Self {
            core,
            js_storage,
            ephemeral_handler: ephemeral_for_wasm,
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
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn hydrate(
        signer: JsSigner,
        storage: JsStorage,
        service_name: Option<String>,
        hash_metric_override: Option<JsToDepth>,
        max_pending_blob_requests: Option<usize>,
        policy: Option<JsPolicy>,
        on_remote_heads: Option<js_sys::Function>,
        on_ephemeral: Option<js_sys::Function>,
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

        let policy = policy.unwrap_or_else(make_open_policy);

        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(max_pending)));
        let powerbox = StoragePowerbox::new(storage, Arc::new(policy));

        let observer = match on_remote_heads {
            Some(f) => crate::remote_heads::JsRemoteHeadsObserver::with_callback(f),
            None => crate::remote_heads::JsRemoteHeadsObserver::new(),
        };
        let sync_handler = SyncHandler::with_remote_heads_observer(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            powerbox.clone(),
            pending_blob_requests.clone(),
            depth_metric.clone(),
            observer.clone(),
        );

        let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
            connections.clone(),
            OpenEphemeralPolicy,
            EphemeralConfig::default(),
            JsClock,
        );
        let ephemeral_for_wasm = ephemeral_handler.clone();

        let send_counter = sync_handler.send_counter().clone();
        let handler = Arc::new(ComposedHandler::new(sync_handler, ephemeral_handler));

        let (core, listener_fut, manager_fut) = Subduction::new(
            handler,
            discovery_id,
            signer,
            sedimentrees,
            connections,
            subscriptions,
            powerbox,
            pending_blob_requests,
            send_counter,
            NonceCache::default(),
            JsTimeout,
            Duration::from_secs(30),
            depth_metric,
            WasmSpawn,
        );

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

        // Always drain the ephemeral channel to prevent "channel full" warnings
        // in EphemeralHandler when no JS callback is registered.
        let observer = on_ephemeral.map(crate::ephemeral::JsEphemeralObserver::new);
        wasm_bindgen_futures::spawn_local(async move {
            while let Ok(event) = ephemeral_rx.recv().await {
                if let Some(ref obs) = observer {
                    obs.on_event(event.id, event.sender, &event.payload);
                }
            }
        });

        Ok(Self {
            core,
            js_storage,
            ephemeral_handler: ephemeral_for_wasm,
        })
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

    /// Connect to a peer via WebSocket and add the connection.
    ///
    /// This performs the cryptographic handshake, verifies the server's identity,
    /// and adds the authenticated connection for syncing.
    ///
    /// Returns the verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or adding the connection fails.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect(
        &self,
        address: &web_sys::Url,
        expected_peer_id: &WasmPeerId,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated =
            WasmWebSocket::connect_authenticated(address, self.core.signer(), expected_peer_id)
                .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .add_connection(
                authenticated.map(|ws| {
                    MessageTransport::new(JsValue::from(ws).unchecked_into::<JsTransport>())
                }),
            )
            .await?;
        Ok(peer_id.into())
    }

    /// Connect to a peer via WebSocket using discovery mode and add the connection.
    ///
    /// Returns the discovered and verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `address` - The WebSocket URL to connect to
    /// * `timeout_milliseconds` - Request timeout in milliseconds (defaults to 30000)
    /// * `service_name` - The service name for discovery (defaults to URL host)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or adding the connection fails.
    #[wasm_bindgen(js_name = connectDiscover)]
    pub async fn connect_discover(
        &self,
        address: &web_sys::Url,
        service_name: Option<String>,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated = WasmWebSocket::connect_discover_authenticated(
            address,
            self.core.signer(),
            service_name,
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .add_connection(
                authenticated.map(|ws| {
                    MessageTransport::new(JsValue::from(ws).unchecked_into::<JsTransport>())
                }),
            )
            .await?;
        Ok(peer_id.into())
    }

    /// Connect to a peer via HTTP long-poll and add the connection.
    ///
    /// Returns the verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `expected_peer_id` - The expected server peer ID (verified during handshake)
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or adding the connection fails.
    #[wasm_bindgen(js_name = connectLongPoll)]
    pub async fn connect_long_poll(
        &self,
        base_url: &str,
        expected_peer_id: &WasmPeerId,
    ) -> Result<WasmPeerId, WasmLongPollConnectError> {
        let (authenticated, _session_id) =
            WasmLongPoll::connect_authenticated(base_url, self.core.signer(), expected_peer_id)
                .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .add_connection(authenticated.map(|lp| {
                let transport: JsTransport =
                    JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into();
                MessageTransport::new(transport)
            }))
            .await?;
        self.ephemeral_handler.subscribe_peer(peer_id).await;
        Ok(peer_id.into())
    }

    /// Connect to a peer via HTTP long-poll using discovery mode.
    ///
    /// Returns the discovered and verified peer ID on success.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The server's HTTP base URL (e.g., `http://localhost:8080`)
    /// * `timeout_milliseconds` - Request timeout in milliseconds (default: 30000)
    /// * `service_name` - The service name for discovery (defaults to `base_url`)
    ///
    /// # Errors
    ///
    /// Returns an error if connection, handshake, or adding the connection fails.
    #[wasm_bindgen(js_name = connectDiscoverLongPoll)]
    pub async fn connect_discover_long_poll(
        &self,
        base_url: &str,
        service_name: Option<String>,
    ) -> Result<WasmPeerId, WasmLongPollConnectError> {
        let (authenticated, _session_id) = WasmLongPoll::connect_discover_authenticated(
            base_url,
            self.core.signer(),
            service_name,
        )
        .await?;

        let peer_id = authenticated.peer_id();
        self.core
            .add_connection(authenticated.map(|lp| {
                let transport: JsTransport =
                    JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into();
                MessageTransport::new(transport)
            }))
            .await?;
        self.ephemeral_handler.subscribe_peer(peer_id).await;
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

    /// Onboard an authenticated transport: add it and sync all sedimentrees.
    ///
    /// Accepts an [`AuthenticatedTransport`](WasmAuthenticatedTransport),
    /// obtained via [`AuthenticatedTransport.setup`](WasmAuthenticatedTransport::setup),
    /// [`AuthenticatedWebSocket.toTransport`], or [`AuthenticatedLongPoll.toTransport`].
    ///
    /// Returns `true` if this is a new peer, `false` if already connected.
    ///
    /// Add an authenticated transport to tracking.
    ///
    /// This does not perform any synchronization. To sync after adding,
    /// call [`fullSyncWithPeer`](Self::full_sync_with_peer).
    ///
    /// Returns `true` if this is a new peer, `false` if already connected.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is rejected by the policy.
    #[wasm_bindgen(js_name = addConnection)]
    pub async fn add_connection(
        &self,
        transport: &WasmAuthenticatedTransport,
    ) -> Result<bool, WasmAddConnectionError> {
        let peer_id = transport.inner().peer_id();
        let is_new = self.core.add_connection(transport.inner().clone()).await?;
        if is_new {
            self.ephemeral_handler.subscribe_peer(peer_id).await;
        }
        Ok(is_new)
    }

    /// Connect to a peer over any [`Transport`](JsTransport) using discovery mode.
    ///
    /// Performs a discovery handshake, then adds the authenticated connection.
    /// The peer's identity is discovered during the handshake.
    ///
    /// # Arguments
    ///
    /// * `transport` - Any JS object with `sendBytes`/`recvBytes`/`disconnect`
    /// * `service_name` - Shared service name for discovery
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake or connection fails.
    #[wasm_bindgen(js_name = connectTransport)]
    pub async fn connect_transport(
        &self,
        transport: JsTransport,
        service_name: String,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated = WasmAuthenticatedTransport::setup_discover(
            transport,
            self.core.signer(),
            Some(service_name),
            None,
        )
        .await?;

        let peer_id = authenticated.inner().peer_id();
        self.core.add_connection(authenticated.into_inner()).await?;
        self.ephemeral_handler.subscribe_peer(peer_id).await;
        Ok(peer_id.into())
    }

    /// Accept a connection from a peer over any [`Transport`](JsTransport).
    ///
    /// Performs the responder side of the handshake, then adds the authenticated
    /// connection. This is the counterpart to [`connectTransport`](Self::connect_transport).
    ///
    /// # Arguments
    ///
    /// * `transport` - Any JS object with `sendBytes`/`recvBytes`/`disconnect`
    /// * `service_name` - Shared service name for discovery
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake or connection fails.
    #[wasm_bindgen(js_name = acceptTransport)]
    pub async fn accept_transport(
        &self,
        transport: JsTransport,
        service_name: String,
    ) -> Result<WasmPeerId, WasmConnectError> {
        let authenticated = WasmAuthenticatedTransport::accept_discover(
            transport,
            self.core.signer(),
            service_name,
            None,
        )
        .await?;

        let peer_id = authenticated.inner().peer_id();
        self.core.add_connection(authenticated.into_inner()).await?;
        self.ephemeral_handler.subscribe_peer(peer_id).await;
        Ok(peer_id.into())
    }

    /// Link two local [`Subduction`](WasmSubduction) instances over a
    /// [`MessageChannel`](web_sys::MessageChannel).
    ///
    /// Creates a `MessageChannel`, performs a discovery handshake between
    /// the two instances, and adds the connections to both. This is the
    /// simplest way to sync two local instances.
    ///
    /// # Errors
    ///
    /// Returns an error if the handshake or connection fails.
    #[wasm_bindgen]
    pub async fn link(a: &WasmSubduction, b: &WasmSubduction) -> Result<(), WasmConnectError> {
        use crate::transport::message_port::WasmMessagePortTransport;

        let channel =
            web_sys::MessageChannel::new().map_err(|e| WasmHandshakeError::Transport(e.into()))?;

        let port1 = channel.port1();
        let port2 = channel.port2();
        port1.start();
        port2.start();

        let transport_a: JsTransport =
            JsValue::from(WasmMessagePortTransport::new(port1.into())).unchecked_into();
        let transport_b: JsTransport =
            JsValue::from(WasmMessagePortTransport::new(port2.into())).unchecked_into();

        let (auth_a, auth_b) = futures::future::try_join(
            WasmAuthenticatedTransport::setup_discover(
                transport_a,
                a.core.signer(),
                Some(DEFAULT_LOCAL_SERVICE_NAME.into()),
                None,
            ),
            WasmAuthenticatedTransport::accept_discover(
                transport_b,
                b.core.signer(),
                DEFAULT_LOCAL_SERVICE_NAME.into(),
                None,
            ),
        )
        .await
        .map_err(WasmConnectError::from)?;

        let (result_a, result_b) = futures::future::try_join(
            a.core.add_connection(auth_a.into_inner()),
            b.core.add_connection(auth_b.into_inner()),
        )
        .await?;

        tracing::info!("linked two Subduction instances (new_a={result_a}, new_b={result_b})");

        // Send outgoing ephemeral subscriptions to the newly connected peers
        let peer_b = b.core.peer_id();
        let peer_a = a.core.peer_id();
        a.ephemeral_handler.subscribe_peer(peer_b).await;
        b.ephemeral_handler.subscribe_peer(peer_a).await;

        Ok(())
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
        let (success, stats, transport_errors) = self
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
            transport_errors: transport_errors
                .into_iter()
                .map(|(_conn, err)| WasmCallError::from(err))
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
    #[wasm_bindgen(js_name = syncWithAllPeers)]
    pub async fn sync_with_all_peers(
        &self,
        id: &WasmSedimentreeId,
        subscribe: bool,
        timeout_milliseconds: Option<u64>,
    ) -> Result<WasmPeerResultMap, WasmIoError> {
        tracing::debug!("WasmSubduction::sync_with_all_peers");
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let peer_map = self
            .core
            .sync_with_all_peers(id.clone().into(), subscribe, timeout)
            .await?;
        tracing::debug!("WasmSubduction::sync_with_all_peers - done");
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
                                .map(|(conn, err)| (conn.into_inner(), WasmCallError::from(err)))
                                .collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect(),
        ))
    }

    /// Sync all known Sedimentree IDs with a single peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer to sync with
    /// * `subscribe` - Whether to subscribe to future updates (default: `true`)
    /// * `timeout_milliseconds` - Per-call timeout in milliseconds
    #[wasm_bindgen(js_name = fullSyncWithPeer)]
    pub async fn full_sync_with_peer(
        &self,
        peer_id: &WasmPeerId,
        subscribe: Option<bool>,
        timeout_milliseconds: Option<u64>,
    ) -> PeerBatchSyncResult {
        let subscribe = subscribe.unwrap_or(true);
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, stats, conn_errs, io_errs) = self
            .core
            .full_sync_with_peer(&peer_id.clone().into(), subscribe, timeout)
            .await;

        for (id, err) in &io_errs {
            tracing::error!(
                "full_sync_with_peer I/O error for sedimentree {:?}: {}",
                id,
                err
            );
        }

        PeerBatchSyncResult {
            success,
            stats: stats.into(),
            transport_errors: conn_errs
                .into_iter()
                .map(|(_conn, err)| WasmCallError::from(err))
                .collect(),
        }
    }

    /// Sync all known Sedimentree IDs with all connected peers.
    #[wasm_bindgen(js_name = fullSyncWithAllPeers)]
    pub async fn full_sync_with_all_peers(
        &self,
        timeout_milliseconds: Option<u64>,
    ) -> PeerBatchSyncResult {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, stats, conn_errs, io_errs) =
            self.core.full_sync_with_all_peers(timeout).await;

        for (id, err) in &io_errs {
            tracing::error!(
                "full_sync_with_all_peers I/O error for sedimentree {:?}: {}",
                id,
                err
            );
        }

        PeerBatchSyncResult {
            success,
            stats: stats.into(),
            transport_errors: conn_errs
                .into_iter()
                .map(|(_conn, err)| WasmCallError::from(err))
                .collect(),
        }
    }

    // ── Ephemeral messaging ────────────────────────────────────────────

    /// Publish an ephemeral message to all subscribers of a topic.
    ///
    /// The payload is opaque bytes — encoding is the caller's responsibility.
    /// Messages are fire-and-forget; delivery is best-effort.
    ///
    /// # Panics
    ///
    /// Panics if the platform's random number generator fails.
    #[wasm_bindgen(js_name = publishEphemeral)]
    #[allow(
        clippy::expect_used,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub async fn publish_ephemeral(&self, topic: &WasmTopic, payload: &[u8]) {
        let nonce: u64 = {
            let mut buf = [0u8; 8];
            getrandom::getrandom(&mut buf).expect("getrandom failed");
            u64::from_le_bytes(buf)
        };
        // Date.now() returns f64 ms since epoch; convert to seconds.
        let timestamp = TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64);
        let msg = EphemeralMessage::new_signed::<Local, _>(
            self.core.signer(),
            Topic::from(topic.clone()),
            nonce,
            timestamp,
            payload.to_vec(),
        )
        .await;
        self.ephemeral_handler.publish(msg).await;
    }

    /// Subscribe to ephemeral messages for the given topics
    /// from all connected peers.
    #[wasm_bindgen(js_name = subscribeEphemeral)]
    pub async fn subscribe_ephemeral(&self, topics: Vec<WasmTopic>) {
        let topics: Vec<Topic> = topics.into_iter().map(Topic::from).collect();
        if let Some(topics) = nonempty::NonEmpty::from_vec(topics) {
            self.ephemeral_handler.subscribe(topics).await;
        }
    }

    /// Unsubscribe from ephemeral messages for the given topics
    /// from all connected peers.
    #[wasm_bindgen(js_name = unsubscribeEphemeral)]
    pub async fn unsubscribe_ephemeral(&self, topics: Vec<WasmTopic>) {
        let topics: Vec<Topic> = topics.into_iter().map(Topic::from).collect();
        if let Some(topics) = nonempty::NonEmpty::from_vec(topics) {
            self.ephemeral_handler.unsubscribe(topics).await;
        }
    }

    // ── Queries ──────────────────────────────────────────────────────────

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
}

/// Result of a peer batch sync request.
#[wasm_bindgen(js_name = PeerBatchSyncResult)]
#[derive(Debug)]
pub struct PeerBatchSyncResult {
    success: bool,
    stats: WasmSyncStats,
    transport_errors: Vec<WasmCallError>,
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
        self.stats.clone()
    }

    /// Errors that occurred during the batch sync.
    #[must_use]
    #[wasm_bindgen(getter, js_name = transportErrors)]
    pub fn transport_errors(&self) -> Vec<js_sys::Error> {
        self.transport_errors
            .iter()
            .map(|e| e.clone().into())
            .collect()
    }
}

/// Map of peer IDs to their batch sync results.
#[wasm_bindgen(js_name = PeerResultMap)]
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct WasmPeerResultMap(
    Map<
        PeerId,
        (
            bool,
            WasmSyncStats,
            Vec<(MessageTransport<JsTransport>, WasmCallError)>,
        ),
    >,
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
                stats: stats.clone(),
                transport_errors: conn_errs.iter().map(|(_conn, err)| err.clone()).collect(),
            })
    }

    /// Get all entries in the peer result map.
    #[must_use]
    pub fn entries(&self) -> Vec<PeerBatchSyncResult> {
        let mut results = Vec::with_capacity(self.0.len());
        for (success, stats, conn_errs) in self.0.values() {
            results.push(PeerBatchSyncResult {
                success: *success,
                stats: stats.clone(),
                transport_errors: conn_errs.iter().map(|(_conn, err)| err.clone()).collect(),
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

/// Wasm wrapper for call errors.
#[wasm_bindgen(js_name = CallError)]
#[derive(Debug, Clone, thiserror::Error)]
#[error(transparent)]
pub struct WasmCallError(
    #[from] subduction_core::connection::managed::CallError<crate::transport::JsTransportError>,
);

impl From<WasmCallError> for js_sys::Error {
    fn from(err: WasmCallError) -> Self {
        let js_err = js_sys::Error::new(&err.to_string());
        js_err.set_name(err.0.error_name());
        js_err
    }
}
