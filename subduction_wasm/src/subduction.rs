//! Subduction node.

use alloc::{
    boxed::Box,
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use async_lock::Mutex;
use core::{fmt::Debug, time::Duration};
use sedimentree_core::collections::Map;

use from_js_ref::FromJsRef;
use future_form::Local;
use futures::{
    FutureExt,
    future::{Either, select},
    stream::Aborted,
};
use js_sys::Uint8Array;
use nonempty::NonEmpty;
use sedimentree_core::{
    blob::Blob,
    depth::{CountLeadingZeroBytes, Depth, DepthMetric},
    id::SedimentreeId,
    loose_commit::id::CommitId,
};
use subduction_core::{
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::manager::Spawn,
    handler::sync::SyncHandler,
    handshake::audience::DiscoveryId,
    multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    storage::powerbox::StoragePowerbox,
    subduction::{
        Subduction,
        pending_blob_requests::{DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests},
        per_peer_sync::PerPeerSync,
    },
    timeout::call::CallTimeout,
    timestamp::TimestampSeconds,
    transport::message::MessageTransport,
};
use subduction_crypto::signed::Signed;
use subduction_ephemeral::{
    config::EphemeralConfig,
    handler::EphemeralHandler,
    message::{EphemeralMessage, EphemeralPayload},
    topic::Topic,
};
use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;

use crate::{
    batch_input::{WasmCommitInput, WasmFragmentInput},
    error::{
        WasmAddConnectionError, WasmConnectError, WasmDisconnectionError, WasmHandshakeError,
        WasmIoError, WasmLongPollConnectError, WasmWriteError,
    },
    fragment::WasmFragmentRequested,
    peer_id::WasmPeerId,
    remote_heads::JsRemoteHeadsObserver,
    signer::JsSigner,
    sync_stats::WasmSyncStats,
    topic::{JsTopic, WasmTopic},
    transport::{
        DEFAULT_LOCAL_SERVICE_NAME, JsTransport,
        authenticated::WasmAuthenticatedTransport,
        longpoll::{JsTimeout, WasmHttpLongPoll, WasmLongPoll},
        websocket::WasmWebSocket,
    },
};
use sedimentree_wasm::{
    commit_id::{JsCommitId, WasmCommitId},
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
pub(crate) const WASM_SHARD_COUNT: usize = 4;

/// Default cap on the number of sedimentrees kept resident in memory in the
/// browser.
///
/// The in-memory sedimentree map is an LRU cache over `IndexedDB`: cold trees
/// are evicted and re-hydrated on demand. A browser tab has a far tighter
/// memory budget (and a hard wasm32 address-space ceiling) than a server,
/// so the default resident set is small. At a few tens of KiB of metadata
/// per tree this is on the order of ~90 MiB resident.
pub(crate) const WASM_DEFAULT_MAX_RESIDENT_TREES: usize = 1_024;

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
    handler::{JsFrameHandler, WasmComposedHandler, WasmKeyhiveHandler},
    policy::{
        JsPolicy,
        ephemeral::{JsEphemeralPolicy, make_open_ephemeral_policy},
        make_open_policy,
    },
    wire::WireMessage,
};

use subduction_keyhive::KeyhiveMessage;

type WasmConn = MessageTransport<JsTransport>;

type WasmEphemeralHandler = EphemeralHandler<Local, WasmConn, JsEphemeralPolicy, JsClock>;

type WasmSubductionCore = Subduction<
    'static,
    Local,
    JsStorage,
    WasmConn,
    WasmComposedHandler,
    JsPolicy,
    JsSigner,
    JsTimeout,
    WasmSpawn,
    WasmHashMetric,
    WASM_SHARD_COUNT,
>;

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
pub struct WasmSubduction {
    core: Arc<WasmSubductionCore>,
    js_storage: JsValue, // helpful for implementations to registering callbacks on the original object
    ephemeral_handler: WasmEphemeralHandler,
    keyhive_handler: Arc<Mutex<Option<JsFrameHandler>>>,
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
    /// Create a new [`Subduction`] instance from an options object.
    ///
    /// ```js
    /// new Subduction({ signer, storage, serviceName: "sync.example.com" });
    /// ```
    ///
    /// `signer` and `storage` are required; all other fields are optional.
    /// See [`SubductionOptions`] for the full field list and defaults.
    ///
    /// # Panics
    ///
    /// Panics if `hashMetricOverride` is provided but the underlying JS value
    /// cannot be cast to a `Function`.
    #[must_use]
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_lines)]
    pub fn new(opts: &JsSubductionOptions) -> Self {
        tracing::debug!("new Subduction node");

        let default_roundtrip_timeout = opts
            .default_timeout_milliseconds()
            .map_or(DEFAULT_ROUNDTRIP_TIMEOUT, |ms| {
                Duration::from_millis(u64::from(ms))
            });

        let opts_storage = opts.storage();
        let js_storage = <JsStorage as AsRef<JsValue>>::as_ref(&opts_storage).clone();

        #[allow(clippy::expect_used)]
        let raw_fn: Option<js_sys::Function> = opts.hash_metric_override().map(|h| {
            JsValue::from(h)
                .dyn_into()
                .expect("hash_metric_override is not a Function")
        });
        let discovery_id = opts
            .service_name()
            .map(|name| DiscoveryId::new(name.as_bytes()));
        let depth_metric = WasmHashMetric(raw_fn);
        let max_pending = opts
            .max_pending_blob_requests()
            .map_or(DEFAULT_MAX_PENDING_BLOB_REQUESTS, |n| n as usize);
        // `None` or an explicit `0` falls back to the default cap.
        let max_resident = match opts.max_resident_trees() {
            Some(n) if n > 0 => n as usize,
            _ => WASM_DEFAULT_MAX_RESIDENT_TREES,
        };

        let policy = opts.policy().unwrap_or_else(make_open_policy);

        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let sedimentrees = Arc::new(BoundedShardedMap::new().with_capacity(max_resident));
        let pending_blob_requests = Arc::new(Mutex::new(PendingBlobRequests::new(max_pending)));
        let powerbox = StoragePowerbox::new(opts_storage, Arc::new(policy));

        let observer = match opts.on_remote_heads() {
            Some(f) => JsRemoteHeadsObserver::with_callback(f),
            None => JsRemoteHeadsObserver::new(),
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

        let eph_policy = opts
            .ephemeral_policy()
            .unwrap_or_else(make_open_ephemeral_policy);
        let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
            connections.clone(),
            eph_policy,
            EphemeralConfig::default(),
            JsClock,
        );
        let ephemeral_for_wasm = ephemeral_handler.clone();

        let send_counter = sync_handler.send_counter().clone();
        let keyhive_handler = Arc::new(Mutex::new(None));
        let handler = Arc::new(WasmComposedHandler::new(
            sync_handler,
            ephemeral_handler,
            WasmKeyhiveHandler::new(keyhive_handler.clone()),
        ));

        let (core, listener_fut, manager_fut) = Subduction::new(
            handler,
            discovery_id,
            opts.signer(),
            sedimentrees,
            connections,
            subscriptions,
            powerbox,
            pending_blob_requests,
            send_counter,
            NonceCache::default(),
            JsTimeout,
            default_roundtrip_timeout,
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
        let observer = opts
            .on_ephemeral()
            .map(crate::ephemeral::JsEphemeralObserver::new);
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
            keyhive_handler,
        }
    }

    /// Persist a whole [`Sedimentree`](sedimentree_wasm::WasmSedimentree)
    /// locally **without** broadcasting (the `store_*` tier).
    ///
    /// Durability only: never waits on a peer. Drive
    /// [`syncWithAllPeers`](Self::sync_with_all_peers) yourself to propagate.
    /// Use [`addSedimentree`](Self::add_sedimentree) for the
    /// store-and-broadcast combinator.
    ///
    /// # Errors
    ///
    /// Returns [`WasmWriteError`] if a referenced blob is missing or storage
    /// I/O fails. This local-only path uses the trusting local putter, so no
    /// network policy is consulted.
    #[wasm_bindgen(js_name = storeSedimentree)]
    pub async fn store_sedimentree(
        &self,
        id: &WasmSedimentreeId,
        sedimentree: &WasmSedimentree,
        blobs: Vec<Uint8Array>,
    ) -> Result<(), WasmWriteError> {
        self.core
            .store_sedimentree(
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

    /// Persist a whole [`Sedimentree`](sedimentree_wasm::WasmSedimentree)
    /// **and** broadcast it to all connected peers (the `add_*` combinator =
    /// [`storeSedimentree`](Self::store_sedimentree) +
    /// [`syncWithAllPeers`](Self::sync_with_all_peers)).
    ///
    /// Returns the per-peer broadcast outcome as a
    /// [`PeerResultMap`](WasmPeerResultMap). This **awaits the broadcast**, so
    /// an unresponsive peer can stall the call for up to `timeout_milliseconds`
    /// (or the configured default when omitted); callers wanting a
    /// non-blocking durable write should use
    /// [`storeSedimentree`](Self::store_sedimentree) instead.
    ///
    /// # Errors
    ///
    /// Returns [`WasmWriteError`] if a referenced blob is missing, or on
    /// storage I/O — including storage errors hit while ingesting inbound data
    /// during the trailing sync. The local store uses the trusting local
    /// putter (no network policy check). Per-peer transport failures during
    /// the broadcast are reported inside the returned map, not as an error.
    #[wasm_bindgen(js_name = addSedimentree)]
    pub async fn add_sedimentree(
        &self,
        id: &WasmSedimentreeId,
        sedimentree: &WasmSedimentree,
        blobs: Vec<Uint8Array>,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerResultMap, WasmWriteError> {
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
        let per_peer = self
            .core
            .add_sedimentree(
                id.clone().into(),
                sedimentree.clone().into(),
                blobs
                    .into_iter()
                    .map(|bytes| bytes.to_vec().into())
                    .collect(),
                timeout,
            )
            .await?;
        Ok(per_peer.into())
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
        timeout_milliseconds: Option<u32>,
    ) -> Result<Option<Vec<Uint8Array>>, WasmIoError> {
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
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

    /// Persist a single commit with its blob locally **without** broadcasting
    /// (the `store_*` tier).
    ///
    /// The commit metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// Durability only: never waits on a peer. Use
    /// [`addCommit`](Self::add_commit) for the store-and-push combinator.
    ///
    /// Returns the [`WasmFragmentRequested`] signal when the commit lands on a
    /// fragment boundary, exactly as [`addCommit`](Self::add_commit) does.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] on storage I/O failure. This local-only
    /// path uses the trusting local putter, so no network policy is consulted.
    #[wasm_bindgen(js_name = storeCommit)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn store_commit(
        &self,
        id: &WasmSedimentreeId,
        head: &WasmCommitId,
        parents: Vec<JsCommitId>,
        blob: &Uint8Array,
    ) -> Result<Option<WasmFragmentRequested>, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_head = CommitId::from(head);
        let core_parents: BTreeSet<CommitId> = parents
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let blob: Blob = blob.clone().to_vec().into();

        let maybe_fragment_requested = self
            .core
            .store_commit(core_id, core_head, core_parents, blob)
            .await?;

        Ok(maybe_fragment_requested.map(WasmFragmentRequested::from))
    }

    /// Add a commit with its associated blob to the storage, then push it to
    /// authorized subscribers (the `add_*` combinator =
    /// [`storeCommit`](Self::store_commit) + best-effort push).
    ///
    /// The commit metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// Propagation is a best-effort `send` to subscribers; it does not block
    /// on peer acks (no per-peer result is produced for single-item pushes).
    /// For a durable write with no propagation, use
    /// [`storeCommit`](Self::store_commit).
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if storage, networking, or policy fail.
    #[wasm_bindgen(js_name = addCommit)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn add_commit(
        &self,
        id: &WasmSedimentreeId,
        head: &WasmCommitId,
        parents: Vec<JsCommitId>,
        blob: &Uint8Array,
    ) -> Result<Option<WasmFragmentRequested>, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_head = CommitId::from(head);
        let core_parents: BTreeSet<CommitId> = parents
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let blob: Blob = blob.clone().to_vec().into();

        let maybe_fragment_requested = self
            .core
            .add_commit(core_id, core_head, core_parents, blob)
            .await?;

        Ok(maybe_fragment_requested.map(WasmFragmentRequested::from))
    }

    /// Persist a single fragment with its blob locally **without**
    /// broadcasting (the `store_*` tier).
    ///
    /// The fragment metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// Durability only: never waits on a peer. Use
    /// [`addFragment`](Self::add_fragment) for the store-and-push combinator.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] on storage I/O failure. This local-only
    /// path uses the trusting local putter, so no network policy is consulted.
    #[wasm_bindgen(js_name = storeFragment)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn store_fragment(
        &self,
        id: &WasmSedimentreeId,
        head: &WasmCommitId,
        boundary: Vec<JsCommitId>,
        checkpoints: Vec<JsCommitId>,
        blob: &Uint8Array,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_head = CommitId::from(head);
        let core_boundary: BTreeSet<CommitId> = boundary
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let core_checkpoints: Vec<CommitId> = checkpoints
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let blob: Blob = blob.clone().to_vec().into();

        self.core
            .store_fragment(core_id, core_head, core_boundary, &core_checkpoints, blob)
            .await?;

        Ok(())
    }

    /// Add a fragment with its associated blob to the storage, then push it to
    /// authorized subscribers (the `add_*` combinator =
    /// [`storeFragment`](Self::store_fragment) + best-effort push).
    ///
    /// The fragment metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// Propagation is a best-effort `send` to subscribers; it does not block
    /// on peer acks. For a durable write with no propagation, use
    /// [`storeFragment`](Self::store_fragment).
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if storage, networking, or policy fail.
    #[wasm_bindgen(js_name = addFragment)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen needs to take Vecs not slices
    pub async fn add_fragment(
        &self,
        id: &WasmSedimentreeId,
        head: &WasmCommitId,
        boundary: Vec<JsCommitId>,
        checkpoints: Vec<JsCommitId>,
        blob: &Uint8Array,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_head = CommitId::from(head);
        let core_boundary: BTreeSet<CommitId> = boundary
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let core_checkpoints: Vec<CommitId> = checkpoints
            .iter()
            .map(|p| CommitId::from(WasmCommitId::from(p)))
            .collect();
        let blob: Blob = blob.clone().to_vec().into();

        self.core
            .add_fragment(core_id, core_head, core_boundary, &core_checkpoints, blob)
            .await?;

        Ok(())
    }

    /// Bulk-insert commits and fragments into a sedimentree **without**
    /// broadcasting (the `store_*` tier).
    ///
    /// Inserts everything first and runs `minimize_tree` once at the end —
    /// avoiding the `O(N²)` minimize work of looping
    /// [`storeCommit`](Self::store_commit) — but performs **no** network
    /// propagation. Useful for ingestion paths (local replay, hydration from
    /// another store) where the caller drives sync separately or not at all.
    ///
    /// Each [`WasmCommitInput`] bundles an unsigned
    /// [`LooseCommit`](sedimentree_core::loose_commit::LooseCommit) with its
    /// blob; each [`WasmFragmentInput`] bundles an unsigned
    /// [`Fragment`](sedimentree_core::fragment::Fragment) with its blob.
    /// Either list may be empty; two empty lists is a no-op.
    ///
    /// Use [`addBatch`](Self::add_batch) for the store-and-broadcast
    /// combinator.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if storage fails.
    #[wasm_bindgen(js_name = storeBuiltBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn store_built_batch(
        &self,
        id: &WasmSedimentreeId,
        commits: Vec<WasmCommitInput>,
        fragments: Vec<WasmFragmentInput>,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_commits = commits
            .into_iter()
            .map(WasmCommitInput::into_core)
            .collect();
        let core_fragments = fragments
            .into_iter()
            .map(WasmFragmentInput::into_core)
            .collect();

        self.core
            .store_built_batch(core_id, core_commits, core_fragments)
            .await?;
        Ok(())
    }

    /// Bulk-insert commits and fragments into a sedimentree, then broadcast
    /// (the `add_*` combinator = [`storeBuiltBatch`](Self::store_built_batch) +
    /// [`syncWithAllPeers`](Self::sync_with_all_peers)).
    ///
    /// Unlike [`addCommit`](Self::add_commit) and
    /// [`addFragment`](Self::add_fragment) — which each re-minimize the tree
    /// and push to peers per call — this method inserts everything first,
    /// runs `minimize_tree` once at the end, and then performs a single
    /// `sync_with_all_peers` to propagate the new state. For workloads that
    /// add many commits or fragments at once this avoids `O(N²)` minimize work
    /// and `N` redundant broadcasts.
    ///
    /// Returns the per-peer broadcast outcome as a
    /// [`PeerResultMap`](WasmPeerResultMap). This **awaits the broadcast**, so
    /// an unresponsive peer can stall the call for up to `timeout_milliseconds`
    /// (or the configured default when omitted); callers wanting a
    /// non-blocking durable write should use
    /// [`storeBuiltBatch`](Self::store_built_batch) instead.
    ///
    /// Either list may be empty; passing two empty lists is a no-op (no
    /// minimize, no broadcast) and yields an empty map.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if a local
    /// [`Storage`](sedimentree_wasm::storage::JsStorage) error is hit while
    /// persisting the batch or while ingesting inbound data during the
    /// trailing broadcast. Per-peer transport failures are reported inside the
    /// returned map, not as an error.
    #[wasm_bindgen(js_name = addBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn add_batch(
        &self,
        id: &WasmSedimentreeId,
        commits: Vec<WasmCommitInput>,
        fragments: Vec<WasmFragmentInput>,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerResultMap, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_commits = commits
            .into_iter()
            .map(WasmCommitInput::into_core)
            .collect();
        let core_fragments = fragments
            .into_iter()
            .map(WasmFragmentInput::into_core)
            .collect();
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));

        let per_peer = self
            .core
            .add_built_batch(core_id, core_commits, core_fragments, timeout)
            .await?;
        Ok(per_peer.into())
    }

    /// Bulk-insert commits into a sedimentree **without** broadcasting (the
    /// `store_*` tier).
    ///
    /// Like [`storeBuiltBatch`](Self::store_built_batch) for the commits half
    /// only. Each [`WasmCommitInput`] bundles an unsigned commit with its
    /// blob; an empty list is a no-op.
    ///
    /// Use [`addCommitsBatch`](Self::add_commits_batch) for the
    /// store-and-broadcast combinator.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if storage fails.
    #[wasm_bindgen(js_name = storeCommitsBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn store_commits_batch(
        &self,
        id: &WasmSedimentreeId,
        commits: Vec<WasmCommitInput>,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_commits = commits
            .into_iter()
            .map(WasmCommitInput::into_core)
            .collect();

        self.core
            .store_built_batch(core_id, core_commits, Vec::new())
            .await?;
        Ok(())
    }

    /// Bulk-insert commits into a sedimentree, then broadcast (the `add_*`
    /// combinator = [`storeCommitsBatch`](Self::store_commits_batch) +
    /// [`syncWithAllPeers`](Self::sync_with_all_peers)).
    ///
    /// Like [`addBatch`](Self::add_batch) for the commits half only. Returns
    /// the per-peer broadcast outcome as a
    /// [`PeerResultMap`](WasmPeerResultMap) and **awaits the broadcast** (an
    /// unresponsive peer can stall the call for up to `timeout_milliseconds`).
    /// An empty list is a no-op and yields an empty map.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if storage fails.
    /// Per-peer transport failures are reported inside the returned map.
    #[wasm_bindgen(js_name = addCommitsBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn add_commits_batch(
        &self,
        id: &WasmSedimentreeId,
        commits: Vec<WasmCommitInput>,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerResultMap, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_commits = commits
            .into_iter()
            .map(WasmCommitInput::into_core)
            .collect();
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));

        let per_peer = self
            .core
            .add_built_batch(core_id, core_commits, Vec::new(), timeout)
            .await?;
        Ok(per_peer.into())
    }

    /// Bulk-insert fragments into a sedimentree **without** broadcasting (the
    /// `store_*` tier).
    ///
    /// Like [`storeBuiltBatch`](Self::store_built_batch) for the fragments
    /// half only. Each [`WasmFragmentInput`] bundles an unsigned fragment with
    /// its blob; an empty list is a no-op.
    ///
    /// Use [`addFragmentsBatch`](Self::add_fragments_batch) for the
    /// store-and-broadcast combinator.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if storage fails.
    #[wasm_bindgen(js_name = storeFragmentsBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn store_fragments_batch(
        &self,
        id: &WasmSedimentreeId,
        fragments: Vec<WasmFragmentInput>,
    ) -> Result<(), WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_fragments = fragments
            .into_iter()
            .map(WasmFragmentInput::into_core)
            .collect();

        self.core
            .store_built_batch(core_id, Vec::new(), core_fragments)
            .await?;
        Ok(())
    }

    /// Bulk-insert fragments into a sedimentree, then broadcast (the `add_*`
    /// combinator = [`storeFragmentsBatch`](Self::store_fragments_batch) +
    /// [`syncWithAllPeers`](Self::sync_with_all_peers)).
    ///
    /// Like [`addBatch`](Self::add_batch) for the fragments half only. Returns
    /// the per-peer broadcast outcome as a
    /// [`PeerResultMap`](WasmPeerResultMap) and **awaits the broadcast** (an
    /// unresponsive peer can stall the call for up to `timeout_milliseconds`).
    /// An empty list is a no-op and yields an empty map.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmWriteError`] if any blob does not match its claimed
    /// [`BlobMeta`](sedimentree_core::blob::BlobMeta), or if storage fails.
    /// Per-peer transport failures are reported inside the returned map.
    #[wasm_bindgen(js_name = addFragmentsBatch)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn add_fragments_batch(
        &self,
        id: &WasmSedimentreeId,
        fragments: Vec<WasmFragmentInput>,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerResultMap, WasmWriteError> {
        let core_id: SedimentreeId = id.clone().into();
        let core_fragments = fragments
            .into_iter()
            .map(WasmFragmentInput::into_core)
            .collect();
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));

        let per_peer = self
            .core
            .add_built_batch(core_id, Vec::new(), core_fragments, timeout)
            .await?;
        Ok(per_peer.into())
    }

    /// Compute the [`WasmDepth`] of a commit identifier under this node's
    /// configured hash metric.
    ///
    /// Combine with [`WasmDepth::isBoundary`] to decide whether a commit is an
    /// eligible fragment head — for example after [`addBatch`](Self::add_batch),
    /// where the per-call `FragmentRequested` signal returned by
    /// [`addCommit`](Self::add_commit) is unavailable.
    #[wasm_bindgen(js_name = commitDepth)]
    #[must_use]
    pub fn commit_depth(&self, commit_id: &WasmCommitId) -> WasmDepth {
        self.core.commit_depth(CommitId::from(commit_id)).into()
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
    /// * `timeout_milliseconds` - Optional per-call total deadline in milliseconds; omit to use the configured default
    ///
    /// # Cancellation
    ///
    /// The call is bounded by `timeout_milliseconds` (or the configured
    /// default). **Abandoning the returned `Promise` — e.g. via
    /// `Promise.race([syncWithPeer(...), timeout(5000)])` — does NOT cancel
    /// the underlying sync:** the work runs on the Wasm executor, not the
    /// `Promise`, so it continues until the deadline elapses, the response
    /// arrives, or the peer disconnects. To stop it early, set a shorter
    /// `timeout_milliseconds` or disconnect the peer
    /// ([`disconnect_from_peer`](Self::disconnect_from_peer) /
    /// [`disconnect_all`](Self::disconnect_all)).
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
        timeout_milliseconds: Option<u32>,
    ) -> Result<PeerBatchSyncResult, WasmIoError> {
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
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
    /// * `timeout_milliseconds` - Optional per-call total deadline in milliseconds; omit to use the configured default
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = syncWithAllPeers)]
    pub async fn sync_with_all_peers(
        &self,
        id: &WasmSedimentreeId,
        subscribe: bool,
        timeout_milliseconds: Option<u32>,
    ) -> Result<WasmPeerResultMap, WasmIoError> {
        tracing::debug!("WasmSubduction::sync_with_all_peers");
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
        let peer_map = self
            .core
            .sync_with_all_peers(id.clone().into(), subscribe, timeout)
            .await?;
        tracing::debug!("WasmSubduction::sync_with_all_peers - done");
        Ok(peer_map.into())
    }

    /// Sync all known Sedimentree IDs with a single peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer to sync with
    /// * `subscribe` - Whether to subscribe to future updates (default: `true`)
    /// * `timeout_milliseconds` - Optional per-call total deadline in milliseconds; omit to use the configured default
    #[wasm_bindgen(js_name = fullSyncWithPeer)]
    pub async fn full_sync_with_peer(
        &self,
        peer_id: &WasmPeerId,
        subscribe: Option<bool>,
        timeout_milliseconds: Option<u32>,
    ) -> PeerBatchSyncResult {
        let subscribe = subscribe.unwrap_or(true);
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
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
        timeout_milliseconds: Option<u32>,
    ) -> PeerBatchSyncResult {
        let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
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
        let ep = EphemeralPayload {
            id: Topic::from(topic.clone()),
            nonce,
            timestamp,
            payload: payload.to_vec(),
        };
        let verified = Signed::seal::<Local, _>(self.core.signer(), ep).await;
        let msg = EphemeralMessage::Ephemeral(Box::new(verified.into_signed()));
        self.ephemeral_handler.publish(msg).await;
    }

    /// Subscribe to ephemeral messages for the given topics
    /// from all connected peers.
    #[wasm_bindgen(js_name = subscribeEphemeral)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn subscribe_ephemeral(&self, topics: Vec<JsTopic>) {
        let topics: Vec<Topic> = topics
            .iter()
            .map(|t| Topic::from(WasmTopic::from(t)))
            .collect();
        if let Some(topics) = NonEmpty::from_vec(topics) {
            self.ephemeral_handler.subscribe(topics).await;
        }
    }

    /// Unsubscribe from ephemeral messages for the given topics
    /// from all connected peers.
    #[wasm_bindgen(js_name = unsubscribeEphemeral)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn unsubscribe_ephemeral(&self, topics: Vec<JsTopic>) {
        let topics: Vec<Topic> = topics
            .iter()
            .map(|t| Topic::from(WasmTopic::from(t)))
            .collect();
        if let Some(topics) = NonEmpty::from_vec(topics) {
            self.ephemeral_handler.unsubscribe(topics).await;
        }
    }

    // ── Keyhive (SUK) frame handling ───────────────────────────────────

    /// Register a handler for keyhive protocol frames.
    ///
    /// When a frame arrives with the `SUK\0` schema header, the CBOR
    /// payload (without the SUK envelope) and the sender's peer ID are
    /// forwarded to the handler's `onMessage` method. When a peer
    /// disconnects, `onPeerDisconnect` is called.
    ///
    /// Only one handler can be registered at a time. Calling this again
    /// replaces the previous handler. Pass `null`/`undefined` to unregister.
    #[wasm_bindgen(js_name = registerFrameHandler)]
    pub async fn register_frame_handler(&self, handler: Option<JsFrameHandler>) {
        *self.keyhive_handler.lock().await = handler;
    }

    /// Send a keyhive frame to a specific peer.
    ///
    /// `payload` is the CBOR-encoded content (e.g., a `SignedMessage`).
    /// Subduction wraps it in the `SUK\0` envelope before sending.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer has no active connection or if sending fails.
    #[wasm_bindgen(js_name = sendKeyhiveMessage)]
    pub async fn send_keyhive_message(
        &self,
        payload: &[u8],
        peer_id: &WasmPeerId,
    ) -> Result<(), JsValue> {
        use subduction_core::connection::Connection;

        let pid: PeerId = peer_id.clone().into();
        let conn = self
            .core
            .get_connection(&pid)
            .await
            .ok_or_else(|| JsValue::from_str("no connection for peer"))?;
        let msg = WireMessage::Keyhive(KeyhiveMessage::new(payload.to_vec()));
        conn.send(&msg)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
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

    /// Get the current heads for every locally known sedimentree.
    ///
    /// An inner empty heads array means the sedimentree exists but has no
    /// heads yet.
    #[must_use]
    #[wasm_bindgen(js_name = getAllHeads)]
    pub async fn get_all_heads(&self) -> Vec<WasmSedimentreeHeads> {
        self.core
            .get_all_heads()
            .await
            .into_iter()
            .map(|(id, heads)| WasmSedimentreeHeads {
                id: id.into(),
                heads: heads.into_iter().map(WasmCommitId::from).collect(),
            })
            .collect()
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

/// Heads of a single sedimentree, returned by
/// [`WasmSubduction::get_all_heads`](WasmSubduction::get_all_heads).
#[wasm_bindgen(js_name = SedimentreeHeads)]
#[derive(Debug, Clone)]
pub struct WasmSedimentreeHeads {
    id: WasmSedimentreeId,
    heads: Vec<WasmCommitId>,
}

#[wasm_bindgen(js_class = SedimentreeHeads)]
impl WasmSedimentreeHeads {
    /// The sedimentree ID these heads belong to.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn id(&self) -> WasmSedimentreeId {
        self.id.clone()
    }

    /// The current heads of the sedimentree.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn heads(&self) -> Vec<WasmCommitId> {
        self.heads.clone()
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

impl From<PerPeerSync<WasmConn, Local, crate::transport::JsTransportError>> for WasmPeerResultMap {
    fn from(per_peer: PerPeerSync<WasmConn, Local, crate::transport::JsTransportError>) -> Self {
        Self(
            per_peer
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
        )
    }
}

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
    fn to_depth(&self, id: CommitId) -> Depth {
        if let Some(func) = &self.0 {
            let wasm_commit_id = WasmCommitId::from(id);

            #[allow(clippy::expect_used)]
            let js_value = func
                .call1(&JsValue::NULL, &JsValue::from(wasm_commit_id))
                .expect("callback failed");

            #[allow(clippy::expect_used)]
            WasmDepth::try_from_js_value(&js_value)
                .expect("invalid Depth returned from callback")
                .into()
        } else {
            CountLeadingZeroBytes.to_depth(id)
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

#[wasm_bindgen(typescript_custom_section)]
const TS_SUBDUCTION_OPTIONS: &str = r#"
/**
 * Options for constructing a `Subduction` node.
 *
 * Passed as a single object literal: `new Subduction({ signer, storage, ... })`.
 * `signer` and `storage` are required; every other field is optional.
 */
export interface SubductionOptions {
    /** The cryptographic signer for this node's identity. */
    signer: Signer;
    /** Storage backend for persisting data. */
    storage: SedimentreeStorage;
    /**
     * Service identifier for discovery mode (e.g. `sync.example.com`). When
     * set, clients can connect without knowing the server's peer ID.
     */
    serviceName?: string;
    /** Custom depth metric function. */
    hashMetricOverride?: (digest: Digest) => Depth;
    /** Maximum number of pending blob requests (default: 10,000). */
    maxPendingBlobRequests?: number;
    /**
     * Cap on the number of sedimentrees kept resident in memory (default:
     * 1024). `0` or omitted uses the default.
     */
    maxResidentTrees?: number;
    /** Connection/storage authorization policy. Defaults to allow-all. */
    policy?: Policy;
    /** Ephemeral message authorization policy. Defaults to allow-all. */
    ephemeralPolicy?: EphemeralPolicy;
    /** Callback fired when a peer's heads change. */
    onRemoteHeads?: Function;
    /** Callback fired on inbound ephemeral messages. */
    onEphemeral?: Function;
    /**
     * Default per-call total deadline (milliseconds) for roundtrip syncs when
     * a call omits its own `timeoutMilliseconds`. Omit for the built-in
     * default (30000).
     */
    defaultTimeoutMilliseconds?: number;
}
"#;

#[wasm_bindgen]
extern "C" {
    /// Options for constructing a [`Subduction`](subduction_core::Subduction)
    /// node. Passed as a single object literal: `new Subduction({ signer,
    /// storage, ... })`.
    ///
    /// Must match the `SubductionOptions` TypeScript interface emitted by this
    /// module.
    #[wasm_bindgen(js_name = SubductionOptions, typescript_type = "SubductionOptions")]
    pub type JsSubductionOptions;

    #[wasm_bindgen(method, getter)]
    fn signer(this: &JsSubductionOptions) -> JsSigner;

    #[wasm_bindgen(method, getter)]
    fn storage(this: &JsSubductionOptions) -> JsStorage;

    #[wasm_bindgen(method, getter, js_name = serviceName)]
    fn service_name(this: &JsSubductionOptions) -> Option<String>;

    #[wasm_bindgen(method, getter, js_name = hashMetricOverride)]
    fn hash_metric_override(this: &JsSubductionOptions) -> Option<JsToDepth>;

    #[wasm_bindgen(method, getter, js_name = maxPendingBlobRequests)]
    fn max_pending_blob_requests(this: &JsSubductionOptions) -> Option<u32>;

    #[wasm_bindgen(method, getter, js_name = maxResidentTrees)]
    fn max_resident_trees(this: &JsSubductionOptions) -> Option<u32>;

    #[wasm_bindgen(method, getter)]
    fn policy(this: &JsSubductionOptions) -> Option<JsPolicy>;

    #[wasm_bindgen(method, getter, js_name = ephemeralPolicy)]
    fn ephemeral_policy(this: &JsSubductionOptions) -> Option<JsEphemeralPolicy>;

    #[wasm_bindgen(method, getter, js_name = onRemoteHeads)]
    fn on_remote_heads(this: &JsSubductionOptions) -> Option<js_sys::Function>;

    #[wasm_bindgen(method, getter, js_name = onEphemeral)]
    fn on_ephemeral(this: &JsSubductionOptions) -> Option<js_sys::Function>;

    #[wasm_bindgen(method, getter, js_name = defaultTimeoutMilliseconds)]
    fn default_timeout_milliseconds(this: &JsSubductionOptions) -> Option<u32>;
}
