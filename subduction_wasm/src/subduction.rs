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
use futures::FutureExt;
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
    handler::sync::SyncHandler,
    handshake::audience::DiscoveryId,
    multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    spawn::Spawn,
    storage::powerbox::StoragePowerbox,
    subduction::{Subduction, per_peer_sync::PerPeerSync},
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
        WasmInitError, WasmIoError, WasmLongPollConnectError, WasmWriteError,
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
    digest::WasmDigest,
    fragment::WasmFragment,
    loose_commit::WasmLooseCommit,
    sedimentree::WasmSedimentree,
    sedimentree_id::WasmSedimentreeId,
    storage::{JsStorage, JsStorageError},
};

pub use crate::runtime::WasmSpawn;
use crate::runtime::{CLOSE_TIMEOUT, ManagedTransport, Runtime, TransportLease};
use futures::future::AbortHandle;

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

/// Send-counter seed from `Date.now()`, scaled to microseconds to match
/// `subduction_core::peer::counter::wall_clock_seed` — a peer migrating
/// between embedders must not restart below its old high-water mark.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn date_now_micros_seed() -> u64 {
    (js_sys::Date::now() * 1_000.0) as u64
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

type WasmConn = MessageTransport<ManagedTransport>;

type WasmEphemeralHandler =
    EphemeralHandler<Local, WasmConn, JsEphemeralPolicy, JsClock, WasmSpawn>;

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

/// Wasm bindings for [`Subduction`](subduction_core::Subduction).
///
/// # Lifecycle
///
/// Call [`close`](Self::close) to stop work and await owned task cleanup. All
/// asynchronous operations reject with `SubductionClosedError` once closing
/// begins; active calls are cancelled with the same error. Their ordinary
/// return values and error names are otherwise unchanged. Pure synchronous
/// accessors (`storage`, `commitDepth`) remain usable until `free()`.
/// Cancellation is not rollback: already-started JS promises may still finish.
#[wasm_bindgen(js_name = Subduction)]
pub struct WasmSubduction {
    core: Arc<WasmSubductionCore>,
    js_storage: JsValue, // helpful for implementations to registering callbacks on the original object
    ephemeral_handler: WasmEphemeralHandler,
    keyhive_handler: Arc<Mutex<Option<JsFrameHandler>>>,
    runtime: Runtime,
    ephemeral_task: AbortHandle,
}

impl WasmSubduction {
    /// Register using the attempt's existing guard. The caller retains rollback
    /// responsibility until it disarms the guard (both guards for `link`).
    async fn register_transport(
        &self,
        authenticated: subduction_core::authenticated::Authenticated<
            MessageTransport<JsTransport>,
            Local,
        >,
        lease: &TransportLease,
    ) -> Result<bool, WasmAddConnectionError> {
        let peer = authenticated.peer_id();
        let managed = authenticated.map(|_| MessageTransport::new(lease.transport()));
        let fresh = self.core.add_connection(managed).await?;
        if fresh {
            self.ephemeral_handler.subscribe_peer(peer).await;
        }
        Ok(fresh)
    }

    fn start_close(&self) {
        self.ephemeral_task.abort();
        let core = self.core.clone();
        let shutdown_core = core.clone();
        self.runtime.close(
            move || shutdown_core.shutdown(),
            async move {
                drop(core.disconnect_all().await);
            }
            .boxed_local(),
            CLOSE_TIMEOUT,
        );
    }
}

impl Drop for WasmSubduction {
    fn drop(&mut self) {
        self.start_close();
    }
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
    /// # Errors
    ///
    /// Returns a [`WasmInitError`] if the required `signer` or `storage` field
    /// is missing (`undefined`/`null`). The TypeScript interface marks these as
    /// required, so this only fires for callers that bypass the type checker.
    ///
    /// # Panics
    ///
    /// Panics if `hashMetricOverride` is provided but the underlying JS value
    /// cannot be cast to a `Function`.
    #[wasm_bindgen(constructor)]
    #[allow(clippy::too_many_lines, clippy::arc_with_non_send_sync)] // Core APIs use Arc even on Local.
    pub fn new(opts: &JsSubductionOptions) -> Result<Self, WasmInitError> {
        tracing::debug!("new Subduction node");

        let signer = opts.signer().ok_or(WasmInitError::MissingSigner)?;
        let opts_storage = opts.storage().ok_or(WasmInitError::MissingStorage)?;

        let default_roundtrip_timeout = opts
            .default_timeout_milliseconds()
            .map_or(DEFAULT_ROUNDTRIP_TIMEOUT, |ms| {
                Duration::from_millis(u64::from(ms))
            });

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
        // `None` or an explicit `0` falls back to the default cap.
        let max_resident = match opts.max_resident_trees() {
            Some(n) if n > 0 => n as usize,
            _ => WASM_DEFAULT_MAX_RESIDENT_TREES,
        };

        let policy = opts.policy().unwrap_or_else(make_open_policy);

        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let sedimentrees = Arc::new(BoundedShardedMap::new().with_capacity(max_resident));
        let powerbox = StoragePowerbox::new(opts_storage, Arc::new(policy));

        let observer = match opts.on_remote_heads() {
            Some(f) => JsRemoteHeadsObserver::with_callback(f),
            None => JsRemoteHeadsObserver::new(),
        };
        let runtime = Runtime::default();
        let spawner = runtime.spawner();
        let sync_handler = SyncHandler::with_remote_heads_observer(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            powerbox.clone(),
            depth_metric.clone(),
            observer.clone(),
            spawner.clone(),
        )
        // Wall-clock seeded so sequences resume above previous values across
        // page reloads; receivers never reset their high-water marks.
        .with_send_counter(PeerCounter::with_seed(date_now_micros_seed));

        let eph_policy = opts
            .ephemeral_policy()
            .unwrap_or_else(make_open_ephemeral_policy);
        let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
            connections.clone(),
            eph_policy,
            EphemeralConfig::default(),
            JsClock,
            spawner.clone(),
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
            signer,
            sedimentrees,
            connections,
            subscriptions,
            powerbox,
            send_counter,
            NonceCache::default(),
            JsTimeout,
            default_roundtrip_timeout,
            depth_metric,
            spawner.clone(),
        );

        // Independently tracked: completion of one must not drop the other
        // while it is draining dispatch work.
        spawner.spawn(
            async move {
                let _ = manager_fut.await;
            }
            .boxed_local(),
        );
        spawner.spawn(
            async move {
                let _ = listener_fut.await;
            }
            .boxed_local(),
        );

        // Always drain the ephemeral channel to prevent "channel full" warnings
        // in EphemeralHandler when no JS callback is registered.
        let observer = opts
            .on_ephemeral()
            .map(crate::ephemeral::JsEphemeralObserver::new);
        let ephemeral_task = spawner.spawn(
            async move {
                while let Ok(event) = ephemeral_rx.recv().await {
                    if let Some(ref obs) = observer {
                        obs.on_event(event.id, event.sender, &event.payload);
                    }
                }
            }
            .boxed_local(),
        );

        Ok(Self {
            core,
            js_storage,
            ephemeral_handler: ephemeral_for_wasm,
            keyhive_handler,
            runtime,
            ephemeral_task,
        })
    }

    /// Stop accepting work, cancel active API calls, disconnect transports, and
    /// drain owned background tasks. Repeated calls observe the same result.
    ///
    /// Shutdown starts synchronously and continues if the promise is abandoned.
    /// A shared five-second deadline bounds graceful draining; remaining Rust
    /// futures are then cancelled. Already-started JS promises and storage writes
    /// are not rolled back. See the crate README for the full lifecycle contract.
    ///
    /// # Errors
    /// Rejects with `SubductionCloseError` on timeout or transport cleanup failure.
    #[wasm_bindgen(unchecked_return_type = "Promise<void>")]
    pub fn close(&self) -> js_sys::Promise {
        self.start_close();
        let runtime = self.runtime.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            runtime.closed().await?;
            Ok(JsValue::UNDEFINED)
        })
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<WasmPeerResultMap, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
    }

    /// Remove a Sedimentree and all associated data.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = removeSedimentree)]
    pub async fn remove_sedimentree(&self, id: &WasmSedimentreeId) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                self.core.remove_sedimentree(id.clone().into()).await?;
                Ok(())
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let socket = WasmWebSocket::open(address)
                    .await
                    .map_err(WasmConnectError::from)?;
                let transport: JsTransport = JsValue::from(socket).unchecked_into();
                let lease = self.runtime.own(transport.clone());
                let authenticated = WasmAuthenticatedTransport::setup(
                    transport,
                    self.core.signer(),
                    expected_peer_id,
                    None,
                )
                .await
                .map_err(WasmConnectError::from)?;
                let peer = authenticated.inner().peer_id();
                self.register_transport(authenticated.into_inner(), &lease)
                    .await?;
                lease.disarm_cleanup();
                Ok(peer.into())
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let socket = WasmWebSocket::open(address)
                    .await
                    .map_err(WasmConnectError::from)?;
                let transport: JsTransport = JsValue::from(socket).unchecked_into();
                let lease = self.runtime.own(transport.clone());
                let service_name = service_name.unwrap_or_else(|| address.host());
                let authenticated = WasmAuthenticatedTransport::setup_discover(
                    transport,
                    self.core.signer(),
                    Some(service_name),
                    None,
                )
                .await
                .map_err(WasmConnectError::from)?;
                let peer = authenticated.inner().peer_id();
                self.register_transport(authenticated.into_inner(), &lease)
                    .await?;
                lease.disarm_cleanup();
                Ok(peer.into())
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmLongPollConnectError>(async {
                let (authenticated, _) = WasmLongPoll::connect_authenticated(
                    base_url,
                    self.core.signer(),
                    expected_peer_id,
                    &self.runtime.spawner(),
                )
                .await
                .map_err(WasmLongPollConnectError::from)?;
                let peer = authenticated.peer_id();
                let authenticated = authenticated.map(|lp| {
                    MessageTransport::<JsTransport>::new(
                        JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into(),
                    )
                });
                let lease = self.runtime.own(authenticated.inner().inner().clone());
                self.register_transport(authenticated, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer.into())
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmLongPollConnectError>(async {
                let (authenticated, _) = WasmLongPoll::connect_discover_authenticated(
                    base_url,
                    self.core.signer(),
                    service_name,
                    &self.runtime.spawner(),
                )
                .await
                .map_err(WasmLongPollConnectError::from)?;
                let peer = authenticated.peer_id();
                let authenticated = authenticated.map(|lp| {
                    MessageTransport::<JsTransport>::new(
                        JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into(),
                    )
                });
                let lease = self.runtime.own(authenticated.inner().inner().clone());
                self.register_transport(authenticated, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer.into())
            })
            .await
    }

    /// Disconnect from all peers.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmDisconnectionError`] if disconnection was not graceful.
    #[wasm_bindgen(js_name = disconnectAll)]
    pub async fn disconnect_all(&self) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmDisconnectionError>(async { Ok(self.core.disconnect_all().await?) })
            .await
    }

    /// Disconnect from a peer by its ID.
    ///
    /// # Errors
    ///
    /// Returns a `WasmDisconnectionError` if disconnection fails.
    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub async fn disconnect_from_peer(&self, peer_id: &WasmPeerId) -> Result<bool, JsValue> {
        self.runtime
            .run::<_, WasmDisconnectionError>(async {
                Ok(self
                    .core
                    .disconnect_from_peer(&peer_id.clone().into())
                    .await?)
            })
            .await
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
    ) -> Result<bool, JsValue> {
        self.runtime
            .run::<_, WasmAddConnectionError>(async {
                let authenticated = transport.inner().clone();
                let lease = self.runtime.own(authenticated.inner().inner().clone());
                let fresh = self.register_transport(authenticated, &lease).await?;
                lease.disarm_cleanup();
                Ok(fresh)
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let lease = self.runtime.own(transport.clone());
                let authenticated = WasmAuthenticatedTransport::setup_discover(
                    transport,
                    self.core.signer(),
                    Some(service_name),
                    None,
                )
                .await?;

                let peer_id = authenticated.inner().peer_id();
                self.register_transport(authenticated.into_inner(), &lease)
                    .await?;
                lease.disarm_cleanup();
                Ok(peer_id.into())
            })
            .await
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
    ) -> Result<WasmPeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let lease = self.runtime.own(transport.clone());
                let authenticated = WasmAuthenticatedTransport::accept_discover(
                    transport,
                    self.core.signer(),
                    service_name,
                    None,
                )
                .await?;

                let peer_id = authenticated.inner().peer_id();
                self.register_transport(authenticated.into_inner(), &lease)
                    .await?;
                lease.disarm_cleanup();
                Ok(peer_id.into())
            })
            .await
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
    pub async fn link(a: &WasmSubduction, b: &WasmSubduction) -> Result<(), JsValue> {
        a.runtime
            .run::<_, JsValue>(b.runtime.run::<_, WasmConnectError>(async {
                use crate::transport::message_port::WasmMessagePortTransport;

                let channel = web_sys::MessageChannel::new()
                    .map_err(|e| WasmHandshakeError::Transport(e.into()))?;

                let port1 = channel.port1();
                let port2 = channel.port2();
                port1.start();
                port2.start();

                let transport_a: JsTransport =
                    JsValue::from(WasmMessagePortTransport::new(port1.into())).unchecked_into();
                let transport_b: JsTransport =
                    JsValue::from(WasmMessagePortTransport::new(port2.into())).unchecked_into();

                let lease_a = a.runtime.own(transport_a.clone());
                let lease_b = b.runtime.own(transport_b.clone());
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
                    a.register_transport(auth_a.into_inner(), &lease_a),
                    b.register_transport(auth_b.into_inner(), &lease_b),
                )
                .await?;

                tracing::info!(
                    "linked two Subduction instances (new_a={result_a}, new_b={result_b})"
                );

                // Keep both guards armed until the whole link succeeds: if
                // either registration fails, disconnect the other side too.
                lease_a.disarm_cleanup();
                lease_b.disarm_cleanup();
                Ok(())
            }))
            .await
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
    ) -> Result<Option<Uint8Array>, JsValue> {
        self.runtime
            .run::<_, JsStorageError>(async {
                Ok(self
                    .core
                    .get_blob(id.clone().into(), digest.clone().into())
                    .await?
                    .map(|blob| Uint8Array::from(blob.as_slice())))
            })
            .await
    }

    /// Get all local blobs for a given Sedimentree ID.
    ///
    /// # Errors
    ///
    /// Returns a [`JsStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getBlobs)]
    pub async fn get_blobs(&self, id: &WasmSedimentreeId) -> Result<Vec<Uint8Array>, JsValue> {
        self.runtime
            .run::<_, JsStorageError>(async {
                #[allow(clippy::expect_used)]
                if let Some(blobs) = self.core.get_blobs(id.clone().into()).await? {
                    Ok(blobs
                        .into_iter()
                        .map(|blob| Uint8Array::from(blob.as_slice()))
                        .collect())
                } else {
                    Ok(Vec::new())
                }
            })
            .await
    }

    /// Stream local contents then live stored updates without starting network work.
    /// Close the iterator or break out of `for await` to release observation.
    ///
    /// # Errors
    /// Throws if closing; read errors reject iterator reads.
    #[wasm_bindgen(js_name = watchStored)]
    pub fn watch_stored(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.runtime.ensure_open()?;
        crate::stored::wrap(
            self.core.watch_stored(id.clone().into()),
            self.runtime.clone(),
            JsValue::UNDEFINED,
        )
    }

    /// Load local contents, then sync and subscribe on current and future
    /// connections, including reconnects. Missing data or peers do not end the
    /// stream. Attempts run concurrently without timeouts; pass an `AbortSignal`
    /// in options to cancel, or close the iterator.
    /// Peer markers certify local accepted-response processing,
    /// not remote acknowledgement or materialization.
    ///
    /// # Errors
    /// Throws if closing or the signal is already aborted. Later aborts reject
    /// pending reads with the signal's reason; loading errors also reject reads.
    #[wasm_bindgen(js_name = loadAndWatch)]
    pub fn load_and_watch(
        &self,
        id: &WasmSedimentreeId,
        options: Option<crate::stored::JsLoadAndWatchOptions>,
    ) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.runtime.ensure_open()?;
        let signal = crate::stored::signal(options)?;
        crate::stored::wrap(
            self.core.load_and_watch(id.clone().into()),
            self.runtime.clone(),
            signal,
        )
    }

    /// Collect blobs using the shared local-first loader without subscriptions.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = fetchBlobs)]
    pub async fn fetch_blobs(
        &self,
        id: &WasmSedimentreeId,
        timeout_milliseconds: Option<u32>,
    ) -> Result<Option<Vec<Uint8Array>>, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
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
            })
            .await
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
    ) -> Result<Option<WasmFragmentRequested>, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    /// Peers that have not subscribed to this sedimentree receive nothing, so
    /// for a brand-new document follow up with
    /// [`syncWithAllPeers`](Self::sync_with_all_peers) (`subscribe = true`),
    /// or use [`addBatch`](Self::add_batch) which does both. For a durable
    /// write with no propagation, use [`storeCommit`](Self::store_commit).
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
    ) -> Result<Option<WasmFragmentRequested>, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
    }

    /// Add a fragment with its associated blob to the storage, then push it to
    /// authorized subscribers (the `add_*` combinator =
    /// [`storeFragment`](Self::store_fragment) + best-effort push).
    ///
    /// The fragment metadata (including `BlobMeta`) is computed internally from
    /// the provided blob, ensuring consistency by construction.
    ///
    /// Propagation is a best-effort `send` to subscribers; it does not block
    /// on peer acks. Unsubscribed peers receive nothing; see
    /// [`addCommit`](Self::add_commit). For a durable write with no
    /// propagation, use [`storeFragment`](Self::store_fragment).
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<WasmPeerResultMap, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
                let core_id: SedimentreeId = id.clone().into();
                let core_commits = commits
                    .into_iter()
                    .map(WasmCommitInput::into_core)
                    .collect();

                self.core
                    .store_built_batch(core_id, core_commits, Vec::new())
                    .await?;
                Ok(())
            })
            .await
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
    ) -> Result<WasmPeerResultMap, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
                let core_id: SedimentreeId = id.clone().into();
                let core_fragments = fragments
                    .into_iter()
                    .map(WasmFragmentInput::into_core)
                    .collect();

                self.core
                    .store_built_batch(core_id, Vec::new(), core_fragments)
                    .await?;
                Ok(())
            })
            .await
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
    ) -> Result<WasmPeerResultMap, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
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
            })
            .await
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
    ) -> Result<PeerBatchSyncResult, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
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
            })
            .await
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
    ) -> Result<WasmPeerResultMap, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                tracing::debug!("WasmSubduction::sync_with_all_peers");
                let timeout = CallTimeout::from(timeout_milliseconds.map(u64::from));
                let peer_map = self
                    .core
                    .sync_with_all_peers(id.clone().into(), subscribe, timeout)
                    .await?;
                tracing::debug!("WasmSubduction::sync_with_all_peers - done");
                Ok(peer_map.into())
            })
            .await
    }

    /// Sync all known Sedimentree IDs with a single peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer to sync with
    /// * `subscribe` - Whether to subscribe to future updates (default: `true`)
    /// * `timeout_milliseconds` - Optional per-call total deadline in milliseconds; omit to use the configured default
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = fullSyncWithPeer)]
    pub async fn full_sync_with_peer(
        &self,
        peer_id: &WasmPeerId,
        subscribe: Option<bool>,
        timeout_milliseconds: Option<u32>,
    ) -> Result<PeerBatchSyncResult, JsValue> {
        self.runtime
            .run_value(async {
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
            })
            .await
    }

    /// Sync all known Sedimentree IDs with all connected peers.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = fullSyncWithAllPeers)]
    pub async fn full_sync_with_all_peers(
        &self,
        timeout_milliseconds: Option<u32>,
    ) -> Result<PeerBatchSyncResult, JsValue> {
        self.runtime
            .run_value(async {
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
            })
            .await
    }

    // ── Ephemeral messaging ────────────────────────────────────────────

    /// Publish an ephemeral message to all subscribers of a topic.
    ///
    /// The payload is opaque bytes — encoding is the caller's responsibility.
    /// Messages are fire-and-forget; delivery is best-effort.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
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
    pub async fn publish_ephemeral(
        &self,
        topic: &WasmTopic,
        payload: &[u8],
    ) -> Result<(), JsValue> {
        self.runtime
            .run_value(async {
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
            })
            .await
    }

    /// Subscribe to ephemeral messages for the given topics
    /// from all connected peers.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = subscribeEphemeral)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn subscribe_ephemeral(&self, topics: Vec<JsTopic>) -> Result<(), JsValue> {
        self.runtime
            .run_value(async {
                let topics: Vec<Topic> = topics
                    .iter()
                    .map(|t| Topic::from(WasmTopic::from(t)))
                    .collect();
                if let Some(topics) = NonEmpty::from_vec(topics) {
                    self.ephemeral_handler.subscribe(topics).await;
                }
            })
            .await
    }

    /// Unsubscribe from ephemeral messages for the given topics
    /// from all connected peers.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = unsubscribeEphemeral)]
    #[allow(clippy::needless_pass_by_value)] // wasm_bindgen takes owned Vecs.
    pub async fn unsubscribe_ephemeral(&self, topics: Vec<JsTopic>) -> Result<(), JsValue> {
        self.runtime
            .run_value(async {
                let topics: Vec<Topic> = topics
                    .iter()
                    .map(|t| Topic::from(WasmTopic::from(t)))
                    .collect();
                if let Some(topics) = NonEmpty::from_vec(topics) {
                    self.ephemeral_handler.unsubscribe(topics).await;
                }
            })
            .await
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
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = registerFrameHandler)]
    pub async fn register_frame_handler(
        &self,
        handler: Option<JsFrameHandler>,
    ) -> Result<(), JsValue> {
        self.runtime
            .run_value(async {
                *self.keyhive_handler.lock().await = handler;
            })
            .await
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
        self.runtime
            .run::<_, JsValue>(async {
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
            })
            .await
    }

    // ── Queries ──────────────────────────────────────────────────────────

    /// Get all known Sedimentree IDs.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = sedimentreeIds)]
    pub async fn sedimentree_ids(&self) -> Result<Vec<WasmSedimentreeId>, JsValue> {
        self.runtime
            .run_value(async {
                self.core
                    .sedimentree_ids()
                    .await
                    .into_iter()
                    .map(WasmSedimentreeId::from)
                    .collect()
            })
            .await
    }

    /// Get all commits for a given Sedimentree ID.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = getCommits)]
    pub async fn get_commits(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<Option<Vec<WasmLooseCommit>>, JsValue> {
        self.runtime
            .run_value(async {
                self.core
                    .get_commits(id.clone().into())
                    .await
                    .map(|commits| commits.into_iter().map(WasmLooseCommit::from).collect())
            })
            .await
    }

    /// Get all fragments for a given Sedimentree ID.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = getFragments)]
    pub async fn get_fragments(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<Option<Vec<WasmFragment>>, JsValue> {
        self.runtime
            .run_value(async {
                self.core
                    .get_fragments(id.clone().into())
                    .await
                    .map(|fragments| fragments.into_iter().map(WasmFragment::from).collect())
            })
            .await
    }

    /// Get the current heads of a single sedimentree.
    ///
    /// Resolves to `undefined` when the tree is unknown, and to an empty array
    /// for a tree that exists but has no heads yet. To query several trees,
    /// call this once per id (e.g. with `Promise.allSettled`).
    ///
    /// # Errors
    ///
    /// Rejects with an `Error` whose `name` is `"SedimentreeStorageError"` if
    /// reading storage fails. If the storage backend rejected, its original
    /// value is attached as `cause`.
    #[wasm_bindgen(js_name = getHeads)]
    pub async fn get_heads(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<Option<Vec<WasmCommitId>>, JsStorageError> {
        Ok(self
            .core
            .get_heads(id.clone().into())
            .await?
            .map(|heads| heads.into_iter().map(WasmCommitId::from).collect()))
    }

    /// Get the current heads for every locally known sedimentree.
    ///
    /// An inner empty heads array means the sedimentree exists but has no
    /// heads yet.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = getAllHeads)]
    pub async fn get_all_heads(&self) -> Result<Vec<WasmSedimentreeHeads>, JsValue> {
        self.runtime
            .run_value(async {
                self.core
                    .get_all_heads()
                    .await
                    .into_iter()
                    .map(|(id, heads)| WasmSedimentreeHeads {
                        id: id.into(),
                        heads: heads.into_iter().map(WasmCommitId::from).collect(),
                    })
                    .collect()
            })
            .await
    }

    /// Get the peer IDs of all connected peers.
    ///
    /// # Errors
    /// See [lifecycle errors](Self#lifecycle).
    #[wasm_bindgen(js_name = getConnectedPeerIds)]
    pub async fn connected_peer_ids(&self) -> Result<Vec<WasmPeerId>, JsValue> {
        self.runtime
            .run_value(async {
                self.core
                    .connected_peer_ids()
                    .await
                    .into_iter()
                    .map(WasmPeerId::from)
                    .collect()
            })
            .await
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
            Vec<(MessageTransport<ManagedTransport>, WasmCallError)>,
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

    // `signer`/`storage` are required by the TS interface, but a JS caller can
    // omit them; `Option` lets `new` detect absence and throw a clear error
    // instead of silently wrapping `undefined`.
    #[wasm_bindgen(method, getter)]
    fn signer(this: &JsSubductionOptions) -> Option<JsSigner>;

    #[wasm_bindgen(method, getter)]
    fn storage(this: &JsSubductionOptions) -> Option<JsStorage>;

    #[wasm_bindgen(method, getter, js_name = serviceName)]
    fn service_name(this: &JsSubductionOptions) -> Option<String>;

    #[wasm_bindgen(method, getter, js_name = hashMetricOverride)]
    fn hash_metric_override(this: &JsSubductionOptions) -> Option<JsToDepth>;

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
