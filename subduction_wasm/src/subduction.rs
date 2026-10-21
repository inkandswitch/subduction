//! Rust node implementation. Only `api::SubductionApi` is exported to JavaScript.
//!
//! Identifiers, metadata, blobs and sync results stay in their core Rust types
//! here. The public API owns input admission and output conversion; this module
//! owns the runtime, authenticated connections, and background task lifetimes.
#![allow(clippy::missing_errors_doc)] // Methods propagate core/storage/lifecycle errors.

use crate::{
    clock::JsClock,
    error::{
        WasmAddConnectionError, WasmConnectError, WasmDisconnectionError, WasmHandshakeError,
        WasmInitError, WasmIoError, WasmLongPollConnectError, WasmWriteError,
    },
    handler::{JsFrameHandler, WasmComposedHandler, WasmKeyhiveHandler},
    policy::{
        JsPolicy,
        ephemeral::{JsEphemeralPolicy, make_open_ephemeral_policy},
        make_open_policy,
    },
    remote_heads::JsRemoteHeadsObserver,
    runtime::{CLOSE_TIMEOUT, ManagedTransport, Runtime, TransportLease},
    signer::JsSigner,
    transport::{
        DEFAULT_LOCAL_SERVICE_NAME, JsTransport, JsTransportError,
        authenticated::{self, AuthenticatedTransport},
        longpoll::{self, JsTimeout, WasmHttpLongPoll},
        websocket::WasmWebSocket,
    },
    wire::WireMessage,
};
use alloc::{
    boxed::Box,
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use async_lock::Mutex;
use core::time::Duration;
use future_form::Local;
use futures::{FutureExt, future::AbortHandle};
use sedimentree_core::{
    blob::Blob,
    collections::Map,
    crypto::digest::Digest,
    depth::{CountLeadingZeroBytes, Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
};
use sedimentree_wasm::storage::{JsStorage, JsStorageError};
use subduction_core::{
    collections::bounded_sharded_map::BoundedShardedMap,
    handler::sync::SyncHandler,
    handshake::audience::DiscoveryId,
    multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    spawn::Spawn,
    storage::powerbox::StoragePowerbox,
    subduction::{
        Subduction,
        per_peer_sync::{PeerSyncEntry, PerPeerSync},
        request::FragmentRequested,
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
use subduction_keyhive::KeyhiveMessage;
use wasm_bindgen::prelude::*;

#[path = "api.rs"]
mod api;
pub use crate::runtime::WasmSpawn;
pub use api::SubductionApi;

pub(crate) const WASM_SHARD_COUNT: usize = 4;
pub(crate) const WASM_DEFAULT_MAX_RESIDENT_TREES: usize = 1_024;

/// Connection type owned by the node runtime.
pub type NodeConnection = MessageTransport<ManagedTransport>;
/// Core per-peer result, without a JS value wrapper.
pub type PeerSyncResult = PeerSyncEntry<NodeConnection, Local, JsTransportError>;
/// Core broadcast result, without a JS map wrapper.
pub type PeerSyncResults = PerPeerSync<NodeConnection, Local, JsTransportError>;
type NodeCore = Subduction<
    'static,
    Local,
    JsStorage,
    NodeConnection,
    WasmComposedHandler,
    JsPolicy,
    JsSigner,
    JsTimeout,
    WasmSpawn,
    JsDepthMetric,
    WASM_SHARD_COUNT,
>;
type NodeEphemeral = EphemeralHandler<Local, NodeConnection, JsEphemeralPolicy, JsClock, WasmSpawn>;

#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn date_now_micros_seed() -> u64 {
    (js_sys::Date::now() * 1_000.0) as u64
}

/// Node state and lifecycle, never a wasm-bindgen class.
///
/// Async methods run under the same close/cancellation guard as the public API.
/// Already-started JS storage promises are not rolled back by cancellation.
pub struct Node {
    core: Arc<NodeCore>,
    js_storage: JsValue,
    ephemeral_handler: NodeEphemeral,
    keyhive_handler: Arc<Mutex<Option<JsFrameHandler>>>,
    runtime: Runtime,
    ephemeral_task: AbortHandle,
}
impl core::fmt::Debug for Node {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Node")
            .field("js_storage", &self.js_storage)
            .finish_non_exhaustive()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.start_close();
    }
}

impl Node {
    /// Construct the runtime from admitted service references and options.
    ///
    /// # Panics
    /// Custom depth metrics must satisfy the existing infallible metric contract.
    #[allow(clippy::too_many_lines, clippy::arc_with_non_send_sync)]
    pub fn new(opts: &JsSubductionOptions) -> Result<Self, WasmInitError> {
        let signer = opts.signer().ok_or(WasmInitError::MissingSigner)?;
        let opts_storage = opts.storage().ok_or(WasmInitError::MissingStorage)?;
        let default_roundtrip_timeout = opts
            .default_timeout_milliseconds()
            .map_or(DEFAULT_ROUNDTRIP_TIMEOUT, |ms| {
                Duration::from_millis(u64::from(ms))
            });
        let js_storage = <JsStorage as AsRef<JsValue>>::as_ref(&opts_storage).clone();
        let depth_metric = JsDepthMetric(opts.hash_metric_override());
        let discovery_id = opts
            .service_name()
            .map(|name| DiscoveryId::new(name.as_bytes()));
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
            observer,
            spawner.clone(),
        )
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
        let ephemeral_for_node = ephemeral_handler.clone();
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
        // Independently tracked: completing one must not drop the other's work.
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
        let observer = opts
            .on_ephemeral()
            .map(crate::ephemeral::JsEphemeralObserver::new);
        let ephemeral_task = spawner.spawn(
            async move {
                // Always drain, including when no observer is installed.
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
            ephemeral_handler: ephemeral_for_node,
            keyhive_handler,
            runtime,
            ephemeral_task,
        })
    }

    async fn register_transport(
        &self,
        authenticated: AuthenticatedTransport,
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
    /// Start bounded shutdown immediately, sharing its completion across callers.
    pub fn close(&self) -> js_sys::Promise {
        self.start_close();
        let runtime = self.runtime.clone();
        wasm_bindgen_futures::future_to_promise(async move {
            runtime.closed().await?;
            Ok(JsValue::UNDEFINED)
        })
    }
    /// Caller-owned storage service.
    #[must_use]
    pub fn storage(&self) -> JsValue {
        self.js_storage.clone()
    }

    /// Authenticate a known WebSocket peer before registering it.
    pub async fn connect(&self, address: &web_sys::Url, peer: &PeerId) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let socket = WasmWebSocket::open(address).await?;
                let transport: JsTransport = JsValue::from(socket).unchecked_into();
                let lease = self.runtime.own(transport.clone());
                let auth = authenticated::setup(transport, self.core.signer(), peer).await?;
                let peer = auth.peer_id();
                self.register_transport(auth, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer)
            })
            .await
    }
    /// Discover and authenticate a WebSocket peer.
    pub async fn connect_discover(
        &self,
        address: &web_sys::Url,
        service: Option<String>,
    ) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let socket = WasmWebSocket::open(address).await?;
                let transport: JsTransport = JsValue::from(socket).unchecked_into();
                let lease = self.runtime.own(transport.clone());
                let auth = authenticated::setup_discover(
                    transport,
                    self.core.signer(),
                    Some(service.unwrap_or_else(|| address.host())),
                )
                .await?;
                let peer = auth.peer_id();
                self.register_transport(auth, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer)
            })
            .await
    }
    /// Authenticate a known long-poll peer using runtime-owned polling tasks.
    pub async fn connect_long_poll(&self, url: &str, peer: &PeerId) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmLongPollConnectError>(async {
                let (auth, _) = longpoll::connect_authenticated(
                    url,
                    self.core.signer(),
                    peer,
                    &self.runtime.spawner(),
                )
                .await?;
                self.register_long_poll(auth).await
            })
            .await
    }
    /// Discover and authenticate a long-poll peer.
    pub async fn connect_discover_long_poll(
        &self,
        url: &str,
        service: Option<String>,
    ) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmLongPollConnectError>(async {
                let (auth, _) = longpoll::connect_discover_authenticated(
                    url,
                    self.core.signer(),
                    service,
                    &self.runtime.spawner(),
                )
                .await?;
                self.register_long_poll(auth).await
            })
            .await
    }
    async fn register_long_poll(
        &self,
        auth: subduction_core::authenticated::Authenticated<
            subduction_http_longpoll::transport::HttpLongPollTransport,
            Local,
        >,
    ) -> Result<PeerId, WasmLongPollConnectError> {
        let peer = auth.peer_id();
        let auth = auth.map(|lp| {
            MessageTransport::<JsTransport>::new(
                JsValue::from(WasmHttpLongPoll::new(lp)).unchecked_into(),
            )
        });
        let lease = self.runtime.own(auth.inner().inner().clone());
        self.register_transport(auth, &lease).await?;
        lease.disarm_cleanup();
        Ok(peer)
    }
    /// Register a core authentication witness. Never accepts a JS identity claim.
    pub async fn add_connection(
        &self,
        transport: &AuthenticatedTransport,
    ) -> Result<bool, JsValue> {
        self.runtime
            .run::<_, WasmAddConnectionError>(async {
                let lease = self.runtime.own(transport.inner().inner().clone());
                let fresh = self.register_transport(transport.clone(), &lease).await?;
                lease.disarm_cleanup();
                Ok(fresh)
            })
            .await
    }
    /// Initiate discovery on a custom transport (Rust integration entrypoint).
    pub async fn connect_transport(
        &self,
        transport: JsTransport,
        service: String,
    ) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let lease = self.runtime.own(transport.clone());
                let auth =
                    authenticated::setup_discover(transport, self.core.signer(), Some(service))
                        .await?;
                let peer = auth.peer_id();
                self.register_transport(auth, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer)
            })
            .await
    }
    /// Accept discovery on a custom transport (Rust integration entrypoint).
    pub async fn accept_transport(
        &self,
        transport: JsTransport,
        service: String,
    ) -> Result<PeerId, JsValue> {
        self.runtime
            .run::<_, WasmConnectError>(async {
                let lease = self.runtime.own(transport.clone());
                let auth =
                    authenticated::accept_discover(transport, self.core.signer(), service).await?;
                let peer = auth.peer_id();
                self.register_transport(auth, &lease).await?;
                lease.disarm_cleanup();
                Ok(peer)
            })
            .await
    }
    /// Link local nodes, retaining both rollback guards until both registrations succeed.
    pub async fn link(a: &Self, b: &Self) -> Result<(), JsValue> {
        a.runtime
            .run::<_, JsValue>(b.runtime.run::<_, WasmConnectError>(async {
                let channel = web_sys::MessageChannel::new()
                    .map_err(|e| WasmHandshakeError::Transport(e.into()))?;
                let port1 = channel.port1();
                let port2 = channel.port2();
                port1.start();
                port2.start();
                let ta = crate::factories::message_port(port1.into());
                let tb = crate::factories::message_port(port2.into());
                let lease_a = a.runtime.own(ta.clone());
                let lease_b = b.runtime.own(tb.clone());
                let (auth_a, auth_b) = futures::future::try_join(
                    authenticated::setup_discover(
                        ta,
                        a.core.signer(),
                        Some(DEFAULT_LOCAL_SERVICE_NAME.into()),
                    ),
                    authenticated::accept_discover(
                        tb,
                        b.core.signer(),
                        DEFAULT_LOCAL_SERVICE_NAME.into(),
                    ),
                )
                .await?;
                futures::future::try_join(
                    a.register_transport(auth_a, &lease_a),
                    b.register_transport(auth_b, &lease_b),
                )
                .await?;
                lease_a.disarm_cleanup();
                lease_b.disarm_cleanup();
                Ok(())
            }))
            .await
    }
    /// Disconnect every peer without closing the node.
    pub async fn disconnect_all(&self) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmDisconnectionError>(async { Ok(self.core.disconnect_all().await?) })
            .await
    }
    /// Disconnect all connections to a peer.
    pub async fn disconnect_from_peer(&self, peer: &PeerId) -> Result<bool, JsValue> {
        self.runtime
            .run::<_, WasmDisconnectionError>(async {
                Ok(self.core.disconnect_from_peer(peer).await?)
            })
            .await
    }
    /// Remove a tree and its local data.
    pub async fn remove_sedimentree(&self, id: &SedimentreeId) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                self.core.remove_sedimentree(*id).await?;
                Ok(())
            })
            .await
    }
    /// Read one owned blob.
    pub async fn get_blob(
        &self,
        id: &SedimentreeId,
        digest: &Digest<Blob>,
    ) -> Result<Option<Blob>, JsValue> {
        self.runtime
            .run::<_, JsStorageError>(self.core.get_blob(*id, *digest))
            .await
    }
    /// Read owned blobs; an unknown tree has no local blobs.
    pub async fn get_blobs(&self, id: &SedimentreeId) -> Result<Vec<Blob>, JsValue> {
        self.runtime
            .run::<_, JsStorageError>(async {
                Ok(self
                    .core
                    .get_blobs(*id)
                    .await?
                    .map(Vec::from)
                    .unwrap_or_default())
            })
            .await
    }
    /// Local-first collection without subscriptions.
    pub async fn fetch_blobs(
        &self,
        id: &SedimentreeId,
        timeout: Option<u32>,
    ) -> Result<Option<Vec<Blob>>, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                self.core
                    .fetch_blobs(*id, call_timeout(timeout))
                    .await
                    .map(|blobs| blobs.map(Vec::from))
                    .map_err(Into::into)
            })
            .await
    }
    /// Observe storage using the shared iterator boundary adapter.
    pub fn watch_stored(
        &self,
        id: &SedimentreeId,
    ) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.runtime.ensure_open()?;
        crate::stored::wrap(
            self.core.watch_stored(*id),
            self.runtime.clone(),
            JsValue::UNDEFINED,
        )
    }
    /// Local-first observation, including current and future connections.
    pub fn load_and_watch(
        &self,
        id: &SedimentreeId,
        options: Option<crate::stored::JsLoadAndWatchOptions>,
    ) -> Result<crate::stored::JsStoredUpdates, JsValue> {
        self.runtime.ensure_open()?;
        let signal = crate::stored::signal(options)?;
        crate::stored::wrap(self.core.load_and_watch(*id), self.runtime.clone(), signal)
    }
    /// Persist a commit using core values (also used by lifecycle integration tests).
    pub async fn store_commit(
        &self,
        id: &SedimentreeId,
        head: &CommitId,
        parents: Vec<CommitId>,
        blob: Blob,
    ) -> Result<Option<FragmentRequested>, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
                Ok(self
                    .core
                    .store_commit(
                        *id,
                        *head,
                        parents.into_iter().collect::<BTreeSet<_>>(),
                        blob,
                    )
                    .await?)
            })
            .await
    }
    /// Persist a commit and push to subscribers.
    pub async fn add_commit(
        &self,
        id: &SedimentreeId,
        head: &CommitId,
        parents: Vec<CommitId>,
        blob: Blob,
    ) -> Result<Option<FragmentRequested>, JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
                Ok(self
                    .core
                    .add_commit(
                        *id,
                        *head,
                        parents.into_iter().collect::<BTreeSet<_>>(),
                        blob,
                    )
                    .await?)
            })
            .await
    }
    /// Persist a batch of core metadata/blob pairs.
    pub async fn store_built_batch(
        &self,
        id: &SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> Result<(), JsValue> {
        self.runtime
            .run::<_, WasmWriteError>(async {
                Ok(self.core.store_built_batch(*id, commits, fragments).await?)
            })
            .await
    }
    /// Sync one tree with a peer, preserving the core result and error identities.
    pub async fn sync_with_peer(
        &self,
        peer: &PeerId,
        id: &SedimentreeId,
        subscribe: bool,
        timeout: Option<u32>,
    ) -> Result<PeerSyncResult, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                Ok(self
                    .core
                    .sync_with_peer(peer, *id, subscribe, call_timeout(timeout))
                    .await?)
            })
            .await
    }
    /// Sync one tree with all peers.
    pub async fn sync_with_all_peers(
        &self,
        id: &SedimentreeId,
        subscribe: bool,
        timeout: Option<u32>,
    ) -> Result<PeerSyncResults, JsValue> {
        self.runtime
            .run::<_, WasmIoError>(async {
                Ok(self
                    .core
                    .sync_with_all_peers(*id, subscribe, call_timeout(timeout))
                    .await?)
            })
            .await
    }
    /// Sync all known trees with one peer. Preserve existing I/O-error logging.
    pub async fn full_sync_with_peer(
        &self,
        peer: &PeerId,
        subscribe: Option<bool>,
        timeout: Option<u32>,
    ) -> Result<PeerSyncResult, JsValue> {
        self.runtime
            .run_value(async {
                let (success, stats, errors, io_errors) = self
                    .core
                    .full_sync_with_peer(peer, subscribe.unwrap_or(true), call_timeout(timeout))
                    .await;
                for (id, err) in &io_errors {
                    tracing::error!(
                        "full_sync_with_peer I/O error for sedimentree {:?}: {}",
                        id,
                        err
                    );
                }
                (success, stats, errors)
            })
            .await
    }
    /// Sync all known trees with all peers. Preserve existing I/O-error logging.
    pub async fn full_sync_with_all_peers(
        &self,
        timeout: Option<u32>,
    ) -> Result<PeerSyncResult, JsValue> {
        self.runtime
            .run_value(async {
                let (success, stats, errors, io_errors) = self
                    .core
                    .full_sync_with_all_peers(call_timeout(timeout))
                    .await;
                for (id, err) in &io_errors {
                    tracing::error!(
                        "full_sync_with_all_peers I/O error for sedimentree {:?}: {}",
                        id,
                        err
                    );
                }
                (success, stats, errors)
            })
            .await
    }
    /// Publish an opaque ephemeral payload.
    ///
    /// # Panics
    /// Panics if the platform random number generator fails.
    #[allow(
        clippy::expect_used,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub async fn publish_ephemeral(&self, topic: &Topic, payload: &[u8]) -> Result<(), JsValue> {
        self.runtime
            .run_value(async {
                let mut buf = [0u8; 8];
                getrandom::getrandom(&mut buf).expect("getrandom failed");
                let ep = EphemeralPayload {
                    id: *topic,
                    nonce: u64::from_le_bytes(buf),
                    timestamp: TimestampSeconds::new((js_sys::Date::now() / 1000.0) as u64),
                    payload: payload.to_vec(),
                };
                let verified = Signed::seal::<Local, _>(self.core.signer(), ep).await;
                self.ephemeral_handler
                    .publish(EphemeralMessage::Ephemeral(Box::new(
                        verified.into_signed(),
                    )))
                    .await;
            })
            .await
    }
    /// Replace or unregister the keyhive handler.
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
    /// Send an opaque keyhive frame to a connected peer.
    pub async fn send_keyhive_message(&self, payload: &[u8], peer: &PeerId) -> Result<(), JsValue> {
        self.runtime
            .run::<_, JsValue>(async {
                use subduction_core::connection::Connection;
                let conn = self
                    .core
                    .get_connection(peer)
                    .await
                    .ok_or_else(|| JsValue::from_str("no connection for peer"))?;
                conn.send(&WireMessage::Keyhive(KeyhiveMessage::new(payload.to_vec())))
                    .await
                    .map_err(|e| JsValue::from_str(&e.to_string()))
            })
            .await
    }
    /// List known trees.
    pub async fn sedimentree_ids(&self) -> Result<Vec<SedimentreeId>, JsValue> {
        self.runtime.run_value(self.core.sedimentree_ids()).await
    }
    /// Read core commit metadata.
    pub async fn get_commits(
        &self,
        id: &SedimentreeId,
    ) -> Result<Option<Vec<LooseCommit>>, JsValue> {
        self.runtime.run_value(self.core.get_commits(*id)).await
    }
    /// Read core fragment metadata.
    pub async fn get_fragments(
        &self,
        id: &SedimentreeId,
    ) -> Result<Option<Vec<Fragment>>, JsValue> {
        self.runtime.run_value(self.core.get_fragments(*id)).await
    }
    /// Read a tree's heads, preserving storage errors and their JS causes.
    pub async fn get_heads(&self, id: &SedimentreeId) -> Result<Option<Vec<CommitId>>, JsValue> {
        self.runtime
            .run::<_, JsStorageError>(self.core.get_heads(*id))
            .await
    }
    /// Read heads for all locally known trees.
    pub async fn get_all_heads(&self) -> Result<Vec<(SedimentreeId, Vec<CommitId>)>, JsValue> {
        self.runtime.run_value(self.core.get_all_heads()).await
    }
    /// List authenticated peers.
    pub async fn connected_peer_ids(&self) -> Result<Vec<PeerId>, JsValue> {
        self.runtime
            .run_value(async { self.core.connected_peer_ids().await.into_iter().collect() })
            .await
    }
}
fn call_timeout(ms: Option<u32>) -> CallTimeout {
    CallTimeout::from(ms.map(u64::from))
}

/// Behavioral adapter for an optional JS metric, not a value wrapper or JS class.
#[derive(Debug, Clone)]
pub(crate) struct JsDepthMetric(Option<js_sys::Function>);
impl DepthMetric for JsDepthMetric {
    #[allow(clippy::expect_used)] // The core metric trait is infallible.
    fn to_depth(&self, id: CommitId) -> Depth {
        if let Some(func) = &self.0 {
            let id = sedimentree_wasm::pojo::hex(id.as_bytes());
            let value = func
                .call1(&JsValue::NULL, &id.into())
                .expect("depth metric callback failed");
            Depth(
                sedimentree_wasm::pojo::u32_value(&value, "hashMetricOverride return value")
                    .expect("invalid depth metric result"),
            )
        } else {
            CountLeadingZeroBytes.to_depth(id)
        }
    }
}

#[wasm_bindgen]
extern "C" {
    /// Admitted constructor options; only service references remain live.
    #[wasm_bindgen(js_name = SubductionOptions, typescript_type = "SubductionOptions")]
    pub type JsSubductionOptions;
    #[wasm_bindgen(method, getter)]
    fn signer(this: &JsSubductionOptions) -> Option<JsSigner>;
    #[wasm_bindgen(method, getter)]
    fn storage(this: &JsSubductionOptions) -> Option<JsStorage>;
    #[wasm_bindgen(method, getter, js_name = serviceName)]
    fn service_name(this: &JsSubductionOptions) -> Option<String>;
    #[wasm_bindgen(method, getter, js_name = hashMetricOverride)]
    fn hash_metric_override(this: &JsSubductionOptions) -> Option<js_sys::Function>;
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
