//! Subduction node.

use alloc::{collections::BTreeSet, string::String, sync::Arc, vec::Vec};
use core::{fmt::Debug, time::Duration};
use sedimentree_core::collections::Map;

use from_js_ref::FromJsRef;
use future_form::Local;
use futures::{
    future::{select, Either},
    stream::Aborted,
    FutureExt,
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
    connection::{handshake::DiscoveryId, manager::Spawn, nonce_cache::NonceCache},
    peer::id::PeerId,
    policy::open::OpenPolicy,
    sharded_map::ShardedMap,
    subduction::{pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS, Subduction},
};
use wasm_bindgen::prelude::*;

use wasm_bindgen::JsCast;

use crate::{
    connection::{
        longpoll::{WasmLongPoll, WasmLongPollConn},
        transport::{TransportCallError, WasmUnifiedTransport},
        websocket::WasmWebSocket,
        JsConnection,
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

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
pub struct WasmSubduction {
    core: Arc<
        Subduction<
            'static,
            Local,
            JsStorage,
            WasmUnifiedTransport,
            OpenPolicy,
            JsSigner,
            WasmHashMetric,
            WASM_SHARD_COUNT,
        >,
    >,
    js_storage: JsValue, // helpful for implementations to registering callbacks on the original object

    /// Keeps HTTP long-poll background tasks alive. Each entry corresponds to
    /// the external cancel channel sender for one long-poll connection. Cleared
    /// on `disconnectAll`.
    lp_cancel_guards: alloc::rc::Rc<async_lock::Mutex<alloc::vec::Vec<async_channel::Sender<()>>>>,
}

impl core::fmt::Debug for WasmSubduction {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WasmSubduction")
            .field("js_storage", &self.js_storage)
            .field("lp_cancel_guards", &"[..]")
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
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(JsCast::unchecked_into);
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree, WASM_SHARD_COUNT> =
            ShardedMap::new();
        let max_pending = max_pending_blob_requests.unwrap_or(DEFAULT_MAX_PENDING_BLOB_REQUESTS);
        let (core, listener_fut, manager_fut) = Subduction::new(
            discovery_id,
            signer,
            storage,
            OpenPolicy,
            NonceCache::default(),
            WasmHashMetric(raw_fn),
            sedimentrees,
            WasmSpawn,
            max_pending,
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

        Self {
            core,
            js_storage,
            lp_cancel_guards: alloc::rc::Rc::new(async_lock::Mutex::new(alloc::vec::Vec::new())),
        }
    }

    /// Hydrate a [`Subduction`] instance from external storage.
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
        tracing::debug!("hydrating new Subduction node");
        let js_storage = <JsStorage as AsRef<JsValue>>::as_ref(&storage).clone();
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(JsCast::unchecked_into);
        let discovery_id = service_name.map(|name| DiscoveryId::new(name.as_bytes()));
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree, WASM_SHARD_COUNT> =
            ShardedMap::new();
        let max_pending = max_pending_blob_requests.unwrap_or(DEFAULT_MAX_PENDING_BLOB_REQUESTS);
        let (core, listener_fut, manager_fut) = Subduction::hydrate(
            discovery_id,
            signer,
            storage,
            OpenPolicy,
            NonceCache::default(),
            WasmHashMetric(raw_fn),
            sedimentrees,
            WasmSpawn,
            max_pending,
        )
        .await?;

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

        Ok(Self {
            core,
            js_storage,
            lp_cancel_guards: alloc::rc::Rc::new(async_lock::Mutex::new(alloc::vec::Vec::new())),
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
        let (authenticated, _session_id, cancel_guard) = WasmLongPoll::connect_authenticated(
            base_url,
            signer,
            expected_peer_id,
            timeout_milliseconds.unwrap_or(30_000),
        )
        .await?;

        self.lp_cancel_guards.lock().await.push(cancel_guard);

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
    /// * `service_name` - The service name for discovery (defaults to base_url)
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
        let (authenticated, _session_id, cancel_guard) =
            WasmLongPoll::connect_discover_authenticated(
                base_url,
                signer,
                timeout_milliseconds,
                service_name,
            )
            .await?;

        self.lp_cancel_guards.lock().await.push(cancel_guard);

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
        // Clear long-poll cancel guards so background tasks stop
        self.lp_cancel_guards.lock().await.clear();
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
#[derive(Debug)]
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
#[derive(Debug, Clone)]
pub struct WasmCallError(WasmCallErrorInner);

/// Inner representation for transport call errors.
#[derive(Debug, Clone)]
enum WasmCallErrorInner {
    WebSocket(String),
    LongPoll(String),
}

impl From<TransportCallError> for WasmCallError {
    fn from(err: TransportCallError) -> Self {
        match err {
            TransportCallError::WebSocket(e) => {
                Self(WasmCallErrorInner::WebSocket(alloc::format!("{e:?}")))
            }
            TransportCallError::LongPoll(e) => {
                Self(WasmCallErrorInner::LongPoll(alloc::format!("{e}")))
            }
        }
    }
}

impl From<WasmCallError> for js_sys::Error {
    fn from(err: WasmCallError) -> Self {
        let msg = match &err.0 {
            WasmCallErrorInner::WebSocket(e) => alloc::format!("WebSocket call error: {e}"),
            WasmCallErrorInner::LongPoll(e) => alloc::format!("LongPoll call error: {e}"),
        };
        let js_err = js_sys::Error::new(&msg);
        js_err.set_name("CallError");
        js_err
    }
}
