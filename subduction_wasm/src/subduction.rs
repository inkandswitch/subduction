//! Subduction node.

use alloc::{format, sync::Arc, vec::Vec};
use core::{fmt::Debug, time::Duration};
use sedimentree_core::collections::Map;

use from_js_ref::FromJsRef;
use futures::{
    future::{select, Either},
    stream::Aborted,
    FutureExt,
};
use futures_kind::Local;
use js_sys::Uint8Array;
use sedimentree_core::{
    blob::{Blob, Digest},
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
};
use sedimentree_core::{id::SedimentreeId, sedimentree::Sedimentree};
use subduction_core::{peer::id::PeerId, sharded_map::ShardedMap, Subduction};
use wasm_bindgen::prelude::*;

use crate::{
    connection::{JsConnection, JsConnectionError},
    connection_id::WasmConnectionId,
    depth::JsToDepth,
    digest::{JsDigest, WasmDigest},
    error::{WasmDisconnectionError, WasmHydrationError, WasmIoError, WasmRegistrationError},
    fragment::{WasmFragment, WasmFragmentRequested},
    loose_commit::WasmLooseCommit,
    peer_id::WasmPeerId,
    sedimentree::WasmSedimentree,
    sedimentree_id::WasmSedimentreeId,
    storage::{JsSubductionStorage, JsSubductionStorageError},
};

use super::depth::WasmDepth;

/// Number of shards for the sedimentree map in Wasm (smaller for client-side).
const WASM_SHARD_COUNT: usize = 4;

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
#[derive(Debug)]
pub struct WasmSubduction {
    core: Arc<
        Subduction<
            'static,
            Local,
            JsSubductionStorage,
            JsConnection,
            WasmHashMetric,
            WASM_SHARD_COUNT,
        >,
    >,
    js_storage: JsValue, // helpful for implementations to registering callbacks on the original object
}

#[wasm_bindgen(js_class = Subduction)]
impl WasmSubduction {
    /// Create a new [`Subduction`] instance.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(storage: JsSubductionStorage, hash_metric_override: Option<JsToDepth>) -> Self {
        tracing::debug!("new Subduction node");
        let js_storage = <JsSubductionStorage as AsRef<JsValue>>::as_ref(&storage).clone();
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(JsCast::unchecked_into);
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree, WASM_SHARD_COUNT> =
            ShardedMap::new();
        let (core, listener_fut, actor_fut) =
            Subduction::new(storage, WasmHashMetric(raw_fn), sedimentrees);

        wasm_bindgen_futures::spawn_local(async move {
            let actor = actor_fut.fuse();
            let listener = listener_fut.fuse();

            match select(actor, listener).await {
                Either::Left((actor_result, _pin)) => {
                    if let Err(Aborted) = actor_result {
                        tracing::error!("Subduction actor aborted");
                    }
                }
                Either::Right((listener_result, _pin)) => {
                    if let Err(Aborted) = listener_result {
                        tracing::error!("Subduction listener aborted");
                    }
                }
            }
        });

        Self { core, js_storage }
    }

    /// Hydrate a [`Subduction`] instance from external storage.
    ///
    /// # Errors
    ///
    /// Returns [`WasmHydrationError`] if hydration fails.
    #[wasm_bindgen]
    pub async fn hydrate(
        storage: JsSubductionStorage,
        hash_metric_override: Option<JsToDepth>,
    ) -> Result<Self, WasmHydrationError> {
        tracing::debug!("hydrating new Subduction node");
        let js_storage = <JsSubductionStorage as AsRef<JsValue>>::as_ref(&storage).clone();
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(JsCast::unchecked_into);
        let sedimentrees: ShardedMap<SedimentreeId, Sedimentree, WASM_SHARD_COUNT> =
            ShardedMap::new();
        let (core, listener_fut, actor_fut) =
            Subduction::hydrate(storage, WasmHashMetric(raw_fn), sedimentrees).await?;

        wasm_bindgen_futures::spawn_local(async move {
            let actor = actor_fut.fuse();
            let listener = listener_fut.fuse();

            match select(actor, listener).await {
                Either::Left((actor_result, _pin)) => {
                    if let Err(Aborted) = actor_result {
                        tracing::error!("Subduction actor aborted");
                    }
                }
                Either::Right((listener_result, _pin)) => {
                    if let Err(Aborted) = listener_result {
                        tracing::error!("Subduction listener aborted");
                    }
                }
            }
        });

        Ok(Self { core, js_storage })
    }

    /// Add a Sedimentree.
    ///
    /// # Errors
    ///
    /// Returns [`WasmIoError`] if there is a problem with storage or networking.
    #[wasm_bindgen(js_name = addSedimentree)]
    pub async fn add_sedimentree(
        &self,
        id: &WasmSedimentreeId,
        sedimentree: &WasmSedimentree,
        blobs: Vec<Uint8Array>,
    ) -> Result<(), WasmIoError> {
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

    /// Attach a connection.
    ///
    /// # Errors
    ///
    /// Returns a `WasmIoError` if attaching the connection fails.
    pub async fn attach(&self, conn: JsConnection) -> Result<Registered, WasmIoError> {
        let (is_new, conn_id) = self.core.attach(conn).await.map_err(WasmIoError::from)?;

        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Disconnect a connection by its ID.
    pub async fn disconnect(&self, conn_id: &WasmConnectionId) -> bool {
        self.core.disconnect(&conn_id.clone().into()).await.is_ok()
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

    /// Register a new connection.
    ///
    /// # Errors
    ///
    /// Returns [`WasmRegistrationError`] if the connection is not allowed.
    pub async fn register(&self, conn: JsConnection) -> Result<Registered, WasmRegistrationError> {
        let (is_new, conn_id) = self.core.register(conn).await?;
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Unregister a connection by its ID.
    ///
    /// Returns `true` if the connection was found and unregistered, and `false` otherwise.
    #[must_use]
    pub async fn unregister(&self, conn_id: &WasmConnectionId) -> bool {
        self.core.unregister(&conn_id.clone().into()).await
    }

    /// Get a local blob by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`JsSubductionStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getLocalBlob)]
    pub async fn get_local_blob(
        &self,
        digest: &WasmDigest,
    ) -> Result<Option<Uint8Array>, JsSubductionStorageError> {
        Ok(self
            .core
            .get_local_blob(digest.clone().into())
            .await?
            .map(|blob| Uint8Array::from(blob.as_slice())))
    }

    /// Get all local blobs for a given Sedimentree ID.
    ///
    /// # Errors
    ///
    /// Returns a [`JsSubductionStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getLocalBlobs)]
    pub async fn get_local_blobs(
        &self,
        id: &WasmSedimentreeId,
    ) -> Result<Vec<Uint8Array>, JsSubductionStorageError> {
        #[allow(clippy::expect_used)]
        if let Some(blobs) = self.core.get_local_blobs(id.clone().into()).await? {
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
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = addCommit)]
    pub async fn add_commit(
        &self,
        id: &WasmSedimentreeId,
        commit: &WasmLooseCommit,
        blob: &Uint8Array,
    ) -> Result<Option<WasmFragmentRequested>, WasmIoError> {
        let core_id = id.clone().into();
        let core_commit = commit.clone().into();
        let blob: Blob = blob.clone().to_vec().into();
        let maybe_fragment_requested = self
            .core
            .add_commit(core_id, &core_commit, blob)
            .await
            .map_err(WasmIoError::from)?;

        Ok(maybe_fragment_requested.map(WasmFragmentRequested::from))
    }

    /// Add a fragment with its associated blob to the storage.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = addFragment)]
    pub async fn add_fragment(
        &self,
        id: &WasmSedimentreeId,
        fragment: &WasmFragment,
        blob: &Uint8Array,
    ) -> Result<(), WasmIoError> {
        let owned_id = id.clone().into();
        let owned_fragment = fragment.clone().into();
        let blob: Blob = blob.clone().to_vec().into();
        self.core
            .add_fragment(owned_id, &owned_fragment, blob)
            .await
            .map_err(WasmIoError::from)?;
        Ok(())
    }

    /// Request blobs by their digests from connected peers.
    #[wasm_bindgen(js_name = requestBlobs)]
    pub async fn request_blobs(&self, digests: Vec<JsDigest>) {
        let digests: Vec<_> = digests
            .iter()
            .map(|js_digest| WasmDigest::from(js_digest).into())
            .collect();
        self.core.request_blobs(digests).await;
    }

    /// Request batch sync for a given Sedimentree ID from a specific peer.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestPeerBatchSync)]
    pub async fn request_peer_batch_sync(
        &self,
        to_ask: &WasmPeerId,
        id: &WasmSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerBatchSyncResult, WasmIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, blobs, conn_errors) = self
            .core
            .request_peer_batch_sync(&to_ask.clone().into(), id.clone().into(), timeout)
            .await
            .map_err(WasmIoError::from)?;

        Ok(PeerBatchSyncResult {
            success,
            blobs: blobs
                .into_iter()
                .map(|blob| Uint8Array::from(blob.as_slice()))
                .collect(),
            conn_errors: conn_errors
                .into_iter()
                .map(|(conn, err)| ConnErrPair {
                    conn,
                    err: WasmCallError::from(err),
                })
                .collect(),
        })
    }

    /// Request batch sync for a given Sedimentree ID from all connected peers.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestAllBatchSync)]
    pub async fn request_all_batch_sync(
        &self,
        id: &WasmSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<WasmPeerResultMap, WasmIoError> {
        tracing::debug!("WasmSubduction::request_all_batch_sync");
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let peer_map = self
            .core
            .request_all_batch_sync(id.clone().into(), timeout)
            .await?;
        tracing::debug!("WasmSubduction::request_all_batch_sync - done");
        Ok(WasmPeerResultMap(
            peer_map
                .into_iter()
                .map(|(peer_id, (success, blobs, conn_errs))| {
                    (
                        peer_id,
                        (
                            success,
                            blobs,
                            conn_errs
                                .into_iter()
                                .map(|(conn, err)| (conn, WasmCallError::from(err)))
                                .collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect(),
        ))
    }

    /// Request batch sync for all known Sedimentree IDs from all connected peers.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestAllBatchSyncAll)]
    pub async fn request_all_batch_sync_all(
        &self,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerBatchSyncResult, WasmIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, blobs, errs) = self
            .core
            .request_all_batch_sync_all(timeout)
            .await
            .map_err(WasmIoError::from)?;

        Ok(PeerBatchSyncResult {
            success,
            blobs: blobs
                .into_iter()
                .map(|blob| Uint8Array::from(blob.as_slice()))
                .collect(),
            conn_errors: errs
                .into_iter()
                .map(|(conn, err)| ConnErrPair {
                    conn,
                    err: WasmCallError::from(err),
                })
                .collect(),
        })
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

    /// Get the peer IDs of all connected peers
    #[wasm_bindgen(js_name = getPeerIds)]
    pub async fn peer_ids(&self) -> Vec<WasmPeerId> {
        self.core
            .peer_ids()
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

/// Result of registering a connection.
#[wasm_bindgen(js_name = Registered)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct Registered {
    is_new: bool,
    conn_id: WasmConnectionId,
}

#[wasm_bindgen(js_class = Registered)]
impl Registered {
    /// Whether the connection was newly registered.
    #[must_use]
    #[wasm_bindgen(getter)]
    #[allow(clippy::missing_const_for_fn)]
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// The connection ID of the registered connection.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn conn_id(&self) -> WasmConnectionId {
        self.conn_id.clone()
    }
}

/// Result of a peer batch sync request.
#[wasm_bindgen(js_name = PeerBatchSyncResult)]
#[derive(Debug)]
pub struct PeerBatchSyncResult {
    success: bool,
    blobs: Vec<Uint8Array>,
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

    /// Whether the batch sync was successful with at least one connection.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn blobs(&self) -> Vec<Uint8Array> {
        self.blobs.clone()
    }

    /// List of connection errors that occurred during the batch sync.
    #[must_use]
    #[wasm_bindgen(getter, js_name = connErrors)]
    pub fn conn_errors(&self) -> Vec<ConnErrPair> {
        self.conn_errors.clone()
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
pub struct WasmPeerResultMap(Map<PeerId, (bool, Vec<Blob>, Vec<(JsConnection, WasmCallError)>)>);

#[wasm_bindgen(js_class = PeerResultMap)]
impl WasmPeerResultMap {
    /// Get the result for a specific peer ID.
    #[must_use]
    #[wasm_bindgen(js_name = getResult)]
    pub fn get_result(&self, peer_id: &WasmPeerId) -> Option<PeerBatchSyncResult> {
        self.0
            .get(&peer_id.clone().into())
            .map(|(success, blobs, conn_errs)| PeerBatchSyncResult {
                success: *success,
                blobs: blobs
                    .iter()
                    .map(|blob| Uint8Array::from(blob.as_slice()))
                    .collect(),
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
        for (success, blobs, conn_errs) in self.0.values() {
            results.push(PeerBatchSyncResult {
                success: *success,
                blobs: blobs
                    .iter()
                    .map(|blob| Uint8Array::from(blob.as_slice()))
                    .collect(),
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
    fn to_depth(&self, digest: Digest) -> Depth {
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

/// Wasm wrapper for call errors from the connection.
#[wasm_bindgen(js_name = CallError)]
#[derive(Debug, Clone)]
pub struct WasmCallError(JsConnectionError);

impl From<JsConnectionError> for WasmCallError {
    fn from(err: JsConnectionError) -> Self {
        Self(err)
    }
}

impl From<WasmCallError> for js_sys::Error {
    fn from(err: WasmCallError) -> Self {
        let js_err = js_sys::Error::new(&format!("{:?}", err.0));
        js_err.set_name("CallError");
        js_err
    }
}
