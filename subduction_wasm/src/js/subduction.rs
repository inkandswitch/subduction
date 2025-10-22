//! Subduction node.

use std::{collections::HashMap, convert::Infallible, fmt::Debug, rc::Rc, time::Duration};

use futures::lock::Mutex;
use js_sys::Uint8Array;
use sedimentree_core::{
    commit::{CountLeadingZeroBytes, DepthStrategy},
    future::Local,
    Blob, Depth, Digest,
};
use subduction_core::{connection::Connection, peer::id::PeerId, Subduction};
use thiserror::Error;
use wasm_bindgen::prelude::*;

use crate::js::{
    connection_callback_reader::JsConnectionCallbackReader,
    connection_id::JsConnectionId,
    digest::JsDigest,
    error::{JsCallError, JsConnectionDisallowed, JsIoError, JsListenError},
    fragment::{JsFragment, JsFragmentRequested},
    loose_commit::JsLooseCommit,
    peer_id::JsPeerId,
    sedimentree_id::JsSedimentreeId,
    storage::JsStorage,
    websocket::JsWebSocket,
};

use super::{
    depth::{JsDepthRef, JsToDepth},
    storage::JsStorageError,
};

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
#[derive(Debug)]
pub struct JsSubduction {
    core: Subduction<Local, JsStorage, JsConnectionCallbackReader<JsWebSocket>, JsHashMetric>,
    commit_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
    fragment_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
    blob_callbacks: Rc<Mutex<Vec<js_sys::Function>>>,
}

#[wasm_bindgen(js_class = Subduction)]
impl JsSubduction {
    /// Create a new [`Subduction`] instance.
    #[must_use]
    #[wasm_bindgen(constructor)]
    pub fn new(storage: JsStorage, hash_metric_override: Option<JsToDepth>) -> Self {
        let raw_fn: Option<js_sys::Function> = hash_metric_override.map(JsCast::unchecked_into);

        Self {
            core: Subduction::new(
                HashMap::new(),
                storage,
                HashMap::new(),
                JsHashMetric(raw_fn),
            ),
            commit_callbacks: Rc::new(Mutex::new(Vec::new())),
            fragment_callbacks: Rc::new(Mutex::new(Vec::new())),
            blob_callbacks: Rc::new(Mutex::new(Vec::new())),
        }
    }

    /// Run the Subduction instance.
    ///
    /// # Errors
    ///
    /// Returns a `JsListenError` if the instance fails to run.
    pub async fn run(&self) -> Result<(), JsListenError> {
        self.core.run().await?;
        Ok(())
    }

    /// Attach a connection.
    ///
    /// # Errors
    ///
    /// Returns a `JsIoError` if attaching the connection fails.
    pub async fn attach(&self, conn: JsWebSocket) -> Result<Registered, JsIoError> {
        let conn_with_callbacks = JsConnectionCallbackReader {
            conn,
            commit_callbacks: self.commit_callbacks.clone(),
            fragment_callbacks: self.fragment_callbacks.clone(),
            blob_callbacks: self.blob_callbacks.clone(),
        };

        let (is_new, conn_id) = self
            .core
            .attach(conn_with_callbacks)
            .await
            .map_err(JsIoError::from)?;

        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Disconnect a connection by its ID.
    pub async fn disconnect(&self, js_conn_id: JsConnectionId) -> bool {
        self.core
            .disconnect(&js_conn_id.into())
            .await
            .unwrap_or_else(|e: Infallible| match e {})
    }

    /// Disconnect from a peer by its ID.
    ///
    /// # Errors
    ///
    /// Returns a `JsDisconnectionError` if disconnection fails.
    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub async fn disconnect_from_peer(
        &self,
        peer_id: JsPeerId,
    ) -> Result<bool, JsDisconnectionError> {
        Ok(self.core.disconnect_from_peer(&peer_id.into()).await?)
    }

    /// Register a new connection.
    ///
    /// # Errors
    ///
    /// Returns [`JsConnectionDisallowed`] if the connection is not allowed.
    pub async fn register(&self, conn: JsWebSocket) -> Result<Registered, JsConnectionDisallowed> {
        let conn_with_callbacks = JsConnectionCallbackReader {
            conn,
            commit_callbacks: self.commit_callbacks.clone(),
            fragment_callbacks: self.fragment_callbacks.clone(),
            blob_callbacks: self.blob_callbacks.clone(),
        };
        let (is_new, conn_id) = self.core.register(conn_with_callbacks).await?;
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Unregister a connection by its ID.
    ///
    /// Returns `true` if the connection was found and unregistered, and `false` otherwise.
    pub async fn unregister(&self, conn_id: JsConnectionId) -> bool {
        self.core.unregister(&conn_id.into()).await
    }

    /// Add a callback for commit events.
    #[wasm_bindgen(js_name = onCommit)]
    pub async fn on_commit(&self, callback: js_sys::Function) {
        let mut lock = self.commit_callbacks.lock().await;
        lock.push(callback);
    }

    /// Remove a callback for commit events.
    #[wasm_bindgen(js_name = offCommit)]
    pub async fn off_commit(&self, callback: js_sys::Function) {
        let mut lock = self.commit_callbacks.lock().await;
        lock.retain(|cb| cb != &callback);
    }

    /// Add a callback for fragment events.
    #[wasm_bindgen(js_name = onFragment)]
    pub async fn on_fragment(&self, callback: js_sys::Function) {
        let mut lock = self.fragment_callbacks.lock().await;
        lock.push(callback);
    }

    /// Remove a callback for fragment events.
    #[wasm_bindgen(js_name = offFragment)]
    pub async fn off_fragment(&self, callback: js_sys::Function) {
        let mut lock = self.fragment_callbacks.lock().await;
        lock.retain(|cb| cb != &callback);
    }

    /// Add a callback for blob events.
    #[wasm_bindgen(js_name = onBlob)]
    pub async fn on_blob(&self, callback: js_sys::Function) {
        let mut lock = self.fragment_callbacks.lock().await;
        lock.push(callback);
    }

    /// Remove a callback for blob events.
    #[wasm_bindgen(js_name = offBlob)]
    pub async fn off_blob(&self, callback: js_sys::Function) {
        let mut lock = self.fragment_callbacks.lock().await;
        lock.retain(|cb| cb != &callback);
    }

    /// Get a local blob by its digest.
    ///
    /// # Errors
    ///
    /// Returns a [`JsStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getLocalBlob)]
    pub async fn get_local_blob(
        &self,
        digest: JsDigest,
    ) -> Result<Option<Uint8Array>, JsStorageError> {
        Ok(self
            .core
            .get_local_blob(digest.into())
            .await?
            .map(|blob| Uint8Array::from(blob.as_slice())))
    }

    /// Get all local blobs for a given Sedimentree ID.
    ///
    /// # Errors
    ///
    /// Returns a [`JsStorageError`] if JS storage fails.
    #[wasm_bindgen(js_name = getLocalBlobs)]
    pub async fn get_local_blobs(
        &self,
        id: JsSedimentreeId,
    ) -> Result<Vec<Uint8Array>, JsStorageError> {
        #[allow(clippy::expect_used)]
        if let Some(blobs) = self.core.get_local_blobs(id.into()).await? {
            Ok(blobs
                .into_iter()
                .map(|blob| Uint8Array::from(blob.as_slice()))
                .collect())
        } else {
            Ok(vec![])
        }
    }

    /// Fetch blobs by their digests, with an optional timeout in milliseconds.
    ///
    /// # Errors
    ///
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = fetchBlobs)]
    pub async fn fetch_blobs(
        &self,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<Option<Vec<Uint8Array>>, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        if let Some(blobs) = self
            .core
            .fetch_blobs(id.into(), timeout)
            .await
            .map_err(JsIoError::from)?
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
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = addCommit)]
    pub async fn add_commit(
        &self,
        id: JsSedimentreeId,
        commit: &JsLooseCommit,
        blob: &Uint8Array,
    ) -> Result<Option<JsFragmentRequested>, JsIoError> {
        let maybe_fragment_requested = self
            .core
            .add_commit(
                id.into(),
                &commit.clone().into(),
                Blob::from(blob.clone().to_vec()),
            )
            .await
            .map_err(JsIoError::from)?;

        Ok(maybe_fragment_requested.map(JsFragmentRequested::from))
    }

    /// Add a fragment with its associated blob to the storage.
    ///
    /// # Errors
    ///
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = addFragment)]
    pub async fn add_fragment(
        &self,
        id: JsSedimentreeId,
        fragment: &JsFragment,
        blob: &Uint8Array,
    ) -> Result<(), JsIoError> {
        let blob: Blob = blob.clone().to_vec().into();
        self.core
            .add_fragment(id.into(), &fragment.clone().into(), blob)
            .await
            .map_err(JsIoError::from)?;
        Ok(())
    }

    /// Request blobs by their digests from connected peers.
    #[wasm_bindgen(js_name = requestBlobs)]
    pub async fn request_blobs(&self, digests: Vec<JsDigest>) {
        let digests: Vec<_> = digests.into_iter().map(Into::into).collect();
        self.core.request_blobs(digests).await;
    }

    /// Request batch sync for a given Sedimentree ID from a specific peer.
    ///
    /// # Errors
    ///
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestPeerBatchSync)]
    pub async fn request_peer_batch_sync(
        &self,
        to_ask: JsPeerId,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerBatchSyncResult, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, conn_errors) = self
            .core
            .request_peer_batch_sync(&to_ask.into(), id.into(), timeout)
            .await
            .map_err(JsIoError::from)?;

        Ok(PeerBatchSyncResult {
            success,
            conn_errors: conn_errors
                .into_iter()
                .map(|(ws, err)| ConnErrPair {
                    ws: ws.conn.clone(),
                    err: JsCallError::from(err),
                })
                .collect(),
        })
    }

    /// Request batch sync for a given Sedimentree ID from all connected peers.
    ///
    /// # Errors
    ///
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestAllBatchSync)]
    pub async fn request_all_batch_sync(
        &self,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerResultMap, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let peer_map = self.core.request_all_batch_sync(id.into(), timeout).await?;
        Ok(PeerResultMap(
            peer_map
                .into_iter()
                .map(|(peer_id, (success, conn_errs))| {
                    (
                        peer_id,
                        (
                            success,
                            conn_errs
                                .into_iter()
                                .map(|(ws, err)| (ws.conn.clone(), JsCallError::from(err)))
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
    /// Returns a [`JsIoError`] if storage or networking fail.
    #[wasm_bindgen(js_name = requestAllBatchSyncAll)]
    pub async fn request_all_batch_sync_all(
        &self,
        timeout_milliseconds: Option<u64>,
    ) -> Result<bool, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        self.core
            .request_all_batch_sync_all(timeout)
            .await
            .map_err(JsIoError::from)
    }

    /// Get all known Sedimentree IDs
    #[wasm_bindgen(js_name = sedimentreeIds)]
    pub async fn seidmentree_ids(&self) -> Vec<JsSedimentreeId> {
        self.core
            .sedimentree_ids()
            .await
            .into_iter()
            .map(JsSedimentreeId::from)
            .collect()
    }

    /// Get all commits for a given Sedimentree ID
    #[wasm_bindgen(js_name = getCommits)]
    pub async fn get_commits(&self, id: JsSedimentreeId) -> Option<Vec<JsLooseCommit>> {
        self.core
            .get_commits(id.into())
            .await
            .map(|commits| commits.into_iter().map(JsLooseCommit::from).collect())
    }

    /// Get all fragments for a given Sedimentree ID
    #[wasm_bindgen(js_name = getFragments)]
    pub async fn get_fragments(&self, id: JsSedimentreeId) -> Option<Vec<JsFragment>> {
        self.core
            .get_fragments(id.into())
            .await
            .map(|fragments| fragments.into_iter().map(JsFragment::from).collect())
    }

    /// Get the peer IDs of all connected peers
    #[wasm_bindgen(js_name = getPeerIds)]
    pub async fn peer_ids(&self) -> Vec<JsPeerId> {
        self.core
            .peer_ids()
            .await
            .into_iter()
            .map(JsPeerId::from)
            .collect()
    }
}

/// Result of registering a connection.
#[wasm_bindgen(js_name = Registered)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(missing_copy_implementations)]
pub struct Registered {
    is_new: bool,
    conn_id: JsConnectionId,
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
    pub fn conn_id(&self) -> JsConnectionId {
        self.conn_id.clone()
    }
}

/// Result of a peer batch sync request.
#[wasm_bindgen(js_name = PeerBatchSyncResult)]
#[derive(Debug)]
pub struct PeerBatchSyncResult {
    success: bool,
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

    /// List of connection errors that occurred during the batch sync.
    #[must_use]
    #[wasm_bindgen(getter, js_name = connErrors)]
    pub fn conn_errors(&self) -> Vec<ConnErrPair> {
        self.conn_errors.clone()
    }
}

/// A pair of a WebSocket connection and an error that occurred during a call.
#[wasm_bindgen(js_name = ConnErrorPair)]
#[derive(Debug, Clone)]
pub struct ConnErrPair {
    ws: JsWebSocket,
    err: JsCallError,
}

#[wasm_bindgen(js_class = ConnErrorPair)]
impl ConnErrPair {
    /// The WebSocket connection that encountered the error.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn ws(&self) -> JsWebSocket {
        self.ws.clone()
    }

    /// The error that occurred during the call.
    #[must_use]
    #[wasm_bindgen(getter)]
    pub fn err(&self) -> JsCallError {
        self.err.clone()
    }
}

/// An error that occurred during disconnection.
#[allow(missing_copy_implementations)]
#[derive(Debug, Error)]
#[error(transparent)]
pub struct JsDisconnectionError(
    #[from] <JsConnectionCallbackReader<JsWebSocket> as Connection<Local>>::DisconnectionError,
);

impl From<JsDisconnectionError> for JsValue {
    fn from(err: JsDisconnectionError) -> Self {
        let err = js_sys::Error::new(&err.to_string());
        err.set_name("DisconnectionError");
        err.into()
    }
}

/// Map of peer IDs to their batch sync results.
#[wasm_bindgen(js_name = PeerResultMap)]
#[derive(Debug)]
pub struct PeerResultMap(HashMap<PeerId, (bool, Vec<(JsWebSocket, JsCallError)>)>);

#[wasm_bindgen(js_class = PeerResultMap)]
impl PeerResultMap {
    /// Get the result for a specific peer ID.
    #[must_use]
    pub fn get_result(&self, peer_id: JsPeerId) -> Option<PeerBatchSyncResult> {
        self.0
            .get(&peer_id.into())
            .map(|(success, conn_errs)| PeerBatchSyncResult {
                success: *success,
                conn_errors: conn_errs
                    .iter()
                    .map(|(ws, err)| ConnErrPair {
                        ws: ws.clone(),
                        err: err.clone(),
                    })
                    .collect(),
            })
    }
}

#[derive(Debug)]
struct JsHashMetric(Option<js_sys::Function>);

impl DepthStrategy for JsHashMetric {
    fn to_depth(&self, digest: Digest) -> Depth {
        if let Some(func) = &self.0 {
            let js_digest = JsDigest::from(digest);

            #[allow(clippy::expect_used)]
            let js_value = func
                .call1(&JsValue::NULL, &JsValue::from(js_digest))
                .expect("callback failed");

            let js_depth_ref: &JsDepthRef = js_value.unchecked_ref();
            js_depth_ref.cast_to_js_depth().into()
        } else {
            CountLeadingZeroBytes.to_depth(digest)
        }
    }
}
