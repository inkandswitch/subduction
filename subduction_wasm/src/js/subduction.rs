//! Subduction node.

use std::{collections::HashMap, time::Duration};

use sedimentree_core::{future::Local, storage::MemoryStorage, Blob};
use subduction_core::{peer::id::PeerId, Subduction};
use wasm_bindgen::prelude::*;
use web_sys::js_sys::Uint8Array;

use super::{
    chunk::{JsChunk, JsChunkRequested},
    connection_id::JsConnectionId,
    digest::JsDigest,
    error::{JsCallError, JsConnectionDisallowed, JsIoError, JsListenError},
    loose_commit::JsLooseCommit,
    peer_id::JsPeerId,
    sedimentree_id::JsSedimentreeId,
    websocket::JsWebSocket,
};

/// Wasm bindings for [`Subduction`](subduction_core::Subduction)
#[wasm_bindgen(js_name = Subduction)]
#[derive(Debug, Clone)]
pub struct JsSubduction(Subduction<Local, MemoryStorage, JsWebSocket>);

#[wasm_bindgen(js_class = Subduction)]
impl JsSubduction {
    /// Create a new [`Subduction`] instance.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self(Subduction::new(
            HashMap::new(),
            MemoryStorage::default(), // FIXME use lcoalstorage or indexeddb
            HashMap::new(),
        ))
    }

    /// Run the Subduction instance.
    pub async fn run(&self) -> Result<(), JsListenError> {
        self.0.run().await?;
        Ok(())
    }

    /// Attach a connection.
    pub async fn attach(&self, conn: JsWebSocket) -> Result<Registered, JsIoError> {
        let (is_new, conn_id) = self.0.attach(conn).await.map_err(JsIoError::from)?;
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Disconnect a connection by its ID.
    pub async fn disconnect(&self, js_conn_id: JsConnectionId) -> bool {
        self.0
            .disconnect(&js_conn_id.into())
            .await
            .expect("Infallable")
    }

    /// Disconnect from a peer by its ID.
    #[wasm_bindgen(js_name = disconnectFromPeer)]
    pub async fn disconnect_from_peer(&self, peer_id: JsPeerId) -> bool {
        self.0
            .disconnect_from_peer(&peer_id.into())
            .await
            .expect("Infallable")
    }

    /// Register a new connection.
    pub async fn register(&self, conn: JsWebSocket) -> Result<Registered, JsConnectionDisallowed> {
        let (is_new, conn_id) = self.0.register(conn).await?;
        Ok(Registered {
            is_new,
            conn_id: conn_id.into(),
        })
    }

    /// Unregister a connection by its ID.
    ///
    /// Returns `true` if the connection was found and unregistered, and `false` otherwise.
    pub async fn unregister(&self, conn_id: JsConnectionId) -> bool {
        self.0.unregister(&conn_id.into()).await
    }

    /// Get a local blob by its digest.
    #[wasm_bindgen(js_name = getLocalBlob)]
    pub async fn get_local_blob(&self, digest: JsDigest) -> Option<Uint8Array> {
        let maybe_blob = self
            .0
            .get_local_blob(digest.into())
            .await
            .expect("Infallible");
        maybe_blob.map(|blob| Uint8Array::from(blob.as_slice()))
    }

    /// Get all local blobs for a given Sedimentree ID.
    #[wasm_bindgen(js_name = getLocalBlobs)]
    pub async fn get_local_blobs(&self, id: JsSedimentreeId) -> Result<Vec<Uint8Array>, String> {
        if let Some(blobs) = self.0.get_local_blobs(id.into()).await.expect("Infallible") {
            Ok(blobs
                .into_iter()
                .map(|blob| Uint8Array::from(blob.as_slice()))
                .collect())
        } else {
            Ok(vec![])
        }
    }

    /// Fetch blobs by their digests, with an optional timeout in milliseconds.
    #[wasm_bindgen(js_name = fetchBlobs)]
    pub async fn fetch_blobs(
        &self,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<Option<Vec<Uint8Array>>, JsIoError> {
        let timeout = timeout_milliseconds.map(|ms| Duration::from_millis(ms));
        if let Some(blobs) = self
            .0
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
    #[wasm_bindgen(js_name = addCommit)]
    pub async fn add_commit(
        &self,
        id: JsSedimentreeId,
        commit: &JsLooseCommit,
        blob: &Uint8Array,
    ) -> Result<Option<JsChunkRequested>, JsIoError> {
        let maybe_chunk_requested = self
            .0
            .add_commit(
                id.into(),
                &commit.clone().into(),
                Blob::from(blob.clone().to_vec()),
            )
            .await
            .map_err(JsIoError::from)?;

        Ok(maybe_chunk_requested.map(JsChunkRequested::from))
    }

    /// Add a chunk with its associated blob to the storage.
    #[wasm_bindgen(js_name = addChunk)]
    pub async fn add_chunk(
        &self,
        id: JsSedimentreeId,
        chunk: &JsChunk,
        blob: &Uint8Array,
    ) -> Result<(), JsIoError> {
        let blob: Blob = blob.clone().to_vec().into();
        self.0
            .add_chunk(id.into(), &chunk.clone().into(), blob)
            .await
            .map_err(JsIoError::from)?;
        Ok(())
    }

    /// Request blobs by their digests from connected peers.
    #[wasm_bindgen(js_name = requestBlobs)]
    pub async fn request_blobs(&self, digests: Vec<JsDigest>) {
        let digests: Vec<_> = digests.into_iter().map(Into::into).collect();
        self.0.request_blobs(digests).await
    }

    /// Request batch sync for a given Sedimentree ID from a specific peer.
    #[wasm_bindgen(js_name = requestPeerBatchSync)]
    pub async fn request_peer_batch_sync(
        &self,
        to_ask: JsPeerId,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerBatchSyncResult, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let (success, conn_errors) = self
            .0
            .request_peer_batch_sync(&to_ask.into(), id.into(), timeout)
            .await
            .map_err(JsIoError::from)?;

        Ok(PeerBatchSyncResult {
            success,
            conn_errors: conn_errors
                .into_iter()
                .map(|(ws, err)| ConnErrPair {
                    ws: ws.clone(),
                    err: JsCallError::from(err),
                })
                .collect(),
        })
    }

    /// Request batch sync for a given Sedimentree ID from all connected peers.
    #[wasm_bindgen(js_name = requestAllBatchSync)]
    pub async fn request_all_batch_sync(
        &self,
        id: JsSedimentreeId,
        timeout_milliseconds: Option<u64>,
    ) -> Result<PeerResultMap, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        let peer_map = self.0.request_all_batch_sync(id.into(), timeout).await?;
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
                                .map(|(ws, err)| (ws.clone(), JsCallError::from(err)))
                                .collect::<Vec<_>>(),
                        ),
                    )
                })
                .collect(),
        ))
    }

    /// Request batch sync for all known Sedimentree IDs from all connected peers.
    #[wasm_bindgen(js_name = requestAllBatchSyncAll)]
    pub async fn request_all_batch_sync_all(
        &self,
        timeout_milliseconds: Option<u64>,
    ) -> Result<bool, JsIoError> {
        let timeout = timeout_milliseconds.map(Duration::from_millis);
        self.0
            .request_all_batch_sync_all(timeout)
            .await
            .map_err(JsIoError::from)
    }

    /// Get all known Sedimentree IDs
    #[wasm_bindgen(js_name = sedimentreeIds)]
    pub async fn seidmentree_ids(&self) -> Vec<JsSedimentreeId> {
        self.0
            .sedimentree_ids()
            .await
            .into_iter()
            .map(JsSedimentreeId::from)
            .collect()
    }

    /// Get all commits for a given Sedimentree ID
    #[wasm_bindgen(js_name = getCommits)]
    pub async fn get_commits(
        &self,
        id: JsSedimentreeId,
    ) -> Result<Option<Vec<JsLooseCommit>>, String> {
        if let Some(commits) = self.0.get_commits(id.into()).await {
            Ok(Some(commits.into_iter().map(JsLooseCommit::from).collect()))
        } else {
            Ok(None)
        }
    }

    /// Get all chunks for a given Sedimentree ID
    #[wasm_bindgen(js_name = getChunks)]
    pub async fn get_chunks(&self, id: JsSedimentreeId) -> Result<Option<Vec<JsChunk>>, String> {
        if let Some(chunks) = self.0.get_chunks(id.into()).await {
            Ok(Some(chunks.into_iter().map(JsChunk::from).collect()))
        } else {
            Ok(None)
        }
    }

    /// Get the peer IDs of all connected peers
    #[wasm_bindgen(js_name = getPeerIds)]
    pub async fn peer_ids(&self) -> Vec<JsPeerId> {
        self.0
            .peer_ids()
            .await
            .into_iter()
            .map(JsPeerId::from)
            .collect()
    }
}

/// Result of registering a connection.
#[wasm_bindgen(js_name = Registered)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Registered {
    is_new: bool,
    conn_id: JsConnectionId,
}

#[wasm_bindgen(js_class = Registered)]
impl Registered {
    /// Whether the connection was newly registered.
    #[wasm_bindgen(getter)]
    pub fn is_new(&self) -> bool {
        self.is_new
    }

    /// The connection ID of the registered connection.
    #[wasm_bindgen(getter)]
    pub fn conn_id(&self) -> JsConnectionId {
        self.conn_id
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
    #[wasm_bindgen(getter)]
    pub fn success(&self) -> bool {
        self.success
    }

    /// List of connection errors that occurred during the batch sync.
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
    #[wasm_bindgen(getter)]
    pub fn ws(&self) -> JsWebSocket {
        self.ws.clone()
    }

    /// The error that occurred during the call.
    #[wasm_bindgen(getter)]
    pub fn err(&self) -> JsCallError {
        self.err.clone()
    }
}

/// Map of peer IDs to their batch sync results.
#[wasm_bindgen(js_name = PeerResultMap)]
#[derive(Debug)]
pub struct PeerResultMap(HashMap<PeerId, (bool, Vec<(JsWebSocket, JsCallError)>)>);

#[wasm_bindgen(js_class = PeerResultMap)]
impl PeerResultMap {
    /// Get the result for a specific peer ID.
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
