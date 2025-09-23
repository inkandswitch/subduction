//! The main synchronization logic and bookkeeping for [`Sedimentree`].

pub mod error;
pub mod request;

use self::request::ChunkRequested;
use crate::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
        Connection, ConnectionDisallowed, ConnectionPolicy,
    },
    peer::id::PeerId,
};
use error::{BlobRequestErr, IoError, ListenError};
use futures::{lock::Mutex, stream::FuturesUnordered, StreamExt};
use sedimentree_core::{
    future::FutureKind, storage::Storage, Blob, Chunk, Depth, Digest, LooseCommit, RemoteDiff,
    Sedimentree, SedimentreeId, SedimentreeSummary,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

/// The main synchronization manager for sedimentrees.
#[derive(Debug, Clone)]
pub struct Subduction<F: FutureKind, S: Storage<F>, C: Connection<F> + PartialEq> {
    sedimentrees: Arc<Mutex<HashMap<SedimentreeId, Sedimentree>>>,
    conn_manager: Arc<Mutex<ConnectionManager<C>>>,
    storage: S,
    _phantom: std::marker::PhantomData<F>,
}

impl<F: FutureKind, S: Storage<F>, C: Connection<F> + PartialEq> Subduction<F, S, C> {
    /// Listen for incoming messages from all connections and handle them appropriately.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a storage or network error occurs.
    pub async fn run(&self) -> Result<(), ListenError<F, S, C>> {
        tracing::debug!("Starting Subduction run loop");
        loop {
            self.listen().await?;
        }
    }

    async fn listen(&self) -> Result<(), ListenError<F, S, C>> {
        tracing::info!("Listening for messages from connections");

        let mut pump = FuturesUnordered::new();
        {
            let mut locked = self.conn_manager.lock().await;
            let mut unstarted = locked.unstarted.drain().collect::<Vec<_>>();
            for conn_id in unstarted.drain(..) {
                #[allow(clippy::expect_used)]
                let conn = locked
                    .connections
                    .get(&conn_id)
                    .expect("connections with IDs should be present")
                    .clone();

                tracing::info!("Spawning listener for connection {:?}", conn_id);
                pump.push(self.fire_once(conn_id, conn));
            }
        }

        while let Some((conn_id, conn, res)) = pump.next().await {
            let () = res?;
            let mut locked = self.conn_manager.lock().await;
            if locked.connections.contains_key(&conn_id) {
                // Re-enque if that connection is still active at the top-level
                pump.push(self.fire_once(conn_id, conn));
            }

            let unstarted_ids = locked.unstarted.drain().collect::<Vec<_>>();
            for conn_id in unstarted_ids {
                if let Some(unstarted) = locked.connections.get(&conn_id) {
                    pump.push(self.fire_once(conn_id, unstarted.clone()));
                }
            }
        }

        Ok(())
    }

    async fn fire_once(
        &self,
        conn_id: ConnectionId,
        conn: C,
    ) -> (ConnectionId, C, Result<(), ListenError<F, S, C>>) {
        let result = async {
            let msg = conn.recv().await.map_err(IoError::ConnRecv)?;
            self.dispatch(conn_id, &conn, msg).await
        }
        .await;
        (conn_id, conn, result)
    }

    async fn dispatch(
        &self,
        conn_id: ConnectionId,
        conn: &C,
        message: Message,
    ) -> Result<(), ListenError<F, S, C>> {
        let from = conn.peer_id();

        tracing::info!("Received message from peer {:?}: {:?}", from, message);

        match message {
            Message::LooseCommit { id, commit, blob } => {
                self.recv_commit(&from, id, &commit, blob).await?;
            }
            Message::Chunk { id, chunk, blob } => {
                self.recv_chunk(&from, id, &chunk, blob).await?;
            }
            Message::BatchSyncRequest(BatchSyncRequest {
                id,
                sedimentree_summary,
                req_id,
            }) => {
                if let Err(ListenError::MissingBlobs(missing)) = self
                    .recv_batch_sync_request(id, &sedimentree_summary, req_id, conn)
                    .await
                {
                    self.request_blobs(missing).await;
                }
            }
            Message::BatchSyncResponse(BatchSyncResponse { id, diff, .. }) => {
                self.recv_batch_sync_response(&from, id, &diff).await?;
            }
            Message::BlobsRequest(digests) => {
                if self
                    .conn_manager
                    .lock()
                    .await
                    .connections
                    .contains_key(&conn_id)
                {
                    match self.recv_blob_request(conn, &digests).await {
                        Ok(()) => {
                            tracing::info!(
                                "Successfully handled blob request from peer {:?}",
                                from
                            );
                        }
                        Err(BlobRequestErr::IoError(e)) => Err(e)?,
                        Err(BlobRequestErr::MissingBlobs(missing)) => {
                            tracing::warn!(
                                "Missing blobs for request from peer {:?}: {:?}",
                                from,
                                missing
                            );
                        }
                    }
                } else {
                    tracing::warn!("No open connection for request ({:?})", conn_id);
                }
            }
            Message::BlobsResponse(blobs) => {
                for blob in blobs {
                    self.storage
                        .save_blob(blob)
                        .await
                        .map_err(IoError::Storage)?;
                }
            }
        }
        Ok(())
    }

    /// Initialize a new `Subduction` with the given storage backend and network adapters.
    pub fn new(
        sedimentrees: HashMap<SedimentreeId, Sedimentree>,
        storage: S,
        connections: HashMap<ConnectionId, C>,
    ) -> Self {
        Self {
            sedimentrees: Arc::new(Mutex::new(sedimentrees)),
            conn_manager: Arc::new(Mutex::new(ConnectionManager {
                next_id: ConnectionId::default(),
                connections,
                unstarted: HashSet::new(),
            })),
            storage,
            _phantom: std::marker::PhantomData,
        }
    }

    /// The storage backend used for persisting sedimentree data.
    ///
    /// # Errors
    ///
    /// * Returns `S::Error` if the storage backend encounters an error.
    pub async fn hydrate(&self) -> Result<(), S::Error> {
        for tree_id in self
            .sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            if let Some(sedimentree) = self.sedimentrees.lock().await.get_mut(&tree_id) {
                for commit in self.storage.load_loose_commits().await? {
                    tracing::trace!("Loaded commit {:?}", commit.digest());
                    sedimentree.add_commit(commit);
                }

                for chunk in self.storage.load_chunks().await? {
                    tracing::trace!("Loaded chunk {:?}", chunk.digest());
                    sedimentree.add_chunk(chunk);
                }
            }
        }

        Ok(())
    }

    /***************
     * CONNECTIONS *
     ***************/

    /// Attach a new [`Connection`] and immediately syncs all known [`Sedimentree`]s.
    ///
    /// # Errors
    ///
    /// * Returns `IoError` if a storage or network error occurs.
    pub async fn attach(&self, conn: C) -> Result<(bool, ConnectionId), IoError<F, S, C>> {
        let peer_id = conn.peer_id();
        tracing::info!("Attaching connection to peer {:?}", peer_id);

        let (fresh, conn_id) = self.register(conn).await?;

        for tree_id in self
            .sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            self.request_peer_batch_sync(&peer_id, tree_id, None)
                .await?;
        }

        Ok((fresh, conn_id))
    }

    /// Gracefully shut down a connection.
    ///
    /// # Errors
    ///
    /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect(&self, conn_id: &ConnectionId) -> Result<bool, C::DisconnectionError> {
        let mut locked = self.conn_manager.lock().await;
        locked.unstarted.remove(conn_id);
        if let Some(mut conn) = locked.connections.remove(conn_id) {
            conn.disconnect().await.map(|()| true)
        } else {
            Ok(false)
        }
    }

    /// Gracefully disconnect from all connections to a given peer ID.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if at least one connection was found and disconnected.
    /// * `Ok(false)` if no connections to the given peer ID were found.
    ///
    /// # Errors
    ///
    /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_from_peer(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<bool, C::DisconnectionError> {
        let mut touched = false;
        let mut locked = self.conn_manager.lock().await;

        let mut conn_meta = Vec::new();
        for (id, conn) in &locked.connections {
            conn_meta.push((*id, conn.peer_id()));
        }

        for (id, conn_peer_id) in &conn_meta {
            if *conn_peer_id == *peer_id {
                touched = true;
                locked.unstarted.remove(id);
                if let Some(mut conn) = locked.connections.remove(id) {
                    conn.disconnect().await?;
                }
            }
        }

        Ok(touched)
    }

    /****************************
     * LOW LEVEL CONNECTION API *
     ****************************/

    /// Low-level registration of a new connection.
    ///
    /// This does not perform any synchronization.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if the connection was successfully registered.
    /// * `Ok(false)` if the connection was already registered.
    ///
    /// # Errors
    ///
    /// * Returns `ConnectionDisallowed` if the connection is not allowed by the policy.
    pub async fn register(&self, conn: C) -> Result<(bool, ConnectionId), ConnectionDisallowed> {
        self.allowed_to_connect(&conn.peer_id()).await?;

        let mut locked = self.conn_manager.lock().await;
        if let Some((conn_id, _conn)) = locked.connections.iter().find(|(_, c)| **c == conn) {
            Ok((false, *conn_id))
        } else {
            let conn_id = locked.next_connection_id();
            locked.unstarted.insert(conn_id);
            locked.connections.insert(conn_id, conn);
            Ok((true, conn_id))
        }
    }

    /// Low-level unregistration of a connection.
    pub async fn unregister(&mut self, conn_id: &ConnectionId) -> bool {
        let mut locked = self.conn_manager.lock().await;
        locked.unstarted.remove(conn_id);
        locked.connections.remove(conn_id).is_some()
    }

    /*********
     * BLOBS *
     *********/

    /// Get a blob from local storage by its digest.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(blob))` if the blob was found locally.
    /// * `Ok(None)` if the blob was not found locally.
    ///
    /// # Errors
    ///
    /// * Returns `S::Error` if the storage backend encounters an error.
    pub async fn get_local_blob(&self, digest: Digest) -> Result<Option<Blob>, S::Error> {
        if let Some(data) = self.storage.load_blob(digest).await? {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    /// Get all blobs associated with a given sedimentree ID from local storage.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(blobs))` if the relevant Sedimentree was found.
    /// * `Ok(None)` if the Sedimentree with the given ID does not exist.
    ///
    /// # Errors
    ///
    /// * Returns `S::Error` if the storage backend encounters an error.
    pub async fn get_local_blobs(&self, id: SedimentreeId) -> Result<Option<Vec<Blob>>, S::Error> {
        if let Some(sedimentree) = self.sedimentrees.lock().await.get(&id) {
            tracing::debug!("Found sedimentree with id {:?}", id);
            let mut results = Vec::new();

            for digest in sedimentree
                .loose_commits()
                .map(|loose| loose.blob().digest())
                .chain(
                    sedimentree
                        .chunks()
                        .map(|chunk| chunk.summary().blob_meta().digest()),
                )
            {
                // TODO include impl for range queries

                if let Some(blob) = self.storage.load_blob(digest).await? {
                    results.push(blob);
                } else {
                    tracing::warn!("Missing blob for digest {:?}", digest);
                }
            }

            Ok(Some(results))
        } else {
            Ok(None)
        }
    }

    /// Get blobs for a [`Sedimentree`].
    ///
    /// If none are found locally, it will attempt to fetch them from connected peers.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(blobs))` if blobs were found locally or fetched from peers.
    /// * `Ok(None)` if the Sedimentree with the given ID does not exist.
    ///
    /// # Errors
    ///
    /// * Returns `IoError` if a storage or network error occurs.
    pub async fn fetch_blobs(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<Blob>>, IoError<F, S, C>> {
        if let Some(blobs) = self.get_local_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(blobs))
        } else {
            if let Some(tree) = self.sedimentrees.lock().await.get(&id) {
                let summary = tree.summarize();
                let locked = self.conn_manager.lock().await;
                for conn in locked.connections.values() {
                    let req_id = conn.next_request_id().await;
                    let BatchSyncResponse {
                        id,
                        diff,
                        req_id: resp_batch_id,
                    } = conn
                        .call(
                            BatchSyncRequest {
                                id,
                                req_id,
                                sedimentree_summary: summary.clone(),
                            },
                            timeout,
                        )
                        .await
                        .map_err(IoError::ConnCall)?;

                    debug_assert_eq!(req_id, resp_batch_id);

                    self.recv_batch_sync_response(&conn.peer_id(), id, &diff)
                        .await?;
                }
            }

            let updated = self.get_local_blobs(id).await.map_err(IoError::Storage)?;

            Ok(updated)
        }
    }

    /// Handle receiving a blob request from a peer.
    ///
    /// # Errors
    ///
    /// * [`BlobRequestErr::IoError`] if a storage or network error occurs,
    ///   or if blobs we expect to be stored are missing (possibly waiting for them from a peer).
    pub async fn recv_blob_request(
        &self,
        conn: &C,
        digests: &[Digest],
    ) -> Result<(), BlobRequestErr<F, S, C>> {
        let mut blobs = Vec::new();
        let mut missing = Vec::new();
        for digest in digests {
            if let Some(blob) = self
                .get_local_blob(*digest)
                .await
                .map_err(IoError::Storage)?
            {
                blobs.push(blob);
            } else {
                missing.push(*digest);
            }
        }

        conn.send(Message::BlobsResponse(blobs))
            .await
            .map_err(IoError::ConnSend)?;

        if missing.is_empty() {
            Ok(())
        } else {
            Err(BlobRequestErr::MissingBlobs(missing))
        }
    }

    /***********************
     * INCREMENTAL CHANGES *
     ***********************/

    /// Add a new (incremental) commit locally and propagate it to all connected peers.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` if the commit is not on a chunk boundary.
    /// * `Ok(Some(ChunkRequested))` if the commit is on a [`Chunk`] boundary.
    ///   In this case, please call `add_chunk` after creating the requested chunk.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn add_commit(
        &mut self,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<Option<ChunkRequested>, IoError<F, S, C>> {
        self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        {
            let locked = self.conn_manager.lock().await;
            let conns = locked.connections.values().collect::<Vec<_>>();
            for conn in conns {
                conn.send(Message::LooseCommit {
                    id,
                    commit: commit.clone(),
                    blob: blob.clone(),
                })
                .await
                .map_err(IoError::ConnSend)?;
            }
        }

        let mut maybe_requested_chunk = None;

        let depth = Depth::from(commit.digest());
        if depth != Depth(0) {
            maybe_requested_chunk = Some(ChunkRequested {
                head: commit.digest(),
                depth,
            });
        }

        Ok(maybe_requested_chunk)
    }

    /// Add a new (incremental) chunk locally and propagate it to all connected peers.
    ///
    /// NOTE this performs no integrity checks;
    /// we assume this is a good chunk at the right depth
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn add_chunk(
        &self,
        id: SedimentreeId,
        chunk: &Chunk,
        blob: Blob,
    ) -> Result<(), IoError<F, S, C>> {
        {
            let mut sed = self.sedimentrees.lock().await;
            let tree = sed.entry(id).or_default();
            tree.add_chunk(chunk.clone());
        }

        self.storage
            .save_blob(blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        {
            let locked = self.conn_manager.lock().await;
            let conns = locked.connections.values().collect::<Vec<_>>();
            for conn in conns {
                conn.send(Message::Chunk {
                    id,
                    chunk: chunk.clone(),
                    blob: blob.clone(),
                })
                .await
                .map_err(IoError::ConnSend)?;
            }
        }

        Ok(())
    }

    /****************************
     * RECEIVE UPDATE FROM PEER *
     ****************************/

    /// Handle receiving a new commit from a peer.
    ///
    /// Also propagates it to all other connected peers.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn recv_commit(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<bool, IoError<F, S, C>> {
        let was_new = self
            .insert_commit_locally(id, commit.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let locked = self.conn_manager.lock().await;
            for conn in locked.connections.values() {
                if conn.peer_id() != *from {
                    conn.send(Message::LooseCommit {
                        id,
                        commit: commit.clone(),
                        blob: blob.clone(),
                    })
                    .await
                    .map_err(IoError::ConnSend)?;
                }
            }
        }

        Ok(was_new)
    }

    /// Handle receiving a new chunk from a peer.
    ///
    /// Also propagates it to all other connected peers.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn recv_chunk(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        chunk: &Chunk,
        blob: Blob,
    ) -> Result<bool, IoError<F, S, C>> {
        let was_new = self
            .insert_chunk_locally(id, chunk.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let locked = self.conn_manager.lock().await;
            for conn in locked.connections.values() {
                if conn.peer_id() != *from {
                    conn.send(Message::Chunk {
                        id,
                        chunk: chunk.clone(),
                        blob: blob.clone(),
                    })
                    .await
                    .map_err(IoError::ConnSend)?;
                }
            }
        }

        Ok(was_new)
    }

    /*********************
     * BATCH SYNCHRONIZE *
     *********************/

    /// Handle receiving a batch sync request from a peer.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn recv_batch_sync_request(
        &self,
        id: SedimentreeId,
        their_summary: &SedimentreeSummary,
        req_id: RequestId,
        conn: &C,
    ) -> Result<(), ListenError<F, S, C>> {
        let mut their_missing_commits = Vec::new();
        let mut their_missing_chunks = Vec::new();
        let mut our_missing_blobs = Vec::new();

        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);
        {
            let mut guard = self.sedimentrees.lock().await;
            let sedimentree = guard.entry(id).or_default();
            tracing::info!(
                "Received batch sync request for sedimentree {:?} with {} commits and {} chunks",
                id,
                their_summary.loose_commits().len(),
                their_summary.chunk_summaries().len()
            );

            let local_sedimentree = sedimentree.clone();
            let diff: RemoteDiff<'_> = local_sedimentree.diff_remote(their_summary);

            for commit in diff.remote_commits {
                sedimentree.add_commit(commit.clone());
            }

            for commit in diff.local_commits {
                if let Some(blob) = self
                    .storage
                    .load_blob(commit.blob().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_commits.push((commit.clone(), blob)); // TODO lots of cloning
                } else {
                    tracing::warn!("Missing blob for commit {:?}", commit.digest(),);
                    our_missing_blobs.push(commit.blob().digest());
                }
            }

            for chunk in diff.local_chunks {
                if let Some(blob) = self
                    .storage
                    .load_blob(chunk.summary().blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_chunks.push((chunk.clone(), blob)); // TODO lots of cloning
                } else {
                    tracing::warn!("Missing blob for chunk {:?} ", chunk.digest(),);
                    our_missing_blobs.push(chunk.summary().blob_meta().digest());
                }
            }
        }

        tracing::info!(
            "Sending batch sync response for sedimentree {:?} with {} missing commits and {} missing chunks",
            id,
            their_missing_commits.len(),
            their_missing_chunks.len()
        );
        conn.send(
            BatchSyncResponse {
                id,
                req_id,
                diff: SyncDiff {
                    missing_commits: their_missing_commits,
                    missing_chunks: their_missing_chunks,
                },
            }
            .into(),
        )
        .await
        .map_err(IoError::ConnSend)?;

        if our_missing_blobs.is_empty() {
            Ok(())
        } else {
            Err(ListenError::MissingBlobs(our_missing_blobs))
        }
    }

    /// Handle receiving a batch sync response from a peer.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs while inserting commits or chunks.
    pub async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: &SyncDiff,
    ) -> Result<(), IoError<F, S, C>> {
        tracing::info!(
            "Received batch sync response for sedimentree {:?} from peer {:?} with {} missing commits and {} missing chunks",
            id,
            from,
            diff.missing_commits.len(),
            diff.missing_chunks.len()
        );

        for (commit, blob) in &diff.missing_commits {
            self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                .await
                .map_err(IoError::Storage)?;
        }

        for (chunk, blob) in &diff.missing_chunks {
            self.insert_chunk_locally(id, chunk.clone(), blob.clone())
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    /// Find blobs from connected peers.
    pub async fn request_blobs(&self, digests: Vec<Digest>) {
        let locked = self.conn_manager.lock().await;
        for conn in locked.connections.values() {
            if let Err(e) = conn.send(Message::BlobsRequest(digests.clone())).await {
                tracing::error!(
                    "Error requesting blobs {:?} from peer {:?}: {:?}",
                    digests,
                    conn.peer_id(),
                    e
                );
            }
        }
    }

    /// Request a batch sync from a given peer for a given sedimentree ID.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if at least one sync was successful.
    /// * `Ok(false)` if no syncs were performed (e.g., no connections, or they all timed out).
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs during the sync process.
    pub async fn request_peer_batch_sync(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<(bool, Vec<(C, C::CallError)>), IoError<F, S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let mut had_success = false;
        let mut peer_conns = Vec::new();
        {
            let locked = self.conn_manager.lock().await;
            for (conn_id, conn) in &locked.connections {
                if conn.peer_id() == *to_ask {
                    peer_conns.push((*conn_id, conn.clone()));
                }
            }
        }

        let mut conn_errs = Vec::new();

        for (conn_id, conn) in peer_conns {
            tracing::info!("Using connection {:?} to peer {:?}", conn_id, to_ask);
            let summary = self
                .sedimentrees
                .lock()
                .await
                .get(&id)
                .map(Sedimentree::summarize)
                .unwrap_or_default();

            let req_id = conn.next_request_id().await;

            let result = conn
                .call(
                    BatchSyncRequest {
                        id,
                        req_id,
                        sedimentree_summary: summary,
                    },
                    timeout,
                )
                .await;

            match result {
                Err(e) => conn_errs.push((conn, e)),
                Ok(BatchSyncResponse {
                    diff:
                        SyncDiff {
                            missing_commits,
                            missing_chunks,
                        },
                    ..
                }) => {
                    for (commit, blob) in missing_commits {
                        self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                            .await
                            .map_err(IoError::Storage)?;
                    }

                    for (chunk, blob) in missing_chunks {
                        self.insert_chunk_locally(id, chunk.clone(), blob.clone())
                            .await
                            .map_err(IoError::Storage)?;
                    }

                    had_success = true;
                    break;
                }
            }
        }

        Ok((had_success, conn_errs))
    }

    /// Request a batch sync from all connected peers for a given sedimentree ID.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if at least one sync was successful.
    /// * `Ok(false)` if no syncs were performed (e.g., no peers connected or they all timed out).
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs during the sync process.
    pub async fn request_all_batch_sync(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<HashMap<PeerId, (bool, Vec<(C, <C as Connection<F>>::CallError)>)>, IoError<F, S, C>>
    {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let mut peers: HashMap<PeerId, Vec<(ConnectionId, C)>> = HashMap::new();
        {
            let locked = self.conn_manager.lock().await; // TODO held long, inefficient!
            for (conn_id, conn) in &locked.connections {
                peers
                    .entry(conn.peer_id())
                    .or_default()
                    .push((*conn_id, conn.clone()));
            }
        }

        tracing::debug!("Found {} peer(s)", peers.len());
        let mut set: FuturesUnordered<_> = peers
            .iter()
            .map(|(peer_id, peer_conns)| async move {
                tracing::debug!(
                    "Requesting batch sync for sedimentree {:?} from {} connections",
                    id,
                    peer_conns.len(),
                );

                let mut had_success = false;
                let mut conn_errs = Vec::new();

                for (conn_id, conn) in peer_conns {
                    tracing::debug!(
                        "Using connection {:?} to peer {:?}",
                        conn_id,
                        conn.peer_id()
                    );
                    let summary = self
                        .sedimentrees
                        .lock()
                        .await
                        .get(&id)
                        .map(Sedimentree::summarize)
                        .unwrap_or_default();

                    let req_id = conn.next_request_id().await;

                    let result = conn
                        .call(
                            BatchSyncRequest {
                                id,
                                req_id,
                                sedimentree_summary: summary,
                            },
                            timeout,
                        )
                        .await;

                    match result {
                        Err(e) => conn_errs.push((conn.clone(), e)),
                        Ok(BatchSyncResponse {
                            diff:
                                SyncDiff {
                                    missing_commits,
                                    missing_chunks,
                                },
                            ..
                        }) => {
                            for (commit, blob) in missing_commits {
                                self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                                    .await
                                    .map_err(IoError::<F, S, C>::Storage)?;
                            }

                            for (chunk, blob) in missing_chunks {
                                self.insert_chunk_locally(id, chunk.clone(), blob.clone())
                                    .await
                                    .map_err(IoError::<F, S, C>::Storage)?;
                            }

                            had_success = true;
                            break;
                        }
                    }
                }

                Ok::<(PeerId, bool, Vec<(C, _)>), IoError<F, S, C>>((
                    *peer_id,
                    had_success,
                    conn_errs,
                ))
            })
            .collect();

        let mut out = HashMap::new();
        while let Some(result) = set.next().await {
            let (peer_id, success, errs) = result?;
            out.insert(peer_id, (success, errs));
        }
        Ok(out)
    }

    /// Request a batch sync from all connected peers for all known sedimentree IDs.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if at least one sync was successful.
    /// * `Ok(false)` if no syncs were performed (e.g., no sedimentrees or no peers).
    ///
    /// # Errors
    ///
    /// * `Err(IoError)` if any I/O error occurs during the sync process.
    pub async fn request_all_batch_sync_all(
        &self,
        timeout: Option<Duration>,
    ) -> Result<bool, IoError<F, S, C>> {
        tracing::info!("Requesting batch sync for all sedimentrees from all peers");
        let tree_ids = self
            .sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut had_success = false;
        for id in tree_ids {
            tracing::debug!("Requesting batch sync for sedimentree {:?}", id);
            let all_results = self.request_all_batch_sync(id, timeout).await?;
            if all_results.values().any(|(success, _)| *success) {
                had_success = true;
            }
        }
        Ok(had_success)
    }

    /********************
     * PUBLIC UTILITIES *
     ********************/

    /// Get an iterator over all known sedimentree IDs.
    pub async fn sedimentree_ids(&self) -> Vec<SedimentreeId> {
        self.sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>()
    }

    /// Get all commits for a given sedimentree ID.
    pub async fn get_commits(&self, id: SedimentreeId) -> Option<Vec<LooseCommit>> {
        self.sedimentrees
            .lock()
            .await
            .get(&id)
            .map(|tree| tree.loose_commits().cloned().collect())
    }

    /// Get all chunks for a given sedimentree ID.
    pub async fn get_chunks(&self, id: SedimentreeId) -> Option<Vec<Chunk>> {
        self.sedimentrees
            .lock()
            .await
            .get(&id)
            .map(|tree| tree.chunks().cloned().collect())
    }

    /// Get the set of all connected peer IDs.
    pub async fn peer_ids(&self) -> HashSet<PeerId> {
        self.conn_manager
            .lock()
            .await
            .connections
            .values()
            .map(Connection::peer_id)
            .collect()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_commit_locally(
        &self,
        id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        tracing::debug!("Inserting commit {:?} locally", commit.digest());
        {
            let mut sed = self.sedimentrees.lock().await;
            let tree = sed.entry(id).or_default();
            if !tree.add_commit(commit.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_loose_commit(commit).await?;
        self.storage.save_blob(blob).await?;

        Ok(true)
    }

    // NOTE no integrity checking, we assume that they made a good chunk at the right depth
    async fn insert_chunk_locally(
        &self,
        id: SedimentreeId,
        chunk: Chunk,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        {
            let mut sed = self.sedimentrees.lock().await;
            let tree = sed.entry(id).or_default();
            if !tree.add_chunk(chunk.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_chunk(chunk).await?;
        self.storage.save_blob(blob).await?;
        Ok(true)
    }
}

impl<F: FutureKind, S: Storage<F>, C: Connection<F> + PartialEq> ConnectionPolicy
    for Subduction<F, S, C>
{
    async fn allowed_to_connect(&self, _peer_id: &PeerId) -> Result<(), ConnectionDisallowed> {
        Ok(()) // TODO currently allows all
    }
}

#[derive(Debug, Default)]
struct ConnectionManager<C> {
    next_id: ConnectionId,
    connections: HashMap<ConnectionId, C>,
    unstarted: HashSet<ConnectionId>,
}

impl<C> ConnectionManager<C> {
    fn next_connection_id(&mut self) -> ConnectionId {
        let mut id = self.next_id.into();
        while self.connections.contains_key(&ConnectionId::new(id)) {
            id = id.wrapping_add(1);
        }
        self.next_id = id.wrapping_add(1).into();
        id.into()
    }
}
