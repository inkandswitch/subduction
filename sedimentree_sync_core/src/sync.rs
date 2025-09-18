//! The main synchronization logic and bookkeeping for [`Sedimentree`].

use crate::{
    connection::{
        BatchSyncRequest, BatchSyncResponse, Connection, ConnectionDisallowed, ConnectionId,
        ConnectionPolicy, Message, RequestId, SyncDiff,
    },
    peer::id::PeerId,
};
use futures::{lock::Mutex, stream::FuturesUnordered, StreamExt};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Depth, Digest, LooseCommit, Sedimentree, SedimentreeId,
    SedimentreeSummary,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;

/// A request for a chunk at a certain depth, starting from a given head.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequested {
    /// The head digest from which the chunk is requested.
    pub head: Digest,

    /// The depth of the requested chunk.
    pub depth: Depth,
}

/// The main synchronization manager for sedimentrees.
#[derive(Debug, Clone)]
pub struct SedimentreeSync<S: Storage, C: Connection> {
    sedimentrees: Arc<Mutex<HashMap<SedimentreeId, Sedimentree>>>,
    conn_manager: Arc<Mutex<ConnectionManager<C>>>,
    storage: S,
}

impl<S: Storage, C: Connection> SedimentreeSync<S, C> {
    /// Listen for incoming messages from all connections and handle them appropriately.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a storage or network error occurs.
    #[tracing::instrument(skip(self))]
    pub async fn run(&self) -> Result<(), ListenError<S, C>> {
        loop {
            self.listen().await?;
        }
    }

    async fn listen(&self) -> Result<(), ListenError<S, C>> {
        let mut pump = FuturesUnordered::new();
        {
            let mut locked = self.conn_manager.lock().await;
            let unstarted = locked.unstarted.drain().collect::<Vec<_>>();
            for conn_id in unstarted {
                tracing::info!("Spawning listener for connection {:?}", conn_id);
                #[allow(clippy::expect_used)]
                let fut = indexed_listen(
                    locked
                        .connections
                        .remove(&conn_id)
                        .expect("connections with IDs should be present"),
                );
                pump.push(fut);
                locked.unstarted.remove(&conn_id);
            }
        }

        while let Some(res) = pump.next().await {
            tracing::info!("Received a message from a connection");
            match res {
                Ok((conn, message)) => {
                    {
                        self.dispatch(&conn, message).await?;
                        let mut locked = self.conn_manager.lock().await;
                        if locked.connections.contains_key(&conn.connection_id()) {
                            // Re-enque if that connection is still active at the top-level
                            pump.push(indexed_listen(conn));
                        }

                        let unstarted_ids = locked.unstarted.drain().collect::<Vec<_>>();
                        for conn_id in unstarted_ids {
                            if let Some(unstarted) = locked.connections.remove(&conn_id) {
                                pump.push(indexed_listen(unstarted));
                            }
                        }
                    }
                }
                Err(e) => Err(IoError::ConnRecv(e))?,
            }
        }

        Ok(())
    }

    async fn dispatch(&self, conn: &C, message: Message) -> Result<(), ListenError<S, C>> {
        let idx = conn.connection_id();
        let from = conn.peer_id();

        tracing::info!("Received message from peer {:?}: {:?}", from, message);

        match message {
            Message::LooseCommit { id, commit, blob } => {
                self.recv_commit(&from, id, &commit, blob).await?;
            }
            Message::Chunk { id, chunk, blob } => self.recv_chunk(&from, id, &chunk, blob).await?,
            Message::BatchSyncRequest(BatchSyncRequest {
                id,
                sedimentree_summary,
                req_id,
            }) => {
                self.recv_batch_sync_request(id, &sedimentree_summary, req_id, &conn)
                    .await?;
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
                    .contains_key(&idx)
                {
                    match self.recv_blob_request(&conn, &digests).await {
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
                    tracing::warn!("No open connection for request ({:?})", idx);
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

    /// Initialize a new `SedimentreeSync` with the given storage backend and network adapters.
    #[tracing::instrument(skip_all, fields(sedimentree_ids = ?sedimentrees.keys()))]
    pub fn new(
        sedimentrees: HashMap<SedimentreeId, Sedimentree>,
        storage: S,
        connections: HashMap<ConnectionId, C>,
    ) -> Self {
        Self {
            sedimentrees: Arc::new(Mutex::new(sedimentrees)),
            conn_manager: Arc::new(Mutex::new(ConnectionManager {
                connections,
                unstarted: HashSet::new(),
            })),
            storage,
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
    #[tracing::instrument(skip(self, conn))]
    pub async fn attach(&self, conn: C) -> Result<(), IoError<S, C>> {
        tracing::info!("Attaching connection to peer {:?}", conn.peer_id());

        let peer_id = conn.peer_id();
        self.register(conn).await?;

        for tree_id in self
            .sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            self.request_peer_batch_sync(&peer_id, tree_id).await?;
        }

        Ok(())
    }

    /// Gracefully shut down a connection.
    ///
    /// # Errors
    ///
    /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect(&self, conn: &C) -> Result<bool, C::DisconnectionError> {
        let mut locked = self.conn_manager.lock().await;
        locked.unstarted.remove(&conn.connection_id());
        if let Some(mut conn) = locked.connections.remove(&conn.connection_id()) {
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

        let conn_meta = locked
            .connections
            .iter()
            .map(|(id, conn)| (*id, conn.peer_id().clone()))
            .collect::<Vec<_>>();

        for (id, conn_peer_id) in &conn_meta {
            if *conn_peer_id == *peer_id {
                touched = true;
                locked.unstarted.remove(&id);
                if let Some(mut conn) = locked.connections.remove(&id) {
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
    pub async fn register(&self, conn: C) -> Result<bool, ConnectionDisallowed> {
        self.allowed_to_connect(&conn.peer_id())?;

        let mut locked = self.conn_manager.lock().await;
        if locked.connections.contains_key(&conn.connection_id()) {
            Ok(false)
        } else {
            locked.unstarted.insert(conn.connection_id());
            locked.connections.insert(conn.connection_id(), conn);
            Ok(true)
        }
    }

    /// Low-level unregistration of a connection.
    ///
    /// This does not perform any disconnection.
    pub async fn unregister(&mut self, conn: &C) -> bool {
        let mut locked = self.conn_manager.lock().await;
        locked.unstarted.remove(&conn.connection_id());
        locked.connections.remove(&conn.connection_id()).is_some()
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
    #[tracing::instrument(skip(self))]
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
    #[tracing::instrument(skip(self))]
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
    #[tracing::instrument(skip(self))]
    pub async fn fetch_blobs(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<Option<Vec<Blob>>, IoError<S, C>> {
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
    ) -> Result<(), BlobRequestErr<S, C>> {
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
    #[tracing::instrument(skip(self))]
    pub async fn add_commit(
        &mut self,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<Option<ChunkRequested>, IoError<S, C>> {
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

    // TODO find next bounary using an iterator?

    /// Add a new (incremental) chunk locally and propagate it to all connected peers.
    ///
    /// NOTE this performs no integrity checks;
    /// we assume this is a good chunk at the right depth
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    #[tracing::instrument(skip(self))]
    pub async fn add_chunk(
        &self,
        id: SedimentreeId,
        chunk: &Chunk,
        blob: Blob,
    ) -> Result<(), IoError<S, C>> {
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
    ) -> Result<(), IoError<S, C>> {
        self.insert_commit_locally(id, commit.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

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

        Ok(())
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
    ) -> Result<(), IoError<S, C>> {
        self.insert_chunk_locally(id, chunk.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

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

        Ok(())
    }

    /*********************
     * BATCH SYNCHRONIZE *
     *********************/

    /// Handle receiving a batch sync request from a peer.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    #[tracing::instrument(skip(self, their_summary, conn))]
    pub async fn recv_batch_sync_request(
        &self,
        id: SedimentreeId,
        their_summary: &SedimentreeSummary,
        req_id: RequestId,
        conn: &C,
    ) -> Result<(), ListenError<S, C>> {
        let mut missing_commits = Vec::new();
        let mut missing_chunks = Vec::new();
        let mut missing_blobs = Vec::new();

        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);
        if let Some(sedimentree) = self.sedimentrees.lock().await.get(&id) {
            tracing::info!(
                "Received batch sync request for sedimentree {:?} with {} commits and {} chunks",
                id,
                their_summary.loose_commits().len(),
                their_summary.chunk_summaries().len()
            );

            for commit in sedimentree.loose_commits() {
                if !their_summary.loose_commits().contains(commit) {
                    if let Some(blob) = self
                        .storage
                        .load_blob(commit.blob().digest())
                        .await
                        .map_err(IoError::Storage)?
                    {
                        missing_commits.push((commit.clone(), blob)); // TODO lots of cloning
                    } else {
                        missing_blobs.push(commit.blob().digest());
                    }
                }
            }

            for chunk in sedimentree.chunks() {
                if !their_summary.chunk_summaries().contains(chunk.summary()) {
                    if let Some(blob) = self
                        .storage
                        .load_blob(chunk.summary().blob_meta().digest())
                        .await
                        .map_err(IoError::Storage)?
                    {
                        missing_chunks.push((chunk.clone(), blob)); // TODO lots of cloning
                    } else {
                        missing_blobs.push(chunk.summary().blob_meta().digest());
                    }
                }
            }
        }

        tracing::info!(
            "Sending batch sync response for sedimentree {:?} with {} missing commits and {} missing chunks",
            id,
            missing_commits.len(),
            missing_chunks.len()
        );
        conn.send(
            BatchSyncResponse {
                id,
                req_id,
                diff: SyncDiff {
                    missing_commits,
                    missing_chunks,
                },
            }
            .into(),
        )
        .await
        .map_err(IoError::ConnSend)?;

        if missing_blobs.is_empty() {
            Ok(())
        } else {
            Err(ListenError::MissingBlobs(missing_blobs))
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
    ) -> Result<(), IoError<S, C>> {
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
    #[tracing::instrument(skip(self))]
    pub async fn request_peer_batch_sync(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
    ) -> Result<bool, IoError<S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let mut had_success = false;
        let locked = self.conn_manager.lock().await;
        let peer_conns = locked
            .connections
            .values()
            .filter(|conn| conn.peer_id() == *to_ask);
        for conn in peer_conns {
            tracing::info!(
                "Using connection {:?} to peer {:?}",
                conn.connection_id(),
                to_ask
            );
            let summary = self
                .sedimentrees
                .lock()
                .await
                .get(&id)
                .map(Sedimentree::summarize)
                .unwrap_or_default();

            let req_id = conn.next_request_id().await;

            let BatchSyncResponse {
                diff:
                    SyncDiff {
                        missing_commits,
                        missing_chunks,
                    },
                ..
            } = conn
                .call(
                    BatchSyncRequest {
                        id,
                        req_id,
                        sedimentree_summary: summary,
                    },
                    Some(Duration::from_secs(30)),
                )
                .await
                .map_err(IoError::ConnCall)?;

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
        }

        Ok(had_success)
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
    #[tracing::instrument(skip(self))]
    pub async fn request_all_batch_sync(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<bool, IoError<S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let mut peers: HashMap<PeerId, Vec<&C>> = HashMap::new();
        let locked = self.conn_manager.lock().await;
        for conn in locked.connections.values() {
            peers.entry(conn.peer_id()).or_default().push(conn);
        }

        let mut had_success = false;

        for peer_conns in peers.values() {
            for conn in peer_conns {
                let summary = self
                    .sedimentrees
                    .lock()
                    .await
                    .get(&id)
                    .map(Sedimentree::summarize)
                    .unwrap_or_default();

                let BatchSyncResponse {
                    diff:
                        SyncDiff {
                            missing_commits,
                            missing_chunks,
                        },
                    ..
                } = conn
                    .call(
                        BatchSyncRequest {
                            id,
                            req_id: conn.next_request_id().await,
                            sedimentree_summary: summary,
                        },
                        timeout,
                    )
                    .await
                    .map_err(IoError::ConnCall)?;

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
            }
        }

        Ok(had_success)
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
    ) -> Result<bool, IoError<S, C>> {
        let tree_ids = self
            .sedimentrees
            .lock()
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut had_success = false;
        for id in tree_ids {
            let success = self.request_all_batch_sync(id, timeout).await?;
            if success {
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

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_commit_locally(
        &self,
        id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> Result<(), S::Error> {
        {
            let mut sed = self.sedimentrees.lock().await;
            let tree = sed.entry(id).or_default();
            if !tree.add_commit(commit.clone()) {
                return Ok(());
            }
        }

        self.storage.save_loose_commit(commit).await?;
        self.storage.save_blob(blob).await?;

        Ok(())
    }

    // NOTE no integrity checking, we assume that they made a good chunk at the right depth
    async fn insert_chunk_locally(
        &self,
        id: SedimentreeId,
        chunk: Chunk,
        blob: Blob,
    ) -> Result<(), S::Error> {
        {
            let mut sed = self.sedimentrees.lock().await;
            let tree = sed.entry(id).or_default();
            if !tree.add_chunk(chunk.clone()) {
                return Ok(());
            }
        }

        self.storage.save_chunk(chunk).await?;
        self.storage.save_blob(blob).await?;
        Ok(())
    }
}

impl<S: Storage, C: Connection> ConnectionPolicy for SedimentreeSync<S, C> {
    fn allowed_to_connect(&self, _peer_id: &PeerId) -> Result<(), ConnectionDisallowed> {
        Ok(()) // TODO currently allows all
    }
}

/// An error that can occur during I/O operations.
///
/// This covers storage and network connection errors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum IoError<S: Storage, C: Connection> {
    /// An error occurred while using storage.
    #[error(transparent)]
    Storage(S::Error),

    /// An error occurred while sending data on the connection.
    #[error(transparent)]
    ConnSend(C::SendError),

    /// An error occurred while receiving data from the connection.
    #[error(transparent)]
    ConnRecv(C::RecvError),

    /// An error occurred during a roundtrip call on the connection.
    #[error(transparent)]
    ConnCall(C::CallError),

    /// The connection was disallowed by the [`ConnectionPolicy`] policy.
    #[error(transparent)]
    ConnPolicy(#[from] ConnectionDisallowed),
}

/// An error that can occur while handling a blob request.
#[derive(Debug, Error)]
pub enum BlobRequestErr<S: Storage, C: Connection> {
    /// An IO error occurred while handling the blob request.
    #[error("IO error: {0}")]
    IoError(#[from] IoError<S, C>),

    /// Some requested blobs were missing locally.
    #[error("Missing blobs: {0:?}")]
    MissingBlobs(Vec<Digest>),
}

/// An error that can occur while handling a batch sync request.
#[derive(Debug, Error)]
pub enum ListenError<S: Storage, C: Connection> {
    /// An IO error occurred while handling the batch sync request.
    #[error(transparent)]
    IoError(#[from] IoError<S, C>),

    /// Missing blobs associated with local chunks or commits.
    #[error("Missing blobs associated to local chunks & commits: {0:?}")]
    MissingBlobs(Vec<Digest>),
}

async fn indexed_listen<K: Connection>(conn: K) -> Result<(K, Message), K::RecvError> {
    let msg = conn.recv().await?;
    Ok((conn, msg))
}

#[derive(Debug)]
struct ConnectionManager<C: Connection> {
    connections: HashMap<ConnectionId, C>,
    unstarted: HashSet<ConnectionId>,
}
