//! The main synchronization logic and bookkeeping for [`Sedimentree`].

use crate::{
    connection::{Connection, ConnectionDisallowed, ConnectionPolicy, Receive, SyncDiff, ToSend},
    peer::id::PeerId,
};
use futures::{stream::FuturesUnordered, StreamExt};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Depth, Digest, LooseCommit, Sedimentree, SedimentreeId,
    SedimentreeSummary,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
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
    sedimentrees: HashMap<SedimentreeId, Sedimentree>,
    storage: S,
    connections: HashMap<usize, C>,
}

impl<S: Storage, C: Connection> SedimentreeSync<S, C> {
    #[tracing::instrument(skip(self))]
    pub async fn listen(&mut self) -> Result<(), IoError<S, C>> {
        // FIXME key: usize shoudl be newtyped
        async fn indexed_listen<K: Connection>(conn: K) -> Result<(K, Receive), K::Error> {
            tracing::debug!("THERE");
            let msg = conn.recv().await?;
            tracing::info!("HERE");
            Ok((conn, msg))
        }

        let mut futs = FuturesUnordered::new();
        for conn in self.connections.values() {
            tracing::info!(
                "Spawning listener for connection {:?}",
                conn.connection_id()
            );
            let fut = indexed_listen(conn.clone());
            futs.push(fut);
        }

        tracing::info!("Listening for messages from {} connections", futs.len());
        while let Some(res) = futs.next().await {
            tracing::info!("Received a message from a connection");
            match res {
                Ok((conn, message)) => {
                    let idx = conn.connection_id();
                    let from = conn.peer_id();

                    // FIXME probably want a global queue for new dynamic connections
                    // Re-enque if that connection is still active at the top-level
                    if self.connections.contains_key(&idx) {
                        futs.push(indexed_listen(conn.clone()));
                    } else {
                        tracing::warn!("No connection found for FIXME");
                    }

                    tracing::info!("Received message from peer {:?}: {:?}", from, message);

                    match message {
                        Receive::LooseCommit { id, commit, blob } => {
                            self.recv_commit(&from, id, &commit, blob).await?
                        }
                        Receive::Chunk { id, chunk, blob } => {
                            self.recv_chunk(&from, id, &chunk, blob).await?
                        }
                        Receive::BatchSyncRequest {
                            id,
                            sedimentree_summary,
                        } => {
                            self.recv_batch_sync_request(id, &sedimentree_summary, &conn)
                                .await?
                        }
                        Receive::BatchSyncResponse { id, diff } => {
                            self.recv_batch_sync_response(&from, id, &diff).await?
                        }
                        Receive::BlobRequest { digests } => {
                            if self.connections.contains_key(&idx) {
                                self.recv_blob_request(&conn, &digests).await?
                            } else {
                                tracing::warn!("No connection found for FIXME");
                            }
                        }
                        Receive::BlobResponse { blobs } => {
                            for blob in blobs {
                                self.storage
                                    .save_blob(blob)
                                    .await
                                    .map_err(IoError::Storage)?;
                            }
                        }
                    }
                }
                Err(e) => { /* decide whether to bail or keep going */ }
            }
        }

        Ok(())
    }

    /// Initialize a new `SedimentreeSync` with the given storage backend and network adapters.
    #[tracing::instrument(skip_all, fields(sedimentree_ids = ?sedimentrees.keys()))]
    pub fn new(
        sedimentrees: HashMap<SedimentreeId, Sedimentree>,
        storage: S,
        connections: HashMap<usize, C>,
    ) -> Self {
        Self {
            sedimentrees,
            storage,
            connections,
        }
    }

    /// The storage backend used for persisting sedimentree data.
    pub async fn hydrate(&mut self) -> Result<(), S::Error> {
        for tree_id in self.sedimentrees.keys().copied().collect::<Vec<_>>() {
            if let Some(sedimentree) = self.sedimentrees.get_mut(&tree_id) {
                for commit in self.storage.load_loose_commits().await? {
                    tracing::trace!("Loaded commit {:?}", commit.digest());
                    sedimentree.add_commit(commit);
                }

                for chunk in self.storage.load_chunks().await? {
                    tracing::trace!("Loaded chunk {:?}", chunk.digest());
                    sedimentree.add_chunk(chunk);
                }

                // FIXME controversial!
                *sedimentree = sedimentree.minimize();
            }
        }

        Ok(())
    }

    /***************
     * CONNECTIONS *
     ***************/

    /// Attach a new [`Connection`] and immediately syncs all known [`Sedimentree`]s.
    #[tracing::instrument(skip(self, conn))]
    pub async fn attach_connection(&mut self, conn: C) -> Result<(), IoError<S, C>> {
        tracing::info!("Attaching connection to peer {:?}", conn.peer_id());

        let peer_id = conn.peer_id();
        self.register_connection(conn)?;

        for tree_id in self.sedimentrees.keys().copied().collect::<Vec<_>>() {
            self.request_peer_batch_sync(&peer_id, tree_id).await?;
        }

        Ok(())
    }

    /// Gracefully shut down a connection.
    pub async fn disconnect(&mut self, conn: &C) -> Result<bool, C::DisconnectionError> {
        if let Some(mut conn) = self.connections.remove(&conn.connection_id()) {
            conn.disconnect().await.map(|()| true)
        } else {
            Ok(false)
        }
    }

    /// Gracefully disconnect from all connections to a given peer ID.
    pub async fn disconnect_from_peer(
        &mut self,
        peer_id: &PeerId,
    ) -> Result<bool, C::DisconnectionError> {
        let mut labels_to_remove = Vec::new();
        for (label, conn) in self.connections.iter() {
            if conn.peer_id() == *peer_id {
                labels_to_remove.push(label.clone());
            }
        }

        if labels_to_remove.is_empty() {
            return Ok(false);
        }

        for label in labels_to_remove {
            if let Some(mut conn) = self.connections.remove(&label) {
                conn.disconnect().await?;
            }
        }

        Ok(true)
    }

    /****************************
     * LOW LEVEL CONNECTION API *
     ****************************/

    /// Low-level registration of a new connection.
    ///
    /// This does not perform any synchronization.
    pub fn register_connection(&mut self, conn: C) -> Result<bool, ConnectionDisallowed> {
        if self.connections.contains_key(&conn.connection_id()) {
            return Ok(false);
        }

        self.allowed_to_connect(&conn.peer_id())?;
        self.connections.insert(conn.connection_id(), conn);
        Ok(true)
    }

    /// Low-level unregistration of a connection.
    ///
    /// This does not perform any disconnection.
    pub fn unregister_connection(&mut self, conn: &C) -> bool {
        self.connections.remove(&conn.connection_id()).is_some()
    }

    /*********
     * BLOBS *
     *********/

    /// Get a blob from local storage by its digest.
    #[tracing::instrument(skip(self))]
    pub async fn get_local_blob(&self, digest: Digest) -> Result<Option<Blob>, S::Error> {
        if let Some(data) = self.storage.load_blob(digest).await? {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

    /// Get all blobs associated with a given sedimentree ID from local storage.
    #[tracing::instrument(skip(self))]
    pub async fn get_local_blobs(&self, id: SedimentreeId) -> Result<Option<Vec<Blob>>, S::Error> {
        if let Some(sedimentree) = self.sedimentrees.get(&id) {
            tracing::info!("Found sedimentree with id {:?}", id);

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
                // FIXME should be different for range queries?
                // let storage_key = StorageKey::new(vec![digest.to_string()]);

                if let Some(blob) = self.storage.load_blob(digest).await? {
                    results.push(blob);
                    break;
                } else {
                    todo!("FIXME can't find blob in storage")
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
    #[tracing::instrument(skip(self))]
    pub async fn fetch_blobs(&self, id: SedimentreeId) -> Result<Option<Vec<Blob>>, IoError<S, C>> {
        if let Some(blobs) = self.get_local_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(blobs))
        } else {
            if let Some(tree) = self.sedimentrees.get(&id) {
                let summary = tree.summarize();

                for conn in self.connections.values() {
                    conn.request_batch_sync(id, &summary)
                        .await
                        .map_err(IoError::Connection)?;
                }
            }

            let updated = self.get_local_blobs(id).await.map_err(IoError::Storage)?;

            Ok(updated)
        }
    }

    // FIXME visibility?
    pub async fn recv_blob_request(
        &mut self,
        conn: &C,
        digests: &[Digest],
    ) -> Result<(), IoError<S, C>> {
        let mut blobs = Vec::new();
        for digest in digests {
            if let Some(blob) = self
                .get_local_blob(*digest)
                .await
                .map_err(IoError::Storage)?
            {
                blobs.push(blob);
            } else {
                todo!("FIXME can't find blob in storage")
            }
        }

        conn.send(ToSend::Blobs { blobs: &blobs })
            .await
            .map_err(IoError::Connection)?;

        Ok(())
    }

    /***********************
     * INCREMENTAL CHANGES *
     ***********************/

    /// Add a new (incremental) commit locally and propagate it to all connected peers.
    #[tracing::instrument(skip(self))]
    pub async fn add_commit(
        &mut self,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<Option<ChunkRequested>, IoError<S, C>> {
        let maybe_requested_chunk = self
            .insert_commit_locally(id, commit.clone(), blob.clone()) // FIXME clone
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            conn.send(ToSend::LooseCommit {
                id,
                commit,
                blob: &blob,
            })
            .await
            .map_err(IoError::Connection)?
        }

        Ok(maybe_requested_chunk)
    }

    // TODO find next bounary using an iterator?

    /// Add a new (incremental) chunk locally and propagate it to all connected peers.
    ///
    /// NOTE this performs no integrity checks;
    /// we assume this is a good chunk at the right depth
    #[tracing::instrument(skip(self))]
    pub async fn add_chunk(
        &mut self,
        id: SedimentreeId,
        chunk: &Chunk,
        blob: Blob,
    ) -> Result<(), IoError<S, C>> {
        let tree = self.sedimentrees.entry(id).or_default();
        tree.add_chunk(chunk.clone());

        self.storage
            .save_blob(blob.clone()) // FIXME clone
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            conn.send(ToSend::Chunk {
                id,
                chunk,
                blob: &blob,
            })
            .await
            .map_err(IoError::Connection)?
        }

        Ok(())
    }

    /****************************
     * RECEIVE UPDATE FROM PEER *
     ****************************/

    /// Handle receiving a new commit from a peer.
    ///
    /// Also propagates it to all other connected peers.
    pub async fn recv_commit(
        &mut self,
        from: &PeerId,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<(), IoError<S, C>> {
        self.insert_commit_locally(id, commit.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            if conn.peer_id() != *from {
                conn.send(ToSend::LooseCommit {
                    id,
                    commit,
                    blob: &blob,
                })
                .await
                .map_err(IoError::Connection)?;
            }
        }

        Ok(())
    }

    /// Handle receiving a new chunk from a peer.
    ///
    /// Also propagates it to all other connected peers.
    pub async fn recv_chunk(
        &mut self,
        from: &PeerId,
        id: SedimentreeId,
        chunk: &Chunk,
        blob: Blob,
    ) -> Result<(), IoError<S, C>> {
        self.insert_chunk_locally(id, chunk.clone(), blob.clone()) // FIXME clone
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            if conn.peer_id() != *from {
                conn.send(ToSend::Chunk {
                    id,
                    chunk,
                    blob: &blob,
                })
                .await
                .map_err(IoError::Connection)?;
            }
        }

        Ok(())
    }

    /*********************
     * BATCH SYNCHRONIZE *
     *********************/

    pub async fn recv_batch_sync_request(
        &mut self,
        id: SedimentreeId,
        their_summary: &SedimentreeSummary,
        conn: &C,
    ) -> Result<(), IoError<S, C>> {
        let mut missing_commits = Vec::new();
        let mut missing_chunks = Vec::new();

        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);
        if let Some(sedimentree) = self.sedimentrees.get(&id) {
            // FIXME let our_summary = sedimentree.summarize();

            tracing::info!(
                "Received batch sync request for sedimentree {:?} with {} commits and {} chunks",
                id,
                their_summary.commits().len(),
                their_summary.chunk_summaries().len()
            );

            for commit in sedimentree.loose_commits() {
                if !their_summary.commits().contains(&commit) {
                    if let Some(blob) = self
                        .storage
                        .load_blob(commit.blob().digest())
                        .await
                        .map_err(IoError::Storage)?
                    {
                        missing_commits.push((commit.clone(), blob)); // FIXME clone
                    } else {
                        todo!()
                    }
                }
            }

            for chunk in sedimentree.chunks() {
                if !their_summary.chunk_summaries().contains(&chunk.summary()) {
                    if let Some(blob) = self
                        .storage
                        .load_blob(chunk.summary().blob_meta().digest())
                        .await
                        .map_err(IoError::Storage)?
                    {
                        missing_chunks.push((chunk.clone(), blob)); // FIXME clone
                    } else {
                        todo!()
                    }
                }
            }
        }
        tracing::info!("DONE FINDING DIFF");

        tracing::info!(
            "Sending batch sync response for sedimentree {:?} with {} missing commits and {} missing chunks",
            id,
            missing_commits.len(),
            missing_chunks.len()
        );
        conn.send(ToSend::BatchSyncResponse {
            id,
            diff: SyncDiff {
                missing_commits: missing_commits,
                missing_chunks: missing_chunks,
            },
        })
        .await
        .map_err(IoError::Connection)?;

        tracing::info!("DONE SENDING DIFF");

        Ok(())
    }

    pub async fn recv_batch_sync_response(
        &mut self,
        from: &PeerId,
        id: SedimentreeId,
        diff: &SyncDiff,
    ) -> Result<(), IoError<S, C>> {
        for (commit, blob) in diff.missing_commits.iter() {
            self.insert_commit_locally(id, commit.clone(), blob.clone()) // FIXME so much cloning
                .await
                .map_err(IoError::Storage)?;
        }

        for (chunk, blob) in diff.missing_chunks.iter() {
            self.insert_chunk_locally(id, chunk.clone(), blob.clone())
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    // Request a batch sync from a given peer for a given sedimentree ID.
    #[tracing::instrument(skip(self))]
    pub async fn request_peer_batch_sync(
        &mut self,
        to_ask: &PeerId,
        id: SedimentreeId,
    ) -> Result<bool, IoError<S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let peer_conns = self
            .connections
            .values()
            .filter(|conn| conn.peer_id() == *to_ask)
            .cloned();

        let mut had_success = false;

        for conn in peer_conns {
            tracing::info!(
                "Using connection {:?} to peer {:?}",
                conn.connection_id(),
                to_ask
            );
            let summary = self
                .sedimentrees
                .get(&id)
                .map(|s| s.summarize())
                .unwrap_or_default();

            tracing::info!("BEFORE");
            let SyncDiff {
                missing_commits,
                missing_chunks,
            } = conn
                .request_batch_sync(id, &summary)
                .await
                .map_err(IoError::Connection)?;
            tracing::info!("AFTER");

            for (commit, blob) in missing_commits {
                self.insert_commit_locally(id, commit.clone(), blob.clone()) // FIXME so much cloning
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

        Ok(had_success)
    }

    /// Request a batch sync from all connected peer for a given sedimentree ID.
    pub async fn request_all_batch_sync(
        &mut self,
        id: SedimentreeId,
    ) -> Result<bool, IoError<S, C>> {
        let peer_conns = self.connections.values().cloned();

        let mut peers: HashMap<PeerId, Vec<C>> = HashMap::new();
        for conn in peer_conns.into_iter() {
            peers.entry(conn.peer_id()).or_default().push(conn);
        }

        let mut had_success = false;
        let mut syced_peers = HashSet::new();

        for (peer_id, peer_conns) in peers.into_iter() {
            'inner: for conn in peer_conns {
                let summary = self
                    .sedimentrees
                    .get(&id)
                    .map(|s| s.summarize())
                    .unwrap_or_default();

                let SyncDiff {
                    missing_commits,
                    missing_chunks,
                } = conn
                    .request_batch_sync(id, &summary)
                    .await
                    .map_err(IoError::Connection)?;

                for (commit, blob) in missing_commits {
                    self.insert_commit_locally(id, commit.clone(), blob.clone()) // FIXME so much cloning
                        .await
                        .map_err(IoError::Storage)?;
                }

                for (chunk, blob) in missing_chunks {
                    self.insert_chunk_locally(id, chunk.clone(), blob.clone())
                        .await
                        .map_err(IoError::Storage)?;
                }

                had_success = true;
                syced_peers.insert(peer_id.clone());
                break 'inner;
            }
        }

        Ok(had_success)
    }

    /********************
     * PUBLIC UTILITIES *
     ********************/

    /// Get an iterator over all known sedimentree IDs.
    pub fn sedimentree_ids(&self) -> impl Iterator<Item = &SedimentreeId> {
        self.sedimentrees.keys()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_commit_locally(
        &mut self,
        id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> Result<Option<ChunkRequested>, S::Error> {
        let digest = commit.digest();
        let tree = self.sedimentrees.entry(id).or_default();
        if !tree.add_commit(commit.clone()) {
            return Ok(None);
        }

        self.storage.save_loose_commit(commit).await?;

        let mut maybe_requested_chunk = None;

        let depth = Depth::from(digest);
        if depth != Depth(0) {
            maybe_requested_chunk = Some(ChunkRequested {
                head: digest,
                depth,
            });
        }

        self.storage.save_blob(blob).await?;

        Ok(maybe_requested_chunk)
    }

    // NOTE no integrity checking, we assume that they made a good chunk at the right depth
    async fn insert_chunk_locally(
        &mut self,
        id: SedimentreeId,
        chunk: Chunk,
        blob: Blob,
    ) -> Result<(), S::Error> {
        let tree = self.sedimentrees.entry(id).or_default();
        if !tree.add_chunk(chunk.clone()) {
            return Ok(());
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

    /// An error occurred while using a network connection.
    #[error(transparent)]
    Connection(C::Error),

    /// The connection was disallowed by the [`ConnectionPolicy`] policy.
    #[error(transparent)]
    Policy(#[from] ConnectionDisallowed),
}
