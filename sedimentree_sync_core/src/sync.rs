//! The main synchronization logic and bookkeeping for [`Sedimentree`].

use crate::{
    connection::{Connection, ConnectionDisallowed, ConnectionPolicy, SyncDiff},
    peer::id::PeerId,
};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Depth, Digest, LooseCommit, Sedimentree, SedimentreeId,
};
use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
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
pub struct SedimentreeSync<S: Storage, C: Connection + Clone> {
    sedimentrees: HashMap<SedimentreeId, Sedimentree>,
    storage: S,
    connections: HashMap<usize, C>,
}

impl<S: Storage, C: Connection + Clone> SedimentreeSync<S, C> {
    /// Initialize a new `SedimentreeSync` with the given storage backend and network adapters.
    #[tracing::instrument(skip_all, fields(sedimentree_ids = ?sedimentrees.keys()))]
    pub fn new(
        sedimentrees: HashMap<SedimentreeId, Sedimentree>, // FIXME remove this field entiely for now, replace with layered store?
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
    pub async fn attach_connection(&mut self, conn: C) -> Result<(), IoError<S, C>> {
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
                    conn.request_batch_sync(&summary)
                        .await
                        .map_err(IoError::Connection)?;
                }
            }

            let updated = self.get_local_blobs(id).await.map_err(IoError::Storage)?;

            Ok(updated)
        }
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
            .insert_commit_locally(id, commit.clone(), blob)
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            conn.send_loose_commit(commit)
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
            .save_blob(blob)
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            conn.send_chunk(chunk).await.map_err(IoError::Connection)?
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
        self.insert_commit_locally(id, commit.clone(), blob)
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            if conn.peer_id() != *from {
                conn.send_loose_commit(commit)
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
        self.insert_chunk_locally(id, chunk.clone(), blob)
            .await
            .map_err(IoError::Storage)?;

        for conn in self.connections.values() {
            if conn.peer_id() != *from {
                conn.send_chunk(chunk).await.map_err(IoError::Connection)?;
            }
        }

        Ok(())
    }

    /*********************
     * BATCH SYNCHRONIZE *
     *********************/

    /// Request a batch sync from a given peer for a given sedimentree ID.
    pub async fn request_peer_batch_sync(
        &mut self,
        to_ask: &PeerId,
        id: SedimentreeId,
    ) -> Result<bool, IoError<S, C>> {
        let peer_conns = self
            .connections
            .values()
            .filter(|conn| conn.peer_id() == *to_ask)
            .cloned();

        let mut had_success = false;

        for conn in peer_conns {
            let summary = self
                .sedimentrees
                .get(&id)
                .map(|s| s.summarize())
                .unwrap_or_default();

            let SyncDiff {
                missing_commits,
                missing_chunks,
            } = conn
                .request_batch_sync(&summary)
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
        let mut syced_peers = BTreeSet::new();

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
                    .request_batch_sync(&summary)
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
    pub fn ids(&self) -> impl Iterator<Item = &SedimentreeId> {
        self.sedimentrees.keys()
    }

    /// Get a reference to a sedimentree by its ID.
    pub fn entry(&mut self, id: SedimentreeId) -> Entry<'_, SedimentreeId, Sedimentree> {
        self.sedimentrees.entry(id)
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

impl<S: Storage, C: Connection + Clone> ConnectionPolicy for SedimentreeSync<S, C> {
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
