use crate::{
    connection::{Connection, ConnectionDisallowed, ConnectionPolicy, SyncDiff},
    peer::id::PeerId,
};
use sedimentree_core::{
    storage::Storage, Blob, Chunk, Depth, Digest, LooseCommit, Sedimentree, SedimentreeId,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChunkRequested {
    pub head: Digest,
    pub depth: Depth,
}

#[derive(Debug, Clone)]
pub struct SedimentreeSync<S: Storage, C: Connection + Clone> {
    // FIXME make this work on a SINGLE sedimentree at a time?!
    sedimentrees: HashMap<SedimentreeId, Sedimentree>,
    storage: S,
    connections: HashMap<usize, C>,
}

impl<S: Storage, C: Connection + Clone> ConnectionPolicy for SedimentreeSync<S, C> {
    fn allowed_to_connect(&self, _peer_id: &PeerId) -> Result<(), ConnectionDisallowed> {
        Ok(()) // TODO currently allows all
    }
}

impl<S: Storage, C: Connection + Clone> SedimentreeSync<S, C> {
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

    /***************
     * CONNECTIONS *
     ***************/

    pub async fn attach_connection(&mut self, conn: C) -> Result<(), IoError<S, C>> {
        let peer_id = conn.peer_id();
        self.register_connection(conn)?;

        for tree_id in self.sedimentrees.keys().copied().collect::<Vec<_>>() {
            self.request_batch_sync(&peer_id, tree_id).await?;
        }

        Ok(())
    }

    pub fn register_connection(&mut self, conn: C) -> Result<bool, ConnectionDisallowed> {
        if self.connections.contains_key(&conn.connection_id()) {
            return Ok(false);
        }

        self.allowed_to_connect(&conn.peer_id())?;
        self.connections.insert(conn.connection_id(), conn);
        Ok(true)
    }

    pub async fn disconnect(&mut self, conn: &C) -> Result<bool, C::DisconnectionError> {
        if let Some(mut conn) = self.connections.remove(&conn.connection_id()) {
            conn.disconnect().await.map(|()| true)
        } else {
            Ok(false)
        }
    }

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

    /*********
     * BLOBS *
     *********/

    #[tracing::instrument(skip(self))]
    pub async fn get_local_blob(&self, digest: Digest) -> Result<Option<Blob>, S::Error> {
        if let Some(data) = self.storage.load_blob(digest).await? {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }

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

    // NOTE no integrity checking, we assume that they made a good chunk at the right depth
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

    pub async fn request_batch_sync(
        &mut self,
        to_ask: &PeerId,
        id: SedimentreeId,
    ) -> Result<bool, IoError<S, C>> {
        let peer_conns = self
            .connections
            .values()
            .filter(|conn| conn.peer_id() == *to_ask)
            .cloned();

        let mut success = false;

        for conn in peer_conns {
            let summary = self.sedimentrees.get(&id).expect("FIXME").summarize();

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

            success = true;
            break;
        }

        Ok(success)
    }

    /********************
     * PUBLIC UTILITIES *
     ********************/

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

    // FIXME make peerid [u8; 32]?
}

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
