//! The main synchronization logic and bookkeeping for [`Sedimentree`].

pub mod error;
pub mod request;

use self::request::FragmentRequested;
use crate::{
    connection::{
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
        Connection, ConnectionDisallowed, ConnectionPolicy,
    },
    peer::id::PeerId,
};
use dashmap::DashMap;
use error::{BlobRequestErr, IoError, ListenError};
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    stream::{FusedStream, FuturesUnordered},
    FutureExt, StreamExt,
};
use nonempty::NonEmpty;
use sedimentree_core::{
    blob::{Blob, Digest},
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
    future::{FutureKind, Local, Sendable},
    storage::Storage,
    Fragment, LooseCommit, RemoteDiff, Sedimentree, SedimentreeId, SedimentreeSummary,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

/// The main synchronization manager for sedimentrees.
#[derive(Debug)]
pub struct Subduction<
    F: FutureKind,
    S: Storage<F>,
    C: Connection<F> + PartialEq,
    M: DepthMetric = CountLeadingZeroBytes,
> {
    depth_metric: M,
    sedimentrees: DashMap<SedimentreeId, Sedimentree>,
    next_connection_id: Arc<AtomicUsize>,
    conns: DashMap<ConnectionId, C>,
    storage: S,

    actor_channel: UnboundedSender<(ConnectionId, C)>,
    msg_queue: async_channel::Receiver<(ConnectionId, C, Message)>,

    _phantom: std::marker::PhantomData<F>,
}

impl<F: FutureKind, S: Storage<F>, C: Connection<F> + PartialEq, M: DepthMetric>
    Subduction<F, S, C, M>
{
    // /// Listen for incoming messages from all connections and handle them appropriately.
    // ///
    // /// This method runs indefinitely, processing messages as they arrive.
    // /// If no peers are connected, it will wait until a peer connects.
    // ///
    // /// # Errors
    // ///
    // /// * Returns `ListenError` if a storage or network error occurs.
    // // FIXME remove run now that we're using a queue in listen
    // pub async fn run(&mut self) -> Result<(), ListenError<F, S, C>> {
    //     tracing::info!("Starting Subduction run loop");
    //     loop {
    //         if let Err(RecvError) = self.ready_waiter.clone().recv().await {
    //             tracing::warn!("FIXME channel closed");
    //         }
    //         self.listen().await?;
    //     }
    // }

    /// Listen for incoming messages from all connections and handle them appropriately.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a storage or network error occurs.
    // FIXME wrap the msg queue or use an mpmc (probably the later)
    pub async fn listen(&self) -> Result<(), ListenError<F, S, C>> {
        while let Ok((conn_id, conn, msg)) = self.msg_queue.recv().await {
            self.dispatch(conn_id, &conn, msg).await?; // FIXME maybe just log if it breaks
            self.actor_channel
                .unbounded_send((conn_id, conn))
                .expect("FIXME")
        }
        Ok(())
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
            Message::Fragment { id, fragment, blob } => {
                self.recv_fragment(&from, id, &fragment, blob).await?;
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
                if self.conns.contains_key(&conn_id) {
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
    pub fn new<'a>(
        sedimentrees: DashMap<SedimentreeId, Sedimentree>,
        storage: S,
        conns: DashMap<ConnectionId, C>,
        depth_metric: M,
    ) -> (Self, Actor<'a, F, C>) {
        // FIXME NOTE: UNSTARTED ACTOR
        let (actor_sender, actor_receiver) = unbounded();
        let (queue_sender, queue_receiver) = async_channel::unbounded();

        let actor = Actor {
            inbox: actor_receiver,
            outbox: queue_sender,
            queue: FuturesUnordered::new(),
        };

        let sd = Self {
            depth_metric,
            sedimentrees,
            next_connection_id: Arc::new(AtomicUsize::new(0)),
            conns,
            storage,
            actor_channel: actor_sender,
            msg_queue: queue_receiver,
            _phantom: std::marker::PhantomData,
        };

        (sd, actor)
    }

    /// Add a [`Sedimentree`] to sync.
    pub fn add_sedimentree(&self, id: SedimentreeId, sedimentree: Sedimentree) {
        let existing = &mut self.sedimentrees.entry(id).or_default();
        existing.merge(sedimentree)
    }

    /// Remove a [`Sedimentree`].
    pub fn remove_sedimentree(&self, id: SedimentreeId) -> bool {
        self.sedimentrees.remove(&id).is_some()
    }

    /// The storage backend used for persisting sedimentree data.
    ///
    /// # Errors
    ///
    /// * Returns `S::Error` if the storage backend encounters an error.
    pub async fn hydrate(&self) -> Result<(), S::Error> {
        for tree_id in self
            .sedimentrees
            .iter()
            .map(|e| *e.key())
            .collect::<Vec<_>>()
        {
            if let Some(mut sedimentree) = self.sedimentrees.get_mut(&tree_id) {
                for commit in self.storage.load_loose_commits().await? {
                    tracing::trace!("Loaded commit {:?}", commit.digest());
                    sedimentree.add_commit(commit);
                }

                for fragment in self.storage.load_fragments().await? {
                    tracing::trace!("Loaded fragment {:?}", fragment.digest());
                    sedimentree.add_fragment(fragment);
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
            .iter()
            .map(|e| *e.key())
            .collect::<Vec<_>>()
        {
            self.request_peer_batch_sync(&peer_id, tree_id, None)
                .await?;
        }

        Ok((fresh, conn_id))
    }

    // /// Gracefully shut down a connection.
    // ///
    // /// # Errors
    // ///
    // /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    // pub async fn disconnect(&self, conn_id: &ConnectionId) -> Result<bool, C::DisconnectionError> {
    //     let mut locked = self.conn_manager.lock().await;
    //     locked.unstarted.remove(conn_id);
    //     if let Some(conn) = locked.connections.remove(conn_id) {
    //         conn.disconnect().await.map(|()| true)
    //     } else {
    //         Ok(false)
    //     }
    // }

    // /// Gracefully disconnect from all connections to a given peer ID.
    // ///
    // /// # Returns
    // ///
    // /// * `Ok(true)` if at least one connection was found and disconnected.
    // /// * `Ok(false)` if no connections to the given peer ID were found.
    // ///
    // /// # Errors
    // ///
    // /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    // pub async fn disconnect_from_peer(
    //     &self,
    //     peer_id: &PeerId,
    // ) -> Result<bool, C::DisconnectionError> {
    //     let mut touched = false;
    //     let mut locked = self.conn_manager.lock().await;

    //     let mut conn_meta = Vec::new();
    //     for (id, conn) in &locked.connections {
    //         conn_meta.push((*id, conn.peer_id()));
    //     }

    //     for (id, conn_peer_id) in &conn_meta {
    //         if *conn_peer_id == *peer_id {
    //             touched = true;
    //             locked.unstarted.remove(id);
    //             if let Some(conn) = locked.connections.remove(id) {
    //                 conn.disconnect().await?;
    //             }
    //         }
    //     }

    //     Ok(touched)
    // }

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
        tracing::info!("registering connection from peer {:?}", conn.peer_id());
        self.allowed_to_connect(&conn.peer_id()).await?;

        if let Some(hit) = self.conns.iter().find(|r| *r.value() == conn) {
            Ok((false, hit.key().clone()))
        } else {
            let counter = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
            let conn_id: ConnectionId = ConnectionId::new(counter);

            self.conns.insert(conn_id, conn.clone());
            self.actor_channel
                .unbounded_send((conn_id, conn))
                .expect("FIXME");

            Ok((true, conn_id))
        }
    }

    /// Low-level unregistration of a connection.
    pub async fn unregister(&self, conn_id: &ConnectionId) -> bool {
        self.conns.remove(conn_id).is_some()
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
    pub async fn get_local_blobs(
        &self,
        id: SedimentreeId,
    ) -> Result<Option<NonEmpty<Blob>>, S::Error> {
        if let Some(sedimentree) = self.sedimentrees.get(&id) {
            tracing::debug!("Found sedimentree with id {:?}", id);
            let mut results = Vec::new();

            for digest in sedimentree
                .loose_commits()
                .map(|loose| loose.blob().digest())
                .chain(
                    sedimentree
                        .fragments()
                        .map(|fragment| fragment.summary().blob_meta().digest()),
                )
            {
                if let Some(blob) = self.storage.load_blob(digest).await? {
                    results.push(blob);
                } else {
                    tracing::warn!("Missing blob for digest {:?}", digest);
                }
            }

            Ok(NonEmpty::from_vec(results))
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
    ) -> Result<Option<NonEmpty<Blob>>, IoError<F, S, C>> {
        if let Some(maybe_blobs) = self.get_local_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            if let Some(tree) = self.sedimentrees.get(&id) {
                let summary = tree.summarize();
                for entry in self.conns.iter() {
                    let conn = entry.value();
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
    /// * `Ok(None)` if the commit is not on a fragment boundary.
    /// * `Ok(Some(FragmentRequested))` if the commit is on a [`Fragment`] boundary.
    ///   In this case, please call `add_fragment` after creating the requested fragment.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn add_commit(
        &self,
        id: SedimentreeId,
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<Option<FragmentRequested>, IoError<F, S, C>> {
        self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        {
            for entry in self.conns.iter() {
                let conn = entry.value();
                conn.send(Message::LooseCommit {
                    id,
                    commit: commit.clone(),
                    blob: blob.clone(),
                })
                .await
                .map_err(IoError::ConnSend)?;
            }
        }

        let mut maybe_requested_fragment = None;

        let depth = self.depth_metric.to_depth(commit.digest());
        if depth != Depth(0) {
            maybe_requested_fragment = Some(FragmentRequested {
                head: commit.digest(),
                depth,
            });
        }

        Ok(maybe_requested_fragment)
    }

    /// Add a new (incremental) fragment locally and propagate it to all connected peers.
    ///
    /// NOTE this performs no integrity checks;
    /// we assume this is a good fragment at the right depth
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn add_fragment(
        &self,
        id: SedimentreeId,
        fragment: &Fragment,
        blob: Blob,
    ) -> Result<(), IoError<F, S, C>> {
        {
            let mut tree = self.sedimentrees.entry(id).or_default();
            tree.add_fragment(fragment.clone());
        }

        self.storage
            .save_blob(blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        {
            for entry in self.conns.iter() {
                let conn = entry.value();
                conn.send(Message::Fragment {
                    id,
                    fragment: fragment.clone(),
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
            for entry in self.conns.iter() {
                let conn = entry.value();
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

    /// Handle receiving a new fragment from a peer.
    ///
    /// Also propagates it to all other connected peers.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn recv_fragment(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        fragment: &Fragment,
        blob: Blob,
    ) -> Result<bool, IoError<F, S, C>> {
        let was_new = self
            .insert_fragment_locally(id, fragment.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            for entry in self.conns.iter() {
                let conn = entry.value();
                if conn.peer_id() != *from {
                    conn.send(Message::Fragment {
                        id,
                        fragment: fragment.clone(),
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
        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);

        let mut their_missing_commits = Vec::new();
        let mut their_missing_fragments = Vec::new();
        let mut our_missing_blobs = Vec::new();

        {
            let mut sedimentree = self.sedimentrees.entry(id).or_default();
            tracing::info!(
                "Received batch sync request for sedimentree {:?} with {} commits and {} fragments",
                id,
                their_summary.loose_commits().len(),
                their_summary.fragment_summaries().len()
            );

            let local_sedimentree = sedimentree.clone();
            let diff: RemoteDiff<'_> =
                local_sedimentree.diff_remote(their_summary, &self.depth_metric);

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

            for fragment in diff.local_fragments {
                if let Some(blob) = self
                    .storage
                    .load_blob(fragment.summary().blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_fragments.push((fragment.clone(), blob)); // TODO lots of cloning
                } else {
                    tracing::warn!("Missing blob for fragment {:?} ", fragment.digest(),);
                    our_missing_blobs.push(fragment.summary().blob_meta().digest());
                }
            }
        }

        tracing::info!(
            "Sending batch sync response for sedimentree {:?} with {} missing commits and {} missing fragments",
            id,
            their_missing_commits.len(),
            their_missing_fragments.len()
        );
        conn.send(
            BatchSyncResponse {
                id,
                req_id,
                diff: SyncDiff {
                    missing_commits: their_missing_commits,
                    missing_fragments: their_missing_fragments,
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
    /// * [`IoError`] if a storage or network error occurs while inserting commits or fragments.
    pub async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: &SyncDiff,
    ) -> Result<(), IoError<F, S, C>> {
        tracing::info!(
            "Received batch sync response for sedimentree {:?} from peer {:?} with {} missing commits and {} missing fragments",
            id,
            from,
            diff.missing_commits.len(),
            diff.missing_fragments.len()
        );

        for (commit, blob) in &diff.missing_commits {
            self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                .await
                .map_err(IoError::Storage)?;
        }

        for (fragment, blob) in &diff.missing_fragments {
            self.insert_fragment_locally(id, fragment.clone(), blob.clone())
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    /// Find blobs from connected peers.
    pub async fn request_blobs(&self, digests: Vec<Digest>) {
        for entry in self.conns.iter() {
            let conn = entry.value();
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
        for entry in &self.conns {
            if entry.value().peer_id() == *to_ask {
                peer_conns.push((entry.key().clone(), entry.value().clone()));
            }
        }

        let mut conn_errs = Vec::new();

        for (conn_id, conn) in peer_conns {
            tracing::info!("Using connection {:?} to peer {:?}", conn_id, to_ask);
            let summary = self
                .sedimentrees
                .get(&id)
                .map(|e| Sedimentree::summarize(e.value()))
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
                            missing_fragments,
                        },
                    ..
                }) => {
                    for (commit, blob) in missing_commits {
                        self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                            .await
                            .map_err(IoError::Storage)?;
                    }

                    for (fragment, blob) in missing_fragments {
                        self.insert_fragment_locally(id, fragment.clone(), blob.clone())
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
            for entry in self.conns.iter() {
                peers
                    .entry(entry.value().peer_id())
                    .or_default()
                    .push((entry.key().clone(), entry.value().clone()));
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
                        .get(&id)
                        .map(|e| Sedimentree::summarize(e.value()))
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
                                    missing_fragments,
                                },
                            ..
                        }) => {
                            for (commit, blob) in missing_commits {
                                self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO potentially a LOT of cloning
                                    .await
                                    .map_err(IoError::<F, S, C>::Storage)?;
                            }

                            for (fragment, blob) in missing_fragments {
                                self.insert_fragment_locally(id, fragment.clone(), blob.clone())
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
            .iter()
            .map(|e| *e.key())
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
    pub fn sedimentree_ids(&self) -> Vec<SedimentreeId> {
        self.sedimentrees
            .iter()
            .map(|e| *e.key())
            .collect::<Vec<_>>()
    }

    /// Get all commits for a given sedimentree ID.
    pub fn get_commits(&self, id: SedimentreeId) -> Option<Vec<LooseCommit>> {
        self.sedimentrees
            .get(&id)
            .map(|tree| tree.loose_commits().cloned().collect())
    }

    /// Get all fragments for a given sedimentree ID.
    pub fn get_fragments(&self, id: SedimentreeId) -> Option<Vec<Fragment>> {
        self.sedimentrees
            .get(&id)
            .map(|tree| tree.fragments().cloned().collect())
    }

    /// Get the set of all connected peer IDs.
    pub fn peer_ids(&self) -> HashSet<PeerId> {
        self.conns.iter().map(|r| r.value().peer_id()).collect()
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
            let mut tree = self.sedimentrees.entry(id).or_default();
            if !tree.add_commit(commit.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_loose_commit(commit).await?;
        self.storage.save_blob(blob).await?;

        Ok(true)
    }

    // NOTE no integrity checking, we assume that they made a good fragment at the right depth
    async fn insert_fragment_locally(
        &self,
        id: SedimentreeId,
        fragment: Fragment,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        {
            let mut tree = self.sedimentrees.entry(id).or_default();
            if !tree.add_fragment(fragment.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_fragment(fragment).await?;
        self.storage.save_blob(blob).await?;
        Ok(true)
    }
}

impl<F: FutureKind, S: Storage<F>, C: Connection<F> + PartialEq, M: DepthMetric> ConnectionPolicy
    for Subduction<F, S, C, M>
{
    async fn allowed_to_connect(&self, _peer_id: &PeerId) -> Result<(), ConnectionDisallowed> {
        Ok(()) // TODO currently allows all
    }
}

trait FireOnce<'a, C: Connection<Self>>: FutureKind {
    fn fire_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()>;
}

impl<'a, C: 'a + Connection<Sendable> + Send> FireOnce<'a, C> for Sendable {
    fn fire_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()> {
        async move {
            let msg = match conn.recv().await {
                Ok(msg) => msg,
                Err(e) => {
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                    return;
                }
            };

            if let Err(e) = sender.send((conn_id, conn, msg)).await {
                tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
            }
        }
        .boxed()
    }
}

impl<'a, C: 'a + Connection<Local>> FireOnce<'a, C> for Local {
    fn fire_once(
        conn_id: ConnectionId,
        conn: C,
        sender: async_channel::Sender<(ConnectionId, C, Message)>,
    ) -> Self::Future<'a, ()> {
        async move {
            match conn.recv().await {
                Ok(msg) => {
                    if let Err(e) = sender.send((conn_id, conn, msg)).await {
                        tracing::error!("unable to send msg about {conn_id} to Subduction: {e:?}");
                    }
                }
                Err(e) => {
                    tracing::error!("error when waiting for {conn_id} to receive: {e:?}");
                }
            }
        }
        .boxed_local()
    }
}

// FIXME rename
pub struct Actor<'a, F: FutureKind, C: Connection<F>> {
    inbox: UnboundedReceiver<(ConnectionId, C)>,
    outbox: async_channel::Sender<(ConnectionId, C, Message)>,
    queue: FuturesUnordered<F::Future<'a, ()>>,
}

impl<'a, F: FireOnce<'a, C>, C: Connection<F>> Actor<'a, F, C> {
    pub async fn listen(&mut self) {
        let mut inbox = self.inbox.by_ref().fuse();

        loop {
            if inbox.is_terminated() && self.queue.is_empty() {
                break;
            }

            futures::select! {
                maybe = inbox.next() => {
                    if let Some((conn_id, conn)) = maybe {
                        self.queue.push(F::fire_once(conn_id, conn, self.outbox.clone()));
                    }
                }

                _maybe = self.queue.next().fuse() => {
                    //  Just drain
                }
            }
        }
    }
}
