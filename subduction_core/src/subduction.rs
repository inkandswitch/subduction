//! The main synchronization logic and bookkeeping for [`Sedimentree`].

pub mod error;
pub mod request;

use crate::{
    connection::{
        actor::{ConnectionActor, ConnectionActorFuture, StartConnectionActor},
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
        recv_once::RecvOnce,
        Connection, ConnectionDisallowed, ConnectionPolicy,
    },
    peer::id::PeerId,
};
use async_channel::{bounded, Sender};
use async_lock::Mutex;
use error::{HydrationError, BlobRequestErr, IoError, ListenError, RegistrationError};
use futures::{
    FutureExt, StreamExt, future::try_join_all, stream::{AbortHandle, AbortRegistration, Abortable, Aborted, FuturesUnordered}
};
use futures_kind::{Local, Sendable};
use nonempty::NonEmpty;
use request::FragmentRequested;
use sedimentree_core::{
    blob::{Blob, Digest},
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
    storage::Storage,
    Fragment, LooseCommit, RemoteDiff, Sedimentree, SedimentreeId, SedimentreeSummary,
};
use alloc::{
    boxed::Box,
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    sync::Arc,
    vec::Vec,
};
use core::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    time::Duration,
};

/// The main synchronization manager for sedimentrees.
#[derive(Debug, Clone)]
pub struct Subduction<
    'a,
    F: SubductionFutureKind<'a, S, C, M>,
    S: Storage<F>,
    C: Connection<F> + PartialEq,
    M: DepthMetric = CountLeadingZeroBytes,
> {
    depth_metric: M,
    sedimentrees: Arc<Mutex<BTreeMap<SedimentreeId, Sedimentree>>>,
    next_connection_id: Arc<AtomicUsize>,
    conns: Arc<Mutex<BTreeMap<ConnectionId, C>>>,
    storage: S,

    actor_channel: Sender<(ConnectionId, C)>,
    msg_queue: async_channel::Receiver<(ConnectionId, C, Message)>,

    abort_actor_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a F>,
}

impl<
        'a,
        F: SubductionFutureKind<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > Subduction<'a, F, S, C, M>
{
    /// Initialize a new `Subduction` with the given storage backend and network adapters.
    #[allow(clippy::type_complexity)]
    pub fn new(
        storage: S,
        depth_metric: M,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, F, S, C, M>,
        ConnectionActorFuture<'a, F, C>,
    ) {
        tracing::info!("initializing Subduction instance");

        let (actor_sender, actor_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(256);
        let actor = ConnectionActor::<'a, F, C>::new(actor_receiver, queue_sender);

        let (abort_actor_handle, abort_actor_reg) = AbortHandle::new_pair();
        let (abort_listener_handle, abort_listener_reg) = AbortHandle::new_pair();

        let sd = Arc::new(Self {
            depth_metric,
            sedimentrees: Arc::new(Mutex::new(BTreeMap::new())),
            next_connection_id: Arc::new(AtomicUsize::new(0)),
            conns: Arc::new(Mutex::new(BTreeMap::new())),
            storage,
            actor_channel: actor_sender,
            msg_queue: queue_receiver,
            abort_actor_handle,
            abort_listener_handle,
            _phantom: PhantomData,
        });

        (
            sd.clone(),
            ListenerFuture::new(F::start_listener(sd, abort_listener_reg)),
            ConnectionActorFuture::new(F::start_actor(actor, abort_actor_reg)),
        )
    }

    /// Hydrate a `Subduction` instance from existing sedimentrees in external storage.
    ///
    /// # Errors
    ///
    /// * Returns [`HydrationError`] if loading from storage fails.
    pub async fn hydrate(storage: S, depth_metric: M
    ) -> Result<(
        Arc<Self>,
        ListenerFuture<'a, F, S, C, M>,
        ConnectionActorFuture<'a, F, C>,
    ), HydrationError<F, S>> {
        let ids = storage.load_all_sedimentree_ids().await.map_err(HydrationError::LoadAllIdsError)?;
        let (subduction, fut_listener, conn_actor) = Self::new(storage, depth_metric);
        for id in ids {
            let loose_commits = subduction.storage.load_loose_commits(id).await.map_err(HydrationError::LoadLooseCommitsError)?;
            let fragments = subduction.storage.load_fragments(id).await.map_err(HydrationError::LoadFragmentsError)?;
            let sedimentree = Sedimentree::new(fragments, loose_commits);

            {
                let mut locked = subduction.sedimentrees.lock().await;
                locked.entry(id).or_default().merge(sedimentree);
            }
        }
        Ok((subduction, fut_listener, conn_actor))
    }

    /// Listen for incoming messages from all connections and handle them appropriately.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a storage or network error occurs.
    pub async fn listen(&self) -> Result<(), ListenError<F, S, C>> {
        tracing::info!("starting Subduction listener");
        while let Ok((conn_id, conn, msg)) = self.msg_queue.recv().await {
            tracing::debug!(
                "Subduction listener received message from {:?}: {:?}",
                conn_id,
                msg
            );

            let should_reregister = if let Err(e) = self.dispatch(conn_id, &conn, msg).await {
                tracing::error!(
                    "error dispatching message from connection {:?}: {}",
                    conn_id,
                    e
                );
                // Connection is broken - unregister it but keep draining messages
                let _ = self.unregister(&conn_id).await;
                tracing::info!("unregistered failed connection {:?}", conn_id);

                // NOTE Re-register temporarily to drain remaining messages from the channel
                // This prevents messages from piling up in inbound_writer
                true
            } else {
                true
            };

            if should_reregister
                && let Err(e) = self.actor_channel.send((conn_id, conn)).await {
                    tracing::error!(
                        "error re-sending connection {:?} to actor channel: {:?}",
                        conn_id,
                        e
                    );
                    // Channel closed, exit loop
                    break;
                }
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
        tracing::info!("dispatch: {:?}: {:?}", from, message);

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
                    tracing::warn!(
                        "missing blobs for batch sync request from peer {:?}: {:?}",
                        from,
                        missing
                    );
                    self.request_blobs(missing).await;
                    self.recv_batch_sync_request(id, &sedimentree_summary, req_id, conn)
                        .await?; // try responing again
                }
            }
            Message::BatchSyncResponse(BatchSyncResponse { id, diff, .. }) => {
                self.recv_batch_sync_response(&from, id, &diff).await?;
            }
            Message::BlobsRequest(digests) => {
                let is_contained = { self.conns.lock().await.contains_key(&conn_id) };
                if is_contained {
                    match self.recv_blob_request(conn, &digests).await {
                        Ok(()) => {
                            tracing::info!(
                                "successfully handled blob request from peer {:?}",
                                from
                            );
                        }
                        Err(BlobRequestErr::IoError(e)) => Err(e)?,
                        Err(BlobRequestErr::MissingBlobs(missing)) => {
                            tracing::warn!(
                                "missing blobs for request from peer {:?}: {:?}",
                                from,
                                missing
                            );
                        }
                    }
                } else {
                    tracing::warn!("no open connection for request ({:?})", conn_id);
                }
            }
            Message::BlobsResponse(blobs) => {
                let len = blobs.len();
                for blob in blobs {
                    self.storage
                        .save_blob(blob)
                        .await
                        .map_err(IoError::Storage)?;
                }
                tracing::info!(
                    "saved {len} blobs from blob response from peer {from}, no reply needed",
                );
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

        let ids = {
          self.sedimentrees
              .lock()
              .await
              .keys()
              .copied()
              .collect::<Vec<_>>()
        };

        for tree_id in ids {
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
        tracing::info!("Disconnecting connection {:?}", conn_id);
        let removed = { self.conns.lock().await.remove(conn_id) };
        if let Some(conn) = removed {
            conn.disconnect().await.map(|()| true)
        } else {
            Ok(false)
        }
    }

    /// Gracefully disconnect from all connections.
    ///
    /// # Errors
    ///
    /// * Returns [`C::DisconnectionError`] if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_all(&self) -> Result<(), C::DisconnectionError> {
        let conns: Vec<_> = {
            let mut guard = self.conns.lock().await;
            core::mem::take(&mut *guard).values().cloned().collect()
        };

        try_join_all(conns.into_iter().map(|conn| async move {
            conn.disconnect().await
        })).await?;

        Ok(())
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
        &self,
        peer_id: &PeerId,
    ) -> Result<bool, C::DisconnectionError> {
        let mut touched = false;

        let mut conn_meta = Vec::new();
        {
            for (key, value) in self.conns.lock().await.iter() {
                conn_meta.push((*key, value.peer_id()));
            }
        }

        for (id, conn_peer_id) in &conn_meta {
            if *conn_peer_id == *peer_id {
                touched = true;
                let removed = { self.conns.lock().await.remove(id) };
                if let Some(conn) = removed
                    && let Err(e) = conn.disconnect().await {
                        tracing::error!("{e}");
                        return Err(e);
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
    pub async fn register(&self, conn: C) -> Result<(bool, ConnectionId), RegistrationError> {
        tracing::info!("registering connection from peer {:?}", conn.peer_id());
        self.allowed_to_connect(&conn.peer_id()).await?;

        let conns = { self.conns.lock().await.clone() };

        if let Some((hit, _)) = conns.iter().find(|(_, value)| **value == conn) {
            Ok((false, *hit))
        } else {
            let counter = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
            let conn_id: ConnectionId = ConnectionId::new(counter);

            self.conns.lock().await.insert(conn_id, conn.clone());
            self.actor_channel
                .send((conn_id, conn))
                .await
                .map_err(|_| RegistrationError::SendToClosedChannel)?;

            Ok((true, conn_id))
        }
    }

    /// Low-level unregistration of a connection.
    pub async fn unregister(&self, conn_id: &ConnectionId) -> bool {
        self.conns.lock().await.remove(conn_id).is_some()
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
        tracing::debug!("Looking for blob with digest {:?}", digest);
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
        tracing::debug!("Getting local blobs for sedimentree with id {:?}", id);
        let tree = { self.sedimentrees.lock().await.get(&id).cloned() };
        if let Some(sedimentree) = tree {
            tracing::debug!("Found sedimentree with id {:?}", id);
            let mut results = Vec::new();

            for digest in sedimentree
                .loose_commits()
                .map(|loose| loose.blob_meta().digest())
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
        tracing::debug!("Fetching blobs for sedimentree with id {:?}", id);
        if let Some(maybe_blobs) = self.get_local_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            let tree = { self.sedimentrees.lock().await.get(&id).cloned() };
            if let Some(tree) = tree {
                let summary = tree.summarize();
                let conns: Vec<_> = { self.conns.lock().await.values().cloned().collect() };
                for conn in conns {
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

                    if let Err(e) = self.recv_batch_sync_response(&conn.peer_id(), id, &diff).await {
                        tracing::error!(
                            "error handling batch sync response from peer {:?}: {}",
                            conn.peer_id(),
                            e
                        );
                    }
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

    /// Add a new sedimentree locally and propagate it to all connected peers.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn add_sedimentree(&self, id: SedimentreeId, sedimentree: Sedimentree, blobs: Vec<Blob>) -> Result<(), IoError<F, S, C>> {
        self.insert_sedimentree_locally(id, sedimentree, blobs)
            .await
            .map_err(IoError::Storage)?;
        self.request_all_batch_sync(id, None).await?;
        Ok(())
    }

    /// Remove a sedimentree locally and delete all associated data from storage.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn remove_sedimentree(&self, id: SedimentreeId) -> Result<(), IoError<F, S, C>> {
        let maybe_sedimentree = {
            self.sedimentrees
                .lock()
                .await
                .remove(&id)
        };

        if let Some(sedimentree) = maybe_sedimentree {
            for commit in sedimentree.loose_commits() {
                self.storage.delete_blob(commit.blob_meta().digest()).await.map_err(IoError::Storage)?;
            }
            self.storage.delete_loose_commits(id).await.map_err(IoError::Storage)?;

            for fragment in sedimentree.fragments() {
                self.storage.delete_blob(fragment.summary().blob_meta().digest()).await.map_err(IoError::Storage)?;
            }
            self.storage.delete_fragments(id).await.map_err(IoError::Storage)?;
        }

        Ok(())
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
        tracing::debug!(
            "adding commit {:?} to sedimentree {:?}",
            commit.digest(),
            id
        );

        self.insert_commit_locally(id, commit.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        {
            let conns = { self.conns.lock().await.values().cloned().collect::<Vec<_>>() };
            for conn in conns {
                tracing::debug!(
                    "Propagating commit {:?} for sedimentree {:?} to peer {:?}",
                    commit.digest(),
                    id,
                    conn.peer_id()
                );

                if let Err(e) = conn.send(Message::LooseCommit {
                    id,
                    commit: commit.clone(),
                    blob: blob.clone(),
                }).await {
                    tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
                }
            }
        }

        let mut maybe_requested_fragment = None;

        let depth = self.depth_metric.to_depth(commit.digest());
        if depth != Depth(0) {
            maybe_requested_fragment = Some(FragmentRequested::new(commit.digest(), depth));
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
        tracing::debug!(
            "Adding fragment {:?} to sedimentree {:?}",
            fragment.digest(),
            id
        );

        {
            self.sedimentrees.lock().await.entry(id).or_default().add_fragment(fragment.clone());
        }

        self.storage
            .save_blob(blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        let conns = { self.conns.lock().await.values().cloned().collect::<Vec<_>>() };
        for conn in conns {
            tracing::debug!(
                "Propagating fragment {:?} for sedimentree {:?} to peer {:?}",
                fragment.digest(),
                id,
                conn.peer_id()
            );
            if let Err(e) = conn.send(Message::Fragment {
                id,
                fragment: fragment.clone(),
                blob: blob.clone(),
            }).await {
                tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
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
        tracing::debug!(
            "receiving commit {:?} for sedimentree {:?} from peer {:?}",
            commit.digest(),
            id,
            from
        );

        let was_new = self
            .insert_commit_locally(id, commit.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let conns = { self.conns.lock().await.values().cloned().collect::<Vec<_>>() };
            for conn in conns {
                if conn.peer_id() != *from
                    && let Err(e) = conn.send(Message::LooseCommit {
                        id,
                        commit: commit.clone(),
                        blob: blob.clone(),
                    })
                    .await {
                        tracing::error!("{e}");
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
        tracing::debug!(
            "receiving fragment {:?} for sedimentree {:?} from peer {:?}",
            fragment.digest(),
            id,
            from
        );

        let was_new = self
            .insert_fragment_locally(id, fragment.clone(), blob.clone()) // TODO lots of cloning
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let conns = { self.conns.lock().await.values().cloned().collect::<Vec<_>>() };
            for conn in conns {
                if conn.peer_id() != *from
                    && let Err(e) = conn.send(Message::Fragment {
                        id,
                        fragment: fragment.clone(),
                        blob: blob.clone(),
                    })
                    .await {
                        tracing::error!("{e}");
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

        let sync_diff = {
            let mut locked = self.sedimentrees.lock().await;
            let sedimentree = locked.entry(id).or_default();
            let local_sedimentree = sedimentree.clone();
            tracing::debug!(
                "received batch sync request for sedimentree {id:?} for req_id {req_id:?} with {} commits and {} fragments",
                their_summary.loose_commits().len(),
                their_summary.fragment_summaries().len()
            );

            let diff: RemoteDiff<'_> =
                local_sedimentree.diff_remote(their_summary, &self.depth_metric);

            for commit in diff.remote_commits {
                sedimentree.add_commit(commit.clone());
            }

            for commit in diff.local_commits {
                if let Some(blob) = self
                    .storage
                    .load_blob(commit.blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_commits.push((commit.clone(), blob)); // TODO lots of cloning
                } else {
                    tracing::warn!("missing blob for commit {:?}", commit.digest(),);
                    our_missing_blobs.push(commit.blob_meta().digest());
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
                    tracing::warn!("missing blob for fragment {:?} ", fragment.digest(),);
                    our_missing_blobs.push(fragment.summary().blob_meta().digest());
                }
            }

            tracing::info!(
                "sending batch sync response for sedimentree {id:?} on req_id {req_id:?}, with {} missing commits and {} missing fragments",
                their_missing_commits.len(),
                their_missing_fragments.len()
            );

            SyncDiff {
                missing_commits: their_missing_commits,
                missing_fragments: their_missing_fragments,
            }
        };

        if let Err(e) = conn.send(
            BatchSyncResponse {
                id,
                req_id,
                diff: sync_diff
            }
            .into(),
        ).await {
            tracing::error!("{e}");
        }

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
            "received batch sync response for sedimentree {:?} from peer {:?} with {} missing commits and {} missing fragments",
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
        let conns = { self.conns.lock().await.values().cloned().collect::<Vec<_>>() };
        for conn in conns {
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
    ) -> Result<(bool, Vec<Blob>, Vec<(C, C::CallError)>), IoError<F, S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let mut blobs = Vec::new();
        let mut had_success = false;
        let mut peer_conns = Vec::new();

        let conns = { self.conns.lock().await.clone() };
        for (conn_id, conn) in conns {
            if conn.peer_id() == *to_ask {
                peer_conns.push((conn_id, conn));
            }
        }

        let mut conn_errs = Vec::new();

        for (conn_id, conn) in peer_conns {
            tracing::info!("Using connection {:?} to peer {:?}", conn_id, to_ask);
            let summary = {
                self
                    .sedimentrees
                    .lock()
                    .await
                    .get(&id)
                    .map(Sedimentree::summarize)
                    .unwrap_or_default()
            };

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
                        blobs.push(blob);
                    }

                    for (fragment, blob) in missing_fragments {
                        self.insert_fragment_locally(id, fragment.clone(), blob.clone())
                            .await
                            .map_err(IoError::Storage)?;
                        blobs.push(blob);
                    }

                    had_success = true;
                    break;
                }
            }
        }

        Ok((had_success, blobs, conn_errs))
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
    #[allow(clippy::too_many_lines)]
    pub async fn request_all_batch_sync(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<
        BTreeMap<PeerId, (bool, Vec<Blob>, Vec<(C, <C as Connection<F>>::CallError)>)>,
        IoError<F, S, C>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let mut peers: BTreeMap<PeerId, Vec<(ConnectionId, C)>> = BTreeMap::new();
        {
            for (conn_id, conn) in self.conns.lock().await.iter() {
                peers
                    .entry(conn.peer_id())
                    .or_default()
                    .push((*conn_id, conn.clone()));
            }
        }

        tracing::debug!("Found {} peer(s)", peers.len());
        let blobs = Arc::new(Mutex::new(BTreeSet::<Blob>::new()));
        let mut set: FuturesUnordered<_> = peers
            .iter()
            .map(|(peer_id, peer_conns)| {
                let inner_blobs = blobs.clone();
                async move {
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
                        let summary = {
                            self
                                .sedimentrees
                                .lock()
                                .await
                                .get(&id)
                                .map(Sedimentree::summarize)
                                .unwrap_or_default()
                        };

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
                                    inner_blobs.lock().await.insert(blob);
                                }

                                for (fragment, blob) in missing_fragments {
                                    self.insert_fragment_locally(
                                        id,
                                        fragment.clone(),
                                        blob.clone(),
                                    )
                                    .await
                                    .map_err(IoError::<F, S, C>::Storage)?;
                                    inner_blobs.lock().await.insert(blob);
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
                }
            })
            .collect();

        let mut out = BTreeMap::new();
        while let Some(result) = set.next().await {
            match result {
                Err(e) =>  {
                    tracing::error!("{e}");
                }
                Ok((peer_id, success, errs)) => {
                    let blob_vec = { blobs.lock().await.iter().cloned().collect::<Vec<Blob>>() };
                    out.insert(peer_id, (success, blob_vec, errs));
                }
            }
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
    ) -> Result<(bool, Vec<Blob>, Vec<(C, <C as Connection<F>>::CallError)>), IoError<F, S, C>>
    {
        tracing::info!("Requesting batch sync for all sedimentrees from all peers");
        let tree_ids = {
            self
                .sedimentrees
                .lock()
                .await
                .keys()
                .copied()
                .collect::<Vec<_>>()
        };

        let mut had_success = false;
        let mut blobs: Vec<Blob> = Vec::new();
        let mut errs = Vec::new();
        for id in tree_ids {
            tracing::debug!("Requesting batch sync for sedimentree {:?}", id);
            let all_results = self.request_all_batch_sync(id, timeout).await?;
            if all_results
                .values()
                .any(|(success, _blobs, _errs)| *success)
            {
                for (_, (_, step_blobs, step_errs)) in all_results {
                    blobs.extend(step_blobs);
                    errs.extend(step_errs);
                }

                had_success = true;
            }
        }
        Ok((had_success, blobs, errs))
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

    /// Get all fragments for a given sedimentree ID.
    pub async fn get_fragments(&self, id: SedimentreeId) -> Option<Vec<Fragment>> {
        self.sedimentrees
            .lock()
            .await
            .get(&id)
            .map(|tree| tree.fragments().cloned().collect())
    }

    /// Get the set of all connected peer IDs.
    pub async fn peer_ids(&self) -> BTreeSet<PeerId> {
        self.conns.lock().await.values().map(Connection::peer_id).collect()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_sedimentree_locally(&self, id: SedimentreeId, sedimentree: Sedimentree, blobs: Vec<Blob>) -> Result<(), S::Error> {
        tracing::debug!("adding sedimentree with id {:?}", id);

        self.storage.save_sedimentree_id(id).await?;

        for commit in sedimentree.loose_commits() {
            self.storage
                .save_loose_commit(id, commit.clone())
                .await?;
        }

        for fragment in sedimentree.fragments() {
            self.storage
                .save_fragment(id, fragment.clone())
                .await?;
        }

        for blob in blobs {
            self.storage.save_blob(blob).await?;
        }

        {
            self.sedimentrees.lock().await.entry(id).or_default().merge(sedimentree);
        }

        Ok(())
    }

    // /// Remove a sedimentree locally.
    // pub async fn remove_sedimentree_locally(&self, id: SedimentreeId) -> Result<bool, IoError<F, S, C>> {
    //     tracing::debug!("removing sedimentree with id {:?}", id);

    //     let maybe_removed = {
    //         let mut locked = self.sedimentrees.lock().await;
    //         locked.remove(&id)
    //     };

    //     if let Some(removed) = maybe_removed {
    //         tracing::debug!(
    //             "removed sedimentree with id {:?}, which had {} commits and {} fragments",
    //             id,
    //             removed.loose_commits().count(),
    //             removed.fragments().count()
    //         );

    //         for commit in removed.loose_commits() {
    //             tracing::debug!("removed commit {:?}", commit.digest());
    //             self.storage
    //                 .delete_blob(commit.blob_meta().digest())
    //                 .await
    //                 .map_err(IoError::Storage)?;
    //         }
    //         for fragment in removed.fragments() {
    //             tracing::debug!("removed fragment {:?}", fragment.digest());
    //         }
    //     } else {
    //         tracing::debug!("no sedimentree with id {:?} found to remove", id);
    //         return Ok(false);
    //     }

    //     self.storage
    //         .delete_sedimentree_id(id)
    //         .await
    //         .map_err(IoError::Storage)?;

    //     self.storage
    //         .delete_loose_commits(id)
    //         .await
    //         .map_err(IoError::Storage)?;

    //     self.storage
    //         .delete_fragments(id)
    //         .await
    //         .map_err(IoError::Storage)?;

    //     Ok(())
    // }


    async fn insert_commit_locally(
        &self,
        id: SedimentreeId,
        commit: LooseCommit,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        tracing::debug!("inserting commit {:?} locally", commit.digest());
        {
            let mut locked = self.sedimentrees.lock().await;
            if !locked.entry(id).or_default().add_commit(commit.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_sedimentree_id(id).await?;
        self.storage.save_loose_commit(id, commit).await?;
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
            let mut locked = self.sedimentrees.lock().await;
            if !locked.entry(id).or_default().add_fragment(fragment.clone()) {
                return Ok(false);
            }
        }

        self.storage.save_sedimentree_id(id).await?;
        self.storage.save_fragment(id, fragment).await?;
        self.storage.save_blob(blob).await?;
        Ok(true)
    }
}

impl<
        'a,
        F: SubductionFutureKind<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > Drop for Subduction<'a, F, S, C, M>
{
    fn drop(&mut self) {
        self.abort_actor_handle.abort();
        self.abort_listener_handle.abort();
    }
}

impl<
        'a,
        F: SubductionFutureKind<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > ConnectionPolicy for Subduction<'a, F, S, C, M>
{
    async fn allowed_to_connect(&self, _peer_id: &PeerId) -> Result<(), ConnectionDisallowed> {
        Ok(()) // TODO currently allows all
    }
}

/// A trait alias for the kinds of futures that can be used with Subduction.
///
/// This helps us switch between `Send` and `!Send` futures via type parameter
/// rather than creating `Send` and `!Send` versions of the entire Subduction implementation.
///
/// Similarly, the trait alias helps us avoid repeating the same complex trait bounds everywhere,
/// and needing to update them in many places if the constraints change.
pub trait SubductionFutureKind<
    'a,
    S: Storage<Self>,
    C: Connection<Self> + PartialEq,
    M: DepthMetric,
>: RecvOnce<'a, C> + StartConnectionActor<'a, C> + StartListener<'a, S, C, M>
{
}

impl<
        'a,
        S: Storage<Self>,
        C: Connection<Self> + PartialEq,
        M: DepthMetric,
        T: RecvOnce<'a, C> + StartConnectionActor<'a, C> + StartListener<'a, S, C, M>,
    > SubductionFutureKind<'a, S, C, M> for T
{
}

/// A trait for starting the listener task for Subduction.
///
/// This lets us abstract over `Send` and `!Send` futures
pub trait StartListener<'a, S: Storage<Self>, C: Connection<Self> + PartialEq, M: DepthMetric>:
    RecvOnce<'a, C> + StartConnectionActor<'a, C>
{
    /// Start the listener task for Subduction.
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, M>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>;
}

impl<
        'a,
        C: Connection<Self> + PartialEq + Send + Sync + 'a,
        S: Storage<Self> + Send + Sync + 'a,
        M: DepthMetric + Send + Sync + 'a,
    > StartListener<'a, S, C, M> for Sendable
where
    S::Error: Send + 'static,
    C::DisconnectionError: Send + 'static,
    C::CallError: Send + 'static,
    C::RecvError: Send + 'static,
    C::SendError: Send + 'static,
{
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, M>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            async move {
                if let Err(e) = subduction.listen().await {
                    tracing::error!("Subduction listen error: {}", e.to_string());
                }
            }
            .boxed(),
            abort_reg,
        )
    }
}

impl<'a, C: Connection<Self> + PartialEq + 'a, S: Storage<Self> + 'a, M: DepthMetric + 'a>
    StartListener<'a, S, C, M> for Local
{
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, M>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            async move {
                if let Err(e) = subduction.listen().await {
                    tracing::error!("Subduction listen error: {}", e.to_string());
                }
            }
            .boxed_local(),
            abort_reg,
        )
    }
}

/// A future representing the listener task for Subduction.
///
/// This lets the caller decide how they want to manage the listener's lifecycle,
/// including the ability to abort it when needed.
#[derive(Debug)]
pub struct ListenerFuture<
    'a,
    F: StartListener<'a, S, C, M>,
    S: Storage<F>,
    C: Connection<F> + PartialEq,
    M: DepthMetric,
> {
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<(S, C, M)>,
}

impl<
        'a,
        F: StartListener<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > ListenerFuture<'a, F, S, C, M>
{
    /// Create a new [`ListenerFuture`] wrapping the given abortable future.
    pub(crate) fn new(fut: Abortable<F::Future<'a, ()>>) -> Self {
        Self {
            fut: Box::pin(fut),
            _phantom: PhantomData,
        }
    }

    /// Check if the listener future has been aborted.
    #[must_use]
    pub fn is_aborted(&self) -> bool {
        self.fut.is_aborted()
    }
}

impl<
        'a,
        F: StartListener<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > Deref for ListenerFuture<'a, F, S, C, M>
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<
        'a,
        F: StartListener<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > Future for ListenerFuture<'a, F, S, C, M>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<
        'a,
        F: StartListener<'a, S, C, M>,
        S: Storage<F>,
        C: Connection<F> + PartialEq,
        M: DepthMetric,
    > Unpin for ListenerFuture<'a, F, S, C, M>
{
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::connection::test_utils::MockConnection;
    use sedimentree_core::{
        commit::CountLeadingZeroBytes,
        storage::MemoryStorage,
        Sedimentree, SedimentreeId,
    };
    use testresult::TestResult;

    mod initialization {
        use super::*;

        #[test]
        fn test_new_creates_empty_subduction() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            // Verify initial state via async runtime would be needed,
            // but we can at least verify construction doesn't panic
            assert!(!subduction.abort_actor_handle.is_aborted());
            assert!(!subduction.abort_listener_handle.is_aborted());
        }

        #[tokio::test]
        async fn test_new_has_empty_sedimentrees() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let ids = subduction.sedimentree_ids().await;
            assert!(ids.is_empty());
        }

        #[tokio::test]
        async fn test_new_has_no_connections() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let peer_ids = subduction.peer_ids().await;
            assert!(peer_ids.is_empty());
        }
    }

    mod sedimentree_operations {
        use super::*;

        #[tokio::test]
        async fn test_sedimentree_ids_returns_empty_initially() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let ids = subduction.sedimentree_ids().await;
            assert_eq!(ids.len(), 0);
        }

        #[tokio::test]
        async fn test_add_sedimentree_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let tree = Sedimentree::default();
            let blobs = Vec::new();

            subduction.add_sedimentree(id, tree, blobs).await?;

            let ids = subduction.sedimentree_ids().await;
            assert_eq!(ids.len(), 1);
            assert!(ids.contains(&id));

            Ok(())
        }

        #[tokio::test]
        async fn test_get_commits_returns_none_for_missing_tree() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let commits = subduction.get_commits(id).await;
            assert!(commits.is_none());
        }

        #[tokio::test]
        async fn test_get_commits_returns_empty_for_empty_tree() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let tree = Sedimentree::default();
            let blobs = Vec::new();

            subduction.add_sedimentree(id, tree, blobs).await?;

            let commits = subduction.get_commits(id).await;
            assert_eq!(commits, Some(Vec::new()));

            Ok(())
        }

        #[tokio::test]
        async fn test_get_fragments_returns_none_for_missing_tree() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let fragments = subduction.get_fragments(id).await;
            assert!(fragments.is_none());
        }

        #[tokio::test]
        async fn test_get_fragments_returns_empty_for_empty_tree() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let tree = Sedimentree::default();
            let blobs = Vec::new();

            subduction.add_sedimentree(id, tree, blobs).await?;

            let fragments = subduction.get_fragments(id).await;
            assert_eq!(fragments, Some(Vec::new()));

            Ok(())
        }

        #[tokio::test]
        async fn test_remove_sedimentree_removes_from_ids() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let tree = Sedimentree::default();
            let blobs = Vec::new();

            subduction.add_sedimentree(id, tree, blobs).await?;
            assert_eq!(subduction.sedimentree_ids().await.len(), 1);

            subduction.remove_sedimentree(id).await?;
            assert_eq!(subduction.sedimentree_ids().await.len(), 0);

            Ok(())
        }
    }

    mod connection_management {
        use super::*;
        use crate::peer::id::PeerId;

        #[tokio::test]
        async fn test_peer_ids_returns_empty_initially() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let peer_ids = subduction.peer_ids().await;
            assert_eq!(peer_ids.len(), 0);
        }

        #[tokio::test]
        async fn test_register_adds_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn = MockConnection::new();
            let (fresh, _conn_id) = subduction.register(conn).await?;

            assert!(fresh);
            assert_eq!(subduction.peer_ids().await.len(), 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_register_same_connection_twice_returns_false() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn = MockConnection::new();
            let (fresh1, conn_id1) = subduction.register(conn).await?;
            let (fresh2, conn_id2) = subduction.register(conn).await?;

            assert!(fresh1);
            assert!(!fresh2);

            assert_eq!(conn_id1, conn_id2);
            assert_eq!(subduction.peer_ids().await.len(), 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_unregister_removes_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn = MockConnection::new();
            let (_fresh, conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            let removed = subduction.unregister(&conn_id).await;
            assert!(removed);
            assert_eq!(subduction.peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_unregister_nonexistent_returns_false() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn_id = ConnectionId::new(999);
            let removed = subduction.unregister(&conn_id).await;
            assert!(!removed);
        }

        #[tokio::test]
        async fn test_register_different_peers_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
            let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;

            assert_eq!(subduction.peer_ids().await.len(), 2);
            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_removes_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn = MockConnection::new();
            let (_fresh, conn_id) = subduction.register(conn).await?;

            let removed = subduction.disconnect(&conn_id).await?;
            assert!(removed);
            assert_eq!(subduction.peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_nonexistent_returns_false() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn_id = ConnectionId::new(999);
            let removed = subduction.disconnect(&conn_id).await?;
            assert!(!removed);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_all_removes_all_connections() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
            let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.peer_ids().await.len(), 2);

            subduction.disconnect_all().await?;
            assert_eq!(subduction.peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_from_peer_removes_specific_peer() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let peer_id1 = PeerId::new([1u8; 32]);
            let peer_id2 = PeerId::new([2u8; 32]);
            let conn1 = MockConnection::with_peer_id(peer_id1);
            let conn2 = MockConnection::with_peer_id(peer_id2);

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.peer_ids().await.len(), 2);

            let removed = subduction.disconnect_from_peer(&peer_id1).await?;
            assert!(removed);
            assert_eq!(subduction.peer_ids().await.len(), 1);
            assert!(!subduction.peer_ids().await.contains(&peer_id1));
            assert!(subduction.peer_ids().await.contains(&peer_id2));

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_from_nonexistent_peer_returns_false() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let peer_id = PeerId::new([1u8; 32]);
            #[allow(clippy::unwrap_used)]
            let removed = subduction.disconnect_from_peer(&peer_id).await.unwrap();
            assert!(!removed);
        }
    }

    mod connection_policy {
        use super::*;
        use crate::peer::id::PeerId;

        #[tokio::test]
        async fn test_allowed_to_connect_allows_all_peers() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let peer_id = PeerId::new([1u8; 32]);
            let result = subduction.allowed_to_connect(&peer_id).await;
            assert!(result.is_ok());
        }

        // TODO also test when the policy says no
    }

    mod blob_operations {
        use super::*;
        use sedimentree_core::blob::Digest;

        #[tokio::test]
        async fn test_get_local_blob_returns_none_for_missing() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let digest = Digest::from([1u8; 32]);
            let blob = subduction.get_local_blob(digest).await.unwrap();
            assert!(blob.is_none());
        }

        #[tokio::test]
        async fn test_get_local_blobs_returns_none_for_missing_tree() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(storage, depth_metric);

            let id = SedimentreeId::new([1u8; 32]);
            let blobs = subduction.get_local_blobs(id).await.unwrap();
            assert!(blobs.is_none());
        }
    }
}
