//! The main synchronization logic and bookkeeping for [`Sedimentree`].

pub mod error;
pub mod request;

use crate::{
    connection::{
        manager::{RunManager, Command, ConnectionManager, Spawn},
        id::ConnectionId,
        message::{BatchSyncRequest, BatchSyncResponse, Message, RequestId, SyncDiff},
        Connection,
    },
    policy::{ConnectionPolicy, StoragePolicy},
    peer::id::PeerId,
    sharded_map::ShardedMap,
};
use async_channel::{bounded, Sender};
use async_lock::Mutex;
use error::{HydrationError, BlobRequestErr, IoError, ListenError, RegistrationError};
use futures::{
    FutureExt, StreamExt, future::try_join_all, stream::{AbortHandle, AbortRegistration, Abortable, Aborted, FuturesUnordered}
};
use futures_kind::{FutureKind, Local, Sendable};
use nonempty::NonEmpty;
use request::FragmentRequested;
use sedimentree_core::{
    blob::{Blob, Digest},
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::{RemoteDiff, Sedimentree, SedimentreeSummary},
    storage::Storage,
};
use alloc::{
    boxed::Box,
    string::ToString,
    sync::Arc,
    vec::Vec,
};
use sedimentree_core::collections::{Map, Set};
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
    F,
    S,
    C,
    M = CountLeadingZeroBytes,
    const N: usize = 256,
>
where
    F: SubductionFutureKind<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    M: DepthMetric, {
    depth_metric: M,
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    next_connection_id: Arc<AtomicUsize>,
    conns: Arc<Mutex<Map<ConnectionId, C>>>,
    storage: S,

    manager_channel: Sender<Command<C>>,
    msg_queue: async_channel::Receiver<(ConnectionId, Message)>,
    connection_closed: async_channel::Receiver<ConnectionId>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a F>,
}

impl<'a, F, S, C, M, const N: usize> Subduction<'a, F, S, C, M, N>
where
    F: SubductionFutureKind<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    /// Initialize a new `Subduction` with the given storage backend, depth metric, sharded `Sedimentree` map, and spawner.
    ///
    /// The spawner is used to spawn individual connection handler tasks.
    ///
    /// The caller is responsible for providing a [`ShardedMap`] with appropriate keys.
    /// For `DoS` resistance, use randomly generated keys via [`ShardedMap::new`] (requires `getrandom` feature)
    /// or provide secure random keys to [`ShardedMap::with_key`].
    #[allow(clippy::type_complexity)]
    pub fn new<Sp: Spawn<F> + Send + Sync + 'static>(
        storage: S,
        depth_metric: M,
        sedimentrees: ShardedMap<SedimentreeId, Sedimentree, N>,
        spawner: Sp,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, F, S, C, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    ) {
        tracing::info!("initializing Subduction instance");

        let (manager_sender, manager_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(256);
        let (closed_sender, closed_receiver) = async_channel::bounded(32);
        let manager = ConnectionManager::<F, C, Sp>::new(
            spawner,
            manager_receiver,
            queue_sender,
            closed_sender,
        );

        let (abort_manager_handle, abort_manager_reg) = AbortHandle::new_pair();
        let (abort_listener_handle, abort_listener_reg) = AbortHandle::new_pair();

        let sd = Arc::new(Self {
            depth_metric,
            sedimentrees: Arc::new(sedimentrees),
            next_connection_id: Arc::new(AtomicUsize::new(0)),
            conns: Arc::new(Mutex::new(Map::new())),
            storage,
            manager_channel: manager_sender,
            msg_queue: queue_receiver,
            connection_closed: closed_receiver,
            abort_manager_handle,
            abort_listener_handle,
            _phantom: PhantomData,
        });

        let manager_fut = manager.run();
        let abortable_manager = Abortable::new(manager_fut, abort_manager_reg);

        (
            sd.clone(),
            ListenerFuture::new(F::start_listener(sd, abort_listener_reg)),
            crate::connection::manager::ManagerFuture::new(abortable_manager),
        )
    }

    /// Hydrate a `Subduction` instance from existing sedimentrees in external storage.
    ///
    /// The caller is responsible for providing a [`ShardedMap`] with appropriate keys.
    /// For `DoS` resistance, use randomly generated keys via [`ShardedMap::new`] (requires `getrandom` feature)
    /// or provide secure random keys to [`ShardedMap::with_key`].
    ///
    /// # Errors
    ///
    /// * Returns [`HydrationError`] if loading from storage fails.
    pub async fn hydrate<Sp: Spawn<F> + Send + Sync + 'static>(
        storage: S,
        depth_metric: M,
        sedimentrees: ShardedMap<SedimentreeId, Sedimentree, N>,
        spawner: Sp,
    ) -> Result<(
        Arc<Self>,
        ListenerFuture<'a, F, S, C, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    ), HydrationError<F, S>> {
        let ids = storage.load_all_sedimentree_ids().await.map_err(HydrationError::LoadAllIdsError)?;
        let (subduction, fut_listener, manager_fut) = Self::new(storage, depth_metric, sedimentrees, spawner);
        for id in ids {
            let loose_commits = subduction.storage.load_loose_commits(id).await.map_err(HydrationError::LoadLooseCommitsError)?;
            let fragments = subduction.storage.load_fragments(id).await.map_err(HydrationError::LoadFragmentsError)?;
            let sedimentree = Sedimentree::new(fragments, loose_commits);

            subduction
                .sedimentrees
                .with_entry_or_default(id, |tree| tree.merge(sedimentree))
                .await;
        }
        Ok((subduction, fut_listener, manager_fut))
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
        use core::pin::pin;
        use futures::future::{select, Either};

        tracing::info!("starting Subduction listener");

        loop {
            let msg_fut = pin!(self.msg_queue.recv().fuse());
            let closed_fut = pin!(self.connection_closed.recv().fuse());

            match select(msg_fut, closed_fut).await {
                Either::Left((msg_result, _)) => {
                    if let Ok((conn_id, msg)) = msg_result {
                        tracing::debug!(
                            "Subduction listener received message from {:?}: {:?}",
                            conn_id,
                            msg
                        );

                        // Look up connection for sending responses
                        let conn = { self.conns.lock().await.get(&conn_id).cloned() };

                        if let Some(conn) = conn {
                            if let Err(e) = self.dispatch(conn_id, &conn, msg).await {
                                tracing::error!(
                                    "error dispatching message from connection {:?}: {}",
                                    conn_id,
                                    e
                                );
                                // Connection is broken - unregister from conns map.
                                // The stream in the actor will naturally end when recv fails.
                                let _ = self.unregister(&conn_id).await;
                                tracing::info!("unregistered failed connection {:?}", conn_id);
                            }
                            // No re-registration needed - the stream handles continuous recv
                        } else {
                            tracing::warn!("Message from unknown/unregistered connection {:?}", conn_id);
                        }
                    } else {
                        tracing::info!("Message queue closed");
                        break;
                    }
                }
                Either::Right((closed_result, _)) => {
                    if let Ok(conn_id) = closed_result {
                        tracing::info!("Connection {:?} closed, unregistering", conn_id);
                        self.unregister(&conn_id).await;
                    }
                }
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
        tracing::info!(
            from = %from,
            message_type = message.variant_name(),
            sedimentree_id = ?message.sedimentree_id(),
            request_id = ?message.request_id(),
            "dispatch"
        );

        #[cfg(feature = "metrics")]
        crate::metrics::message_dispatched(message.variant_name());

        #[cfg(feature = "metrics")]
        let _timer = crate::metrics::DispatchTimer::new();

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
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_request();

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
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_response();

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

        let ids = self.sedimentrees.into_keys().await;

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
        todo!("self.is_connect_allowed(&conn.peer_id()).await?;");

        let conns = { self.conns.lock().await.clone() };

        if let Some((hit, _)) = conns.iter().find(|(_, value)| **value == conn) {
            Ok((false, *hit))
        } else {
            let counter = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
            let conn_id: ConnectionId = ConnectionId::new(counter);

            self.conns.lock().await.insert(conn_id, conn.clone());
            self.manager_channel
                .send(Command::Add(conn_id, conn))
                .await
                .map_err(|_| RegistrationError::SendToClosedChannel)?;

            #[cfg(feature = "metrics")]
            crate::metrics::connection_opened();

            Ok((true, conn_id))
        }
    }

    /// Low-level unregistration of a connection.
    pub async fn unregister(&self, conn_id: &ConnectionId) -> bool {
        let removed = self.conns.lock().await.remove(conn_id).is_some();

        #[cfg(feature = "metrics")]
        if removed {
            crate::metrics::connection_closed();
        }

        removed
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
        let tree = self.sedimentrees.get_cloned(&id).await;
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
            let tree = self.sedimentrees.get_cloned(&id).await;
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

        conn.send(&Message::BlobsResponse(blobs))
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
        let maybe_sedimentree = self.sedimentrees.remove(&id).await;

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

        self.insert_commit_locally(id, commit.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

        let msg = Message::LooseCommit { id, commit: commit.clone(), blob };
        {
            let conns_with_ids: Vec<(ConnectionId, C)> = {
                self.conns.lock().await.iter().map(|(id, c)| (*id, c.clone())).collect()
            };
            for (conn_id, conn) in conns_with_ids {
                tracing::debug!(
                    "Propagating commit {:?} for sedimentree {:?} to peer {:?}",
                    msg.request_id(),
                    id,
                    conn.peer_id()
                );

                if let Err(e) = conn.send(&msg).await {
                    tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
                    self.unregister(&conn_id).await;
                    tracing::info!("unregistered failed connection {:?}", conn_id);
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

        self.sedimentrees
            .with_entry_or_default(id, |tree| tree.add_fragment(fragment.clone()))
            .await;

        self.storage
            .save_blob(blob.clone())
            .await
            .map_err(IoError::Storage)?;

        let msg = Message::Fragment {
            id,
            fragment: fragment.clone(),
            blob,
        };
        let conns_with_ids: Vec<(ConnectionId, C)> = {
            self.conns.lock().await.iter().map(|(id, c)| (*id, c.clone())).collect()
        };
        for (conn_id, conn) in conns_with_ids {
            tracing::debug!(
                "Propagating fragment {:?} for sedimentree {:?} to peer {:?}",
                fragment.digest(),
                id,
                conn.peer_id()
            );
            if let Err(e) = conn.send(&msg).await {
                tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
                self.unregister(&conn_id).await;
                tracing::info!("unregistered failed connection {:?}", conn_id);
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
            let msg = Message::LooseCommit {
                id,
                commit: commit.clone(),
                blob,
            };
            let conns_with_ids: Vec<(ConnectionId, C)> = {
                self.conns.lock().await.iter().map(|(id, c)| (*id, c.clone())).collect()
            };
            for (conn_id, conn) in conns_with_ids {
                if conn.peer_id() != *from
                    && let Err(e) = conn.send(&msg).await
                {
                    tracing::error!("{e}");
                    self.unregister(&conn_id).await;
                    tracing::info!("unregistered failed connection {:?}", conn_id);
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
            .insert_fragment_locally(id, fragment.clone(), blob.clone())
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let msg = Message::Fragment {
                id,
                fragment: fragment.clone(),
                blob,
            };
            let conns_with_ids: Vec<(ConnectionId, C)> = {
                self.conns.lock().await.iter().map(|(id, c)| (*id, c.clone())).collect()
            };
            for (conn_id, conn) in conns_with_ids {
                if conn.peer_id() != *from
                    && let Err(e) = conn.send(&msg).await
                {
                    tracing::error!("{e}");
                    self.unregister(&conn_id).await;
                    tracing::info!("unregistered failed connection {:?}", conn_id);
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
            let mut locked = self.sedimentrees.get_shard_containing(&id).lock().await;
            let sedimentree = locked.entry(id).or_default();
            tracing::debug!(
                "received batch sync request for sedimentree {id:?} for req_id {req_id:?} with {} commits and {} fragments",
                their_summary.loose_commits().len(),
                their_summary.fragment_summaries().len()
            );

            let (commits_to_add, local_commits, local_fragments) = {
                let diff: RemoteDiff<'_> = sedimentree.diff_remote(their_summary);
                (
                    diff.remote_commits.iter().map(|c| (*c).clone()).collect::<Vec<LooseCommit>>(),
                    diff.local_commits.iter().map(|c| (*c).clone()).collect::<Vec<LooseCommit>>(),
                    diff.local_fragments.iter().map(|f| (*f).clone()).collect::<Vec<Fragment>>(),
                )
            };

            for commit in commits_to_add {
                sedimentree.add_commit(commit);
            }

            for commit in local_commits {
                if let Some(blob) = self
                    .storage
                    .load_blob(commit.blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_commits.push((commit, blob));
                } else {
                    tracing::warn!("missing blob for commit {:?}", commit.digest(),);
                    our_missing_blobs.push(commit.blob_meta().digest());
                }
            }

            for fragment in local_fragments {
                if let Some(blob) = self
                    .storage
                    .load_blob(fragment.summary().blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?
                {
                    their_missing_fragments.push((fragment, blob));
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

        let msg: Message = BatchSyncResponse {
            id,
            req_id,
            diff: sync_diff
        }
        .into();
        if let Err(e) = conn.send(&msg).await {
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
        let msg = Message::BlobsRequest(digests);
        let conns_with_ids: Vec<(ConnectionId, C)> = {
            self.conns.lock().await.iter().map(|(id, c)| (*id, c.clone())).collect()
        };
        for (conn_id, conn) in conns_with_ids {
            if let Err(e) = conn.send(&msg).await {
                tracing::error!(
                    "Error requesting blobs {:?} from peer {:?}: {:?}",
                    msg,
                    conn.peer_id(),
                    e
                );
                self.unregister(&conn_id).await;
                tracing::info!("unregistered failed connection {:?}", conn_id);
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
            let summary = self
                .sedimentrees
                .get_cloned(&id)
                .await
                .map(|t| t.summarize())
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
        Map<PeerId, (bool, Vec<Blob>, Vec<(C, <C as Connection<F>>::CallError)>)>,
        IoError<F, S, C>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let mut peers: Map<PeerId, Vec<(ConnectionId, C)>> = Map::new();
        {
            for (conn_id, conn) in self.conns.lock().await.iter() {
                peers
                    .entry(conn.peer_id())
                    .or_default()
                    .push((*conn_id, conn.clone()));
            }
        }

        tracing::debug!("Found {} peer(s)", peers.len());
        let blobs = Arc::new(Mutex::new(Set::<Blob>::new()));
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
                        let summary = self
                            .sedimentrees
                            .get_cloned(&id)
                            .await
                            .map(|t| t.summarize())
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

        let mut out = Map::new();
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
        let tree_ids = self.sedimentrees.into_keys().await;

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
        self.sedimentrees.into_keys().await
    }

    /// Get all commits for a given sedimentree ID.
    pub async fn get_commits(&self, id: SedimentreeId) -> Option<Vec<LooseCommit>> {
        self.sedimentrees
            .get_cloned(&id)
            .await
            .map(|tree| tree.loose_commits().cloned().collect())
    }

    /// Get all fragments for a given sedimentree ID.
    pub async fn get_fragments(&self, id: SedimentreeId) -> Option<Vec<Fragment>> {
        self.sedimentrees
            .get_cloned(&id)
            .await
            .map(|tree| tree.fragments().cloned().collect())
    }

    /// Get the set of all connected peer IDs.
    pub async fn peer_ids(&self) -> Set<PeerId> {
        self.conns.lock().await.values().map(Connection::peer_id).collect()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_sedimentree_locally(&self, id: SedimentreeId, sedimentree: Sedimentree, blobs: Vec<Blob>) -> Result<(), S::Error> {
        tracing::debug!("adding sedimentree with id {:?}", id);

        // Save blobs first so they're available when commit/fragment callbacks fire
        for blob in blobs {
            self.storage.save_blob(blob).await?;
        }

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

        self.sedimentrees
            .with_entry_or_default(id, |tree| tree.merge(sedimentree))
            .await;

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
        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_commit(commit.clone()))
            .await;
        if !was_added {
            return Ok(false);
        }

        // Save blob first so callback wrappers can load it by digest
        self.storage.save_blob(blob).await?;
        self.storage.save_sedimentree_id(id).await?;
        self.storage.save_loose_commit(id, commit).await?;

        Ok(true)
    }

    // NOTE no integrity checking, we assume that they made a good fragment at the right depth
    async fn insert_fragment_locally(
        &self,
        id: SedimentreeId,
        fragment: Fragment,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_fragment(fragment.clone()))
            .await;
        if !was_added {
            return Ok(false);
        }

        // Save blob first so callback wrappers can load it by digest
        self.storage.save_blob(blob).await?;
        self.storage.save_sedimentree_id(id).await?;
        self.storage.save_fragment(id, fragment).await?;
        Ok(true)
    }
}

impl<'a, F, S, C, M, const N: usize> Drop for Subduction<'a, F, S, C, M, N>
where
    F: SubductionFutureKind<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    fn drop(&mut self) {
        self.abort_manager_handle.abort();
        self.abort_listener_handle.abort();
    }
}

impl<'a, F, S, C, M, const N: usize> ConnectionPolicy<F> for Subduction<'a, F, S, C, M, N>
where
    F: SubductionFutureKind<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    fn is_connect_allowed(&self, _peer_id: PeerId) -> F::Future<'_, bool> {
        todo!()
    }
}

impl<'a, F, S, C, M, const N: usize> StoragePolicy<F> for Subduction<'a, F, S, C, M, N>
where
    F: SubductionFutureKind<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    fn is_fetch_allowed(&self, _peer: PeerId, _sedimentree_id: SedimentreeId) -> F::Future<'_, bool> {
        todo!()
    }

    fn is_put_allowed(
        &self,
        _requestor: PeerId,
        _author: PeerId,
        _sedimentree_id: SedimentreeId,
    ) -> F::Future<'_, bool> {
        todo!()
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
    C: Connection<Self> + PartialEq + 'a,
    M: DepthMetric,
    const N: usize,
>: StartListener<'a, S, C, M, N>
{
}

impl<
        'a,
        S: Storage<Self>,
        C: Connection<Self> + PartialEq + 'a,
        M: DepthMetric,
        const N: usize,
        T: StartListener<'a, S, C, M, N>,
    > SubductionFutureKind<'a, S, C, M, N> for T
{
}

/// A trait for starting the listener task for Subduction.
///
/// This lets us abstract over `Send` and `!Send` futures
pub trait StartListener<'a, S: Storage<Self>, C: Connection<Self> + PartialEq + 'a, M: DepthMetric, const N: usize>:
    FutureKind + RunManager<C> + Sized
{
    /// Start the listener task for Subduction.
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        Self: Sized;
}

#[futures_kind::kinds(
    Sendable where
        C: Connection<Sendable> + PartialEq + Clone + Send + Sync + 'static,
        S: Storage<Sendable> + Send + Sync + 'a,
        M: DepthMetric + Send + Sync + 'a,
        S::Error: Send + 'static,
        C::DisconnectionError: Send + 'static,
        C::CallError: Send + 'static,
        C::RecvError: Send + 'static,
        C::SendError: Send + 'static,
    Local where
        C: Connection<Local> + PartialEq + Clone + 'static,
        S: Storage<Local> + 'a,
        M: DepthMetric + 'a
)]
impl<'a, K: FutureKind, C, S, M, const N: usize> StartListener<'a, S, C, M, N> for K {
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            K::into_kind(async move {
                if let Err(e) = subduction.listen().await {
                    tracing::error!("Subduction listen error: {}", e.to_string());
                }
            }),
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
    F,
    S,
    C,
    M,
    const N: usize = 256,
>
where
    F: StartListener<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<(S, C, M)>,
}

impl<'a, F, S, C, M, const N: usize> ListenerFuture<'a, F, S, C, M, N>
where
    F: StartListener<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
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

impl<'a, F, S, C, M, const N: usize> Deref for ListenerFuture<'a, F, S, C, M, N>
where
    F: StartListener<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<'a, F, S, C, M, const N: usize> Future for ListenerFuture<'a, F, S, C, M, N>
where
    F: StartListener<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<'a, F, S, C, M, const N: usize> Unpin for ListenerFuture<'a, F, S, C, M, N>
where
    F: StartListener<'a, S, C, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    M: DepthMetric,
{
}

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::connection::test_utils::MockConnection;
    use sedimentree_core::{
        commit::CountLeadingZeroBytes,
        id::SedimentreeId,
        sedimentree::Sedimentree,
        storage::MemoryStorage,
    };
    use testresult::TestResult;

    mod initialization {
        use super::*;

        #[test]
        fn test_new_creates_empty_subduction() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let ids = subduction.sedimentree_ids().await;
            assert!(ids.is_empty());
        }

        #[tokio::test]
        async fn test_new_has_no_connections() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let ids = subduction.sedimentree_ids().await;
            assert_eq!(ids.len(), 0);
        }

        #[tokio::test]
        async fn test_add_sedimentree_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let id = SedimentreeId::new([1u8; 32]);
            let commits = subduction.get_commits(id).await;
            assert!(commits.is_none());
        }

        #[tokio::test]
        async fn test_get_commits_returns_empty_for_empty_tree() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let id = SedimentreeId::new([1u8; 32]);
            let fragments = subduction.get_fragments(id).await;
            assert!(fragments.is_none());
        }

        #[tokio::test]
        async fn test_get_fragments_returns_empty_for_empty_tree() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let peer_ids = subduction.peer_ids().await;
            assert_eq!(peer_ids.len(), 0);
        }

        #[tokio::test]
        async fn test_register_adds_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let conn_id = ConnectionId::new(999);
            let removed = subduction.unregister(&conn_id).await;
            assert!(!removed);
        }

        #[tokio::test]
        async fn test_register_different_peers_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

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
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let digest = Digest::from([1u8; 32]);
            let blob = subduction.get_local_blob(digest).await.unwrap();
            assert!(blob.is_none());
        }

        #[tokio::test]
        async fn test_get_local_blobs_returns_none_for_missing_tree() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            let id = SedimentreeId::new([1u8; 32]);
            let blobs = subduction.get_local_blobs(id).await.unwrap();
            assert!(blobs.is_none());
        }
    }

    mod connection_cleanup_on_send_failure {
        use super::*;
        use crate::connection::test_utils::FailingSendMockConnection;
        use crate::peer::id::PeerId;
        use sedimentree_core::{
            blob::{Blob, BlobMeta, Digest},
            fragment::Fragment,
            loose_commit::LooseCommit,
        };

        fn make_test_commit() -> (LooseCommit, Blob) {
            let contents = vec![0u8; 32];
            let blob = Blob::new(contents.clone());
            let blob_meta = BlobMeta::new(&contents);
            let digest = Digest::from([0u8; 32]);
            let commit = LooseCommit::new(digest, vec![], blob_meta);
            (commit, blob)
        }

        fn make_test_fragment() -> (Fragment, Blob) {
            let contents = vec![0u8; 32];
            let blob = Blob::new(contents.clone());
            let blob_meta = BlobMeta::new(&contents);
            let head = Digest::from([1u8; 32]);
            let boundary = vec![Digest::from([2u8; 32])];
            let checkpoints = vec![Digest::from([3u8; 32])];
            let fragment = Fragment::new(head, boundary, checkpoints, blob_meta);
            (fragment, blob)
        }

        #[tokio::test]
        async fn test_add_commit_unregisters_connection_on_send_failure() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let (_fresh, _conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            // Add a commit - the send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction.add_commit(id, &commit, blob).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.peer_ids().await.len(),
                0,
                "Connection should be unregistered after send failure"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_add_fragment_unregisters_connection_on_send_failure() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let (_fresh, _conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            // Add a fragment - the send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (fragment, blob) = make_test_fragment();

            let _ = subduction.add_fragment(id, &fragment, blob).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.peer_ids().await.len(),
                0,
                "Connection should be unregistered after send failure"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recv_commit_unregisters_connection_on_send_failure() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register a failing connection with a different peer ID than the sender
            let sender_peer_id = PeerId::new([1u8; 32]);
            let other_peer_id = PeerId::new([2u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
            let (_fresh, _conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            // Receive a commit from a different peer - the propagation send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction.recv_commit(&sender_peer_id, id, &commit, blob).await;

            // Connection should be unregistered after send failure during propagation
            assert_eq!(
                subduction.peer_ids().await.len(),
                0,
                "Connection should be unregistered after send failure"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_recv_fragment_unregisters_connection_on_send_failure() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register a failing connection with a different peer ID than the sender
            let sender_peer_id = PeerId::new([1u8; 32]);
            let other_peer_id = PeerId::new([2u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
            let (_fresh, _conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            // Receive a fragment from a different peer - the propagation send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (fragment, blob) = make_test_fragment();

            let _ = subduction
                .recv_fragment(&sender_peer_id, id, &fragment, blob)
                .await;

            // Connection should be unregistered after send failure during propagation
            assert_eq!(
                subduction.peer_ids().await.len(),
                0,
                "Connection should be unregistered after send failure"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_request_blobs_unregisters_connection_on_send_failure() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let (_fresh, _conn_id) = subduction.register(conn).await?;
            assert_eq!(subduction.peer_ids().await.len(), 1);

            // Request blobs - the send will fail
            let digests = vec![Digest::from([1u8; 32])];
            subduction.request_blobs(digests).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.peer_ids().await.len(),
                0,
                "Connection should be unregistered after send failure"
            );

            Ok(())
        }

        #[tokio::test]
        async fn test_multiple_connections_only_failing_ones_removed() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _>::new(
                    storage,
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                );

            // Register two connections that will succeed
            let peer_id1 = PeerId::new([1u8; 32]);
            let peer_id2 = PeerId::new([2u8; 32]);
            let conn1 = MockConnection::with_peer_id(peer_id1);
            let conn2 = MockConnection::with_peer_id(peer_id2);

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.peer_ids().await.len(), 2);

            // Add a commit - sends will succeed
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction.add_commit(id, &commit, blob).await;

            // Both connections should still be registered (sends succeeded)
            assert_eq!(
                subduction.peer_ids().await.len(),
                2,
                "Both connections should remain registered when sends succeed"
            );

            Ok(())
        }
    }

    mod message_flow_diagnostics {
        //! Diagnostic tests to investigate the "one behind" issue.
        //!
        //! The "one behind" bug manifests as: new sedimentrees don't appear until
        //! the NEXT one is created. These tests inject messages through channels
        //! to observe timing and storage behavior.
        //!
        //! Tests run for both `Sendable` and `Local` future kinds to ensure
        //! behavior is consistent across native and WASM-like environments.

        use super::*;
        use crate::connection::test_utils::ChannelMockConnection;
        use crate::peer::id::PeerId;
        use core::time::Duration;
        use futures_kind::Local;
        use sedimentree_core::{
            blob::{Blob, BlobMeta, Digest},
            loose_commit::LooseCommit,
        };

        fn make_test_commit_with_data(data: &[u8]) -> (LooseCommit, Blob) {
            let blob = Blob::new(data.to_vec());
            let blob_meta = BlobMeta::new(data);
            let digest = Digest::hash(data);
            let commit = LooseCommit::new(digest, vec![], blob_meta);
            (commit, blob)
        }

        // ==========================================
        // SENDABLE TESTS (native threaded)
        // ==========================================

        #[tokio::test]
        async fn test_sendable_single_commit() -> TestResult {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Sendable, _, ChannelMockConnection, _>::new(
                    storage,
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([42u8; 32]);
            let (commit, blob) = make_test_commit_with_data(b"test commit");

            handle.inbound_tx.send(Message::LooseCommit {
                id: sedimentree_id, commit, blob,
            }).await?;

            tokio::time::sleep(Duration::from_millis(50)).await;

            let ids = subduction.sedimentree_ids().await;
            assert!(ids.contains(&sedimentree_id), "[SENDABLE] Sedimentree should be visible. Found: {ids:?}");
            assert_eq!(subduction.get_commits(sedimentree_id).await.map(|c| c.len()), Some(1));

            actor_task.abort();
            listener_task.abort();
            Ok(())
        }

        #[tokio::test]
        async fn test_sendable_multiple_sequential() -> TestResult {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Sendable, _, ChannelMockConnection, _>::new(
                    storage,
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            for i in 0..3u8 {
                let sedimentree_id = SedimentreeId::new([i; 32]);
                let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                handle.inbound_tx.send(Message::LooseCommit {
                    id: sedimentree_id, commit, blob,
                }).await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let ids = subduction.sedimentree_ids().await;
                assert_eq!(ids.len(), (i + 1) as usize,
                    "[SENDABLE] After commit {i}: expected {} sedimentrees, found {}. ONE BEHIND BUG!",
                    i + 1, ids.len());
                assert!(ids.contains(&sedimentree_id));
            }

            actor_task.abort();
            listener_task.abort();
            Ok(())
        }

        #[tokio::test]
        async fn test_sendable_same_sedimentree() -> TestResult {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Sendable, _, ChannelMockConnection, _>::new(
                    storage,
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([99u8; 32]);

            for i in 0..3usize {
                let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                handle.inbound_tx.send(Message::LooseCommit {
                    id: sedimentree_id, commit, blob,
                }).await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let count = subduction.get_commits(sedimentree_id).await.map_or(0, |c| c.len());
                assert_eq!(count, i + 1,
                    "[SENDABLE] After commit {i}, expected {} commits but found {}. DELAYED!",
                    i + 1, count);
            }

            actor_task.abort();
            listener_task.abort();
            Ok(())
        }

        // ==========================================
        // LOCAL TESTS (WASM-like single-threaded)
        // ==========================================

        #[tokio::test]
        async fn test_local_single_commit() -> TestResult {
            tokio::task::LocalSet::new().run_until(async {
                let storage = MemoryStorage::new();
                let (subduction, listener_fut, actor_fut) =
                    Subduction::<'_, Local, _, ChannelMockConnection, _>::new(
                        storage,
                        CountLeadingZeroBytes,
                        ShardedMap::with_key(0, 0),
                    );

                let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
                subduction.register(conn).await?;

                let actor_task = tokio::task::spawn_local(actor_fut);
                let listener_task = tokio::task::spawn_local(listener_fut);
                tokio::time::sleep(Duration::from_millis(10)).await;

                let sedimentree_id = SedimentreeId::new([42u8; 32]);
                let (commit, blob) = make_test_commit_with_data(b"test commit");

                handle.inbound_tx.send(Message::LooseCommit {
                    id: sedimentree_id, commit, blob,
                }).await?;

                tokio::time::sleep(Duration::from_millis(50)).await;

                let ids = subduction.sedimentree_ids().await;
                assert!(ids.contains(&sedimentree_id), "[LOCAL] Sedimentree should be visible. Found: {ids:?}");
                assert_eq!(subduction.get_commits(sedimentree_id).await.map(|c| c.len()), Some(1));

                actor_task.abort();
                listener_task.abort();
                Ok::<_, Box<dyn std::error::Error>>(())
            }).await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_local_multiple_sequential() -> TestResult {
            tokio::task::LocalSet::new().run_until(async {
                let storage = MemoryStorage::new();
                let (subduction, listener_fut, actor_fut) =
                    Subduction::<'_, Local, _, ChannelMockConnection, _>::new(
                        storage,
                        CountLeadingZeroBytes,
                        ShardedMap::with_key(0, 0),
                    );

                let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
                subduction.register(conn).await?;

                let actor_task = tokio::task::spawn_local(actor_fut);
                let listener_task = tokio::task::spawn_local(listener_fut);
                tokio::time::sleep(Duration::from_millis(10)).await;

                for i in 0..3u8 {
                    let sedimentree_id = SedimentreeId::new([i; 32]);
                    let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                    handle.inbound_tx.send(Message::LooseCommit {
                        id: sedimentree_id, commit, blob,
                    }).await?;

                    tokio::time::sleep(Duration::from_millis(20)).await;

                    let ids = subduction.sedimentree_ids().await;
                    assert_eq!(ids.len(), (i + 1) as usize,
                        "[LOCAL] After commit {i}: expected {} sedimentrees, found {}. ONE BEHIND BUG!",
                        i + 1, ids.len());
                    assert!(ids.contains(&sedimentree_id));
                }

                actor_task.abort();
                listener_task.abort();
                Ok::<_, Box<dyn std::error::Error>>(())
            }).await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_local_same_sedimentree() -> TestResult {
            tokio::task::LocalSet::new().run_until(async {
                let storage = MemoryStorage::new();
                let (subduction, listener_fut, actor_fut) =
                    Subduction::<'_, Local, _, ChannelMockConnection, _>::new(
                        storage,
                        CountLeadingZeroBytes,
                        ShardedMap::with_key(0, 0),
                    );

                let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
                subduction.register(conn).await?;

                let actor_task = tokio::task::spawn_local(actor_fut);
                let listener_task = tokio::task::spawn_local(listener_fut);
                tokio::time::sleep(Duration::from_millis(10)).await;

                let sedimentree_id = SedimentreeId::new([99u8; 32]);

                for i in 0..3usize {
                    let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                    handle.inbound_tx.send(Message::LooseCommit {
                        id: sedimentree_id, commit, blob,
                    }).await?;

                    tokio::time::sleep(Duration::from_millis(20)).await;

                    let count = subduction.get_commits(sedimentree_id).await.map_or(0, |c| c.len());
                    assert_eq!(count, i + 1,
                        "[LOCAL] After commit {i}, expected {} commits but found {}. DELAYED!",
                        i + 1, count);
                }

                actor_task.abort();
                listener_task.abort();
                Ok::<_, Box<dyn std::error::Error>>(())
            }).await?;
            Ok(())
        }
    }
}
