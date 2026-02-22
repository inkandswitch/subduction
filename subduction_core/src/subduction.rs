//! The main synchronization logic and bookkeeping for [`Sedimentree`].
//!
//! # API Guide
//!
//! ## API Levels
//!
//! Subduction provides two levels of API for connection management:
//!
//! | Level | Methods | Use Case |
//! |-------|---------|----------|
//! | **High-level** | [`attach`], [`disconnect`] | Most applications — handles sync automatically |
//! | **Low-level** | [`register`], [`unregister`] | Custom sync logic, testing, or fine-grained control |
//!
//! **Prefer the high-level API** unless you need explicit control over when sync occurs.
//!
//! ### High-Level: `attach` / `disconnect`
//!
//! - [`Subduction::attach`] — Register connection + perform initial batch sync
//! - [`Subduction::disconnect`] — Graceful connection shutdown
//! - [`Subduction::disconnect_all`] — Disconnect all connections
//! - [`Subduction::disconnect_from_peer`] — Disconnect all connections from a peer
//!
//! ### Low-Level: `register` / `unregister`
//!
//! - [`Subduction::register`] — Add connection to tracking (no automatic sync)
//! - [`Subduction::unregister`] — Remove connection from tracking
//!
//! ## Naming Conventions
//!
//! ### Getters: `get_*` vs `fetch_*`
//!
//! | Prefix | Behavior | Example |
//! |--------|----------|---------|
//! | `get_*` | Local only — returns data from storage/memory | [`get_blob`], [`get_blobs`], [`get_commits`] |
//! | `fetch_*` | Local first, network fallback if not found | [`fetch_blobs`] |
//!
//! ### Sync Methods
//!
//! | Method | Scope |
//! |--------|-------|
//! | [`sync_with_peer`] | Sync one sedimentree from one peer |
//! | [`sync_all`] | Sync one sedimentree from all connected peers |
//! | [`full_sync`] | Sync all sedimentrees from all connected peers |
//!
//! ### Data Operations
//!
//! | Method | Description |
//! |--------|-------------|
//! | [`add_sedimentree`] | Add a sedimentree locally and broadcast to subscribers |
//! | [`add_commit`] | Add a commit locally and broadcast to subscribers |
//! | [`add_fragment`] | Add a fragment locally and broadcast to subscribers |
//! | [`remove_sedimentree`] | Remove a sedimentree and associated data |
//!
//! [`attach`]: Subduction::attach
//! [`disconnect`]: Subduction::disconnect
//! [`disconnect_all`]: Subduction::disconnect_all
//! [`disconnect_from_peer`]: Subduction::disconnect_from_peer
//! [`register`]: Subduction::register
//! [`unregister`]: Subduction::unregister
//! [`get_blob`]: Subduction::get_blob
//! [`get_blobs`]: Subduction::get_blobs
//! [`get_commits`]: Subduction::get_commits
//! [`fetch_blobs`]: Subduction::fetch_blobs
//! [`sync_with_peer`]: Subduction::sync_with_peer
//! [`sync_all`]: Subduction::sync_all
//! [`full_sync`]: Subduction::full_sync
//! [`add_sedimentree`]: Subduction::add_sedimentree
//! [`add_commit`]: Subduction::add_commit
//! [`add_fragment`]: Subduction::add_fragment
//! [`remove_sedimentree`]: Subduction::remove_sedimentree

pub mod error;
pub mod pending_blob_requests;
pub mod request;

use crate::{
    connection::{
        Connection,
        authenticated::Authenticated,
        backoff::Backoff,
        handshake::DiscoveryId,
        id::ConnectionId,
        manager::{Command, ConnectionManager, RunManager, Spawn},
        message::{
            BatchSyncRequest, BatchSyncResponse, DataRequestRejected, Message, RemoveSubscriptions,
            RequestId, RequestedData, SyncDiff, SyncResult,
        },
        nonce_cache::NonceCache,
        stats::{SendCount, SyncStats},
    },
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
};
use alloc::{boxed::Box, collections::BTreeSet, string::ToString, sync::Arc, vec::Vec};
use async_channel::{Sender, bounded};
use async_lock::Mutex;
use core::{
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use error::{
    AttachError, BlobRequestErr, HydrationError, IoError, ListenError, RegistrationError,
    SendRequestedDataError, Unauthorized, WriteError,
};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{
    FutureExt, StreamExt,
    future::try_join_all,
    stream::{AbortHandle, AbortRegistration, Abortable, Aborted, FuturesUnordered},
};
use nonempty::NonEmpty;
use request::FragmentRequested;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    collections::{
        Entry, Map, Set,
        nonempty_ext::{NonEmptyExt, RemoveResult},
    },
    commit::CountLeadingZeroBytes,
    crypto::{
        digest::Digest,
        fingerprint::{Fingerprint, FingerprintSeed},
    },
    depth::{Depth, DepthMetric},
    fragment::{Fragment, id::FragmentId},
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_crypto::{signed::Signed, signer::Signer, verified_meta::VerifiedMeta};

use pending_blob_requests::PendingBlobRequests;

/// The main synchronization manager for sedimentrees.
#[derive(Debug, Clone)]
#[allow(clippy::type_complexity)]
pub struct Subduction<
    'a,
    F: SubductionFutureForm<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric = CountLeadingZeroBytes,
    const N: usize = 256,
> {
    signer: Sig,
    discovery_id: Option<DiscoveryId>,

    depth_metric: M,
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    storage: StoragePowerbox<S, P>,

    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    nonce_tracker: Arc<NonceCache>,

    /// Backoff state per connection, keyed by [`ConnectionId`].
    reconnect_backoff: Arc<Mutex<Map<ConnectionId, Backoff>>>,

    /// Outgoing subscriptions: sedimentrees we're subscribed to from each peer.
    ///
    /// Used to restore subscriptions after reconnection.
    outgoing_subscriptions: Arc<Mutex<Map<PeerId, Set<SedimentreeId>>>>,

    /// Blob digests we have requested and are expecting to receive.
    ///
    /// Used to reject unsolicited [`BlobsResponse`] messages — only blobs
    /// whose `(SedimentreeId, Digest)` pairs appear in this set are saved.
    /// Uses LRU eviction when capacity is exceeded (safety valve).
    /// Primary cleanup happens on sync completion.
    pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,

    manager_channel: Sender<Command<Authenticated<C, F>>>,
    msg_queue: async_channel::Receiver<(Authenticated<C, F>, Message)>,
    connection_closed: async_channel::Receiver<(ConnectionId, Authenticated<C, F>)>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a F>,
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, P, Sig, M, N> + 'static,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> Subduction<'a, F, S, C, P, Sig, M, N>
{
    /// Initialize a new `Subduction` with the given storage backend, policy, signer, depth metric, sharded `Sedimentree` map, and spawner.
    ///
    /// The spawner is used to spawn individual connection handler tasks.
    ///
    /// The caller is responsible for providing a [`ShardedMap`] with appropriate keys.
    /// For `DoS` resistance, use randomly generated keys via [`ShardedMap::new`] (requires `getrandom` feature)
    /// or provide secure random keys to [`ShardedMap::with_key`].
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn new<Sp: Spawn<F> + Send + Sync + 'static>(
        discovery_id: Option<DiscoveryId>,
        signer: Sig,
        storage: S,
        policy: P,
        nonce_cache: NonceCache,
        depth_metric: M,
        sedimentrees: ShardedMap<SedimentreeId, Sedimentree, N>,
        spawner: Sp,
        max_pending_blob_requests: usize,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, F, S, C, P, Sig, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    ) {
        tracing::info!("initializing Subduction instance");

        let (manager_sender, manager_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(256);
        let (closed_sender, closed_receiver) = async_channel::bounded(32);
        let manager = ConnectionManager::<F, Authenticated<C, F>, Sp>::new(
            spawner,
            manager_receiver,
            queue_sender,
            closed_sender,
        );

        let (abort_manager_handle, abort_manager_reg) = AbortHandle::new_pair();
        let (abort_listener_handle, abort_listener_reg) = AbortHandle::new_pair();

        let sd = Arc::new(Self {
            discovery_id,
            signer,
            depth_metric,
            sedimentrees: Arc::new(sedimentrees),
            connections: Arc::new(Mutex::new(Map::new())),
            subscriptions: Arc::new(Mutex::new(Map::new())),
            storage: StoragePowerbox::new(storage, Arc::new(policy)),
            nonce_tracker: Arc::new(nonce_cache),
            reconnect_backoff: Arc::new(Mutex::new(Map::new())),
            outgoing_subscriptions: Arc::new(Mutex::new(Map::new())),
            pending_blob_requests: Arc::new(Mutex::new(PendingBlobRequests::new(
                max_pending_blob_requests,
            ))),
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
    #[allow(clippy::too_many_arguments)]
    pub async fn hydrate<Sp: Spawn<F> + Send + Sync + 'static>(
        discovery_id: Option<DiscoveryId>,
        signer: Sig,
        storage: S,
        policy: P,
        nonce_cache: NonceCache,
        depth_metric: M,
        sedimentrees: ShardedMap<SedimentreeId, Sedimentree, N>,
        spawner: Sp,
        max_pending_blob_requests: usize,
    ) -> Result<
        (
            Arc<Self>,
            ListenerFuture<'a, F, S, C, P, Sig, M, N>,
            crate::connection::manager::ManagerFuture<F>,
        ),
        HydrationError<F, S>,
    > {
        let ids = storage
            .load_all_sedimentree_ids()
            .await
            .map_err(HydrationError::LoadAllIdsError)?;
        let (subduction, fut_listener, manager_fut) = Self::new(
            discovery_id,
            signer,
            storage,
            policy,
            nonce_cache,
            depth_metric,
            sedimentrees,
            spawner,
            max_pending_blob_requests,
        );
        for id in ids {
            let signed_loose_commits = subduction
                .storage
                .hydration_access()
                .load_loose_commits::<F>(id)
                .await
                .map_err(HydrationError::LoadLooseCommitsError)?;
            let signed_fragments = subduction
                .storage
                .hydration_access()
                .load_fragments::<F>(id)
                .await
                .map_err(HydrationError::LoadFragmentsError)?;

            // Extract payloads from trusted storage (already verified before storage)
            let loose_commits: Vec<_> = signed_loose_commits
                .into_iter()
                .map(|verified| verified.payload().clone())
                .collect();
            let fragments: Vec<_> = signed_fragments
                .into_iter()
                .map(|verified| verified.payload().clone())
                .collect();

            let sedimentree = Sedimentree::new(fragments, loose_commits);

            subduction
                .sedimentrees
                .with_entry_or_default(id, |tree| tree.merge(sedimentree))
                .await;
        }
        Ok((subduction, fut_listener, manager_fut))
    }

    /// Get the configured discovery ID for this instance.
    ///
    /// Returns the discovery ID this server advertises, or `None` if not set.
    #[must_use]
    pub const fn discovery_id(&self) -> Option<DiscoveryId> {
        self.discovery_id
    }

    /// Get a reference to the signer.
    ///
    /// Use this for signing handshake challenges/responses.
    #[must_use]
    pub const fn signer(&self) -> &Sig {
        &self.signer
    }

    /// Get this instance's peer ID (derived from the signer's verifying key).
    #[must_use]
    pub fn peer_id(&self) -> PeerId {
        PeerId::from(self.signer.verifying_key())
    }

    /// Returns a reference to the nonce cache for replay protection.
    #[must_use]
    pub fn nonce_cache(&self) -> &NonceCache {
        &self.nonce_tracker
    }

    /// Returns a reference to the sedimentrees map.
    ///
    /// This is only available with the `test_utils` feature or in tests.
    #[cfg(any(feature = "test_utils", test))]
    #[must_use]
    pub fn sedimentrees(&self) -> &Arc<ShardedMap<SedimentreeId, Sedimentree, N>> {
        &self.sedimentrees
    }

    /// Get a connection to a peer, if one exists.
    ///
    /// Returns the first available connection to the peer. Use this to get a
    /// connection once and reuse it for multiple operations, avoiding repeated
    /// lock acquisition on the connections map.
    pub async fn get_connection(&self, peer_id: &PeerId) -> Option<Authenticated<C, F>> {
        self.connections
            .lock()
            .await
            .get(peer_id)
            .map(|ne| ne.head.clone())
    }

    /***********************
     * RECONNECTION SUPPORT *
     ***********************/

    /// Get the backoff state for a connection, creating default state if needed.
    ///
    /// Returns the delay for the next reconnect attempt.
    pub async fn get_reconnect_delay(&self, conn_id: ConnectionId) -> Duration {
        let mut backoffs = self.reconnect_backoff.lock().await;
        let backoff = backoffs.entry(conn_id).or_default();
        backoff.next_delay()
    }

    /// Get the sedimentrees we're subscribed to from a peer.
    ///
    /// Used to restore subscriptions after successful reconnection.
    pub async fn get_peer_subscriptions(&self, peer_id: PeerId) -> Set<SedimentreeId> {
        self.outgoing_subscriptions
            .lock()
            .await
            .get(&peer_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Called after a successful reconnection to re-add the connection.
    ///
    /// This re-registers the connection with the manager using the same [`ConnectionId`].
    ///
    /// # Errors
    ///
    /// Returns an error if the manager channel is closed.
    pub async fn on_reconnect_success(
        &self,
        conn_id: ConnectionId,
        conn: Authenticated<C, F>,
    ) -> Result<(), ()> {
        tracing::info!(
            %conn_id,
            peer_id = %conn.peer_id(),
            "reconnection successful"
        );

        // Send ReAdd command to manager
        self.manager_channel
            .send(Command::ReAdd(conn_id, conn.clone()))
            .await
            .map_err(|_| ())?;

        // Re-add to connections map
        let peer_id = conn.peer_id();
        let mut connections = self.connections.lock().await;
        match connections.get_mut(&peer_id) {
            Some(peer_conns) => {
                peer_conns.push(conn);
            }
            None => {
                connections.insert(peer_id, NonEmpty::new(conn));
            }
        }

        Ok(())
    }

    /// Reset backoff state after a connection has been healthy for a period.
    ///
    /// Should be called after the connection has been stable (e.g., 10 seconds).
    pub async fn reset_backoff(&self, conn_id: ConnectionId) {
        if let Some(backoff) = self.reconnect_backoff.lock().await.get_mut(&conn_id) {
            backoff.reset();
            tracing::debug!(%conn_id, "backoff reset after healthy period");
        }
    }

    /// Called after a fatal reconnection failure to clean up state.
    ///
    /// Removes the backoff state for this connection.
    pub async fn on_reconnect_failed(&self, conn_id: ConnectionId) {
        tracing::warn!(%conn_id, "reconnection failed (fatal), cleaning up state");
        self.reconnect_backoff.lock().await.remove(&conn_id);
    }

    /// Track an outgoing subscription for reconnection restoration.
    ///
    /// Called internally when `sync_with_peer` is called with `subscribe: true`.
    async fn track_outgoing_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
        self.outgoing_subscriptions
            .lock()
            .await
            .entry(peer_id)
            .or_default()
            .insert(sedimentree_id);
    }

    /// Listen for incoming messages from all connections and handle them appropriately.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// Listen for incoming messages with concurrent dispatch.
    ///
    /// Dispatches messages concurrently using [`FuturesUnordered`], which
    /// significantly improves throughput when handling many independent
    /// requests (e.g., batch sync requests for different sedimentrees).
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a storage or network error occurs.
    pub async fn listen(self: Arc<Self>) -> Result<(), ListenError<F, S, C>> {
        tracing::info!("starting Subduction listener with concurrent dispatch");

        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

        loop {
            futures::select_biased! {
                // First priority: handle completed dispatch tasks
                result = in_flight.select_next_some() => {
                    #[allow(clippy::type_complexity)]
                    let (conn, dispatch_result): (Authenticated<C, F>, Result<(), ListenError<F, S, C>>) = result;
                    if let Err(e) = dispatch_result {
                        tracing::error!(
                            peer = %conn.peer_id(),
                            "error dispatching message: {e}"
                        );
                        // Connection is broken - unregister from conns map.
                        self.unregister(&conn).await;
                        tracing::info!("unregistered failed connection from peer {}", conn.peer_id());
                    }
                }
                // Second: receive new messages
                msg_result = self.msg_queue.recv().fuse() => {
                    if let Ok((conn, msg)) = msg_result {
                        let peer_id = conn.peer_id();
                        tracing::debug!(
                            "Subduction listener received message from peer {}: {:?}",
                            peer_id,
                            msg
                        );

                        // Spawn dispatch as concurrent task
                        let this = self.clone();
                        let conn_clone = conn.clone();

                        in_flight.push(async move {
                            let result = this.dispatch(&conn_clone, msg).await;
                            (conn_clone, result)
                        });
                    } else {
                        tracing::info!("Message queue closed");
                        // Drain remaining in-flight tasks before exiting
                        while let Some((conn, result)) = in_flight.next().await {
                            if let Err(e) = result {
                                tracing::error!(
                                    peer = %conn.peer_id(),
                                    "error dispatching message during shutdown: {e}"
                                );
                            }
                        }
                        break;
                    }
                }
                // Third: handle closed connections
                closed_result = self.connection_closed.recv().fuse() => {
                    if let Ok((conn_id, conn)) = closed_result {
                        let peer_id = conn.peer_id();
                        tracing::info!(
                            "Connection {conn_id} from peer {peer_id} closed, unregistering"
                        );
                        self.unregister(&conn).await;
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn dispatch(
        &self,
        conn: &Authenticated<C, F>,
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
                fingerprint_summary,
                req_id,
                subscribe,
            }) => {
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_request();

                // Add subscription if requested
                if subscribe {
                    self.add_subscription(from, id).await;
                    tracing::debug!("added subscription for peer {from} to sedimentree {id:?}");
                }

                // With compound storage, blobs are always stored with commits/fragments,
                // so recv_batch_sync_request will never return MissingBlobs
                self.recv_batch_sync_request(id, &fingerprint_summary, req_id, conn)
                    .await?;
            }
            Message::BatchSyncResponse(BatchSyncResponse { id, result, .. }) => {
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_response();

                match result {
                    SyncResult::Ok(diff) => {
                        self.recv_batch_sync_response(&from, id, diff).await?;
                    }
                    SyncResult::NotFound => {
                        tracing::info!("peer {from} reports sedimentree {id:?} not found");
                    }
                    SyncResult::Unauthorized => {
                        tracing::info!(
                            "peer {from} reports we are unauthorized to access sedimentree {id:?}"
                        );
                    }
                }
            }
            Message::BlobsRequest { id, digests } => {
                match self.recv_blob_request(conn, id, &digests).await {
                    Ok(()) => {
                        tracing::info!("successfully handled blob request from peer {:?}", from);
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
            }
            Message::BlobsResponse { id, blobs } => {
                // With compound storage, blobs are stored together with commits/fragments.
                // Standalone blob responses are no longer used for storage, but we still
                // need to handle the message type for protocol compatibility.
                // Clear any pending requests for these blobs.
                let accepted_count = {
                    let mut pending = self.pending_blob_requests.lock().await;
                    let mut count = 0usize;
                    for blob in &blobs {
                        let digest = Digest::hash(blob);
                        if pending.remove(id, digest) {
                            count += 1;
                        }
                    }
                    count
                };

                tracing::debug!(
                    "blob response from peer {from} for {id:?}: {accepted_count}/{} blobs acknowledged (compound storage - blobs stored with commits)",
                    blobs.len()
                );
            }
            Message::RemoveSubscriptions(RemoveSubscriptions { ids }) => {
                self.remove_subscriptions(from, &ids).await;
                tracing::debug!("removed subscriptions for peer {from}: {ids:?}");
            }
            Message::DataRequestRejected(DataRequestRejected { id }) => {
                tracing::info!("peer {from} rejected our data request for sedimentree {id:?}");
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
    /// * Returns `AttachError::Registration` if the connection is rejected by the policy.
    /// * Returns `AttachError::Io` if a storage or network error occurs.
    pub async fn attach(
        &self,
        conn: Authenticated<C, F>,
    ) -> Result<bool, AttachError<F, S, C, P::ConnectionDisallowed>> {
        let peer_id = conn.peer_id();
        tracing::info!("Attaching connection to peer {}", peer_id);

        let fresh = self.register(conn).await?;

        let ids = self.sedimentrees.into_keys().await;

        for tree_id in ids {
            self.sync_with_peer(&peer_id, tree_id, true, None).await?;
        }

        Ok(fresh)
    }

    /// Gracefully shut down a specific connection.
    ///
    /// # Errors
    ///
    /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect(
        &self,
        conn: &Authenticated<C, F>,
    ) -> Result<bool, C::DisconnectionError> {
        let peer_id = conn.peer_id();
        tracing::info!("Disconnecting connection from peer {}", peer_id);

        let mut connections = self.connections.lock().await;
        if let Some(peer_conns) = connections.remove(&peer_id) {
            match peer_conns.remove_item(conn) {
                RemoveResult::Removed(remaining) => {
                    // Put the remaining connections back
                    connections.insert(peer_id, remaining);
                    conn.disconnect().await.map(|()| true)
                }
                RemoveResult::WasLast(_) => {
                    // Don't put anything back, peer entry stays removed
                    conn.disconnect().await.map(|()| true)
                }
                RemoveResult::NotFound(original) => {
                    // Connection wasn't in the list, put original back
                    connections.insert(peer_id, original);
                    Ok(false)
                }
            }
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
        let all_conns: Vec<Authenticated<C, F>> = {
            let mut guard = self.connections.lock().await;
            core::mem::take(&mut *guard)
                .into_values()
                .flat_map(NonEmpty::into_iter)
                .collect()
        };

        try_join_all(
            all_conns
                .into_iter()
                .map(|conn| async move { conn.disconnect().await }),
        )
        .await?;

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
        let peer_conns = { self.connections.lock().await.remove(peer_id) };

        if let Some(conns) = peer_conns {
            for conn in conns {
                if let Err(e) = conn.disconnect().await {
                    tracing::error!("{e}");
                    return Err(e);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
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
    /// * `Ok(true)` if the connection is fresh (first for this peer or new connection).
    /// * `Ok(false)` if the exact connection was already registered.
    ///
    /// # Errors
    ///
    /// * Returns `ConnectionDisallowed` if the connection is not allowed by the policy.
    pub async fn register(
        &self,
        conn: Authenticated<C, F>,
    ) -> Result<bool, RegistrationError<P::ConnectionDisallowed>> {
        let peer_id = conn.peer_id();
        tracing::info!("registering connection from peer {}", peer_id);

        self.storage
            .policy()
            .authorize_connect(peer_id)
            .await
            .map_err(RegistrationError::ConnectionDisallowed)?;

        let mut connections = self.connections.lock().await;

        // Check if this exact connection is already registered
        if connections
            .get(&peer_id)
            .is_some_and(|peer_conns| peer_conns.iter().any(|c| c == &conn))
        {
            return Ok(false);
        }

        // Add connection to the peer's connection list
        match connections.get_mut(&peer_id) {
            Some(peer_conns) => {
                peer_conns.push(conn.clone());
            }
            None => {
                connections.insert(peer_id, NonEmpty::new(conn.clone()));
            }
        }

        // Release the lock before sending to avoid deadlock
        drop(connections);

        self.manager_channel
            .send(Command::Add(conn))
            .await
            .map_err(|_| RegistrationError::SendToClosedChannel)?;

        #[cfg(feature = "metrics")]
        crate::metrics::connection_opened();

        Ok(true)
    }

    /// Unregister a connection.
    ///
    /// Uses `NonEmptyExt::remove_item` to handle the three cases:
    /// - Connection not found
    /// - Connection removed, peer still has other connections
    /// - Connection removed, was the last connection for this peer
    ///
    /// Returns `Some(true)` if this was the last connection for the peer,
    /// `Some(false)` if the peer still has connections,
    /// `None` if the connection wasn't found.
    pub async fn unregister(&self, conn: &Authenticated<C, F>) -> Option<bool> {
        let peer_id = conn.peer_id();
        let mut connections = self.connections.lock().await;

        if let Some(peer_conns) = connections.remove(&peer_id) {
            match peer_conns.remove_item(conn) {
                RemoveResult::Removed(remaining) => {
                    connections.insert(peer_id, remaining);

                    #[cfg(feature = "metrics")]
                    crate::metrics::connection_closed();

                    Some(false) // Peer still has connections
                }
                RemoveResult::WasLast(_) => {
                    // Don't put anything back, peer entry stays removed
                    // Also remove peer from all subscriptions
                    drop(connections); // Release connection lock before acquiring subscription lock
                    self.remove_peer_from_subscriptions(peer_id).await;

                    #[cfg(feature = "metrics")]
                    crate::metrics::connection_closed();

                    Some(true) // Was the last connection for this peer
                }
                RemoveResult::NotFound(original) => {
                    // Put the original back
                    connections.insert(peer_id, original);
                    None // Connection wasn't found
                }
            }
        } else {
            None // Peer not found
        }
    }

    /// Get all connections as a flat list.
    ///
    /// This is useful for iterating over all connections to send messages.
    async fn all_connections(&self) -> Vec<Authenticated<C, F>> {
        self.connections
            .lock()
            .await
            .values()
            .flat_map(|ne| ne.iter().cloned())
            .collect()
    }

    /// Remove a peer from all subscription sets.
    ///
    /// Called when the last connection for a peer drops.
    async fn remove_peer_from_subscriptions(&self, peer_id: PeerId) {
        let mut subscriptions = self.subscriptions.lock().await;
        subscriptions.retain(|_id, peers| {
            peers.remove(&peer_id);
            !peers.is_empty()
        });
    }

    /// Remove a peer's subscriptions for specific sedimentree IDs.
    async fn remove_subscriptions(&self, peer_id: PeerId, ids: &[SedimentreeId]) {
        let mut subscriptions = self.subscriptions.lock().await;
        for id in ids {
            if let Some(peers) = subscriptions.get_mut(id) {
                peers.remove(&peer_id);
                if peers.is_empty() {
                    subscriptions.remove(id);
                }
            }
        }
    }

    /// Add a subscription for a peer to a sedimentree.
    pub(crate) async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
        let mut subscriptions = self.subscriptions.lock().await;
        subscriptions
            .entry(sedimentree_id)
            .or_default()
            .insert(peer_id);
    }

    /// Get connections for subscribers authorized to receive updates for a sedimentree.
    ///
    /// This is used when forwarding updates: we only send to subscribers who have Pull access.
    async fn get_authorized_subscriber_conns(
        &self,
        sedimentree_id: SedimentreeId,
        exclude_peer: &PeerId,
    ) -> Vec<Authenticated<C, F>> {
        // Get the subscribers for this sedimentree
        let subscriber_ids: Vec<PeerId> = {
            let subscriptions = self.subscriptions.lock().await;
            subscriptions
                .get(&sedimentree_id)
                .map(|peers| peers.iter().copied().collect())
                .unwrap_or_default()
        };

        if subscriber_ids.is_empty() {
            return Vec::new();
        }

        // For each subscriber, check if they're authorized to fetch this sedimentree
        let mut authorized_peers = Vec::new();
        for peer_id in subscriber_ids {
            if peer_id == *exclude_peer {
                continue;
            }
            // Check if this peer can fetch this sedimentree
            let can_fetch = self
                .storage
                .policy()
                .filter_authorized_fetch(peer_id, alloc::vec![sedimentree_id])
                .await;
            if !can_fetch.is_empty() {
                authorized_peers.push(peer_id);
            }
        }

        // Get connections for authorized peers
        let connections = self.connections.lock().await;
        authorized_peers
            .into_iter()
            .flat_map(|pid| {
                connections
                    .get(&pid)
                    .map(|conns| conns.iter().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect()
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
    pub async fn get_blob(
        &self,
        id: SedimentreeId,
        digest: Digest<Blob>,
    ) -> Result<Option<Blob>, S::Error> {
        tracing::debug!(?id, ?digest, "Looking for blob");

        // With compound storage, blobs are stored with their commits/fragments.
        // We need to search through commits and fragments to find the matching blob.
        let local_access = self.storage.hydration_access();

        // Search commits
        for verified in local_access.load_loose_commits::<F>(id).await? {
            if verified.payload().blob_meta().digest() == digest {
                return Ok(Some(verified.blob().clone()));
            }
        }

        // Search fragments
        for verified in local_access.load_fragments::<F>(id).await? {
            if verified.payload().summary().blob_meta().digest() == digest {
                return Ok(Some(verified.blob().clone()));
            }
        }

        Ok(None)
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
    pub async fn get_blobs(&self, id: SedimentreeId) -> Result<Option<NonEmpty<Blob>>, S::Error> {
        tracing::debug!("Getting local blobs for sedimentree with id {:?}", id);
        let tree = self.sedimentrees.get_cloned(&id).await;
        if tree.is_none() {
            return Ok(None);
        }

        tracing::debug!("Found sedimentree with id {:?}", id);
        let local_access = self.storage.hydration_access();
        let mut results = Vec::new();

        // With compound storage, blobs are stored with their commits/fragments
        for verified in local_access.load_loose_commits::<F>(id).await? {
            results.push(verified.blob().clone());
        }

        for verified in local_access.load_fragments::<F>(id).await? {
            results.push(verified.blob().clone());
        }

        Ok(NonEmpty::from_vec(results))
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
        if let Some(maybe_blobs) = self.get_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            let tree = self.sedimentrees.get_cloned(&id).await;
            if let Some(tree) = tree {
                let conns = self.all_connections().await;
                for conn in conns {
                    let seed = FingerprintSeed::random();
                    let summary = tree.fingerprint_summarize(&seed);
                    let req_id = conn.next_request_id().await;
                    let BatchSyncResponse {
                        id,
                        result,
                        req_id: resp_batch_id,
                    } = conn
                        .call(
                            BatchSyncRequest {
                                id,
                                req_id,
                                fingerprint_summary: summary,
                                subscribe: false,
                            },
                            timeout,
                        )
                        .await
                        .map_err(IoError::ConnCall)?;

                    debug_assert_eq!(req_id, resp_batch_id);

                    let diff = match result {
                        SyncResult::Ok(diff) => diff,
                        SyncResult::NotFound => {
                            tracing::debug!(
                                "peer {:?} reports sedimentree {id:?} not found",
                                conn.peer_id()
                            );
                            continue;
                        }
                        SyncResult::Unauthorized => {
                            tracing::debug!(
                                "peer {:?} reports we are unauthorized for sedimentree {id:?}",
                                conn.peer_id()
                            );
                            continue;
                        }
                    };

                    // Send back data the responder requested (bidirectional sync)
                    if !diff.requesting.is_empty()
                        && let Err(e) = self
                            .send_requested_data(&conn, id, &seed, &diff.requesting)
                            .await
                    {
                        if matches!(e, SendRequestedDataError::Unauthorized(_)) {
                            let msg: Message = DataRequestRejected { id }.into();
                            if let Err(send_err) = conn.send(&msg).await {
                                tracing::error!("failed to send DataRequestRejected: {send_err}");
                            }
                        }
                        tracing::warn!(
                            "failed to send requested data to peer {:?}: {e}",
                            conn.peer_id()
                        );
                    }

                    if let Err(e) = self
                        .recv_batch_sync_response(&conn.peer_id(), id, diff)
                        .await
                    {
                        tracing::error!(
                            "error handling batch sync response from peer {:?}: {}",
                            conn.peer_id(),
                            e
                        );
                    }
                }
            }

            let updated = self.get_blobs(id).await.map_err(IoError::Storage)?;

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
        conn: &Authenticated<C, F>,
        id: SedimentreeId,
        digests: &[Digest<Blob>],
    ) -> Result<(), BlobRequestErr<F, S, C>> {
        let mut blobs = Vec::new();
        let mut missing = Vec::new();
        for digest in digests {
            if let Some(blob) = self.get_blob(id, *digest).await.map_err(IoError::Storage)? {
                blobs.push(blob);
            } else {
                missing.push(*digest);
            }
        }

        conn.send(&Message::BlobsResponse { id, blobs })
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
    /// * [`WriteError::Io`] if a storage or network error occurs.
    /// * [`WriteError::PutDisallowed`] if the storage policy rejects the write.
    pub async fn add_sedimentree(
        &self,
        id: SedimentreeId,
        sedimentree: Sedimentree,
        blobs: Vec<Blob>,
    ) -> Result<(), WriteError<F, S, C, P::PutDisallowed>> {
        use sedimentree_core::collections::Map;

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        // Index blobs by digest for matching with commits/fragments
        let blobs_by_digest: Map<Digest<Blob>, Blob> = blobs
            .into_iter()
            .map(|b| (Digest::hash(&b), b))
            .collect();

        // Sign commits and pair with their blobs
        let mut verified_commits = Vec::with_capacity(sedimentree.loose_commits().count());
        for commit in sedimentree.loose_commits() {
            let blob_digest = commit.blob_meta().digest();
            let blob = blobs_by_digest
                .get(&blob_digest)
                .cloned()
                .ok_or_else(|| WriteError::MissingBlob(blob_digest))?;
            let verified_sig = Signed::seal::<F, _>(&self.signer, commit.clone()).await;
            let verified_meta = VerifiedMeta::new(verified_sig, blob)
                .map_err(|e| WriteError::Io(IoError::BlobMismatch(e)))?;
            verified_commits.push(verified_meta);
        }

        // Sign fragments and pair with their blobs
        let mut verified_fragments = Vec::with_capacity(sedimentree.fragments().count());
        for fragment in sedimentree.fragments() {
            let blob_digest = fragment.summary().blob_meta().digest();
            let blob = blobs_by_digest
                .get(&blob_digest)
                .cloned()
                .ok_or_else(|| WriteError::MissingBlob(blob_digest))?;
            let verified_sig = Signed::seal::<F, _>(&self.signer, fragment.clone()).await;
            let verified_meta = VerifiedMeta::new(verified_sig, blob)
                .map_err(|e| WriteError::Io(IoError::BlobMismatch(e)))?;
            verified_fragments.push(verified_meta);
        }

        self.insert_sedimentree_locally(&putter, verified_commits, verified_fragments)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.sync_all(id, true, None).await?;
        Ok(())
    }

    /// Remove a sedimentree locally and delete all associated data from storage.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn remove_sedimentree(&self, id: SedimentreeId) -> Result<(), IoError<F, S, C>> {
        let maybe_sedimentree = self.sedimentrees.remove(&id).await;

        if maybe_sedimentree.is_some() {
            let destroyer = self.storage.local_destroyer(id);

            // With compound storage, deleting commits/fragments also deletes their blobs
            destroyer
                .delete_loose_commits()
                .await
                .map_err(IoError::Storage)?;

            destroyer
                .delete_fragments()
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    /***********************
     * INCREMENTAL CHANGES *
     ***********************/

    /// Add a new (incremental) commit locally and propagate it to all connected peers.
    ///
    /// The commit is constructed internally from the provided parts, ensuring
    /// that the blob metadata is computed correctly from the blob.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` if the commit is not on a fragment boundary.
    /// * `Ok(Some(FragmentRequested))` if the commit is on a [`Fragment`] boundary.
    ///   In this case, please call `add_fragment` after creating the requested fragment.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] if a storage or network error occurs.
    /// * [`WriteError::PutDisallowed`] if the storage policy rejects the write.
    pub async fn add_commit(
        &self,
        id: SedimentreeId,
        parents: BTreeSet<Digest<LooseCommit>>,
        blob: Blob,
    ) -> Result<Option<FragmentRequested>, WriteError<F, S, C, P::PutDisallowed>> {
        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        let verified_blob = VerifiedBlobMeta::new(blob);
        let verified_meta: VerifiedMeta<LooseCommit> =
            VerifiedMeta::seal::<F, _>(&self.signer, (id, parents), verified_blob).await;

        let commit_digest = Digest::hash(verified_meta.payload());
        tracing::debug!("adding commit {:?} to sedimentree {:?}", commit_digest, id);

        let signed_for_wire = verified_meta.signed().clone();
        let blob = verified_meta.blob().clone();

        self.insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        let msg = Message::LooseCommit {
            id,
            commit: signed_for_wire,
            blob,
        };
        {
            let conns = {
                let subscriber_conns = self.get_authorized_subscriber_conns(id, &self_id).await;
                if subscriber_conns.is_empty() {
                    tracing::debug!(
                        "No subscribers for sedimentree {:?}, broadcasting to all connections",
                        id
                    );
                    self.all_connections().await
                } else {
                    subscriber_conns
                }
            };

            for conn in conns {
                let peer_id = conn.peer_id();
                tracing::debug!(
                    "Propagating commit {:?} for sedimentree {:?} to {}",
                    msg.request_id(),
                    id,
                    peer_id
                );

                if let Err(e) = conn.send(&msg).await {
                    tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
                    self.unregister(&conn).await;
                    tracing::info!("unregistered failed connection from peer {}", peer_id);
                }
            }
        }

        let mut maybe_requested_fragment = None;
        let depth = self.depth_metric.to_depth(commit_digest);
        if depth != Depth(0) {
            maybe_requested_fragment = Some(FragmentRequested::new(commit_digest, depth));
        }

        Ok(maybe_requested_fragment)
    }

    /// Add a new (incremental) fragment locally and propagate it to all connected peers.
    ///
    /// The fragment is constructed internally from the provided parts, ensuring
    /// that the blob metadata is computed correctly from the blob.
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
        head: Digest<LooseCommit>,
        boundary: BTreeSet<Digest<LooseCommit>>,
        checkpoints: &[Digest<LooseCommit>],
        blob: Blob,
    ) -> Result<(), WriteError<F, S, C, P::PutDisallowed>> {
        let verified_blob = VerifiedBlobMeta::new(blob);

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        let verified_meta: VerifiedMeta<Fragment> = VerifiedMeta::seal::<F, _>(
            &self.signer,
            (id, head, boundary, checkpoints.to_vec()),
            verified_blob,
        )
        .await;
        let fragment_digest = verified_meta.payload().digest();

        tracing::debug!(
            "Adding fragment {:?} to sedimentree {:?}",
            fragment_digest,
            id
        );
        let signed_for_wire = verified_meta.signed().clone();
        let blob = verified_meta.blob().clone();

        self.insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        let msg = Message::Fragment {
            id,
            fragment: signed_for_wire,
            blob,
        };

        let conns = {
            let subscriber_conns = self.get_authorized_subscriber_conns(id, &self_id).await;
            if subscriber_conns.is_empty() {
                tracing::debug!(
                    "No subscribers for sedimentree {:?}, broadcasting fragment to all connections",
                    id
                );
                self.all_connections().await
            } else {
                subscriber_conns
            }
        };

        for conn in conns {
            let peer_id = conn.peer_id();
            tracing::debug!(
                "Propagating fragment {:?} for sedimentree {:?} to {}",
                fragment_digest,
                id,
                peer_id
            );
            if let Err(e) = conn.send(&msg).await {
                tracing::error!("{}", IoError::<F, S, C>::ConnSend(e));
                self.unregister(&conn).await;
                tracing::info!("unregistered failed connection from peer {}", peer_id);
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
        signed_commit: &Signed<LooseCommit>,
        blob: Blob,
    ) -> Result<bool, IoError<F, S, C>> {
        let verified = match signed_commit.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "commit signature verification failed from peer {:?}: {e}",
                    from
                );
                return Ok(false);
            }
        };

        let author = PeerId::from(verified.issuer());
        tracing::debug!(
            "receiving commit {:?} for sedimentree {:?} from peer {:?} (author {:?})",
            Digest::hash(verified.payload()),
            id,
            from,
            author
        );

        let putter = match self.storage.get_putter::<F>(*from, author, id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "policy rejected commit from peer {:?} (author {:?}) for sedimentree {:?}: {e}",
                    from,
                    author,
                    id
                );
                return Ok(false);
            }
        };

        let signed_for_wire = verified.signed().clone();

        // Verify blob matches claimed metadata
        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("blob mismatch from peer {:?}: {e}", from);
                return Err(IoError::BlobMismatch(e));
            }
        };

        let was_new = self
            .insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let msg = Message::LooseCommit {
                id,
                commit: signed_for_wire,
                blob,
            };

            let conns = self.get_authorized_subscriber_conns(id, from).await;
            for conn in conns {
                let peer_id = conn.peer_id();
                if let Err(e) = conn.send(&msg).await {
                    tracing::error!("{e}");
                    self.unregister(&conn).await;
                    tracing::info!("unregistered failed connection from peer {}", peer_id);
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
        signed_fragment: &Signed<Fragment>,
        blob: Blob,
    ) -> Result<bool, IoError<F, S, C>> {
        // Verify the signature
        let verified = match signed_fragment.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "fragment signature verification failed from peer {:?}: {e}",
                    from
                );
                return Ok(false);
            }
        };

        let author = PeerId::from(verified.issuer());
        tracing::debug!(
            "receiving fragment {:?} for sedimentree {:?} from peer {:?} (author {:?})",
            verified.payload().digest(),
            id,
            from,
            author
        );

        // Authorize using verified author, not sender
        let putter = match self.storage.get_putter::<F>(*from, author, id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "policy rejected fragment from peer {:?} (author {:?}) for sedimentree {:?}: {e}",
                    from,
                    author,
                    id
                );
                return Ok(false);
            }
        };

        // Clone signed for forwarding before consuming verified
        let signed_for_wire = verified.signed().clone();

        // Verify blob matches claimed metadata
        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("blob mismatch from peer {:?}: {e}", from);
                return Err(IoError::BlobMismatch(e));
            }
        };

        let was_new = self
            .insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let msg = Message::Fragment {
                id,
                fragment: signed_for_wire,
                blob,
            };
            // Forward to authorized subscribers only
            let conns = self.get_authorized_subscriber_conns(id, from).await;
            for conn in conns {
                let peer_id = conn.peer_id();
                if let Err(e) = conn.send(&msg).await {
                    tracing::error!("{e}");
                    self.unregister(&conn).await;
                    tracing::info!("unregistered failed connection from peer {}", peer_id);
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
    /// * [`ListenError::IoError`] if a storage or network error occurs.
    #[allow(clippy::too_many_lines)]
    pub async fn recv_batch_sync_request(
        &self,
        id: SedimentreeId,
        their_fingerprints: &FingerprintSummary,
        req_id: RequestId,
        conn: &Authenticated<C, F>,
    ) -> Result<(), ListenError<F, S, C>> {
        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);

        let peer_id = conn.peer_id();
        let fetcher = match self.storage.get_fetcher::<F>(peer_id, id).await {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!(
                    %peer_id,
                    ?id,
                    error = %e,
                    "policy rejected fetch request"
                );
                // Send unauthorized response and return Ok - this is not a connection error
                let msg: Message = BatchSyncResponse {
                    id,
                    req_id,
                    result: SyncResult::Unauthorized,
                }
                .into();
                if let Err(e) = conn.send(&msg).await {
                    tracing::error!("failed to send unauthorized response: {e}");
                }
                return Ok(());
            }
        };

        let mut their_missing_commits = Vec::new();
        let mut their_missing_fragments = Vec::new();

        let verified_commits = fetcher
            .load_loose_commits()
            .await
            .map_err(IoError::Storage)?;
        let verified_fragments = fetcher.load_fragments().await.map_err(IoError::Storage)?;

        // Build lookup maps keyed by digest
        let commit_by_digest: Map<Digest<LooseCommit>, VerifiedMeta<LooseCommit>> =
            verified_commits
                .into_iter()
                .map(|vm| (Digest::hash(vm.payload()), vm))
                .collect();
        let fragment_by_digest: Map<Digest<Fragment>, VerifiedMeta<Fragment>> = verified_fragments
            .into_iter()
            .map(|vm| (vm.payload().digest(), vm))
            .collect();

        // Compute the diff under the shard lock, then release it before blob I/O.
        // This prevents slow blob loads from blocking all operations on
        // other sedimentrees that share the same shard.
        let (
            local_commit_digests,
            local_fragment_digests,
            our_missing_commit_fingerprints,
            our_missing_fragment_fingerprints,
        ) = {
            let mut locked = self.sedimentrees.get_shard_containing(&id).lock().await;

            // If sedimentree not in memory, hydrate it from storage
            if let Entry::Vacant(entry) = locked.entry(id) {
                let loose_commits: Vec<_> = commit_by_digest
                    .values()
                    .map(|vm| vm.payload().clone())
                    .collect();
                let fragments: Vec<_> = fragment_by_digest
                    .values()
                    .map(|vm| vm.payload().clone())
                    .collect();

                if !loose_commits.is_empty() || !fragments.is_empty() {
                    let sedimentree = Sedimentree::new(fragments, loose_commits);
                    entry.insert(sedimentree);
                    tracing::debug!("hydrated sedimentree {id:?} from storage for batch sync");
                }
            }

            let sedimentree = locked.entry(id).or_default();
            tracing::debug!(
                "received batch sync request for sedimentree {id:?} for req_id {req_id:?} with {} commit fps and {} fragment fps",
                their_fingerprints.commit_fingerprints().len(),
                their_fingerprints.fragment_fingerprints().len()
            );

            let diff = sedimentree.diff_remote_fingerprints(their_fingerprints);
            (
                diff.local_only_commits
                    .iter()
                    .map(|c| Digest::hash(*c))
                    .collect::<Vec<_>>(),
                diff.local_only_fragments
                    .iter()
                    .map(|f| f.digest())
                    .collect::<Vec<_>>(),
                diff.remote_only_commit_fingerprints,
                diff.remote_only_fragment_fingerprints,
            )
            // NOTE: We intentionally do NOT add remote commits to the in-memory tree here.
            // The commits_to_add are just metadata from the summary — we don't have the actual
            // signed commits or blobs yet. We'll add them when they arrive via LooseCommit
            // messages (in response to our "requesting" field).
        };
        // Shard lock released. Blob I/O below does not block other sedimentrees.

        // With compound storage, blobs are always stored with their commits/fragments
        for digest in local_commit_digests {
            if let Some(verified) = commit_by_digest.get(&digest) {
                their_missing_commits.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        for digest in local_fragment_digests {
            if let Some(verified) = fragment_by_digest.get(&digest) {
                their_missing_fragments.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        tracing::info!(
            "sending batch sync response for sedimentree {id:?} on req_id {req_id:?}, with {} missing commits and {} missing fragments, requesting {} commits and {} fragments",
            their_missing_commits.len(),
            their_missing_fragments.len(),
            our_missing_commit_fingerprints.len(),
            our_missing_fragment_fingerprints.len(),
        );

        let sync_diff = SyncDiff {
            missing_commits: their_missing_commits,
            missing_fragments: their_missing_fragments,
            requesting: RequestedData {
                commit_fingerprints: our_missing_commit_fingerprints,
                fragment_fingerprints: our_missing_fragment_fingerprints,
            },
        };

        let msg: Message = BatchSyncResponse {
            id,
            req_id,
            result: SyncResult::Ok(sync_diff),
        }
        .into();
        if let Err(e) = conn.send(&msg).await {
            tracing::error!("{e}");
        }

        // With compound storage, blobs are always available with commits/fragments
        Ok(())
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
        diff: SyncDiff,
    ) -> Result<(), IoError<F, S, C>> {
        tracing::info!(
            "received batch sync response for sedimentree {:?} from peer {:?} with {} missing commits and {} missing fragments",
            id,
            from,
            diff.missing_commits.len(),
            diff.missing_fragments.len()
        );

        let putter = match self.storage.get_putter::<F>(*from, *from, id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    "policy rejected batch sync from peer {:?} for sedimentree {:?}: {e}",
                    from,
                    id
                );
                return Ok(());
            }
        };

        for (signed_commit, blob) in diff.missing_commits {
            let verified = match signed_commit.try_verify() {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("batch sync commit signature verification failed: {e}");
                    continue;
                }
            };
            let verified_meta = match VerifiedMeta::new(verified, blob) {
                Ok(vm) => vm,
                Err(e) => {
                    tracing::warn!("batch sync commit blob mismatch: {e}");
                    continue;
                }
            };
            self.insert_commit_locally(&putter, verified_meta)
                .await
                .map_err(IoError::Storage)?;
        }

        for (signed_fragment, blob) in diff.missing_fragments {
            let verified = match signed_fragment.try_verify() {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("batch sync fragment signature verification failed: {e}");
                    continue;
                }
            };
            let verified_meta = match VerifiedMeta::new(verified, blob) {
                Ok(vm) => vm,
                Err(e) => {
                    tracing::warn!("batch sync fragment blob mismatch: {e}");
                    continue;
                }
            };
            self.insert_fragment_locally(&putter, verified_meta)
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    /// Find blobs from connected peers for a specific sedimentree.
    pub async fn request_blobs(&self, id: SedimentreeId, digests: Vec<Digest<Blob>>) {
        {
            let mut pending = self.pending_blob_requests.lock().await;
            for digest in &digests {
                pending.insert(id, *digest);
            }
        }

        let msg = Message::BlobsRequest { id, digests };
        let conns = self.all_connections().await;
        for conn in conns {
            let peer_id = conn.peer_id();
            if let Err(e) = conn.send(&msg).await {
                tracing::error!(
                    "Error requesting blobs {:?} from peer {}: {:?}",
                    msg,
                    peer_id,
                    e
                );
                self.unregister(&conn).await;
                tracing::info!("unregistered failed connection from peer {}", peer_id);
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
    #[allow(clippy::too_many_lines)]
    pub async fn sync_with_peer(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Result<(bool, SyncStats, Vec<(Authenticated<C, F>, C::CallError)>), IoError<F, S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let mut stats = SyncStats::new();
        let mut had_success = false;

        let peer_conns: Vec<Authenticated<C, F>> = {
            self.connections
                .lock()
                .await
                .get(to_ask)
                .map(|ne| ne.iter().cloned().collect())
                .unwrap_or_default()
        };

        let mut conn_errs = Vec::new();

        for conn in peer_conns {
            tracing::info!("Using connection to peer {}", to_ask);
            let seed = FingerprintSeed::random();
            let fp_summary = self.sedimentrees.get_cloned(&id).await.map_or_else(
                || FingerprintSummary::new(seed, BTreeSet::new(), BTreeSet::new()),
                |t| t.fingerprint_summarize(&seed),
            );

            tracing::debug!(
                "Sending fingerprint summary for {:?}: {} commit fps, {} fragment fps",
                id,
                fp_summary.commit_fingerprints().len(),
                fp_summary.fragment_fingerprints().len()
            );

            let req_id = conn.next_request_id().await;

            let result = conn
                .call(
                    BatchSyncRequest {
                        id,
                        req_id,
                        fingerprint_summary: fp_summary,
                        subscribe,
                    },
                    timeout,
                )
                .await;

            match result {
                Err(e) => conn_errs.push((conn, e)),
                Ok(BatchSyncResponse { result, .. }) => {
                    let SyncDiff {
                        missing_commits,
                        missing_fragments,
                        requesting,
                    } = match result {
                        SyncResult::Ok(diff) => diff,
                        SyncResult::NotFound => {
                            tracing::debug!("peer {to_ask:?} reports sedimentree {id:?} not found");
                            continue;
                        }
                        SyncResult::Unauthorized => {
                            tracing::debug!(
                                "peer {to_ask:?} reports we are unauthorized for sedimentree {id:?}"
                            );
                            continue;
                        }
                    };

                    let putter = match self.storage.get_putter::<F>(*to_ask, *to_ask, id).await {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::warn!(
                                "policy rejected sync from peer {:?} for sedimentree {:?}: {e}",
                                to_ask,
                                id
                            );
                            continue;
                        }
                    };

                    // Track counts for stats
                    let commits_to_receive = missing_commits.len();
                    let fragments_to_receive = missing_fragments.len();

                    for (signed_commit, blob) in missing_commits {
                        let verified = match signed_commit.try_verify() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("sync commit signature verification failed: {e}");
                                continue;
                            }
                        };
                        let verified_meta = match VerifiedMeta::new(verified, blob) {
                            Ok(vm) => vm,
                            Err(e) => {
                                tracing::warn!("sync commit blob mismatch: {e}");
                                continue;
                            }
                        };
                        self.insert_commit_locally(&putter, verified_meta)
                            .await
                            .map_err(IoError::Storage)?;
                    }

                    for (signed_fragment, blob) in missing_fragments {
                        let verified = match signed_fragment.try_verify() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("sync fragment signature verification failed: {e}");
                                continue;
                            }
                        };
                        let verified_meta = match VerifiedMeta::new(verified, blob) {
                            Ok(vm) => vm,
                            Err(e) => {
                                tracing::warn!("sync fragment blob mismatch: {e}");
                                continue;
                            }
                        };
                        self.insert_fragment_locally(&putter, verified_meta)
                            .await
                            .map_err(IoError::Storage)?;
                    }

                    // Update received stats (count what was offered, not verified)
                    stats.commits_received += commits_to_receive;
                    stats.fragments_received += fragments_to_receive;

                    tracing::debug!(
                        "Received response for {:?}: {} commits received, peer requesting {} commits and {} fragments",
                        id,
                        commits_to_receive,
                        requesting.commit_fingerprints.len(),
                        requesting.fragment_fingerprints.len()
                    );

                    // Send back data the responder requested (bidirectional sync)
                    if !requesting.is_empty() {
                        tracing::debug!("Calling send_requested_data for {:?}", id);
                        match self
                            .send_requested_data(&conn, id, &seed, &requesting)
                            .await
                        {
                            Ok(sent) => {
                                tracing::debug!(
                                    "send_requested_data returned: {} commits, {} fragments",
                                    sent.commits,
                                    sent.fragments
                                );
                                stats.commits_sent += sent.commits;
                                stats.fragments_sent += sent.fragments;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "failed to send requested data to peer {:?}: {e}",
                                    to_ask
                                );
                            }
                        }
                    }

                    // Mutual subscription: we subscribed to them, so also add them
                    // to our subscriptions so our commits get pushed to them
                    if subscribe {
                        self.track_outgoing_subscription(*to_ask, id).await;
                        self.add_subscription(*to_ask, id).await;
                        tracing::debug!(
                            "mutual subscription: added peer {to_ask} to our subscriptions for {id:?}"
                        );
                    }

                    had_success = true;
                    break;
                }
            }
        }

        Ok((had_success, stats, conn_errs))
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
    pub async fn sync_all(
        &self,
        id: SedimentreeId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Result<
        Map<
            PeerId,
            (
                bool,
                SyncStats,
                Vec<(Authenticated<C, F>, <C as Connection<F>>::CallError)>,
            ),
        >,
        IoError<F, S, C>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let peers: Map<PeerId, Vec<Authenticated<C, F>>> = {
            self.connections
                .lock()
                .await
                .iter()
                .map(|(peer_id, conns)| (*peer_id, conns.iter().cloned().collect()))
                .collect()
        };
        tracing::debug!("Found {} peer(s)", peers.len());

        let mut set: FuturesUnordered<_> = peers
            .iter()
            .map(|(peer_id, peer_conns)| {
                async move {
                    tracing::debug!(
                        "Requesting batch sync for sedimentree {:?} from {} connections",
                        id,
                        peer_conns.len(),
                    );

                    let mut had_success = false;
                    let mut conn_errs = Vec::new();
                    let mut stats = SyncStats::new();

                    for conn in peer_conns {
                        tracing::debug!("Using connection to peer {}", conn.peer_id());
                        let seed = FingerprintSeed::random();
                        let fp_summary = self
                            .sedimentrees
                            .get_cloned(&id)
                            .await
                            .map_or_else(
                                || FingerprintSummary::new(seed, BTreeSet::new(), BTreeSet::new()),
                                |t| t.fingerprint_summarize(&seed),
                            );

                        let req_id = conn.next_request_id().await;

                        let result = conn
                            .call(
                                BatchSyncRequest {
                                    id,
                                    req_id,
                                    fingerprint_summary: fp_summary,
                                    subscribe,
                                },
                                timeout,
                            )
                            .await;

                        match result {
                            Err(e) => conn_errs.push((conn.clone(), e)),
                            Ok(BatchSyncResponse { result, .. }) => {
                                let SyncDiff {
                                    missing_commits,
                                    missing_fragments,
                                    requesting,
                                } = match result {
                                    SyncResult::Ok(diff) => diff,
                                    SyncResult::NotFound => {
                                        tracing::debug!(
                                            "peer {peer_id:?} reports sedimentree {id:?} not found"
                                        );
                                        continue;
                                    }
                                    SyncResult::Unauthorized => {
                                        tracing::debug!(
                                            "peer {peer_id:?} reports we are unauthorized for sedimentree {id:?}"
                                        );
                                        continue;
                                    }
                                };

                                let putter =
                                    match self.storage.get_putter::<F>(*peer_id, *peer_id, id).await
                                    {
                                        Ok(p) => p,
                                        Err(e) => {
                                            tracing::warn!(
                                                "policy rejected sync from peer {:?} for sedimentree {:?}: {e}",
                                                peer_id,
                                                id
                                            );
                                            continue;
                                        }
                                    };

                                // Track counts for stats
                                let commits_to_receive = missing_commits.len();
                                let fragments_to_receive = missing_fragments.len();

                                tracing::debug!(
                                    sedimentree_id = ?id,
                                    commits_received = commits_to_receive,
                                    fragments_received = fragments_to_receive,
                                    peer_requesting_commits = requesting.commit_fingerprints.len(),
                                    peer_requesting_fragments = requesting.fragment_fingerprints.len(),
                                    "sync_all: response received"
                                );

                                for (signed_commit, blob) in missing_commits {
                                    let verified = match signed_commit.try_verify() {
                                        Ok(v) => v,
                                        Err(e) => {
                                            tracing::warn!(
                                                "full sync commit signature verification failed: {e}"
                                            );
                                            continue;
                                        }
                                    };
                                    let verified_meta = match VerifiedMeta::new(verified, blob) {
                                        Ok(vm) => vm,
                                        Err(e) => {
                                            tracing::warn!("full sync commit blob mismatch: {e}");
                                            continue;
                                        }
                                    };
                                    self.insert_commit_locally(&putter, verified_meta)
                                        .await
                                        .map_err(IoError::Storage)?;
                                }

                                for (signed_fragment, blob) in missing_fragments {
                                    let verified = match signed_fragment.try_verify() {
                                        Ok(v) => v,
                                        Err(e) => {
                                            tracing::warn!(
                                                "full sync fragment signature verification failed: {e}"
                                            );
                                            continue;
                                        }
                                    };
                                    let verified_meta = match VerifiedMeta::new(verified, blob) {
                                        Ok(vm) => vm,
                                        Err(e) => {
                                            tracing::warn!("full sync fragment blob mismatch: {e}");
                                            continue;
                                        }
                                    };
                                    self.insert_fragment_locally(&putter, verified_meta)
                                        .await
                                        .map_err(IoError::Storage)?;
                                }

                                // Update received stats
                                stats.commits_received += commits_to_receive;
                                stats.fragments_received += fragments_to_receive;

                                // Send back data the responder requested (bidirectional sync)
                                if !requesting.is_empty() {
                                    match self.send_requested_data(conn, id, &seed, &requesting).await {
                                        Ok(sent) => {
                                            stats.commits_sent += sent.commits;
                                            stats.fragments_sent += sent.fragments;
                                        }
                                        Err(ref e @ SendRequestedDataError::Unauthorized(_)) => {
                                            let msg: Message = DataRequestRejected { id }.into();
                                            if let Err(send_err) = conn.send(&msg).await {
                                                tracing::error!("failed to send DataRequestRejected: {send_err}");
                                            }
                                            tracing::warn!(
                                                "failed to send requested data to peer {:?}: {e}",
                                                peer_id
                                            );
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                "failed to send requested data to peer {:?}: {e}",
                                                peer_id
                                            );
                                        }
                                    }
                                }

                                // Mutual subscription: we subscribed to them, so also add them
                                // to our subscriptions so our commits get pushed to them
                                if subscribe {
                                    self.track_outgoing_subscription(*peer_id, id).await;
                                    self.add_subscription(*peer_id, id).await;
                                    tracing::debug!(
                                        "mutual subscription: added peer {peer_id} to our subscriptions for {id:?}"
                                    );
                                }

                                had_success = true;
                                break;
                            }
                        }
                    }

                    Ok::<(PeerId, bool, SyncStats, Vec<(Authenticated<C, F>, _)>), IoError<F, S, C>>((
                        *peer_id,
                        had_success,
                        stats,
                        conn_errs,
                    ))
                }
            })
            .collect();

        let mut out = Map::new();
        while let Some(result) = set.next().await {
            match result {
                Err(e) => {
                    tracing::error!("{e}");
                }
                Ok((peer_id, success, stats, errs)) => {
                    out.insert(peer_id, (success, stats, errs));
                }
            }
        }
        Ok(out)
    }

    /// Perform a full sync of all sedimentrees with all peers.
    ///
    /// Syncs all sedimentrees concurrently using [`FuturesUnordered`], which
    /// provides significantly better throughput when there are many sedimentrees
    /// and network latency is high.
    ///
    /// Best-effort: per-sedimentree I/O errors are collected rather than
    /// aborting the entire sync, so other sedimentrees can still make progress.
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - `bool` — `true` if at least one sync exchanged data successfully
    /// - [`SyncStats`] — aggregate counts of commits/fragments sent and received
    /// - `Vec<(Authenticated<C, F>, C::CallError)>` — per-peer call errors encountered during sync
    /// - `Vec<(SedimentreeId, IoError)>` — per-sedimentree I/O errors
    pub async fn full_sync(
        &self,
        timeout: Option<Duration>,
    ) -> (
        bool,
        SyncStats,
        Vec<(Authenticated<C, F>, <C as Connection<F>>::CallError)>,
        Vec<(SedimentreeId, IoError<F, S, C>)>,
    ) {
        tracing::info!("Requesting batch sync for all sedimentrees from all peers");
        let tree_ids = self.sedimentrees.into_keys().await;

        let mut sync_futures: FuturesUnordered<_> = tree_ids
            .into_iter()
            .map(|id| async move {
                tracing::debug!("Requesting batch sync for sedimentree {:?}", id);
                let result = self.sync_all(id, true, timeout).await;
                (id, result)
            })
            .collect();

        let mut had_success = false;
        let mut stats = SyncStats::new();
        let mut call_errs = Vec::new();
        let mut io_errs = Vec::new();

        while let Some((id, result)) = sync_futures.next().await {
            match result {
                Ok(all_results) => {
                    if all_results
                        .values()
                        .any(|(success, _stats, _errs)| *success)
                    {
                        had_success = true;
                    }

                    for (_, (_, step_stats, step_errs)) in all_results {
                        stats.commits_received += step_stats.commits_received;
                        stats.fragments_received += step_stats.fragments_received;
                        stats.commits_sent += step_stats.commits_sent;
                        stats.fragments_sent += step_stats.fragments_sent;
                        call_errs.extend(step_errs);
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to sync sedimentree {:?}: {}", id, e);
                    io_errs.push((id, e));
                }
            }
        }

        (had_success, stats, call_errs, io_errs)
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
    pub async fn connected_peer_ids(&self) -> Set<PeerId> {
        self.connections.lock().await.keys().copied().collect()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_sedimentree_locally(
        &self,
        putter: &Putter<F, S>,
        verified_commits: Vec<VerifiedMeta<LooseCommit>>,
        verified_fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Result<(), S::Error> {
        let id = putter.sedimentree_id();
        tracing::debug!("adding sedimentree with id {:?}", id);

        putter.save_sedimentree_id().await?;

        // Extract payloads for in-memory tree, save compound (commit+blob) to storage
        let mut loose_commits = Vec::with_capacity(verified_commits.len());
        for verified in verified_commits {
            loose_commits.push(verified.payload().clone());
            putter.save_commit(verified).await?;
        }

        let mut fragments = Vec::with_capacity(verified_fragments.len());
        for verified in verified_fragments {
            fragments.push(verified.payload().clone());
            putter.save_fragment(verified).await?;
        }

        let sedimentree = Sedimentree::new(fragments, loose_commits);
        self.sedimentrees
            .with_entry_or_default(id, |tree| tree.merge(sedimentree))
            .await;

        Ok(())
    }

    /// Send requested data back to a peer (fire-and-forget for bidirectional sync).
    ///
    /// Loads the requested commits and fragments from storage and sends them
    /// as individual messages. Returns the count of successfully sent items.
    /// Errors in sending individual items are logged but don't prevent sending
    /// other items.
    ///
    /// # Errors
    ///
    /// * [`SendRequestedDataError::Unauthorized`] if the peer is not authorized to fetch.
    /// * [`SendRequestedDataError::Io`] if storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn send_requested_data(
        &self,
        conn: &Authenticated<C, F>,
        id: SedimentreeId,
        seed: &FingerprintSeed,
        requesting: &RequestedData,
    ) -> Result<SendCount, SendRequestedDataError<F, S, C>> {
        if requesting.is_empty() {
            return Ok(SendCount::default());
        }

        let peer_id = conn.peer_id();
        tracing::debug!(
            "sending {} requested commits and {} requested fragments to peer {:?}",
            requesting.commit_fingerprints.len(),
            requesting.fragment_fingerprints.len(),
            peer_id
        );

        let fetcher = match self.storage.get_fetcher::<F>(peer_id, id).await {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!(
                    %peer_id,
                    ?id,
                    error = %e,
                    "policy rejected data request"
                );
                return Err(SendRequestedDataError::Unauthorized(Unauthorized {
                    peer: peer_id,
                    sedimentree_id: id,
                }));
            }
        };

        // Resolve requested fingerprints → digests via reverse-lookup tables
        let (requested_commit_digests, requested_fragment_digests) = {
            let sedimentree = self.sedimentrees.get_cloned(&id).await.unwrap_or_default();

            let commit_fp_to_digest: Map<Fingerprint<CommitId>, Digest<LooseCommit>> = sedimentree
                .loose_commits()
                .map(|c| (Fingerprint::new(seed, &c.commit_id()), Digest::hash(c)))
                .collect();

            let fragment_fp_to_digest: Map<Fingerprint<FragmentId>, Digest<Fragment>> = sedimentree
                .fragments()
                .map(|f| (Fingerprint::new(seed, &f.fragment_id()), f.digest()))
                .collect();

            let commit_digests: Vec<Digest<LooseCommit>> = requesting
                .commit_fingerprints
                .iter()
                .filter_map(|fp| {
                    let resolved = commit_fp_to_digest.get(fp).copied();
                    if resolved.is_none() {
                        tracing::warn!("requested commit fingerprint {fp} not found locally");
                    }
                    resolved
                })
                .collect();

            let fragment_digests: Vec<Digest<Fragment>> = requesting
                .fragment_fingerprints
                .iter()
                .filter_map(|fp| {
                    let resolved = fragment_fp_to_digest.get(fp).copied();
                    if resolved.is_none() {
                        tracing::warn!("requested fragment fingerprint {fp} not found locally");
                    }
                    resolved
                })
                .collect();

            (commit_digests, fragment_digests)
        };

        // Load commits and fragments from storage (compound with blobs), build wire messages
        let (commit_messages, fragment_messages) = {
            // With compound storage, load_loose_commits returns VerifiedMeta which contains both signed data and blob
            let commit_by_digest: Map<Digest<LooseCommit>, VerifiedMeta<LooseCommit>> =
                if requested_commit_digests.is_empty() {
                    Map::default()
                } else {
                    fetcher
                        .load_loose_commits()
                        .await
                        .map_err(IoError::Storage)?
                        .into_iter()
                        .map(|vm| (Digest::hash(vm.payload()), vm))
                        .collect()
                };

            let fragment_by_digest: Map<Digest<Fragment>, VerifiedMeta<Fragment>> =
                if requested_fragment_digests.is_empty() {
                    Map::default()
                } else {
                    fetcher
                        .load_fragments()
                        .await
                        .map_err(IoError::Storage)?
                        .into_iter()
                        .map(|vm| (vm.payload().digest(), vm))
                        .collect()
                };

            let commit_msgs: Vec<Message> = requested_commit_digests
                .iter()
                .filter_map(|commit_digest| {
                    let verified = commit_by_digest.get(commit_digest)?;
                    Some(Message::LooseCommit {
                        id,
                        commit: verified.signed().clone(),
                        blob: verified.blob().clone(),
                    })
                })
                .collect();

            let fragment_msgs: Vec<Message> = requested_fragment_digests
                .iter()
                .filter_map(|fragment_digest| {
                    let verified = fragment_by_digest.get(fragment_digest)?;
                    Some(Message::Fragment {
                        id,
                        fragment: verified.signed().clone(),
                        blob: verified.blob().clone(),
                    })
                })
                .collect();

            (commit_msgs, fragment_msgs)
        };

        // Send all messages concurrently using FuturesUnordered
        let mut send_futures: FuturesUnordered<_> = commit_messages
            .into_iter()
            .chain(fragment_messages.into_iter())
            .map(|msg| {
                let is_commit = matches!(msg, Message::LooseCommit { .. });
                async move {
                    let result = conn.send(&msg).await;
                    (is_commit, result)
                }
            })
            .collect();

        let mut commits_sent = 0;
        let mut fragments_sent = 0;

        while let Some((is_commit, result)) = send_futures.next().await {
            match result {
                Ok(()) => {
                    if is_commit {
                        commits_sent += 1;
                    } else {
                        fragments_sent += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("failed to send requested data: {}", e);
                }
            }
        }

        Ok(SendCount {
            commits: commits_sent,
            fragments: fragments_sent,
        })
    }

    /// Insert a commit locally, persisting to storage before updating in-memory state.
    ///
    /// Storage writes happen first for cancel safety: if the future is
    /// dropped between the storage write and the in-memory update,
    /// [`hydrate`](Self::hydrate) will rebuild correctly from storage.
    /// Content-addressed storage makes the writes idempotent, so
    /// duplicates are harmless (just a redundant I/O round-trip).
    ///
    /// # Errors
    ///
    /// Returns a storage error if persistence fails.
    async fn insert_commit_locally(
        &self,
        putter: &Putter<F, S>,
        verified_meta: VerifiedMeta<LooseCommit>,
    ) -> Result<bool, S::Error> {
        let id = putter.sedimentree_id();
        let commit = verified_meta.payload().clone();

        tracing::debug!("inserting commit {:?} locally", Digest::hash(&commit));

        // Persist to storage first (cancel-safe: idempotent CAS writes)
        // VerifiedMeta guarantees blob matches claimed metadata at construction
        putter.save_sedimentree_id().await?;
        putter.save_commit(verified_meta).await?;

        // Update in-memory tree last (returns false if already present)
        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_commit(commit))
            .await;

        Ok(was_added)
    }

    /// Insert a fragment locally, persisting to storage before updating in-memory state.
    ///
    /// See [`insert_commit_locally`](Self::insert_commit_locally) for cancel safety rationale.
    ///
    /// # Errors
    ///
    /// Returns a storage error if persistence fails.
    async fn insert_fragment_locally(
        &self,
        putter: &Putter<F, S>,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<bool, S::Error> {
        let id = putter.sedimentree_id();
        let fragment = verified_meta.payload().clone();

        // Persist to storage first (cancel-safe: idempotent CAS writes)
        // VerifiedMeta guarantees blob matches claimed metadata at construction
        putter.save_sedimentree_id().await?;
        putter.save_fragment(verified_meta).await?;

        // Update in-memory tree last
        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_fragment(fragment))
            .await;

        Ok(was_added)
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> Drop for Subduction<'a, F, S, C, P, Sig, M, N>
{
    fn drop(&mut self) {
        self.abort_manager_handle.abort();
        self.abort_listener_handle.abort();
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> ConnectionPolicy<F> for Subduction<'a, F, S, C, P, Sig, M, N>
{
    type ConnectionDisallowed = P::ConnectionDisallowed;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> F::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        self.storage.policy().authorize_connect(peer_id)
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> StoragePolicy<F> for Subduction<'a, F, S, C, P, Sig, M, N>
{
    type FetchDisallowed = P::FetchDisallowed;
    type PutDisallowed = P::PutDisallowed;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> F::Future<'_, Result<(), Self::FetchDisallowed>> {
        self.storage.policy().authorize_fetch(peer, sedimentree_id)
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> F::Future<'_, Result<(), Self::PutDisallowed>> {
        self.storage
            .policy()
            .authorize_put(requestor, author, sedimentree_id)
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> F::Future<'_, Vec<SedimentreeId>> {
        self.storage.policy().filter_authorized_fetch(peer, ids)
    }
}

/// A trait alias for the kinds of futures that can be used with Subduction.
///
/// This helps us switch between `Send` and `!Send` futures via type parameter
/// rather than creating `Send` and `!Send` versions of the entire Subduction implementation.
///
/// Similarly, the trait alias helps us avoid repeating the same complex trait bounds everywhere,
/// and needing to update them in many places if the constraints change.
pub trait SubductionFutureForm<
    'a,
    S: Storage<Self>,
    C: Connection<Self> + PartialEq + 'a,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
>: StartListener<'a, S, C, P, Sig, M, N>
{
}

impl<
    'a,
    S: Storage<Self>,
    C: Connection<Self> + PartialEq + 'a,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
    U: StartListener<'a, S, C, P, Sig, M, N>,
> SubductionFutureForm<'a, S, C, P, Sig, M, N> for U
{
}

/// A trait for starting the listener task for Subduction.
///
/// This lets us abstract over `Send` and `!Send` futures
pub trait StartListener<
    'a,
    S: Storage<Self>,
    C: Connection<Self> + PartialEq + 'a,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
>: FutureForm + RunManager<Authenticated<C, Self>> + Sized
{
    /// Start the listener task for Subduction.
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, P, Sig, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        Self: Sized;
}

#[future_form(
    Sendable where
        C: Connection<Sendable> + PartialEq + Clone + Send + Sync + 'static,
        S: Storage<Sendable> + Send + Sync + 'a,
        P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'a,
        P::PutDisallowed: Send + 'static,
        P::FetchDisallowed: Send + 'static,
        Sig: Signer<Sendable> + Send + Sync + 'a,
        M: DepthMetric + Send + Sync + 'a,
        S::Error: Send + 'static,
        C::DisconnectionError: Send + 'static,
        C::CallError: Send + 'static,
        C::RecvError: Send + 'static,
        C::SendError: Send + 'static,
    Local where
        C: Connection<Local> + PartialEq + Clone + 'static,
        S: Storage<Local> + 'a,
        P: ConnectionPolicy<Local> + StoragePolicy<Local> + 'a,
        Sig: Signer<Local> + 'a,
        M: DepthMetric + 'a
)]
impl<'a, K: FutureForm, C, S, P, Sig, M, const N: usize> StartListener<'a, S, C, P, Sig, M, N>
    for K
{
    fn start_listener(
        subduction: Arc<Subduction<'a, Self, S, C, P, Sig, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            K::from_future(async move {
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
    F: StartListener<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize = 256,
> {
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<(S, C, P, Sig, M)>,
}

impl<
    'a,
    F: StartListener<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> ListenerFuture<'a, F, S, C, P, Sig, M, N>
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
    F: StartListener<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> Deref for ListenerFuture<'a, F, S, C, P, Sig, M, N>
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<
    'a,
    F: StartListener<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> Future for ListenerFuture<'a, F, S, C, P, Sig, M, N>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<
    'a,
    F: StartListener<'a, S, C, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F> + PartialEq + 'a,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    M: DepthMetric,
    const N: usize,
> Unpin for ListenerFuture<'a, F, S, C, P, Sig, M, N>
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection::{
            nonce_cache::NonceCache,
            test_utils::{FailingSendMockConnection, TestSpawn, test_signer},
        },
        policy::open::OpenPolicy,
        sharded_map::ShardedMap,
        storage::memory::MemoryStorage,
        subduction::pending_blob_requests::DEFAULT_MAX_PENDING_BLOB_REQUESTS,
    };
    use alloc::collections::BTreeSet;
    use future_form::Sendable;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        commit::CountLeadingZeroBytes,
        crypto::digest::Digest,
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::LooseCommit,
    };
    use subduction_crypto::signed::Signed;
    use testresult::TestResult;

    fn make_commit_parts() -> (BTreeSet<Digest<LooseCommit>>, Blob) {
        let contents = vec![0u8; 32];
        let blob = Blob::new(contents);
        (BTreeSet::new(), blob)
    }

    async fn make_signed_test_commit(id: &SedimentreeId) -> (Signed<LooseCommit>, Blob) {
        let (parents, blob) = make_commit_parts();
        let blob_meta = BlobMeta::new(&blob);
        let commit = LooseCommit::new(*id, parents, blob_meta);
        let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
        (verified.into_signed(), blob)
    }

    #[allow(clippy::type_complexity)]
    fn make_fragment_parts() -> (
        Digest<LooseCommit>,
        BTreeSet<Digest<LooseCommit>>,
        Vec<Digest<LooseCommit>>,
        Blob,
    ) {
        let contents = vec![0u8; 32];
        let blob = Blob::new(contents);
        let head = Digest::<LooseCommit>::force_from_bytes([1u8; 32]);
        let boundary = BTreeSet::from([Digest::<LooseCommit>::force_from_bytes([2u8; 32])]);
        let checkpoints = vec![Digest::<LooseCommit>::force_from_bytes([3u8; 32])];
        (head, boundary, checkpoints, blob)
    }

    async fn make_signed_test_fragment(id: &SedimentreeId) -> (Signed<Fragment>, Blob) {
        let (head, boundary, checkpoints, blob) = make_fragment_parts();
        let blob_meta = BlobMeta::new(&blob);
        let fragment = Fragment::new(*id, head, boundary, &checkpoints, blob_meta);
        let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
        (verified.into_signed(), blob)
    }

    #[tokio::test]
    async fn test_recv_commit_unregisters_connection_on_send_failure() -> TestResult {
        let storage = MemoryStorage::new();
        let depth_metric = CountLeadingZeroBytes;

        let (subduction, _listener_fut, _actor_fut) =
            Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
                None,
                test_signer(),
                storage,
                OpenPolicy,
                NonceCache::default(),
                depth_metric,
                ShardedMap::with_key(0, 0),
                TestSpawn,
                DEFAULT_MAX_PENDING_BLOB_REQUESTS,
            );

        // Register a failing connection with a different peer ID than the sender
        let sender_peer_id = PeerId::new([1u8; 32]);
        let other_peer_id = PeerId::new([2u8; 32]);
        let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
        let _fresh = subduction.register(conn.authenticated()).await?;
        assert_eq!(subduction.connected_peer_ids().await.len(), 1);

        // Subscribe other_peer to the sedimentree so forwarding will be attempted
        let id = SedimentreeId::new([1u8; 32]);
        subduction.add_subscription(other_peer_id, id).await;

        // Receive a commit from a different peer - the propagation send will fail
        let (signed_commit, blob) = make_signed_test_commit(&id).await;

        let _ = subduction
            .recv_commit(&sender_peer_id, id, &signed_commit, blob)
            .await;

        // Connection should be unregistered after send failure during propagation
        assert_eq!(
            subduction.connected_peer_ids().await.len(),
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
            Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
                None,
                test_signer(),
                storage,
                OpenPolicy,
                NonceCache::default(),
                depth_metric,
                ShardedMap::with_key(0, 0),
                TestSpawn,
                DEFAULT_MAX_PENDING_BLOB_REQUESTS,
            );

        // Register a failing connection with a different peer ID than the sender
        let sender_peer_id = PeerId::new([1u8; 32]);
        let other_peer_id = PeerId::new([2u8; 32]);
        let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
        let _fresh = subduction.register(conn.authenticated()).await?;
        assert_eq!(subduction.connected_peer_ids().await.len(), 1);

        // Subscribe other_peer to the sedimentree so forwarding will be attempted
        let id = SedimentreeId::new([1u8; 32]);
        subduction.add_subscription(other_peer_id, id).await;

        // Receive a fragment from a different peer - the propagation send will fail
        let (signed_fragment, blob) = make_signed_test_fragment(&id).await;

        let _ = subduction
            .recv_fragment(&sender_peer_id, id, &signed_fragment, blob)
            .await;

        // Connection should be unregistered after send failure during propagation
        assert_eq!(
            subduction.connected_peer_ids().await.len(),
            0,
            "Connection should be unregistered after send failure"
        );

        Ok(())
    }
}
