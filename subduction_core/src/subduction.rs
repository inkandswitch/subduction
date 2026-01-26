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
pub mod request;

use crate::{
    connection::{
        Connection,
        handshake::DiscoveryId,
        manager::{Command, ConnectionManager, RunManager, Spawn},
        message::{
            BatchSyncRequest, BatchSyncResponse, Message, RemoveSubscriptions, RequestId, SyncDiff,
        },
        nonce_cache::NonceCache,
    },
    crypto::{signed::Signed, signer::Signer, verified::Verified},
    peer::id::PeerId,
    policy::{ConnectionPolicy, StoragePolicy},
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter},
};
use alloc::{boxed::Box, string::ToString, sync::Arc, vec::Vec};
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
    WriteError,
};
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{
    FutureExt, StreamExt,
    future::try_join_all,
    stream::{AbortHandle, AbortRegistration, Abortable, Aborted, FuturesUnordered},
};
use nonempty::NonEmpty;
use request::FragmentRequested;
use sedimentree_core::collections::{
    Map, Set,
    nonempty_ext::{NonEmptyExt, RemoveResult},
};
use sedimentree_core::{
    blob::{Blob, Digest},
    commit::CountLeadingZeroBytes,
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::LooseCommit,
    sedimentree::{RemoteDiff, Sedimentree, SedimentreeSummary},
};

use crate::storage::Storage;

/// The main synchronization manager for sedimentrees.
#[derive(Debug, Clone)]
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

    connections: Arc<Mutex<Map<PeerId, NonEmpty<C>>>>,
    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    nonce_tracker: Arc<NonceCache>,

    manager_channel: Sender<Command<C>>,
    msg_queue: async_channel::Receiver<(C, Message)>,
    connection_closed: async_channel::Receiver<C>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a F>,
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
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, F, S, C, P, Sig, M, N>,
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
            discovery_id,
            signer,
            depth_metric,
            sedimentrees: Arc::new(sedimentrees),
            connections: Arc::new(Mutex::new(Map::new())),
            subscriptions: Arc::new(Mutex::new(Map::new())),
            storage: StoragePowerbox::new(storage, Arc::new(policy)),
            nonce_tracker: Arc::new(nonce_cache),
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
        );
        for id in ids {
            let signed_loose_commits = subduction
                .storage
                .load_loose_commits(id)
                .await
                .map_err(HydrationError::LoadLooseCommitsError)?;
            let signed_fragments = subduction
                .storage
                .load_fragments(id)
                .await
                .map_err(HydrationError::LoadFragmentsError)?;

            // Extract payloads from trusted storage (already verified before storage)
            let loose_commits: Vec<_> = signed_loose_commits
                .into_iter()
                .filter_map(|(_, signed)| signed.decode_payload().ok())
                .collect();
            let fragments: Vec<_> = signed_fragments
                .into_iter()
                .filter_map(|(_, signed)| signed.decode_payload().ok())
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
        self.signer.peer_id()
    }

    /// Returns a reference to the nonce cache for replay protection.
    #[must_use]
    pub fn nonce_cache(&self) -> &NonceCache {
        &self.nonce_tracker
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
        use futures::future::{Either, select};

        tracing::info!("starting Subduction listener");

        loop {
            let msg_fut = pin!(self.msg_queue.recv().fuse());
            let closed_fut = pin!(self.connection_closed.recv().fuse());

            match select(msg_fut, closed_fut).await {
                Either::Left((msg_result, _)) => {
                    if let Ok((conn, msg)) = msg_result {
                        let peer_id = conn.peer_id();
                        tracing::debug!(
                            "Subduction listener received message from peer {}: {:?}",
                            peer_id,
                            msg
                        );

                        if let Err(e) = self.dispatch(&conn, msg).await {
                            tracing::error!(
                                "error dispatching message from peer {}: {}",
                                peer_id,
                                e
                            );
                            // Connection is broken - unregister from conns map.
                            // The stream in the actor will naturally end when recv fails.
                            let _ = self.unregister(&conn).await;
                            tracing::info!("unregistered failed connection from peer {}", peer_id);
                        }
                        // No re-registration needed - the stream handles continuous recv
                    } else {
                        tracing::info!("Message queue closed");
                        break;
                    }
                }
                Either::Right((closed_result, _)) => {
                    if let Ok(conn) = closed_result {
                        let peer_id = conn.peer_id();
                        tracing::info!("Connection from peer {} closed, unregistering", peer_id);
                        self.unregister(&conn).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn dispatch(&self, conn: &C, message: Message) -> Result<(), ListenError<F, S, C>> {
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
                subscribe,
            }) => {
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_request();

                // Add subscription if requested
                if subscribe {
                    self.add_subscription(from, id).await;
                    tracing::debug!("added subscription for peer {from} to sedimentree {id:?}");
                }

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

                self.recv_batch_sync_response(&from, id, diff).await?;
            }
            Message::BlobsRequest(digests) => match self.recv_blob_request(conn, &digests).await {
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
            },
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
            Message::RemoveSubscriptions(RemoveSubscriptions { ids }) => {
                self.remove_subscriptions(from, &ids).await;
                tracing::debug!("removed subscriptions for peer {from}: {ids:?}");
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
        conn: C,
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
    pub async fn disconnect(&self, conn: &C) -> Result<bool, C::DisconnectionError> {
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
        let all_conns: Vec<C> = {
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
        conn: C,
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
    pub async fn unregister(&self, conn: &C) -> Option<bool> {
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
    async fn all_connections(&self) -> Vec<C> {
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
    async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
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
    ) -> Vec<C> {
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
    pub async fn get_blob(&self, digest: Digest) -> Result<Option<Blob>, S::Error> {
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
    pub async fn get_blobs(&self, id: SedimentreeId) -> Result<Option<NonEmpty<Blob>>, S::Error> {
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
        if let Some(maybe_blobs) = self.get_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            let tree = self.sedimentrees.get_cloned(&id).await;
            if let Some(tree) = tree {
                let summary = tree.summarize();
                let conns = self.all_connections().await;
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
                                subscribe: false,
                            },
                            timeout,
                        )
                        .await
                        .map_err(IoError::ConnCall)?;

                    debug_assert_eq!(req_id, resp_batch_id);

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
        conn: &C,
        digests: &[Digest],
    ) -> Result<(), BlobRequestErr<F, S, C>> {
        let mut blobs = Vec::new();
        let mut missing = Vec::new();
        for digest in digests {
            if let Some(blob) = self.get_blob(*digest).await.map_err(IoError::Storage)? {
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
    /// * [`WriteError::Io`] if a storage or network error occurs.
    /// * [`WriteError::PutDisallowed`] if the storage policy rejects the write.
    pub async fn add_sedimentree(
        &self,
        id: SedimentreeId,
        sedimentree: Sedimentree,
        blobs: Vec<Blob>,
    ) -> Result<(), WriteError<F, S, C, P::PutDisallowed>> {
        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        // Sign all commits and fragments with our signer (seal returns Verified)
        let mut verified_commits = Vec::with_capacity(sedimentree.loose_commits().count());
        for commit in sedimentree.loose_commits() {
            let verified = Signed::seal::<F, _>(&self.signer, commit.clone()).await;
            verified_commits.push(verified);
        }

        let mut verified_fragments = Vec::with_capacity(sedimentree.fragments().count());
        for fragment in sedimentree.fragments() {
            let verified = Signed::seal::<F, _>(&self.signer, fragment.clone()).await;
            verified_fragments.push(verified);
        }

        self.insert_sedimentree_locally(&putter, verified_commits, verified_fragments, blobs)
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

        if let Some(sedimentree) = maybe_sedimentree {
            let destroyer = self.storage.local_destroyer(id);

            for commit in sedimentree.loose_commits() {
                destroyer
                    .delete_blob(commit.blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?;
            }
            destroyer
                .delete_loose_commits()
                .await
                .map_err(IoError::Storage)?;

            for fragment in sedimentree.fragments() {
                destroyer
                    .delete_blob(fragment.summary().blob_meta().digest())
                    .await
                    .map_err(IoError::Storage)?;
            }
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
        commit: &LooseCommit,
        blob: Blob,
    ) -> Result<Option<FragmentRequested>, WriteError<F, S, C, P::PutDisallowed>> {
        tracing::debug!(
            "adding commit {:?} to sedimentree {:?}",
            commit.digest(),
            id
        );

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        // Sign the commit with our signer (seal returns Verified)
        let verified = Signed::seal::<F, _>(&self.signer, commit.clone()).await;
        let signed_for_wire = verified.signed().clone();

        self.insert_commit_locally(&putter, verified, blob.clone())
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        let msg = Message::LooseCommit {
            id,
            commit: signed_for_wire,
            blob,
        };
        {
            let conns = self.all_connections().await;
            for conn in conns {
                let peer_id = conn.peer_id();
                tracing::debug!(
                    "Propagating commit {:?} for sedimentree {:?} to peer {}",
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

        // Sign the fragment with our signer (seal returns Verified)
        let verified = Signed::seal::<F, _>(&self.signer, fragment.clone()).await;
        let signed_for_wire = verified.signed().clone();

        self.sedimentrees
            .with_entry_or_default(id, |tree| tree.add_fragment(fragment.clone()))
            .await;

        self.storage
            .save_blob(blob.clone())
            .await
            .map_err(IoError::Storage)?;

        let msg = Message::Fragment {
            id,
            fragment: signed_for_wire,
            blob,
        };

        let conns = self.all_connections().await;
        for conn in conns {
            let peer_id = conn.peer_id();
            tracing::debug!(
                "Propagating fragment {:?} for sedimentree {:?} to peer {}",
                fragment.digest(),
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
        // Verify the signature
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
                    "policy rejected commit from peer {:?} (author {:?}) for sedimentree {:?}: {e}",
                    from,
                    author,
                    id
                );
                return Ok(false);
            }
        };

        // Clone signed for forwarding before consuming verified
        let signed_for_wire = verified.signed().clone();

        let was_new = self
            .insert_commit_locally(&putter, verified, blob.clone())
            .await
            .map_err(IoError::Storage)?;

        if was_new {
            let msg = Message::LooseCommit {
                id,
                commit: signed_for_wire,
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

        let was_new = self
            .insert_fragment_locally(&putter, verified, blob.clone())
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

        // Load signed commits and fragments from storage
        let signed_commits = self
            .storage
            .load_loose_commits(id)
            .await
            .map_err(IoError::Storage)?;
        let signed_fragments = self
            .storage
            .load_fragments(id)
            .await
            .map_err(IoError::Storage)?;

        // Storage returns (Digest, Signed<T>) tuples — no decoding needed for lookup
        let commit_by_digest: Map<Digest, Signed<LooseCommit>> =
            signed_commits.into_iter().collect();
        let fragment_by_digest: Map<Digest, Signed<Fragment>> =
            signed_fragments.into_iter().collect();

        let sync_diff = {
            let mut locked = self.sedimentrees.get_shard_containing(&id).lock().await;
            let sedimentree = locked.entry(id).or_default();
            tracing::debug!(
                "received batch sync request for sedimentree {id:?} for req_id {req_id:?} with {} commits and {} fragments",
                their_summary.loose_commits().len(),
                their_summary.fragment_summaries().len()
            );

            let (commits_to_add, local_commit_digests, local_fragment_digests) = {
                let diff: RemoteDiff<'_> = sedimentree.diff_remote(their_summary);
                (
                    diff.remote_commits
                        .iter()
                        .map(|c| (*c).clone())
                        .collect::<Vec<LooseCommit>>(),
                    diff.local_commits
                        .iter()
                        .map(|c| c.digest())
                        .collect::<Vec<_>>(),
                    diff.local_fragments
                        .iter()
                        .map(|f| f.digest())
                        .collect::<Vec<_>>(),
                )
            };

            for commit in commits_to_add {
                sedimentree.add_commit(commit);
            }

            for digest in local_commit_digests {
                if let Some(signed_commit) = commit_by_digest.get(&digest) {
                    if let Ok(payload) = signed_commit.decode_payload() {
                        let blob_digest = payload.blob_meta().digest();
                        if let Some(blob) = self
                            .storage
                            .load_blob(blob_digest)
                            .await
                            .map_err(IoError::Storage)?
                        {
                            their_missing_commits.push((signed_commit.clone(), blob));
                        } else {
                            tracing::warn!("missing blob for commit {:?}", digest);
                            our_missing_blobs.push(blob_digest);
                        }
                    }
                }
            }

            for digest in local_fragment_digests {
                if let Some(signed_fragment) = fragment_by_digest.get(&digest) {
                    if let Ok(payload) = signed_fragment.decode_payload() {
                        let blob_digest = payload.summary().blob_meta().digest();
                        if let Some(blob) = self
                            .storage
                            .load_blob(blob_digest)
                            .await
                            .map_err(IoError::Storage)?
                        {
                            their_missing_fragments.push((signed_fragment.clone(), blob));
                        } else {
                            tracing::warn!("missing blob for fragment {:?}", digest);
                            our_missing_blobs.push(blob_digest);
                        }
                    }
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
            diff: sync_diff,
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
            self.insert_commit_locally(&putter, verified, blob)
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
            self.insert_fragment_locally(&putter, verified, blob)
                .await
                .map_err(IoError::Storage)?;
        }

        Ok(())
    }

    /// Find blobs from connected peers.
    pub async fn request_blobs(&self, digests: Vec<Digest>) {
        let msg = Message::BlobsRequest(digests);
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
    pub async fn sync_with_peer(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Result<(bool, Vec<Blob>, Vec<(C, C::CallError)>), IoError<F, S, C>> {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

        let mut blobs = Vec::new();
        let mut had_success = false;

        let peer_conns: Vec<C> = {
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
                        subscribe,
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

                    for (signed_commit, blob) in missing_commits {
                        let verified = match signed_commit.try_verify() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("sync commit signature verification failed: {e}");
                                continue;
                            }
                        };
                        blobs.push(blob.clone());
                        self.insert_commit_locally(&putter, verified, blob)
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
                        blobs.push(blob.clone());
                        self.insert_fragment_locally(&putter, verified, blob)
                            .await
                            .map_err(IoError::Storage)?;
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
    pub async fn sync_all(
        &self,
        id: SedimentreeId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Result<
        Map<PeerId, (bool, Vec<Blob>, Vec<(C, <C as Connection<F>>::CallError)>)>,
        IoError<F, S, C>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let peers: Map<PeerId, Vec<C>> = {
            self.connections
                .lock()
                .await
                .iter()
                .map(|(peer_id, conns)| (*peer_id, conns.iter().cloned().collect()))
                .collect()
        };
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

                    for conn in peer_conns {
                        tracing::debug!("Using connection to peer {}", conn.peer_id());
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
                                    subscribe,
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
                                    inner_blobs.lock().await.insert(blob.clone());
                                    self.insert_commit_locally(&putter, verified, blob)
                                        .await
                                        .map_err(IoError::<F, S, C>::Storage)?;
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
                                    inner_blobs.lock().await.insert(blob.clone());
                                    self.insert_fragment_locally(&putter, verified, blob)
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
                }
            })
            .collect();

        let mut out = Map::new();
        while let Some(result) = set.next().await {
            match result {
                Err(e) => {
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
    pub async fn full_sync(
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
            let all_results = self.sync_all(id, true, timeout).await?;
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
    pub async fn connected_peer_ids(&self) -> Set<PeerId> {
        self.connections.lock().await.keys().copied().collect()
    }

    /*******************
     * PRIVATE METHODS *
     *******************/

    async fn insert_sedimentree_locally(
        &self,
        putter: &Putter<F, S>,
        verified_commits: Vec<Verified<LooseCommit>>,
        verified_fragments: Vec<Verified<Fragment>>,
        blobs: Vec<Blob>,
    ) -> Result<(), S::Error> {
        let id = putter.sedimentree_id();
        tracing::debug!("adding sedimentree with id {:?}", id);

        // Save blobs first so they're available when commit/fragment callbacks fire
        for blob in blobs {
            putter.save_blob(blob).await?;
        }

        putter.save_sedimentree_id().await?;

        // Extract payloads for in-memory tree, save verified versions to storage
        let mut loose_commits = Vec::with_capacity(verified_commits.len());
        for verified in verified_commits {
            loose_commits.push(verified.payload().clone());
            putter.save_loose_commit(verified).await?;
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
        putter: &Putter<F, S>,
        verified: Verified<LooseCommit>,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        let id = putter.sedimentree_id();
        let commit = verified.payload().clone();

        tracing::debug!("inserting commit {:?} locally", commit.digest());

        // Check in-memory tree first to skip storage for duplicates
        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_commit(commit))
            .await;
        if !was_added {
            return Ok(false);
        }

        // Save blob first so callback wrappers can load it by digest
        putter.save_blob(blob).await?;
        putter.save_sedimentree_id().await?;
        putter.save_loose_commit(verified).await?;

        Ok(true)
    }

    // NOTE no integrity checking, we assume that they made a good fragment at the right depth
    async fn insert_fragment_locally(
        &self,
        putter: &Putter<F, S>,
        verified: Verified<Fragment>,
        blob: Blob,
    ) -> Result<bool, S::Error> {
        let id = putter.sedimentree_id();
        let fragment = verified.payload().clone();

        let was_added = self
            .sedimentrees
            .with_entry_or_default(id, |tree| tree.add_fragment(fragment))
            .await;
        if !was_added {
            return Ok(false);
        }

        // Save blob first so callback wrappers can load it by digest
        putter.save_blob(blob).await?;
        putter.save_sedimentree_id().await?;
        putter.save_fragment(verified).await?;
        Ok(true)
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
>: FutureForm + RunManager<C> + Sized
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

#[cfg(all(test, feature = "test_utils"))]
mod tests {
    use super::*;
    use crate::connection::nonce_cache::NonceCache;
    use crate::connection::test_utils::MockConnection;
    use crate::crypto::signer::MemorySigner;
    use crate::policy::OpenPolicy;
    use futures::future::{BoxFuture, LocalBoxFuture};
    use sedimentree_core::{
        commit::CountLeadingZeroBytes, id::SedimentreeId, sedimentree::Sedimentree,
        storage::MemoryStorage,
    };
    use testresult::TestResult;

    /// Create a test signer with deterministic key bytes.
    fn test_signer() -> MemorySigner {
        MemorySigner::from_bytes(&[42u8; 32])
    }

    /// A spawner that doesn't actually spawn (for tests that don't need task execution).
    struct TestSpawn;

    impl Spawn<Sendable> for TestSpawn {
        fn spawn(&self, _fut: BoxFuture<'static, ()>) -> AbortHandle {
            let (handle, _reg) = AbortHandle::new_pair();
            handle
        }
    }

    impl Spawn<future_form::Local> for TestSpawn {
        fn spawn(&self, _fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
            let (handle, _reg) = AbortHandle::new_pair();
            handle
        }
    }

    /// A spawner that uses tokio::spawn for tests that need actual task execution.
    struct TokioSpawn;

    impl Spawn<Sendable> for TokioSpawn {
        fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandle {
            use futures::future::Abortable;
            let (handle, reg) = AbortHandle::new_pair();
            tokio::spawn(Abortable::new(fut, reg));
            handle
        }
    }

    impl Spawn<future_form::Local> for TokioSpawn {
        fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> AbortHandle {
            use futures::future::Abortable;
            let (handle, reg) = AbortHandle::new_pair();
            tokio::task::spawn_local(Abortable::new(fut, reg));
            handle
        }
    }

    mod initialization {
        use super::*;

        #[test]
        fn test_new_creates_empty_subduction() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Verify initial state via async runtime would be needed,
            // but we can at least verify construction doesn't panic
            assert!(!subduction.abort_manager_handle.is_aborted());
            assert!(!subduction.abort_listener_handle.is_aborted());
        }

        #[tokio::test]
        async fn test_new_has_empty_sedimentrees() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let ids = subduction.sedimentree_ids().await;
            assert!(ids.is_empty());
        }

        #[tokio::test]
        async fn test_new_has_no_connections() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let peer_ids = subduction.connected_peer_ids().await;
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let ids = subduction.sedimentree_ids().await;
            assert_eq!(ids.len(), 0);
        }

        #[tokio::test]
        async fn test_add_sedimentree_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let peer_ids = subduction.connected_peer_ids().await;
            assert_eq!(peer_ids.len(), 0);
        }

        #[tokio::test]
        async fn test_register_adds_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn = MockConnection::new();
            let fresh = subduction.register(conn).await?;

            assert!(fresh);
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_register_same_connection_twice_returns_false() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn = MockConnection::new();
            let fresh1 = subduction.register(conn).await?;
            let fresh2 = subduction.register(conn).await?;

            assert!(fresh1);
            assert!(!fresh2);
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            Ok(())
        }

        #[tokio::test]
        async fn test_unregister_removes_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn = MockConnection::new();
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            let removed = subduction.unregister(&conn).await;
            assert_eq!(removed, Some(true));
            assert_eq!(subduction.connected_peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_unregister_nonexistent_returns_false() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Unregister a connection that was never registered
            let conn = MockConnection::with_peer_id(PeerId::new([99u8; 32]));
            let removed = subduction.unregister(&conn).await;
            assert_eq!(removed, None);
        }

        #[tokio::test]
        async fn test_register_different_peers_increases_count() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
            let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;

            assert_eq!(subduction.connected_peer_ids().await.len(), 2);
            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_removes_connection() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn = MockConnection::new();
            let _fresh = subduction.register(conn).await?;

            let removed = subduction.disconnect(&conn).await?;
            assert!(removed);
            assert_eq!(subduction.connected_peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_nonexistent_returns_false() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn = MockConnection::with_peer_id(PeerId::new([99u8; 32]));
            let removed = subduction.disconnect(&conn).await?;
            assert!(!removed);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_all_removes_all_connections() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let conn1 = MockConnection::with_peer_id(PeerId::new([1u8; 32]));
            let conn2 = MockConnection::with_peer_id(PeerId::new([2u8; 32]));

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 2);

            subduction.disconnect_all().await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 0);

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_from_peer_removes_specific_peer() -> TestResult {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let peer_id1 = PeerId::new([1u8; 32]);
            let peer_id2 = PeerId::new([2u8; 32]);
            let conn1 = MockConnection::with_peer_id(peer_id1);
            let conn2 = MockConnection::with_peer_id(peer_id2);

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 2);

            let removed = subduction.disconnect_from_peer(&peer_id1).await?;
            assert!(removed);
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);
            assert!(!subduction.connected_peer_ids().await.contains(&peer_id1));
            assert!(subduction.connected_peer_ids().await.contains(&peer_id2));

            Ok(())
        }

        #[tokio::test]
        async fn test_disconnect_from_nonexistent_peer_returns_false() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let peer_id = PeerId::new([1u8; 32]);
            let result = subduction.authorize_connect(peer_id).await;
            assert!(result.is_ok());
        }

        // TODO also test when the policy says no
    }

    mod blob_operations {
        use super::*;
        use sedimentree_core::blob::Digest;

        #[tokio::test]
        async fn test_get_blob_returns_none_for_missing() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let digest = Digest::from([1u8; 32]);
            let blob = subduction.get_blob(digest).await.unwrap();
            assert!(blob.is_none());
        }

        #[tokio::test]
        async fn test_get_blobs_returns_none_for_missing_tree() {
            let storage = MemoryStorage::new();
            let depth_metric = CountLeadingZeroBytes;

            let (subduction, _listener_fut, _actor_fut) =
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            let id = SedimentreeId::new([1u8; 32]);
            let blobs = subduction.get_blobs(id).await.unwrap();
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
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            // Add a commit - the send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction.add_commit(id, &commit, blob).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.connected_peer_ids().await.len(),
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
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            // Add a fragment - the send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (fragment, blob) = make_test_fragment();

            let _ = subduction.add_fragment(id, &fragment, blob).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.connected_peer_ids().await.len(),
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
                Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Register a failing connection with a different peer ID than the sender
            let sender_peer_id = PeerId::new([1u8; 32]);
            let other_peer_id = PeerId::new([2u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            // Receive a commit from a different peer - the propagation send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction
                .recv_commit(&sender_peer_id, id, &commit, blob)
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
                );

            // Register a failing connection with a different peer ID than the sender
            let sender_peer_id = PeerId::new([1u8; 32]);
            let other_peer_id = PeerId::new([2u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            // Receive a fragment from a different peer - the propagation send will fail
            let id = SedimentreeId::new([1u8; 32]);
            let (fragment, blob) = make_test_fragment();

            let _ = subduction
                .recv_fragment(&sender_peer_id, id, &fragment, blob)
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
        async fn test_request_blobs_unregisters_connection_on_send_failure() -> TestResult {
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
                );

            // Register a failing connection
            let peer_id = PeerId::new([1u8; 32]);
            let conn = FailingSendMockConnection::with_peer_id(peer_id);
            let _fresh = subduction.register(conn).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 1);

            // Request blobs - the send will fail
            let digests = vec![Digest::from([1u8; 32])];
            subduction.request_blobs(digests).await;

            // Connection should be unregistered after send failure
            assert_eq!(
                subduction.connected_peer_ids().await.len(),
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
                Subduction::<'_, Sendable, _, MockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    depth_metric,
                    ShardedMap::with_key(0, 0),
                    TestSpawn,
                );

            // Register two connections that will succeed
            let peer_id1 = PeerId::new([1u8; 32]);
            let peer_id2 = PeerId::new([2u8; 32]);
            let conn1 = MockConnection::with_peer_id(peer_id1);
            let conn2 = MockConnection::with_peer_id(peer_id2);

            subduction.register(conn1).await?;
            subduction.register(conn2).await?;
            assert_eq!(subduction.connected_peer_ids().await.len(), 2);

            // Add a commit - sends will succeed
            let id = SedimentreeId::new([1u8; 32]);
            let (commit, blob) = make_test_commit();

            let _ = subduction.add_commit(id, &commit, blob).await;

            // Both connections should still be registered (sends succeeded)
            assert_eq!(
                subduction.connected_peer_ids().await.len(),
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
        use future_form::Local;
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
                Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([42u8; 32]);
            let (commit, blob) = make_test_commit_with_data(b"test commit");

            handle
                .inbound_tx
                .send(Message::LooseCommit {
                    id: sedimentree_id,
                    commit,
                    blob,
                })
                .await?;

            tokio::time::sleep(Duration::from_millis(50)).await;

            let ids = subduction.sedimentree_ids().await;
            assert!(
                ids.contains(&sedimentree_id),
                "[SENDABLE] Sedimentree should be visible. Found: {ids:?}"
            );
            assert_eq!(
                subduction
                    .get_commits(sedimentree_id)
                    .await
                    .map(|c| c.len()),
                Some(1)
            );

            actor_task.abort();
            listener_task.abort();
            Ok(())
        }

        #[tokio::test]
        async fn test_sendable_multiple_sequential() -> TestResult {
            let storage = MemoryStorage::new();
            let (subduction, listener_fut, actor_fut) =
                Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            for i in 0..3u8 {
                let sedimentree_id = SedimentreeId::new([i; 32]);
                let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                handle
                    .inbound_tx
                    .send(Message::LooseCommit {
                        id: sedimentree_id,
                        commit,
                        blob,
                    })
                    .await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let ids = subduction.sedimentree_ids().await;
                assert_eq!(
                    ids.len(),
                    (i + 1) as usize,
                    "[SENDABLE] After commit {i}: expected {} sedimentrees, found {}. ONE BEHIND BUG!",
                    i + 1,
                    ids.len()
                );
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
                Subduction::<'_, Sendable, _, ChannelMockConnection, _, _, _>::new(
                    None,
                    test_signer(),
                    storage,
                    OpenPolicy,
                    NonceCache::default(),
                    CountLeadingZeroBytes,
                    ShardedMap::with_key(0, 0),
                    TokioSpawn,
                );

            let (conn, handle) = ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
            subduction.register(conn).await?;

            let actor_task = tokio::spawn(actor_fut);
            let listener_task = tokio::spawn(listener_fut);
            tokio::time::sleep(Duration::from_millis(10)).await;

            let sedimentree_id = SedimentreeId::new([99u8; 32]);

            for i in 0..3usize {
                let (commit, blob) = make_test_commit_with_data(format!("commit {i}").as_bytes());

                handle
                    .inbound_tx
                    .send(Message::LooseCommit {
                        id: sedimentree_id,
                        commit,
                        blob,
                    })
                    .await?;

                tokio::time::sleep(Duration::from_millis(20)).await;

                let count = subduction
                    .get_commits(sedimentree_id)
                    .await
                    .map_or(0, |c| c.len());
                assert_eq!(
                    count,
                    i + 1,
                    "[SENDABLE] After commit {i}, expected {} commits but found {}. DELAYED!",
                    i + 1,
                    count
                );
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
            tokio::task::LocalSet::new()
                .run_until(async {
                    let storage = MemoryStorage::new();
                    let (subduction, listener_fut, actor_fut) =
                        Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                            None,
                            test_signer(),
                            storage,
                            OpenPolicy,
                            NonceCache::default(),
                            CountLeadingZeroBytes,
                            ShardedMap::with_key(0, 0),
                            TokioSpawn,
                        );

                    let (conn, handle) =
                        ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
                    subduction.register(conn).await?;

                    let actor_task = tokio::task::spawn_local(actor_fut);
                    let listener_task = tokio::task::spawn_local(listener_fut);
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    let sedimentree_id = SedimentreeId::new([42u8; 32]);
                    let (commit, blob) = make_test_commit_with_data(b"test commit");

                    handle
                        .inbound_tx
                        .send(Message::LooseCommit {
                            id: sedimentree_id,
                            commit,
                            blob,
                        })
                        .await?;

                    tokio::time::sleep(Duration::from_millis(50)).await;

                    let ids = subduction.sedimentree_ids().await;
                    assert!(
                        ids.contains(&sedimentree_id),
                        "[LOCAL] Sedimentree should be visible. Found: {ids:?}"
                    );
                    assert_eq!(
                        subduction
                            .get_commits(sedimentree_id)
                            .await
                            .map(|c| c.len()),
                        Some(1)
                    );

                    actor_task.abort();
                    listener_task.abort();
                    Ok::<_, Box<dyn std::error::Error>>(())
                })
                .await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_local_multiple_sequential() -> TestResult {
            tokio::task::LocalSet::new().run_until(async {
                let storage = MemoryStorage::new();
                let (subduction, listener_fut, actor_fut) =
                    Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                        None,
                        test_signer(),
                        storage,
                        OpenPolicy,
                        NonceCache::default(),
                        CountLeadingZeroBytes,
                        ShardedMap::with_key(0, 0),
                        TokioSpawn,
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
            tokio::task::LocalSet::new()
                .run_until(async {
                    let storage = MemoryStorage::new();
                    let (subduction, listener_fut, actor_fut) =
                        Subduction::<'_, Local, _, ChannelMockConnection, _, _, _>::new(
                            None,
                            test_signer(),
                            storage,
                            OpenPolicy,
                            NonceCache::default(),
                            CountLeadingZeroBytes,
                            ShardedMap::with_key(0, 0),
                            TokioSpawn,
                        );

                    let (conn, handle) =
                        ChannelMockConnection::new_with_handle(PeerId::new([1u8; 32]));
                    subduction.register(conn).await?;

                    let actor_task = tokio::task::spawn_local(actor_fut);
                    let listener_task = tokio::task::spawn_local(listener_fut);
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    let sedimentree_id = SedimentreeId::new([99u8; 32]);

                    for i in 0..3usize {
                        let (commit, blob) =
                            make_test_commit_with_data(format!("commit {i}").as_bytes());

                        handle
                            .inbound_tx
                            .send(Message::LooseCommit {
                                id: sedimentree_id,
                                commit,
                                blob,
                            })
                            .await?;

                        tokio::time::sleep(Duration::from_millis(20)).await;

                        let count = subduction
                            .get_commits(sedimentree_id)
                            .await
                            .map_or(0, |c| c.len());
                        assert_eq!(
                            count,
                            i + 1,
                            "[LOCAL] After commit {i}, expected {} commits but found {}. DELAYED!",
                            i + 1,
                            count
                        );
                    }

                    actor_task.abort();
                    listener_task.abort();
                    Ok::<_, Box<dyn std::error::Error>>(())
                })
                .await?;
            Ok(())
        }
    }
}
