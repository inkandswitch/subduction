//! The main synchronization logic and bookkeeping for [`Sedimentree`].
//!
//! # API Guide
//!
//! ## Connection Management
//!
//! | Method | Description |
//! |--------|-------------|
//! | [`add_connection`] | Add a connection (no automatic sync) |
//! | [`disconnect`] | Graceful connection shutdown |
//! | [`disconnect_all`] | Disconnect all connections |
//! | [`disconnect_from_peer`] | Disconnect all connections from a peer |
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
//! |                        | 1 sedimentree      | all sedimentrees        |
//! |------------------------|--------------------|-------------------------|
//! | **1 peer**             | [`sync_with_peer`]      | [`full_sync_with_peer`]      |
//! | **all peers**          | [`sync_with_all_peers`] | [`full_sync_with_all_peers`] |
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
//! [`disconnect`]: Subduction::disconnect
//! [`disconnect_all`]: Subduction::disconnect_all
//! [`disconnect_from_peer`]: Subduction::disconnect_from_peer
//! [`add_connection`]: Subduction::add_connection
//! [`get_blob`]: Subduction::get_blob
//! [`get_blobs`]: Subduction::get_blobs
//! [`get_commits`]: Subduction::get_commits
//! [`fetch_blobs`]: Subduction::fetch_blobs
//! [`sync_with_peer`]: Subduction::sync_with_peer
//! [`sync_with_all_peers`]: Subduction::sync_with_all_peers
//! [`full_sync_with_peer`]: Subduction::full_sync_with_peer
//! [`full_sync_with_all_peers`]: Subduction::full_sync_with_all_peers
//! [`add_sedimentree`]: Subduction::add_sedimentree
//! [`add_commit`]: Subduction::add_commit
//! [`add_fragment`]: Subduction::add_fragment
//! [`remove_sedimentree`]: Subduction::remove_sedimentree

pub mod builder;
pub mod error;
pub mod fragment_batch_item;
pub mod listener_future;
pub mod per_peer_sync;
pub mod request;

pub mod dispatch_completion;
pub(crate) mod ingest;
pub(crate) mod peers;
pub(crate) mod spawn_guard;

use crate::{
    authenticated::Authenticated,
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::{
        Connection,
        backoff::Backoff,
        id::ConnectionId,
        managed::{CallError, ManagedCall, ManagedConnection},
        manager::{Command, ConnectionManager, RunManager},
        message::{
            BatchSyncRequest, BatchSyncResponse, DataRequestRejected, RequestedData, SyncDiff,
            SyncMessage, SyncResult, TryAsBatchSyncResponse, TryAsSubscribeRequest,
        },
        stats::{SendCount, SyncStats},
    },
    handler::Handler,
    handshake::audience::DiscoveryId,
    multiplexer::Multiplexer,
    nonce_cache::NonceCache,
    peer::{counter::PeerCounter, id::PeerId},
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    spawn::Spawn,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
    sync_session::{DynSyncSessionObserver, SyncSession, SyncSessionKind},
    timeout::{Timeout, call::CallTimeout},
};
use alloc::{collections::BTreeSet, sync::Arc, vec::Vec};
use async_channel::{Sender, bounded};
use async_lock::{Mutex, SemaphoreGuardArc};
use core::{marker::PhantomData, time::Duration};
use dispatch_completion::{DispatchCompletion, DispatchOutcome};
use error::{
    AddConnectionError, IoError, ListenError, SendRequestedDataError, Unauthorized, WriteError,
};
use fragment_batch_item::FragmentBatchItem;
use future_form::{FutureForm, Local, Sendable, future_form};
use futures::{
    FutureExt, StreamExt,
    future::try_join_all,
    stream::{AbortHandle, AbortRegistration, Abortable, FuturesUnordered},
};
use listener_future::ListenerFuture;
use nonempty::NonEmpty;
use per_peer_sync::PerPeerSync;
use request::FragmentRequested;
use sedimentree_core::{
    blob::{Blob, verified::VerifiedBlobMeta},
    codec::{decode::Decode, encode::Encode},
    collections::{
        Map, Set,
        nonempty_ext::{NonEmptyExt, RemoveResult},
    },
    crypto::{digest::Digest, fingerprint::FingerprintSeed},
    depth::{CountLeadingZeroBytes, Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintResolver, Sedimentree, minimized::MinimizedSedimentree},
};
use spawn_guard::AbortOnDrop;
use subduction_crypto::{
    signed::Signed, signer::Signer, verified_author::VerifiedAuthor, verified_meta::VerifiedMeta,
};

/// The main synchronization manager for sedimentrees.
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct Subduction<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + Clone + 'static,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    // `Clone` is required transitively (the struct derives `Clone` and stores
    // `spawner: Sp`); stating it here keeps the struct signature consistent
    // with the `Sp: Clone` bounds on `new()` / `SubductionBuilder::build()`
    // and improves error messages.
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> {
    handler: Arc<Hdl>,
    signer: Sign,
    discovery_id: Option<DiscoveryId>,

    timer: Timer,
    depth_metric: Metric,
    sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
    storage: StoragePowerbox<Store, Auth>,

    /// Active connections, keyed by peer ID.
    ///
    /// Lock ordering: this is the outer lock relative to
    /// [`multiplexers`](Self::multiplexers) and to the per-peer send
    /// counter. Code that needs `connections` plus either of those must
    /// take `connections` first and hold it while touching them; the
    /// reverse order risks deadlock. Mutating connections + multiplexers
    /// under one `connections` critical section (in [`add_connection`] and
    /// the teardown paths) keeps the "connected peer has a multiplexer,
    /// and vice versa" invariant from being observed half-applied, and
    /// clearing the send counter under the same lock keeps a concurrent
    /// reconnect from having its fresh counter reset.
    ///
    /// [`add_connection`]: Self::add_connection
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,

    /// Per-connection multiplexers for request-response correlation.
    ///
    /// Keyed by peer ID, with one [`Multiplexer`] per connection. Used by
    /// [`sync_with_peer`](Self::sync_with_peer) to make roundtrip calls
    /// and by the listen loop to route [`BatchSyncResponse`] messages.
    ///
    /// Lock ordering: inner lock relative to
    /// [`connections`](Self::connections). Read paths that need only this
    /// lock (response routing, mux lookup) may take it alone; paths that
    /// also need `connections` must take `connections` first.
    multiplexers: Arc<Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>>>,

    /// Default per-call deadline for roundtrip calls (`BatchSyncRequest` →
    /// `BatchSyncResponse`) when a caller passes `timeout: None`. This is
    /// caller-side policy applied over a cancel-safe wait; the multiplexer
    /// holds no clock. See [`DEFAULT_ROUNDTRIP_TIMEOUT`](crate::multiplexer::DEFAULT_ROUNDTRIP_TIMEOUT).
    default_roundtrip_timeout: Duration,

    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    nonce_tracker: Arc<NonceCache>,

    /// Backoff state per connection, keyed by [`ConnectionId`].
    reconnect_backoff: Arc<Mutex<Map<ConnectionId, Backoff>>>,

    /// Outgoing subscriptions: sedimentrees we're subscribed to from each peer.
    ///
    /// Used to restore subscriptions after reconnection.
    outgoing_subscriptions: Arc<Mutex<Map<PeerId, Set<SedimentreeId>>>>,

    /// Shared monotonic counter for outgoing messages, per peer.
    ///
    /// Shared with all handlers so that every message to a given peer
    /// draws from the same monotonic sequence regardless of which handler
    /// produced it.
    send_counter: PeerCounter,
    sync_session_observer: Arc<Mutex<Option<DynSyncSessionObserver>>>,

    manager_channel: Sender<Command<Authenticated<Conn, Async>>>,
    msg_queue:
        async_channel::Receiver<(Authenticated<Conn, Async>, Hdl::Message, SemaphoreGuardArc)>,
    response_queue: async_channel::Receiver<(Authenticated<Conn, Async>, Hdl::Message)>,
    connection_closed: async_channel::Receiver<(ConnectionId, Authenticated<Conn, Async>)>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    /// Runtime spawner, retained so post-construction operations (the
    /// per-document cold-start fan-out) can spawn onto the worker pool.
    /// On `Sendable` this is `tokio::spawn`; on `Local`, `spawn_local`.
    spawner: Sp,

    _phantom: core::marker::PhantomData<&'a Async>,
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS> + 'static,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
where
    Hdl::Message: From<SyncMessage>,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
    ManagedConnection<Conn, Async, Timer>: ManagedCall<
            Async,
            Hdl::Message,
            SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
        >,
{
    /// Initialize a new `Subduction` instance.
    ///
    /// The caller constructs all shared state (`sedimentrees`, `connections`,
    /// `subscriptions`, `storage`) and the `handler`
    /// externally, then passes them in. This lets the handler hold its own
    /// `Arc` clones of whatever shared state it needs.
    ///
    /// For the standard sync protocol, pass a [`SyncHandler`] constructed
    /// from the same `Arc`s.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sedimentrees = Arc::new(BoundedShardedMap::new());
    /// let connections = Arc::new(Mutex::new(Map::new()));
    /// let subscriptions = Arc::new(Mutex::new(Map::new()));
    /// let storage = StoragePowerbox::new(storage, Arc::new(policy));
    ///
    /// let handler = Arc::new(SyncHandler::new(
    ///     sedimentrees.clone(),
    ///     connections.clone(),
    ///     subscriptions.clone(),
    ///     storage.clone(),
    ///     depth_metric.clone(),
    /// ));
    ///
    /// let (sd, listener, manager) = Subduction::new(
    ///     handler,
    ///     discovery_id,
    ///     signer,
    ///     sedimentrees,
    ///     connections,
    ///     subscriptions,
    ///     storage,
    ///     nonce_cache,
    ///     depth_metric,
    ///     spawner,
    /// );
    /// ```
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn new(
        handler: Arc<Hdl>,
        discovery_id: Option<DiscoveryId>,
        signer: Sign,
        sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<Store, Auth>,
        send_counter: PeerCounter,
        nonce_cache: NonceCache,
        timer: Timer,
        default_roundtrip_timeout: Duration,
        depth_metric: Metric,
        spawner: Sp,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: StartListener<'a, Store, Conn, Hdl::Message, Hdl, Auth, Sign, Metric, SHARDS>,
        Timer: Send + Sync + 'a,
        Sp: Clone + Send + Sync + 'static,
        'a: 'static,
    {
        tracing::info!("initializing Subduction instance");

        let (manager_sender, manager_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(4096);
        let (response_sender, response_receiver) = async_channel::bounded(8192);
        let (closed_sender, closed_receiver) = async_channel::bounded(32);
        let stored_spawner = spawner.clone();
        let manager = ConnectionManager::<Async, Authenticated<Conn, Async>, Hdl::Message, Sp>::new(
            spawner,
            manager_receiver,
            queue_sender,
            response_sender,
            |msg: &Hdl::Message| msg.try_as_batch_sync_response().is_some(),
            closed_sender,
        );

        let (abort_manager_handle, abort_manager_reg) = AbortHandle::new_pair();
        let (abort_listener_handle, abort_listener_reg) = AbortHandle::new_pair();

        let sd = Arc::new(Self {
            handler,
            discovery_id,
            signer,
            timer,
            default_roundtrip_timeout,
            depth_metric,
            sedimentrees,
            connections,
            multiplexers: Arc::new(Mutex::new(Map::new())),
            subscriptions,
            storage,
            nonce_tracker: Arc::new(nonce_cache),
            reconnect_backoff: Arc::new(Mutex::new(Map::new())),
            outgoing_subscriptions: Arc::new(Mutex::new(Map::new())),
            send_counter,
            sync_session_observer: Arc::new(Mutex::new(None)),
            manager_channel: manager_sender,
            msg_queue: queue_receiver,
            response_queue: response_receiver,
            connection_closed: closed_receiver,
            abort_manager_handle,
            abort_listener_handle,
            spawner: stored_spawner,
            _phantom: PhantomData,
        });

        let manager_fut = manager.run();
        let abortable_manager = Abortable::new(manager_fut, abort_manager_reg);

        (
            sd.clone(),
            ListenerFuture::<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>::new(
                Async::start_listener(sd, abort_listener_reg),
            ),
            crate::connection::manager::ManagerFuture::new(abortable_manager),
        )
    }

    /// Get the configured discovery ID for this instance.
    ///
    /// Returns the discovery ID this server advertises, or `None` if not set.
    #[must_use]
    pub const fn discovery_id(&self) -> Option<DiscoveryId> {
        self.discovery_id
    }

    /// A method to set a [`SyncSessionObserver`] implementation.
    ///
    /// This should just be another parameter to `new` but that
    /// would be an invasive change just for a sketch.
    ///
    /// [`SyncSessionObserver`]: crate::sync_session::SyncSessionObserver
    /// [`new`]: Subduction::new
    ///
    /// # Panics
    /// Don't call this in parallel.
    pub fn set_sync_session_observer(&self, observer: DynSyncSessionObserver) {
        let Some(mut lock) = self.sync_session_observer.try_lock() else {
            unreachable!("sync session observer lock uncontended during setup")
        };
        *lock = Some(observer);
    }

    async fn emit_sync_session(&self, session: SyncSession) {
        let observer = self.sync_session_observer.lock().await.clone();
        if let Some(observer) = observer {
            observer.on_sync_session(session);
        }
    }

    /// Get a reference to the signer.
    ///
    /// Use this for signing handshake challenges/responses.
    #[must_use]
    pub const fn signer(&self) -> &Sign {
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

    /// Compute the [`Depth`] of a commit identifier under this node's
    /// [`DepthMetric`].
    ///
    /// Useful when callers (e.g., language bindings) need the per-commit
    /// boundary check — `commit_depth(id).is_boundary()` — without
    /// reimplementing the metric outside of Rust.
    #[must_use]
    pub fn commit_depth(&self, commit_id: CommitId) -> Depth {
        self.depth_metric.to_depth(commit_id)
    }

    /// Get a connection to a peer, if one exists.
    ///
    /// Returns the first available connection to the peer. Use this to get a
    /// connection once and reuse it for multiple operations, avoiding repeated
    /// lock acquisition on the connections map.
    pub async fn get_connection(&self, peer_id: &PeerId) -> Option<Authenticated<Conn, Async>> {
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
        conn: Authenticated<Conn, Async>,
    ) -> Result<(), ()> {
        tracing::info!(
            %conn_id,
            peer_id = %conn.peer_id(),
            "reconnection successful"
        );

        // Send ReAdd command to manager
        self.manager_channel
            .send(Command::ReAdd(conn_id, conn.clone(), conn.peer_id()))
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

        #[cfg(feature = "metrics")]
        crate::metrics::connection_opened();

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

    /***************
     * CONNECTIONS *
     ***************/

    /// Gracefully shut down the manager and listener loops by closing
    /// the channels they read from. Unlike [`Drop`] (which aborts mid-
    /// await), the listener drains its outstanding spawned `Handler::handle`
    /// dispatch tasks before exiting. Idempotent.
    pub fn shutdown(&self) {
        self.manager_channel.close();
        self.msg_queue.close();
    }

    /// Gracefully shut down a specific connection.
    ///
    /// # Errors
    ///
    /// * Returns `Conn::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect(
        &self,
        conn: &Authenticated<Conn, Async>,
    ) -> Result<bool, Conn::DisconnectionError> {
        let peer_id = conn.peer_id();
        tracing::info!(peer = %peer_id, "disconnecting connection from peer");

        // Detach the muxes inside the same critical section that removes
        // the connection, so a concurrent `add_connection` can't have its
        // fresh mux clobbered. Cancel them afterwards, off the lock.
        let detached = {
            let mut connections = self.connections.lock().await;
            match connections.remove(&peer_id) {
                None => return Ok(false),
                Some(peer_conns) => match peer_conns.remove_item(conn) {
                    RemoveResult::Removed(remaining) => {
                        connections.insert(peer_id, remaining);
                        None
                    }
                    RemoveResult::WasLast(_) => Some(
                        self.detach_peer_muxes_locked(&mut connections, &peer_id)
                            .await,
                    ),
                    RemoveResult::NotFound(original) => {
                        connections.insert(peer_id, original);
                        return Ok(false);
                    }
                },
            }
        };

        if let Some(muxes) = detached {
            self.teardown_peer(&peer_id, muxes, 1).await;
        } else {
            #[cfg(feature = "metrics")]
            crate::metrics::connection_closed();
        }

        conn.disconnect().await.map(|()| true)
    }

    /// Gracefully disconnect from all connections.
    ///
    /// # Errors
    ///
    /// * Returns [`Conn::DisconnectionError`] if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_all(&self) -> Result<(), Conn::DisconnectionError> {
        // Drain connections and muxes in one `connections` critical
        // section (outer before inner), then cancel off the lock. This is
        // the bulk equivalent of `teardown_peer` for every peer at once.
        let (all_conns, removed_muxes): (Vec<Authenticated<Conn, Async>>, Vec<Arc<Multiplexer>>) = {
            let mut guard = self.connections.lock().await;
            let conns = core::mem::take(&mut *guard)
                .into_values()
                .flat_map(NonEmpty::into_iter)
                .collect();
            let muxes = core::mem::take(&mut *self.multiplexers.lock().await)
                .into_values()
                .flatten()
                .collect();
            (conns, muxes)
        };

        Self::cancel_detached_muxes(removed_muxes).await;
        self.subscriptions.lock().await.clear();
        self.send_counter.clear_all().await;

        #[cfg(feature = "metrics")]
        for _ in &all_conns {
            crate::metrics::connection_closed();
        }

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
    /// * Returns `Conn::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_from_peer(
        &self,
        peer_id: &PeerId,
    ) -> Result<bool, Conn::DisconnectionError> {
        // Remove the peer's connections and detach its muxes in one
        // `connections` critical section (outer before inner) so a
        // concurrent reconnect can't be clobbered.
        let removed = {
            let mut connections = self.connections.lock().await;
            match connections.remove(peer_id) {
                Some(conns) => {
                    let muxes = self
                        .detach_peer_muxes_locked(&mut connections, peer_id)
                        .await;
                    Some((conns, muxes))
                }
                None => None,
            }
        };

        if let Some((conns, muxes)) = removed {
            // Removing every connection is by definition removing the last.
            self.teardown_peer(peer_id, muxes, conns.len()).await;

            for conn in conns {
                if let Err(e) = conn.disconnect().await {
                    tracing::error!(peer = %peer_id, error = %e, "failed to disconnect connection");
                    return Err(e);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Add a connection to tracking.
    ///
    /// This does not perform any synchronization. To sync after adding,
    /// call [`full_sync_with_peer`](Self::full_sync_with_peer).
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if the connection is fresh (first for this peer or new connection).
    /// * `Ok(false)` if the exact connection was already added.
    ///
    /// # Errors
    ///
    /// * Returns `ConnectionDisallowed` if the connection is not allowed by the policy.
    pub async fn add_connection(
        &self,
        conn: Authenticated<Conn, Async>,
    ) -> Result<bool, AddConnectionError<Auth::ConnectionDisallowed>> {
        let peer_id = conn.peer_id();
        tracing::info!(peer = %peer_id, "adding connection from peer");

        self.storage
            .policy()
            .authorize_connect(peer_id)
            .await
            .map_err(AddConnectionError::ConnectionDisallowed)?;

        {
            // Insert the connection and create its mux under one
            // `connections` critical section (outer before inner) so the
            // connection/mux invariant is never observed half-applied.
            let mut connections = self.connections.lock().await;

            if connections
                .get(&peer_id)
                .is_some_and(|peer_conns| peer_conns.iter().any(|c| c == &conn))
            {
                return Ok(false);
            }

            match connections.get_mut(&peer_id) {
                Some(peer_conns) => {
                    peer_conns.push(conn.clone());
                }
                None => {
                    connections.insert(peer_id, NonEmpty::new(conn.clone()));
                }
            }

            let mux = Arc::new(Multiplexer::new(peer_id, self.default_roundtrip_timeout));
            let mut multiplexers = self.multiplexers.lock().await;
            match multiplexers.get_mut(&peer_id) {
                Some(muxes) => muxes.push(mux),
                None => {
                    multiplexers.insert(peer_id, alloc::vec![mux]);
                }
            }
        }

        self.manager_channel
            .send(Command::Add(conn, peer_id))
            .await
            .map_err(|_| AddConnectionError::SendToClosedChannel)?;

        #[cfg(feature = "metrics")]
        crate::metrics::connection_opened();

        Ok(true)
    }

    /// Reconcile tracking for a connection whose transport has **already
    /// failed** — e.g. a send returned `Err`, or the per-connection
    /// receive loop exited and reported the closure via the
    /// `connection_closed` channel. There is no live transport to close,
    /// so this only updates in-memory state (connection map, multiplexers,
    /// send counter, subscriptions, metrics).
    ///
    /// To proactively shut down a connection you believe is still live,
    /// use [`disconnect`](Self::disconnect), which also closes the
    /// transport via `Connection::disconnect`.
    ///
    /// Returns `Some(true)` if this was the last connection for the peer,
    /// `Some(false)` if the peer still has connections,
    /// `None` if the connection wasn't found.
    ///
    /// Crate-internal: driven by the listen loop and broadcast-failure
    /// paths. Tests reach it via
    /// [`remove_connection_for_test`](Self::remove_connection_for_test).
    pub(crate) async fn remove_connection(
        &self,
        conn: &Authenticated<Conn, Async>,
    ) -> Option<bool> {
        let peer_id = conn.peer_id();

        let detached = {
            let mut connections = self.connections.lock().await;
            match connections.remove(&peer_id) {
                None => return None,
                Some(peer_conns) => match peer_conns.remove_item(conn) {
                    RemoveResult::Removed(remaining) => {
                        connections.insert(peer_id, remaining);
                        None
                    }
                    RemoveResult::WasLast(_) => Some(
                        self.detach_peer_muxes_locked(&mut connections, &peer_id)
                            .await,
                    ),
                    RemoveResult::NotFound(original) => {
                        connections.insert(peer_id, original);
                        return None;
                    }
                },
            }
        };

        if let Some(muxes) = detached {
            self.teardown_peer(&peer_id, muxes, 1).await;
            Some(true)
        } else {
            #[cfg(feature = "metrics")]
            crate::metrics::connection_closed();
            Some(false)
        }
    }

    /// Test-only public access to the crate-internal
    /// [`remove_connection`](Self::remove_connection) teardown path, so
    /// integration tests can exercise it directly.
    #[cfg(any(feature = "test_utils", test))]
    pub async fn remove_connection_for_test(
        &self,
        conn: &Authenticated<Conn, Async>,
    ) -> Option<bool> {
        self.remove_connection(conn).await
    }

    /// Remove a peer's multiplexers from the map and return them for the
    /// caller to cancel after releasing the lock.
    ///
    /// Takes the held `connections` guard by `&mut` so the mux removal
    /// happens inside the caller's `connections` critical section; this is
    /// what keeps a concurrent [`add_connection`](Self::add_connection)
    /// from having its fresh mux clobbered. Cancellation is left to the
    /// caller via [`cancel_detached_muxes`](Self::cancel_detached_muxes)
    /// because its `.await` must not be held across the `connections` lock.
    async fn detach_peer_muxes_locked(
        &self,
        _connections_guard: &mut Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>,
        peer_id: &PeerId,
    ) -> Vec<Arc<Multiplexer>> {
        self.multiplexers
            .lock()
            .await
            .remove(peer_id)
            .unwrap_or_default()
    }

    /// Cancel the pending calls on a set of detached multiplexers.
    ///
    /// Resolves any awaiting `sync_with_*` callers with
    /// [`CallError::ResponseDropped`]
    /// immediately rather than letting them wait out the per-call timeout.
    /// Call this *after* the `connections` lock has been released (see
    /// [`detach_peer_muxes_locked`](Self::detach_peer_muxes_locked)).
    async fn cancel_detached_muxes(muxes: Vec<Arc<Multiplexer>>) {
        for mux in muxes {
            mux.cancel_all_pending().await;
        }
    }

    /// Finish per-peer teardown after the peer's last connection is gone
    /// and its muxes have been detached via
    /// [`detach_peer_muxes_locked`](Self::detach_peer_muxes_locked).
    ///
    /// Shared post-lock cleanup for [`disconnect`](Self::disconnect) and
    /// [`disconnect_from_peer`](Self::disconnect_from_peer) so they clean
    /// up the same state in the same order. Emits `conn_count`
    /// `connection_closed` metrics.
    async fn teardown_peer(
        &self,
        peer_id: &PeerId,
        muxes: Vec<Arc<Multiplexer>>,
        conn_count: usize,
    ) {
        Self::cancel_detached_muxes(muxes).await;
        peers::remove_peer_from_subscriptions(&self.subscriptions, *peer_id).await;

        // Clear the send counter while holding the `connections` lock and
        // only if the peer is still gone. The caller removed the peer
        // under the lock, but released it before calling us, so a
        // concurrent `add_connection` could have re-added the peer (and
        // started stamping messages with a fresh counter). `clear_peer`
        // is contracted for fully-gone peers only; resetting it
        // mid-session would break the strictly-increasing guarantee.
        {
            let connections = self.connections.lock().await;
            if !connections.contains_key(peer_id) {
                self.send_counter.clear_peer(peer_id).await;
            }
        }

        #[cfg(feature = "metrics")]
        for _ in 0..conn_count {
            crate::metrics::connection_closed();
        }

        #[cfg(not(feature = "metrics"))]
        let _ = conn_count;
    }

    /// Get all connections as a flat list.
    ///
    /// This is useful for iterating over all connections to send messages.
    async fn all_connections(&self) -> Vec<Authenticated<Conn, Async>> {
        self.connections
            .lock()
            .await
            .values()
            .flat_map(|ne| ne.iter().cloned())
            .collect()
    }

    /// Add a subscription for a peer to a sedimentree.
    pub(crate) async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
        peers::add_subscription(&self.subscriptions, peer_id, sedimentree_id).await;
    }

    /// Get connections for subscribers authorized to receive updates for a sedimentree.
    ///
    /// This is used when forwarding updates: we only send to subscribers who have Pull access.
    async fn get_authorized_subscriber_conns(
        &self,
        sedimentree_id: SedimentreeId,
        exclude_peer: &PeerId,
    ) -> Vec<Authenticated<Conn, Async>> {
        peers::get_authorized_subscriber_conns(
            &self.subscriptions,
            &self.storage,
            &self.connections,
            sedimentree_id,
            exclude_peer,
        )
        .await
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
    /// * Returns `Store::Error` if the storage backend encounters an error.
    pub async fn get_blob(
        &self,
        id: SedimentreeId,
        digest: Digest<Blob>,
    ) -> Result<Option<Blob>, Store::Error> {
        tracing::debug!(?id, ?digest, "Looking for blob");
        ingest::get_blob(&self.storage, id, digest).await
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
    /// * Returns `Store::Error` if the storage backend encounters an error.
    pub async fn get_blobs(
        &self,
        id: SedimentreeId,
    ) -> Result<Option<NonEmpty<Blob>>, Store::Error> {
        tracing::debug!(tree = ?id, "getting local blobs for sedimentree");

        // Read blobs straight from durable storage. Storage is the source of
        // truth, so we do not gate on in-RAM cache residency (an evicted tree
        // must still return its blobs). `None` ⇔ storage holds nothing.
        let local_access = self.storage.hydration_access();
        let mut results = Vec::new();

        // With compound storage, blobs are stored with their commits/fragments
        for verified in local_access.load_loose_commits::<Async>(id).await? {
            results.push(verified.blob().clone());
        }

        for verified in local_access.load_fragments::<Async>(id).await? {
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
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    pub async fn fetch_blobs(
        &self,
        id: SedimentreeId,
        timeout: CallTimeout,
    ) -> Result<Option<NonEmpty<Blob>>, IoError<Async, Store, Conn, Hdl::Message>> {
        // Resolve caller policy to the low-level deadline (`None` = uncapped).
        let timeout = timeout.resolve(self.default_roundtrip_timeout);

        tracing::debug!(tree = ?id, "fetching blobs for sedimentree");
        if let Some(maybe_blobs) = self.get_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            // No blobs locally. Hydrate (cache or storage) to decide whether
            // the tree exists; only then ask peers for its data. The hydrated
            // tree is already minimized for wire use.
            let tree = self.get_or_hydrate(id).await?;
            if let Some(tree) = tree {
                let conns = self.all_connections().await;
                for conn in conns {
                    let peer_id = conn.peer_id();
                    let seed = FingerprintSeed::random();
                    let resolver = tree.fingerprint_resolver(&seed);

                    // Mux missing means the peer was torn down between the
                    // connection snapshot and here; skip rather than panic.
                    let Some(mux) = ({
                        let muxes = self.multiplexers.lock().await;
                        muxes.get(&peer_id).and_then(|v| v.first()).cloned()
                    }) else {
                        tracing::debug!(peer = %peer_id, "multiplexer for peer gone (concurrent teardown); skipping");
                        continue;
                    };
                    let managed = ManagedConnection::new(conn.clone(), mux, self.timer.clone());
                    let req_id = managed.next_request_id();
                    let BatchSyncResponse {
                        id,
                        result,
                        req_id: resp_batch_id,
                        ..
                    } = ManagedCall::<Async, Hdl::Message>::call(
                        &managed,
                        BatchSyncRequest {
                            id,
                            req_id,
                            fingerprint_summary: resolver.summary().clone(),
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
                            tracing::debug!(peer = %conn.peer_id(), tree = ?id, "peer reports sedimentree not found");
                            continue;
                        }
                        SyncResult::Unauthorized => {
                            tracing::debug!(peer = %conn.peer_id(), tree = ?id, "peer reports we are unauthorized for sedimentree");
                            continue;
                        }
                    };

                    // Send back data the responder requested (bidirectional sync)
                    if !diff.requesting.is_empty()
                        && let Err(e) = self
                            .send_requested_data(&conn, id, &resolver, &diff.requesting)
                            .await
                    {
                        if matches!(e, SendRequestedDataError::Unauthorized(_)) {
                            let msg: Hdl::Message =
                                SyncMessage::from(DataRequestRejected { id }).into();
                            if let Err(send_err) = conn.send(&msg).await {
                                tracing::info!(
                                    peer = %conn.peer_id(),
                                    error = %send_err,
                                    "peer disconnected while sending DataRequestRejected"
                                );
                            }
                        }
                        tracing::warn!(
                            peer = %conn.peer_id(),
                            error = %e,
                            "failed to send requested data to peer"
                        );
                    }

                    if let Err(e) = self
                        .recv_batch_sync_response(&conn.peer_id(), id, diff)
                        .await
                    {
                        tracing::error!(
                            peer = %conn.peer_id(),
                            error = %e,
                            "error handling batch sync response from peer"
                        );
                    }
                }
            }

            let updated = self.get_blobs(id).await.map_err(IoError::Storage)?;

            Ok(updated)
        }
    }

    /// Remove a sedimentree locally and delete all associated data from storage.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn remove_sedimentree(
        &self,
        id: SedimentreeId,
    ) -> Result<(), IoError<Async, Store, Conn, Hdl::Message>> {
        // Drop the in-RAM cache entry (if resident) and delete from durable
        // storage unconditionally. We must NOT gate storage deletion on RAM
        // residency: with the LRU cache, an existing tree may have been
        // evicted from memory, yet still needs deleting from disk.
        self.sedimentrees.remove(&id).await;

        let destroyer = self.storage.local_destroyer(id);

        // With compound storage, deleting commits/fragments also deletes their blobs.
        destroyer
            .delete_loose_commits()
            .await
            .map_err(IoError::Storage)?;

        destroyer
            .delete_fragments()
            .await
            .map_err(IoError::Storage)?;

        // Existence is recorded by the id index; remove it so the tree no
        // longer appears in `sedimentree_ids` / enumeration.
        destroyer
            .delete_sedimentree_id()
            .await
            .map_err(IoError::Storage)?;

        Ok(())
    }

    /***********************
     * INCREMENTAL CHANGES *
     ***********************/

    /// Persist a new (incremental) commit locally — **no network**.
    ///
    /// The persistence half of [`add_commit`](Self::add_commit): constructs
    /// and signs the commit, inserts it into local storage and the
    /// in-memory tree, and re-minimizes. It never contacts peers, so it
    /// returns as soon as the write is durable. Propagate later via
    /// [`add_commit`](Self::add_commit) (which pushes the delta) or a sync.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` if the commit is not on a fragment boundary.
    /// * `Ok(Some(FragmentRequested))` if the commit is on a [`Fragment`]
    ///   boundary — create the requested fragment and call
    ///   [`store_fragment`](Self::store_fragment) / [`add_fragment`](Self::add_fragment).
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a storage error occurs.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes.
    pub async fn store_commit(
        &self,
        id: SedimentreeId,
        head: CommitId,
        parents: BTreeSet<CommitId>,
        blob: Blob,
    ) -> Result<
        Option<FragmentRequested>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        let putter = self.storage.local_putter::<Async>(id);

        let verified_blob = VerifiedBlobMeta::new(blob);
        let verified_meta: VerifiedMeta<LooseCommit> =
            VerifiedMeta::seal::<Async, _>(&self.signer, (id, head, parents), verified_blob).await;
        let commit_head = verified_meta.payload().head();

        self.insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.minimize_tree(id).await;

        let depth = self.depth_metric.to_depth(commit_head);
        Ok(depth
            .is_boundary()
            .then(|| FragmentRequested::new(commit_head, depth)))
    }

    /// Add a new (incremental) commit locally and propagate it to all connected peers.
    ///
    /// The commit is constructed internally from the provided parts, ensuring
    /// that the blob metadata is computed correctly from the blob.
    ///
    /// This is the store+propagate combinator for a single commit;
    /// propagation is a best-effort push (`Connection::send`) to authorized
    /// subscribers, so it does not block on peer acks. For a durable write
    /// with no propagation, use [`store_commit`](Self::store_commit).
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
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_commit(
        &self,
        id: SedimentreeId,
        head: CommitId,
        parents: BTreeSet<CommitId>,
        blob: Blob,
    ) -> Result<
        Option<FragmentRequested>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        let self_id = self.peer_id();
        let putter = self.storage.local_putter::<Async>(id);

        let verified_blob = VerifiedBlobMeta::new(blob);
        let verified_meta: VerifiedMeta<LooseCommit> =
            VerifiedMeta::seal::<Async, _>(&self.signer, (id, head, parents), verified_blob).await;

        let commit_head = verified_meta.payload().head();
        tracing::debug!(commit = ?commit_head, tree = ?id, "adding commit to sedimentree");

        let signed_for_wire = verified_meta.signed().clone();
        let blob = verified_meta.blob().clone();

        self.insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.minimize_tree(id).await;

        // Read the just-written tree's heads via the cache (counts as an
        // access — appropriate, the tree was just written so it is hot).
        let heads = self
            .sedimentrees
            .get_cloned(&id)
            .await
            .map(|mut s| s.heads(&self.depth_metric))
            .unwrap_or_default();
        {
            let conns = {
                let subscriber_conns = self.get_authorized_subscriber_conns(id, &self_id).await;
                if subscriber_conns.is_empty() {
                    tracing::debug!(tree = ?id, "no subscribers for sedimentree, broadcasting to all connections");
                    self.all_connections().await
                } else {
                    subscriber_conns
                }
            };

            for conn in conns {
                let peer_id = conn.peer_id();
                tracing::debug!(tree = ?id, peer = %peer_id, "propagating commit for sedimentree");

                let msg: Hdl::Message = SyncMessage::LooseCommit {
                    id,
                    commit: signed_for_wire.clone(),
                    blob: blob.clone(),
                    sender_heads: RemoteHeads {
                        counter: self.send_counter.next(peer_id).await,
                        heads: heads.clone(),
                    },
                }
                .into();

                if let Err(e) = conn.send(&msg).await {
                    tracing::warn!(
                        peer = %peer_id,
                        error = %IoError::<Async, Store, Conn, Hdl::Message>::ConnSend(e),
                        "peer disconnected"
                    );
                    // No prune: a failed send means the transport is closed, so
                    // the read loop's canonical teardown removes it. Pruning here
                    // would skip the `on_peer_disconnect` hook.
                }
            }
        }

        let mut maybe_requested_fragment = None;
        let depth = self.depth_metric.to_depth(commit_head);
        if depth.is_boundary() {
            maybe_requested_fragment = Some(FragmentRequested::new(commit_head, depth));
        }

        Ok(maybe_requested_fragment)
    }

    /// Persist a new (incremental) fragment locally — **no network**.
    ///
    /// The persistence half of [`add_fragment`](Self::add_fragment):
    /// constructs and signs the fragment, inserts it into local storage and
    /// the in-memory tree, and re-minimizes. It never contacts peers, so it
    /// returns as soon as the write is durable. Propagate later via
    /// [`add_fragment`](Self::add_fragment) or a sync.
    ///
    /// NOTE this performs no integrity checks; we assume this is a good
    /// fragment at the right depth.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a storage error occurs.
    pub async fn store_fragment(
        &self,
        id: SedimentreeId,
        head: CommitId,
        boundary: BTreeSet<CommitId>,
        checkpoints: &[CommitId],
        blob: Blob,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        let verified_blob = VerifiedBlobMeta::new(blob);
        let putter = self.storage.local_putter::<Async>(id);

        let verified_meta: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Async, _>(
            &self.signer,
            (id, head, boundary, checkpoints.to_vec()),
            verified_blob,
        )
        .await;

        self.insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.minimize_tree(id).await;

        Ok(())
    }

    /// Add a new (incremental) fragment locally and propagate it to all connected peers.
    ///
    /// The fragment is constructed internally from the provided parts, ensuring
    /// that the blob metadata is computed correctly from the blob.
    ///
    /// This is the store+propagate combinator for a single fragment;
    /// propagation is a best-effort push (`Connection::send`) to authorized
    /// subscribers, so it does not block on peer acks. For a durable write
    /// with no propagation, use [`store_fragment`](Self::store_fragment).
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
        head: CommitId,
        boundary: BTreeSet<CommitId>,
        checkpoints: &[CommitId],
        blob: Blob,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        let verified_blob = VerifiedBlobMeta::new(blob);

        let self_id = self.peer_id();
        let putter = self.storage.local_putter::<Async>(id);

        let verified_meta: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Async, _>(
            &self.signer,
            (id, head, boundary, checkpoints.to_vec()),
            verified_blob,
        )
        .await;
        let fragment_digest = Digest::hash(verified_meta.payload());

        tracing::debug!(digest = ?fragment_digest, tree = ?id, "adding fragment to sedimentree");
        let signed_for_wire = verified_meta.signed().clone();
        let blob = verified_meta.blob().clone();

        self.insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.minimize_tree(id).await;

        // Read the just-written tree's heads via the cache (counts as an
        // access — the tree was just written so it is hot).
        let heads = self
            .sedimentrees
            .get_cloned(&id)
            .await
            .map(|mut s| s.heads(&self.depth_metric))
            .unwrap_or_default();

        let conns = {
            let subscriber_conns = self.get_authorized_subscriber_conns(id, &self_id).await;
            if subscriber_conns.is_empty() {
                tracing::debug!(tree = ?id, "no subscribers for sedimentree, broadcasting fragment to all connections");
                self.all_connections().await
            } else {
                subscriber_conns
            }
        };

        for conn in conns {
            let peer_id = conn.peer_id();
            tracing::debug!(digest = ?fragment_digest, tree = ?id, peer = %peer_id, "propagating fragment for sedimentree");

            let msg: Hdl::Message = SyncMessage::Fragment {
                id,
                fragment: signed_for_wire.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads {
                    counter: self.send_counter.next(peer_id).await,
                    heads: heads.clone(),
                },
            }
            .into();

            if let Err(e) = conn.send(&msg).await {
                tracing::warn!(
                    peer = %peer_id,
                    error = %IoError::<Async, Store, Conn, Hdl::Message>::ConnSend(e),
                    "peer disconnected"
                );
                // No prune: a failed send means the transport is closed, so
                // the read loop's canonical teardown removes it. Pruning here
                // would skip the `on_peer_disconnect` hook.
            }
        }

        Ok(())
    }

    // ── Batch / Bulk Ingestion ──────────────────────────────────────────

    /// Bulk-insert commits without per-commit minimization or broadcasting.
    ///
    /// Unlike [`add_commit`](Self::add_commit), which calls `minimize_tree`
    /// and broadcasts to peers after _every_ commit (O(n^2) for n commits),
    /// this method inserts all commits first and runs `minimize_tree` once
    /// at the end.
    ///
    /// No messages are broadcast to peers — use
    /// [`sync_with_peer`](Self::sync_with_peer) afterward to propagate.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a storage error occurs.
    ///   On non-transactional [`Storage`](crate::storage::traits::Storage)
    ///   backends `save_batch` may have persisted some commits before
    ///   surfacing an error; this method then returns early and leaves the
    ///   in-memory tree behind the on-disk state until rehydrate.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn store_commits_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Blob)>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if commits.is_empty() {
            return Ok(());
        }

        let putter = self.storage.local_putter::<Async>(id);
        let count = commits.len();
        tracing::info!(count, tree = ?id, "bulk-inserting commits into sedimentree");

        // Sign first; defer storage I/O so we can flush via a single
        // `save_batch` call instead of N `save_commit` round trips.
        let mut verified_commits: Vec<VerifiedMeta<LooseCommit>> = Vec::with_capacity(count);
        let mut commit_payloads: Vec<LooseCommit> = Vec::with_capacity(count);
        for (head, parents, blob) in commits {
            let verified_blob = VerifiedBlobMeta::new(blob);
            let verified_meta: VerifiedMeta<LooseCommit> =
                VerifiedMeta::seal::<Async, _>(&self.signer, (id, head, parents), verified_blob)
                    .await;
            commit_payloads.push(verified_meta.payload().clone());
            verified_commits.push(verified_meta);
        }

        putter
            .save_batch(verified_commits, Vec::new())
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.with_tree_hydrated(id, move |tree| {
            for commit in commit_payloads {
                tree.add_commit(commit);
            }
        })
        .await
        .map_err(WriteError::Io)?;
        self.minimize_tree(id).await;

        tracing::info!(count, "bulk-insert of commits complete, tree minimized");
        Ok(())
    }

    /// Bulk-insert fragments without per-fragment minimization or broadcasting.
    ///
    /// Unlike [`add_fragment`](Self::add_fragment), which calls `minimize_tree`
    /// and broadcasts to peers after _every_ fragment, this method inserts all
    /// fragments first and runs `minimize_tree` once at the end.
    ///
    /// No messages are broadcast to peers — use
    /// [`sync_with_peer`](Self::sync_with_peer) afterward to propagate.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a storage error occurs.
    ///   On non-transactional [`Storage`](crate::storage::traits::Storage)
    ///   backends `save_batch` may have persisted some fragments before
    ///   surfacing an error; this method then returns early and leaves the
    ///   in-memory tree behind the on-disk state until rehydrate.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn store_fragments_batch(
        &self,
        id: SedimentreeId,
        fragments: Vec<FragmentBatchItem>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if fragments.is_empty() {
            return Ok(());
        }

        let putter = self.storage.local_putter::<Async>(id);
        let count = fragments.len();
        tracing::info!(count, tree = ?id, "bulk-inserting fragments into sedimentree");

        // Sign first; defer storage I/O so we can flush via a single
        // `save_batch` call instead of N `save_fragment` round trips.
        let mut verified_fragments: Vec<VerifiedMeta<Fragment>> = Vec::with_capacity(count);
        let mut fragment_payloads: Vec<Fragment> = Vec::with_capacity(count);
        for item in fragments {
            let FragmentBatchItem {
                head,
                boundary,
                checkpoints,
                blob,
            } = item;
            let verified_blob = VerifiedBlobMeta::new(blob);
            let verified_meta: VerifiedMeta<Fragment> = VerifiedMeta::seal::<Async, _>(
                &self.signer,
                (id, head, boundary, checkpoints),
                verified_blob,
            )
            .await;
            fragment_payloads.push(verified_meta.payload().clone());
            verified_fragments.push(verified_meta);
        }

        putter
            .save_batch(Vec::new(), verified_fragments)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.with_tree_hydrated(id, move |tree| {
            for fragment in fragment_payloads {
                tree.add_fragment(fragment);
            }
        })
        .await
        .map_err(WriteError::Io)?;
        self.minimize_tree(id).await;
        tracing::info!(count, "bulk-insert of fragments complete, tree minimized");
        Ok(())
    }

    /// Persist already-built [`LooseCommit`] and [`Fragment`] payloads
    /// alongside their blobs to local storage and the in-memory tree —
    /// **no network**.
    ///
    /// The persistence half of [`add_built_batch`](Self::add_built_batch):
    /// it signs each payload, flushes to storage in a single `save_batch`
    /// call, applies the in-memory updates under one shard lock, and
    /// minimizes once. It never contacts peers, so it returns the instant
    /// the write is durable — a wedged peer cannot stall it. Pair with
    /// [`sync_with_all_peers`](Self::sync_with_all_peers) (or let the host
    /// drive it in the background) to propagate.
    ///
    /// Commits are inserted before fragments. Passing two empty vectors is a
    /// no-op (no minimize).
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a storage error occurs.
    ///   On non-transactional [`Storage`](crate::storage::traits::Storage)
    ///   backends `save_batch` may have persisted some items before
    ///   surfacing an error; this method then returns early and leaves the
    ///   in-memory tree behind the on-disk state until rehydrate.
    /// * [`WriteError::Io`] (`IoError::BlobMismatch`) if any provided blob's
    ///   digest does not match its payload's claimed
    ///   [`BlobMeta`](sedimentree_core::blob::BlobMeta).
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn store_built_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if commits.is_empty() && fragments.is_empty() {
            return Ok(());
        }

        let putter = self.storage.local_putter::<Async>(id);
        let commit_count = commits.len();
        let fragment_count = fragments.len();
        tracing::info!(
            commit_count,
            fragment_count,
            tree = ?id,
            "bulk-inserting commits and fragments into sedimentree (no broadcast)"
        );

        // Sign every commit and fragment first; defer all storage I/O so we
        // can hit the adapter exactly once via `save_batch`.
        let mut verified_commits: Vec<VerifiedMeta<LooseCommit>> = Vec::with_capacity(commit_count);
        let mut commit_payloads: Vec<LooseCommit> = Vec::with_capacity(commit_count);
        for (commit, blob) in commits {
            let verified_sig = Signed::seal::<Async, _>(&self.signer, commit).await;
            let verified_meta = VerifiedMeta::new(verified_sig, blob)
                .map_err(|e| WriteError::Io(IoError::BlobMismatch(e)))?;
            commit_payloads.push(verified_meta.payload().clone());
            verified_commits.push(verified_meta);
        }

        let mut verified_fragments: Vec<VerifiedMeta<Fragment>> =
            Vec::with_capacity(fragment_count);
        let mut fragment_payloads: Vec<Fragment> = Vec::with_capacity(fragment_count);
        for (fragment, blob) in fragments {
            let verified_sig = Signed::seal::<Async, _>(&self.signer, fragment).await;
            let verified_meta = VerifiedMeta::new(verified_sig, blob)
                .map_err(|e| WriteError::Io(IoError::BlobMismatch(e)))?;
            fragment_payloads.push(verified_meta.payload().clone());
            verified_fragments.push(verified_meta);
        }

        putter
            .save_batch(verified_commits, verified_fragments)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.with_tree_hydrated(id, move |tree| {
            for commit in commit_payloads {
                tree.add_commit(commit);
            }
            for fragment in fragment_payloads {
                tree.add_fragment(fragment);
            }
        })
        .await
        .map_err(WriteError::Io)?;
        self.minimize_tree(id).await;

        tracing::info!(
            commit_count,
            fragment_count,
            "bulk-insert of commits and fragments complete, tree minimized"
        );
        Ok(())
    }

    /// Handle receiving a batch sync response from a peer.
    ///
    /// Ingests all commits and fragments from the diff, then re-minimizes
    /// the in-memory sedimentree to maintain the minimal covering invariant.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs while inserting commits or fragments.
    pub async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: SyncDiff,
    ) -> Result<(), IoError<Async, Store, Conn, Hdl::Message>> {
        ingest::recv_batch_sync_response(&self.sedimentrees, &self.storage, from, id, diff).await?;
        self.minimize_tree(id).await;
        Ok(())
    }

    /// Persist a whole sedimentree (its commits and fragments, paired with
    /// `blobs`) to local storage and the in-memory tree — **no network**.
    ///
    /// The persistence half of [`add_sedimentree`](Self::add_sedimentree):
    /// it signs each item, pairs it with its blob, and flushes locally. It
    /// never contacts peers, so it returns as soon as the write is durable.
    /// Pair with [`sync_with_all_peers`](Self::sync_with_all_peers) to
    /// propagate.
    ///
    /// # Errors
    ///
    /// * [`WriteError::MissingBlob`] if a commit/fragment references a blob
    ///   not present in `blobs`.
    /// * [`WriteError::Io`] if a storage error occurs.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn store_sedimentree(
        &self,
        id: SedimentreeId,
        sedimentree: Sedimentree,
        blobs: Vec<Blob>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        use sedimentree_core::collections::Map;

        let putter = self.storage.local_putter::<Async>(id);

        // Index blobs by digest for matching with commits/fragments
        let blobs_by_digest: Map<Digest<Blob>, Blob> =
            blobs.into_iter().map(|b| (Digest::hash(&b), b)).collect();

        // Sign commits and pair with their blobs
        let mut verified_commits = Vec::with_capacity(sedimentree.loose_commits().count());
        for commit in sedimentree.loose_commits() {
            let blob_digest = commit.blob_meta().digest();
            let blob = blobs_by_digest
                .get(&blob_digest)
                .cloned()
                .ok_or_else(|| WriteError::MissingBlob(blob_digest))?;
            let verified_sig = Signed::seal::<Async, _>(&self.signer, commit.clone()).await;
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
            let verified_sig = Signed::seal::<Async, _>(&self.signer, fragment.clone()).await;
            let verified_meta = VerifiedMeta::new(verified_sig, blob)
                .map_err(|e| WriteError::Io(IoError::BlobMismatch(e)))?;
            verified_fragments.push(verified_meta);
        }

        self.insert_sedimentree_locally(&putter, verified_commits, verified_fragments)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        Ok(())
    }

    /// Persist a whole sedimentree **and** propagate it to peers — the
    /// convenience combinator of
    /// [`store_sedimentree`](Self::store_sedimentree) +
    /// [`sync_with_all_peers`](Self::sync_with_all_peers).
    ///
    /// Awaits the broadcast and returns its per-peer outcome
    /// ([`PerPeerSync`]); a wedged peer can stall this for up to `timeout`
    /// ([`CallTimeout::Default`] = configured `roundtrip_timeout`). For a
    /// durable write that does not wait on peers, use
    /// [`store_sedimentree`](Self::store_sedimentree) and drive sync
    /// separately.
    ///
    /// # Errors
    ///
    /// * [`WriteError::MissingBlob`] / [`WriteError::Io`] as for
    ///   [`store_sedimentree`](Self::store_sedimentree). Per-peer transport
    ///   failures are reported in the returned map, not as `Err`.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes.
    pub async fn add_sedimentree(
        &self,
        id: SedimentreeId,
        sedimentree: Sedimentree,
        blobs: Vec<Blob>,
        timeout: CallTimeout,
    ) -> Result<
        PerPeerSync<Conn, Async, <Conn as Connection<Async, Hdl::Message>>::SendError>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        self.store_sedimentree(id, sedimentree, blobs).await?;
        let per_peer = self
            .sync_with_all_peers(id, true, timeout)
            .await
            .map_err(WriteError::Io)?;
        Ok(per_peer)
    }

    /// Persist already-built [`LooseCommit`] and [`Fragment`] payloads
    /// **and** propagate them to peers — the convenience combinator of
    /// [`store_built_batch`](Self::store_built_batch) +
    /// [`sync_with_all_peers`](Self::sync_with_all_peers).
    ///
    /// Used by callers that already have payloads in their canonical,
    /// post-truncation form (notably the Wasm bindings, which carry
    /// [`Fragment`] values whose checkpoints are already truncated) and
    /// want both phases in one call.
    ///
    /// This **awaits the broadcast** and returns its per-peer outcome
    /// ([`PerPeerSync`]). A byte-connected but protocol-unresponsive peer
    /// can therefore stall this call for up to `timeout` (or the
    /// configured `roundtrip_timeout` for [`CallTimeout::Default`]). Callers that need
    /// the durable write to return *without* waiting on peers should call
    /// [`store_built_batch`](Self::store_built_batch) and drive
    /// [`sync_with_all_peers`](Self::sync_with_all_peers) separately (or let
    /// the host run it in the background).
    ///
    /// Commits are inserted before fragments. Passing two empty vectors is a
    /// no-op (no minimize, no broadcast) and yields an empty map.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a local storage error
    ///   occurs while persisting the batch, or if ingestion of inbound data
    ///   during the trailing [`sync_with_all_peers`](Self::sync_with_all_peers)
    ///   call hits a storage error. On non-transactional
    ///   [`Storage`](crate::storage::traits::Storage) backends `save_batch`
    ///   may have persisted some items before surfacing an error; in that
    ///   case this method returns early and leaves the in-memory tree
    ///   behind the on-disk state until rehydrate.
    /// * [`WriteError::Io`] (`IoError::BlobMismatch`) if any provided blob's
    ///   digest does not match its payload's claimed
    ///   [`BlobMeta`](sedimentree_core::blob::BlobMeta).
    ///
    /// Per-peer transport failures during the broadcast are *not* surfaced as
    /// `Err`; they appear in the returned [`PerPeerSync`] map (per-peer
    /// success flag + per-connection
    /// [`CallError`]s).
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_built_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
        timeout: CallTimeout,
    ) -> Result<
        PerPeerSync<Conn, Async, <Conn as Connection<Async, Hdl::Message>>::SendError>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        if commits.is_empty() && fragments.is_empty() {
            return Ok(PerPeerSync::default());
        }

        // Storage write first (cancel-safety: storage is the source of truth;
        // a cancel between this and the broadcast self-heals on rehydrate).
        self.store_built_batch(id, commits, fragments).await?;
        let per_peer = self
            .sync_with_all_peers(id, true, timeout)
            .await
            .map_err(WriteError::Io)?;
        Ok(per_peer)
    }

    /// Bulk-insert commits **and** propagate them to peers — the convenience
    /// combinator of [`store_commits_batch`](Self::store_commits_batch) +
    /// [`sync_with_all_peers`](Self::sync_with_all_peers).
    ///
    /// Prefer this over looping [`add_commit`](Self::add_commit): the batch
    /// path runs a single `save_batch` and one `minimize`, then a single
    /// broadcast, instead of re-minimizing and pushing per commit.
    ///
    /// This **awaits the broadcast** and returns its per-peer outcome
    /// ([`PerPeerSync`]); a byte-connected but protocol-unresponsive peer can
    /// stall the call for up to `timeout` (or the configured
    /// `roundtrip_timeout` for [`CallTimeout::Default`]). Callers that want the
    /// durable write to return *without* waiting on peers should call
    /// [`store_commits_batch`](Self::store_commits_batch) and drive
    /// [`sync_with_all_peers`](Self::sync_with_all_peers) separately. An empty
    /// list is a no-op (no minimize, no broadcast) and yields an empty map.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a local storage error
    ///   occurs while persisting the batch, or if ingestion during the
    ///   trailing broadcast hits a storage error.
    ///
    /// Per-peer transport failures during the broadcast are *not* surfaced as
    /// `Err`; they appear in the returned [`PerPeerSync`] map.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_commits_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Blob)>,
        timeout: CallTimeout,
    ) -> Result<
        PerPeerSync<Conn, Async, <Conn as Connection<Async, Hdl::Message>>::SendError>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        if commits.is_empty() {
            return Ok(PerPeerSync::default());
        }

        self.store_commits_batch(id, commits).await?;
        let per_peer = self
            .sync_with_all_peers(id, true, timeout)
            .await
            .map_err(WriteError::Io)?;
        Ok(per_peer)
    }

    /// Bulk-insert fragments **and** propagate them to peers — the convenience
    /// combinator of [`store_fragments_batch`](Self::store_fragments_batch) +
    /// [`sync_with_all_peers`](Self::sync_with_all_peers).
    ///
    /// Prefer this over looping [`add_fragment`](Self::add_fragment): the
    /// batch path runs a single `save_batch` and one `minimize`, then a single
    /// broadcast, instead of re-minimizing and pushing per fragment.
    ///
    /// This **awaits the broadcast** and returns its per-peer outcome
    /// ([`PerPeerSync`]); a byte-connected but protocol-unresponsive peer can
    /// stall the call for up to `timeout` (or the configured
    /// `roundtrip_timeout` for [`CallTimeout::Default`]). Callers that want the
    /// durable write to return *without* waiting on peers should call
    /// [`store_fragments_batch`](Self::store_fragments_batch) and drive
    /// [`sync_with_all_peers`](Self::sync_with_all_peers) separately. An empty
    /// list is a no-op (no minimize, no broadcast) and yields an empty map.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] (`IoError::Storage`) if a local storage error
    ///   occurs while persisting the batch, or if ingestion during the
    ///   trailing broadcast hits a storage error.
    ///
    /// Per-peer transport failures during the broadcast are *not* surfaced as
    /// `Err`; they appear in the returned [`PerPeerSync`] map.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_fragments_batch(
        &self,
        id: SedimentreeId,
        fragments: Vec<FragmentBatchItem>,
        timeout: CallTimeout,
    ) -> Result<
        PerPeerSync<Conn, Async, <Conn as Connection<Async, Hdl::Message>>::SendError>,
        WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>,
    > {
        if fragments.is_empty() {
            return Ok(PerPeerSync::default());
        }

        self.store_fragments_batch(id, fragments).await?;
        let per_peer = self
            .sync_with_all_peers(id, true, timeout)
            .await
            .map_err(WriteError::Io)?;
        Ok(per_peer)
    }

    /// Forward an inbound subscription for `id` to every connected peer
    /// other than `originator` not already subscribed for `id`, via
    /// [`sync_with_peer`](Self::sync_with_peer) with `subscribe = true`.
    /// This keeps relay topologies reachable: updates an upstream peer
    /// later pushes flow back through us to the originator. Best-effort;
    /// per-peer errors are logged and ignored. See
    /// `design/sync/subscriptions.md#upstream-propagation-relay-topologies`.
    ///
    /// # Concurrency
    ///
    /// Handler futures run concurrently, so two subscribes for the same
    /// `id` can race here. The claim step pre-inserts `(peer, id)` into
    /// [`outgoing_subscriptions`] under one short-held lock before any
    /// await, so a concurrent caller skips a pair already claimed.
    ///
    /// The claim is kept only if `sync_with_peer` actually establishes
    /// the subscription (`Ok((true, ..))`, which also calls
    /// [`track_outgoing_subscription`](Self::track_outgoing_subscription)).
    /// Any other outcome — `Err`, timeout, or `NotFound`/`Unauthorized`
    /// (both `Ok((false, ..))`) — rolls the claim back so a later
    /// subscribe retries, keeping [`outgoing_subscriptions`] limited to
    /// established subscriptions (as [`get_peer_subscriptions`] assumes
    /// when replaying them on reconnect).
    ///
    /// [`outgoing_subscriptions`]: Self::outgoing_subscriptions
    /// [`get_peer_subscriptions`]: Self::get_peer_subscriptions
    pub(crate) async fn propagate_subscription(&self, id: SedimentreeId, originator: PeerId) {
        let peers: Vec<PeerId> = {
            let conns = self.connections.lock().await;
            conns.keys().copied().filter(|p| *p != originator).collect()
        };

        if peers.is_empty() {
            return;
        }

        // Claim each pair under one lock (see `# Concurrency`); the lock
        // is never held across the `sync_with_peer` awaits below.
        let to_propagate: Vec<PeerId> = {
            let mut subs = self.outgoing_subscriptions.lock().await;
            peers
                .into_iter()
                .filter(|peer| subs.entry(*peer).or_default().insert(id))
                .collect()
        };

        let mut propagations: FuturesUnordered<_> = to_propagate
            .into_iter()
            .map(|peer| async move {
                tracing::debug!(tree = ?id, peer = %peer, "propagating subscription upstream to peer");

                let established = match self
                    .sync_with_peer(&peer, id, true, CallTimeout::Default)
                    .await
                {
                    Ok((had_success, _, _)) => had_success,
                    Err(e) => {
                        tracing::debug!(peer = %peer, tree = ?id, error = %e, "subscribe propagation failed");
                        false
                    }
                };
                (peer, established)
            })
            .collect();

        while let Some((peer, established)) = propagations.next().await {
            if !established {
                let mut subs = self.outgoing_subscriptions.lock().await;
                if let Some(set) = subs.get_mut(&peer) {
                    set.remove(&id);
                    if set.is_empty() {
                        subs.remove(&peer);
                    }
                }
            }
        }
    }

    /// Request a batch sync from a given peer for a given sedimentree ID.
    ///
    /// # Returns
    ///
    /// A tuple `(succeeded, stats, per-connection call errors)`: `succeeded`
    /// is `true` if at least one connection to the peer synced successfully;
    /// `stats` aggregates the sync; and the [`Vec`] carries per-connection
    /// [`CallError`]s for connections
    /// that failed. `succeeded = false` with an empty error list means no
    /// connections to the peer were available.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs during the sync process.
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    #[tracing::instrument(
        name = "sync_with_peer",
        level = "debug",
        skip_all,
        fields(peer = %to_ask, tree = ?id, subscribe)
    )]
    pub async fn sync_with_peer(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> Result<
        (
            bool,
            SyncStats,
            Vec<(
                Authenticated<Conn, Async>,
                CallError<<Conn as Connection<Async, Hdl::Message>>::SendError>,
            )>,
        ),
        IoError<Async, Store, Conn, Hdl::Message>,
    > {
        // Resolve caller policy to the low-level deadline (`None` = uncapped).
        let timeout = timeout.resolve(self.default_roundtrip_timeout);

        tracing::info!(tree = ?id, peer = %to_ask, "requesting batch sync for sedimentree");

        #[cfg(feature = "metrics")]
        let sync_start = std::time::Instant::now();

        let mut stats = SyncStats::new();
        let mut had_success = false;

        let peer_conns: Vec<Authenticated<Conn, Async>> = {
            self.connections
                .lock()
                .await
                .get(to_ask)
                .map(|ne| ne.iter().cloned().collect())
                .unwrap_or_default()
        };

        let mut conn_errs = Vec::new();

        for conn in peer_conns {
            tracing::info!(peer = %to_ask, "using connection to peer");
            let mut session = SyncSession::new(id, conn.peer_id(), SyncSessionKind::OutboundBatch);
            let seed = FingerprintSeed::random();
            // A nonexistent tree syncs as empty: we advertise nothing and the
            // peer sends us everything it has. The hydrated tree is already
            // minimized for the wire diff.
            let tree = self.get_or_hydrate(id).await?.unwrap_or_default();
            let resolver = tree.fingerprint_resolver(&seed);

            tracing::debug!(
                tree = ?id,
                commit_fps = resolver.summary().commit_fingerprints().len(),
                fragment_fps = resolver.summary().fragment_fingerprints().len(),
                "sending fingerprint summary"
            );

            // Mux missing means the peer was torn down between the
            // connection snapshot and here; report it dropped, don't panic.
            let Some(mux) = ({
                let muxes = self.multiplexers.lock().await;
                muxes.get(to_ask).and_then(|v| v.first()).cloned()
            }) else {
                tracing::debug!(peer = %to_ask, "multiplexer for peer gone (concurrent teardown); skipping");
                conn_errs.push((conn.clone(), CallError::ResponseDropped));
                continue;
            };
            let managed = ManagedConnection::new(conn.clone(), mux, self.timer.clone());
            let req_id = managed.next_request_id();

            let result = ManagedCall::<Async, Hdl::Message>::call(
                &managed,
                BatchSyncRequest {
                    id,
                    req_id,
                    fingerprint_summary: resolver.summary().clone(),
                    subscribe,
                },
                timeout,
            )
            .await;

            match result {
                Err(e) => conn_errs.push((conn, e)),
                Ok(BatchSyncResponse {
                    result,
                    responder_heads,
                    ..
                }) => {
                    self.handler
                        .notify_remote_heads(id, conn.peer_id(), responder_heads.clone());
                    stats.remote_heads = responder_heads;
                    session.remote_heads = Some(stats.remote_heads.clone());
                    let SyncDiff {
                        missing_commits,
                        missing_fragments,
                        requesting,
                    } = match result {
                        SyncResult::Ok(diff) => diff,
                        SyncResult::NotFound => {
                            tracing::debug!(peer = %to_ask, tree = ?id, "peer reports sedimentree not found");
                            continue;
                        }
                        SyncResult::Unauthorized => {
                            tracing::debug!(peer = %to_ask, tree = ?id, "peer reports we are unauthorized for sedimentree");
                            continue;
                        }
                    };
                    // Track counts for stats
                    let commits_to_receive = missing_commits.len();
                    let fragments_to_receive = missing_fragments.len();

                    // Ingest in one batched pass: `recv_batch_sync_response` groups
                    // items by author and writes each batch in a single `save_batch`.
                    // `requesting` is handled separately below, so the ingester's
                    // copy is left empty.
                    let ingest_summary = ingest::recv_batch_sync_response(
                        &self.sedimentrees,
                        &self.storage,
                        to_ask,
                        id,
                        SyncDiff {
                            missing_commits,
                            missing_fragments,
                            requesting: RequestedData::default(),
                        },
                    )
                    .await?;
                    session
                        .received_commit_ids
                        .extend(ingest_summary.commit_ids);
                    session
                        .received_fragment_ids
                        .extend(ingest_summary.fragment_ids);
                    self.minimize_tree(id).await;

                    // Update received stats (count what was offered, not verified)
                    stats.commits_received += commits_to_receive;
                    stats.fragments_received += fragments_to_receive;

                    tracing::debug!(
                        tree = ?id,
                        commits_received = commits_to_receive,
                        requesting_commits = requesting.commit_fingerprints.len(),
                        requesting_fragments = requesting.fragment_fingerprints.len(),
                        "received response"
                    );

                    // Send back data the responder requested (bidirectional sync)
                    if !requesting.is_empty() {
                        tracing::debug!(tree = ?id, "calling send_requested_data");
                        match self
                            .send_requested_data(&conn, id, &resolver, &requesting)
                            .await
                        {
                            Ok(sent) => {
                                tracing::debug!(
                                    commits = sent.commits,
                                    fragments = sent.fragments,
                                    "send_requested_data returned"
                                );
                                stats.commits_sent += sent.commits;
                                stats.fragments_sent += sent.fragments;
                                session.sent_commit_ids = sent.commit_ids;
                                session.sent_fragment_ids = sent.fragment_ids;
                            }
                            Err(e) => {
                                tracing::warn!(peer = %to_ask, error = %e, "failed to send requested data to peer");
                            }
                        }
                    }

                    // Mutual subscription: we subscribed to them, so also add them
                    // to our subscriptions so our commits get pushed to them
                    if subscribe {
                        self.track_outgoing_subscription(*to_ask, id).await;
                        self.add_subscription(*to_ask, id).await;
                        tracing::debug!(peer = %to_ask, tree = ?id, "mutual subscription: added peer to our subscriptions");
                    }

                    self.emit_sync_session(session).await;

                    had_success = true;
                    break;
                }
            }
        }

        #[cfg(feature = "metrics")]
        {
            // Duration is for successful round-trips only; all-failed syncs are
            // counted via `sync_call_failure` below. Recording them here would
            // skew the histogram toward the timeout instead of real latency.
            if had_success {
                crate::metrics::sync_duration(sync_start.elapsed().as_secs_f64());
            }
            crate::metrics::sync_data_exchanged(
                stats.commits_received,
                stats.fragments_received,
                stats.commits_sent,
                stats.fragments_sent,
            );
            for (_, err) in &conn_errs {
                crate::metrics::sync_call_failure(err.error_name());
            }
        }

        Ok((had_success, stats, conn_errs))
    }

    /// Request a batch sync from all connected peers for a given sedimentree ID.
    ///
    /// # Returns
    ///
    /// A [`PerPeerSync`] map keyed by [`PeerId`]; each entry is
    /// `(succeeded, stats, per-connection call errors)`, so callers can see
    /// which peers acked and which failed. Peers that could not be reached
    /// appear with `succeeded = false` and/or per-connection
    /// [`CallError`]s. An empty map
    /// means no peers were connected.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs during the sync process.
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    #[tracing::instrument(
        name = "sync_with_all_peers",
        level = "debug",
        skip_all,
        fields(tree = ?id, subscribe)
    )]
    pub async fn sync_with_all_peers(
        &self,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> Result<
        PerPeerSync<Conn, Async, <Conn as Connection<Async, Hdl::Message>>::SendError>,
        IoError<Async, Store, Conn, Hdl::Message>,
    > {
        // Resolve caller policy to the low-level deadline (`None` = uncapped).
        let timeout = timeout.resolve(self.default_roundtrip_timeout);

        tracing::info!(tree = ?id, "requesting batch sync for sedimentree from all peers");
        let peers: Map<PeerId, Vec<Authenticated<Conn, Async>>> = {
            self.connections
                .lock()
                .await
                .iter()
                .map(|(peer_id, conns)| (*peer_id, conns.iter().cloned().collect()))
                .collect()
        };
        tracing::debug!(count = peers.len(), "found peer(s)");

        // Hydrate the tree once (from the LRU cache or storage) and share it
        // across all per-peer tasks, rather than re-reading it per peer. A
        // nonexistent tree syncs as empty.
        let shared_tree = self.get_or_hydrate(id).await?.unwrap_or_default();

        let mut set: FuturesUnordered<_> = peers
            .iter()
            .map(|(peer_id, peer_conns)| {
                let shared_tree = shared_tree.clone();
                async move {
                    tracing::debug!(
                        tree = ?id,
                        connections = peer_conns.len(),
                        "requesting batch sync for sedimentree"
                    );

                    let mut had_success = false;
                    let mut conn_errs = Vec::new();
                    let mut stats = SyncStats::new();

                    for conn in peer_conns {
                        tracing::debug!(peer = %conn.peer_id(), "using connection to peer");
                        let mut session =
                            SyncSession::new(id, conn.peer_id(), SyncSessionKind::OutboundBatch);
                        let seed = FingerprintSeed::random();
                        let resolver = shared_tree.fingerprint_resolver(&seed);

                        // Mux missing means the peer was torn down between
                        // the connection snapshot and here; report it
                        // dropped, don't panic.
                        let Some(mux) = ({
                            let muxes = self.multiplexers.lock().await;
                            muxes.get(peer_id).and_then(|v| v.first()).cloned()
                        }) else {
                            tracing::debug!(peer = %peer_id, "multiplexer for peer gone (concurrent teardown); skipping");
                            conn_errs.push((
                                conn.clone(),
                                CallError::ResponseDropped,
                            ));
                            continue;
                        };
                        let managed = ManagedConnection::new(conn.clone(), mux, self.timer.clone());
                        let req_id = managed.next_request_id();

                        let result = ManagedCall::<Async, Hdl::Message>::call(
                                &managed,
                                BatchSyncRequest {
                                    id,
                                    req_id,
                                    fingerprint_summary: resolver.summary().clone(),
                                    subscribe,
                                },
                                timeout,
                            )
                            .await;

                        match result {
                            Err(e) => conn_errs.push((conn.clone(), e)),
                            Ok(BatchSyncResponse {
                                result,
                                responder_heads,
                                ..
                            }) => {
                                self.handler.notify_remote_heads(
                                    id,
                                    *peer_id,
                                    responder_heads.clone(),
                                );
                                stats.remote_heads = responder_heads;
                                session.remote_heads = Some(stats.remote_heads.clone());
                                let SyncDiff {
                                    missing_commits,
                                    missing_fragments,
                                    requesting,
                                } = match result {
                                    SyncResult::Ok(diff) => diff,
                                    SyncResult::NotFound => {
                                        tracing::debug!(peer = %peer_id, tree = ?id, "peer reports sedimentree not found");
                                        continue;
                                    }
                                    SyncResult::Unauthorized => {
                                        tracing::debug!(peer = %peer_id, tree = ?id, "peer reports we are unauthorized for sedimentree");
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
                                    "sync_with_all_peers: response received"
                                );

                                // Ingest in one batched pass; see `sync_with_peer`.
                                // `requesting` is handled separately below.
                                let ingest_summary = ingest::recv_batch_sync_response(
                                    &self.sedimentrees,
                                    &self.storage,
                                    peer_id,
                                    id,
                                    SyncDiff {
                                        missing_commits,
                                        missing_fragments,
                                        requesting: RequestedData::default(),
                                    },
                                )
                                .await?;
                                session.received_commit_ids.extend(ingest_summary.commit_ids);
                                session
                                    .received_fragment_ids
                                    .extend(ingest_summary.fragment_ids);
                                self.minimize_tree(id).await;

                                // Update received stats
                                stats.commits_received += commits_to_receive;
                                stats.fragments_received += fragments_to_receive;

                                // Send back data the responder requested (bidirectional sync)
                                if !requesting.is_empty() {
                                    match self.send_requested_data(conn, id, &resolver, &requesting).await {
                                        Ok(sent) => {
                                            stats.commits_sent += sent.commits;
                                            stats.fragments_sent += sent.fragments;
                                            session.sent_commit_ids = sent.commit_ids;
                                            session.sent_fragment_ids = sent.fragment_ids;
                                        }
                                        Err(ref e @ SendRequestedDataError::Unauthorized(_)) => {
                                            let msg: Hdl::Message = SyncMessage::from(DataRequestRejected { id }).into();
                                            if let Err(send_err) = conn.send(&msg).await {
                                                tracing::warn!(peer = %peer_id, error = %send_err, "peer disconnected while sending DataRequestRejected");
                                            }
                                            tracing::warn!(peer = %peer_id, error = %e, "failed to send requested data to peer");
                                        }
                                        Err(e) => {
                                            tracing::warn!(peer = %peer_id, error = %e, "failed to send requested data to peer");
                                        }
                                    }
                                }

                                // Mutual subscription: we subscribed to them, so also add them
                                // to our subscriptions so our commits get pushed to them
                                if subscribe {
                                    self.track_outgoing_subscription(*peer_id, id).await;
                                    self.add_subscription(*peer_id, id).await;
                                    tracing::debug!(peer = %peer_id, tree = ?id, "mutual subscription: added peer to our subscriptions");
                                }

                                self.emit_sync_session(session).await;

                                had_success = true;
                                break;
                            }
                        }
                    }

                    Ok::<(PeerId, bool, SyncStats, Vec<(Authenticated<Conn, Async>, _)>), IoError<Async, Store, Conn, Hdl::Message>>((
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
                    tracing::error!(error = %e, "per-peer sync task failed");
                }
                Ok((peer_id, success, stats, errs)) => {
                    out.insert(peer_id, (success, stats, errs));
                }
            }
        }
        Ok(PerPeerSync::new(out))
    }

    /// Sync all known [`Sedimentree`]s with all connected peers.
    pub async fn full_sync_with_all_peers(
        &self,
        timeout: CallTimeout,
    ) -> (
        bool,
        SyncStats,
        Vec<(
            Authenticated<Conn, Async>,
            CallError<<Conn as Connection<Async, Hdl::Message>>::SendError>,
        )>,
        Vec<(SedimentreeId, IoError<Async, Store, Conn, Hdl::Message>)>,
    ) {
        tracing::info!("Requesting batch sync for all sedimentrees from all peers");
        // Enumerate from storage (complete across evictions); per-tree sync
        // hydrates through the bounded LRU cache.
        let tree_ids = self.sedimentree_ids().await;

        let mut sync_futures: FuturesUnordered<_> = tree_ids
            .into_iter()
            .map(|id| async move {
                tracing::debug!(tree = ?id, "requesting batch sync for sedimentree");
                let result = self.sync_with_all_peers(id, true, timeout).await;
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
                    tracing::error!(tree = ?id, error = %e, "failed to sync sedimentree");
                    io_errs.push((id, e));
                }
            }
        }

        (had_success, stats, call_errs, io_errs)
    }

    /********************
     * PUBLIC UTILITIES *
     ********************/

    /// Get all known sedimentree IDs.
    ///
    /// Enumerates from durable storage (not just the in-RAM cache), so the
    /// list stays complete even after cold trees are evicted from memory.
    /// On a storage error, logs and falls back to the resident set.
    pub async fn sedimentree_ids(&self) -> Vec<SedimentreeId> {
        match self
            .storage
            .hydration_access()
            .load_all_sedimentree_ids::<Async>()
            .await
        {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %e, "sedimentree_ids: storage enumeration failed; falling back to resident set");
                self.sedimentrees.into_keys().await
            }
        }
    }

    /// Number of sedimentrees currently resident in the in-memory LRU cache.
    ///
    /// This is the cache occupancy (bounded by the resident cap), *not* the
    /// total number of trees in durable storage — compare the two to gauge
    /// eviction pressure.
    pub async fn resident_sedimentree_count(&self) -> usize {
        self.sedimentrees.len().await
    }

    /// Get all commits for a given sedimentree ID.
    ///
    /// Hydrates from storage on an in-RAM cache miss, so an evicted tree
    /// returns its real commits. Returns `None` when the tree does not exist
    /// in storage. On a storage error, logs and returns `None`.
    pub async fn get_commits(&self, id: SedimentreeId) -> Option<Vec<LooseCommit>> {
        let tree = self.hydrated_or_log(id).await?;
        Some(tree.loose_commits().cloned().collect())
    }

    /// Get all fragments for a given sedimentree ID.
    ///
    /// Hydrates from storage on an in-RAM cache miss. Returns `None` when the
    /// tree does not exist in storage (see [`get_commits`]).
    ///
    /// [`get_commits`]: Self::get_commits
    pub async fn get_fragments(&self, id: SedimentreeId) -> Option<Vec<Fragment>> {
        let tree = self.hydrated_or_log(id).await?;
        Some(tree.fragments().cloned().collect())
    }

    /// Hydrate a tree, logging and returning `None` on storage error or when
    /// the tree does not exist. Helper for the best-effort getters.
    async fn hydrated_or_log(&self, id: SedimentreeId) -> Option<Sedimentree> {
        match self.get_or_hydrate(id).await {
            Ok(maybe_tree) => maybe_tree,
            Err(e) => {
                tracing::warn!(tree = ?id, error = %e, "hydration failed for sedimentree");
                None
            }
        }
    }

    /// Get the current heads for every known sedimentree.
    ///
    /// Enumerates from durable storage so the result is complete even after
    /// cold trees have been evicted from the in-RAM cache. Each tree is
    /// hydrated on demand through the bounded LRU cache, so memory stays
    /// bounded across the sweep (an uncommon, potentially slow operation at
    /// large document counts).
    ///
    /// An inner empty `Vec<CommitId>` means the sedimentree exists but has
    /// no heads — this is also returned for an id whose hydration *fails*
    /// transiently (logged), so a storage error is not silently reported as
    /// "tree does not exist". An id that no longer exists (e.g. deleted
    /// between enumeration and hydration) is omitted.
    pub async fn get_all_heads(&self) -> Vec<(SedimentreeId, Vec<CommitId>)> {
        let ids = self.sedimentree_ids().await;
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            match self.get_or_hydrate(id).await {
                Ok(Some(tree)) => out.push((id, tree.heads(&self.depth_metric))),
                // Tree genuinely gone (raced with a delete): omit it.
                Ok(None) => {}
                // Transient hydration failure: keep the id (it was just
                // enumerated from storage) with empty, advisory heads.
                Err(e) => {
                    tracing::warn!(tree = ?id, error = %e, "get_all_heads: hydration failed");
                    out.push((id, Vec::new()));
                }
            }
        }
        out
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
        putter: &Putter<Async, Store>,
        verified_commits: Vec<VerifiedMeta<LooseCommit>>,
        verified_fragments: Vec<VerifiedMeta<Fragment>>,
    ) -> Result<(), Store::Error> {
        let id = putter.sedimentree_id();
        tracing::debug!(tree = ?id, "adding sedimentree");

        // ID registration happens inside `save_batch` per the
        // `Storage::save_batch` contract — no explicit
        // `save_sedimentree_id` call needed here.

        // Extract payloads for the in-memory tree before moving the verified
        // metas into a single `save_batch` call.
        let loose_commits: Vec<LooseCommit> = verified_commits
            .iter()
            .map(|v| v.payload().clone())
            .collect();
        let fragments: Vec<Fragment> = verified_fragments
            .iter()
            .map(|v| v.payload().clone())
            .collect();

        putter
            .save_batch(verified_commits, verified_fragments)
            .await?;

        let sedimentree = Sedimentree::new(fragments, loose_commits);
        // Hydrate-on-miss so merging into an evicted tree preserves its full
        // durable history.
        let access = self.storage.hydration_access();
        self.sedimentrees
            .with_entry_hydrated(
                id,
                || ingest::load_tree::<Async, _>(&access, id),
                |tree| tree.merge(sedimentree),
            )
            .await?;

        Ok(())
    }

    /// Send requested data back to a peer (fire-and-forget for bidirectional sync).
    ///
    /// Uses the pre-captured `FingerprintResolver` to reverse-lookup echoed
    /// fingerprints to content-addressed digests. The resolver must have been
    /// created from the same tree state that generated the original
    /// `FingerprintSummary` — this is enforced at the type level since
    /// `FingerprintResolver` can only be constructed via
    /// `Sedimentree::fingerprint_resolver`.
    ///
    /// # Errors
    ///
    /// * [`SendRequestedDataError::Unauthorized`] if the peer is not authorized to fetch.
    /// * [`SendRequestedDataError::Io`] if storage operations fail.
    #[allow(clippy::too_many_lines)]
    pub async fn send_requested_data(
        &self,
        conn: &Authenticated<Conn, Async>,
        id: SedimentreeId,
        resolver: &FingerprintResolver,
        requesting: &RequestedData,
    ) -> Result<SendCount, SendRequestedDataError<Async, Store, Conn, Hdl::Message>> {
        if requesting.is_empty() {
            return Ok(SendCount::default());
        }

        let peer_id = conn.peer_id();
        tracing::debug!(
            commits = requesting.commit_fingerprints.len(),
            fragments = requesting.fragment_fingerprints.len(),
            peer = %peer_id,
            "sending requested commits and fragments to peer"
        );

        let fetcher = match self.storage.get_fetcher::<Async>(peer_id, id).await {
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

        // Resolve requested fingerprints → digests via the pre-captured resolver.
        // The resolver was built from the tree state at fingerprint time,
        // so minimize cannot invalidate these lookups. Hydrate from storage
        // on a cache miss so an evicted tree reports its real heads; a
        // nonexistent tree has no heads.
        let heads = self
            .get_or_hydrate(id)
            .await?
            .map(|tree| tree.heads(&self.depth_metric))
            .unwrap_or_default();

        let requested_commit_ids: Vec<CommitId> = requesting
            .commit_fingerprints
            .iter()
            .filter_map(|fp| {
                let id = resolver.resolve_commit(fp);
                if id.is_none() {
                    // Coverage-only fingerprints (fragment head/boundary) are in
                    // the summary but intentionally not in the resolver. The remote
                    // echoing them back is expected when it doesn't have that CommitId.
                    tracing::debug!(fingerprint = %fp, "requested commit fingerprint not found in resolver");
                }
                id
            })
            .collect();

        // Honor the request as-is: the remote explicitly listed these
        // FPs in `requesting`, meaning it knows it doesn't have them.
        // The diff layer (`Sedimentree::diff_remote_fingerprints`) has
        // already trimmed the candidate set via its fragment-aware
        // pruning, so the bandwidth cost is bounded by what the remote
        // asked for.
        //
        // Do NOT second-guess `requesting` with a transitive-ancestor
        // check: a peer holding a loose commit gives no guarantee about
        // its ancestors (partial sync, restored snapshots, etc.), so
        // dropping a requested FP because the local thinks the remote
        // already has its ancestors can silently strand the remote in
        // a "missing-ancestor" state from which it cannot recover.

        let requested_fragment_ids: Vec<CommitId> = requesting
            .fragment_fingerprints
            .iter()
            .filter_map(|fp| {
                let id = resolver.resolve_fragment(fp);
                if id.is_none() {
                    tracing::warn!(fingerprint = %fp, "requested fragment fingerprint not found in resolver");
                }
                id
            })
            .collect();

        // Load commits and fragments from storage (compound with blobs), build wire messages
        let (commit_messages, fragment_messages) = {
            // With compound storage, load_loose_commits returns VerifiedMeta which contains both signed data and blob
            let commit_by_id: Map<CommitId, VerifiedMeta<LooseCommit>> =
                if requested_commit_ids.is_empty() {
                    Map::default()
                } else {
                    let mut map = Map::new();
                    for vm in fetcher
                        .load_loose_commits()
                        .await
                        .map_err(IoError::Storage)?
                    {
                        map.entry(vm.payload().head()).or_insert(vm);
                    }
                    map
                };

            let fragment_by_id: Map<CommitId, VerifiedMeta<Fragment>> =
                if requested_fragment_ids.is_empty() {
                    Map::default()
                } else {
                    fetcher
                        .load_fragments()
                        .await
                        .map_err(IoError::Storage)?
                        .into_iter()
                        .map(|vm| (vm.payload().head(), vm))
                        .collect()
                };

            let mut commit_msgs = Vec::new();
            for commit_id in &requested_commit_ids {
                if let Some(verified) = commit_by_id.get(commit_id) {
                    let msg: Hdl::Message = SyncMessage::LooseCommit {
                        id,
                        commit: verified.signed().clone(),
                        blob: verified.blob().clone(),
                        sender_heads: RemoteHeads {
                            counter: self.send_counter.next(conn.peer_id()).await,
                            heads: heads.clone(),
                        },
                    }
                    .into();
                    commit_msgs.push((*commit_id, msg));
                }
            }

            let mut fragment_msgs = Vec::new();
            for frag_id in &requested_fragment_ids {
                if let Some(verified) = fragment_by_id.get(frag_id) {
                    let msg: Hdl::Message = SyncMessage::Fragment {
                        id,
                        fragment: verified.signed().clone(),
                        blob: verified.blob().clone(),
                        sender_heads: RemoteHeads {
                            counter: self.send_counter.next(conn.peer_id()).await,
                            heads: heads.clone(),
                        },
                    }
                    .into();
                    fragment_msgs.push((*frag_id, msg));
                }
            }

            (commit_msgs, fragment_msgs)
        };

        let mut commits_sent = 0;
        let mut fragments_sent = 0;
        let mut commit_ids = Vec::new();
        let mut fragment_ids = Vec::new();

        for (item_id, msg) in commit_messages {
            let result = conn.send(&msg).await;
            match result {
                Ok(()) => {
                    commits_sent += 1;
                    commit_ids.push(item_id);
                }
                Err(e) => {
                    tracing::warn!("failed to send requested data: {}", e);
                }
            }
        }

        for (item_id, msg) in fragment_messages {
            let result = conn.send(&msg).await;
            match result {
                Ok(()) => {
                    fragments_sent += 1;
                    fragment_ids.push(item_id);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to send requested data");
                }
            }
        }

        Ok(SendCount {
            commits: commits_sent,
            fragments: fragments_sent,
            commit_ids,
            fragment_ids,
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
        putter: &Putter<Async, Store>,
        verified_meta: VerifiedMeta<LooseCommit>,
    ) -> Result<bool, Store::Error> {
        ingest::insert_commit_locally(&self.sedimentrees, putter, verified_meta).await
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
        putter: &Putter<Async, Store>,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<bool, Store::Error> {
        ingest::insert_fragment_locally(&self.sedimentrees, putter, verified_meta).await
    }

    /// Re-minimize a sedimentree in the in-memory cache.
    ///
    /// Prunes dominated fragments and loose commits covered by fragments,
    /// keeping only the minimal covering. Storage retains the full history.
    async fn minimize_tree(&self, id: SedimentreeId) {
        ingest::minimize_tree(&self.sedimentrees, &self.depth_metric, id).await;
    }

    /// Get a sedimentree from the in-memory LRU cache, hydrating it from
    /// durable storage on a miss. The returned tree is already minimized for
    /// wire use (fingerprint summaries / resolvers). Returns `None` if the
    /// tree does not exist. See [`ingest::get_or_hydrate`].
    async fn get_or_hydrate(
        &self,
        id: SedimentreeId,
    ) -> Result<Option<Sedimentree>, IoError<Async, Store, Conn, Hdl::Message>> {
        ingest::get_or_hydrate(&self.sedimentrees, &self.storage, &self.depth_metric, id)
            .await
            .map_err(IoError::Storage)
    }

    /// Mutate the in-memory tree for `id`, hydrating it from storage first if
    /// it is not resident.
    ///
    /// The write-path counterpart to [`get_or_hydrate`](Self::get_or_hydrate):
    /// a mutation applied to an evicted tree starts from its full durable
    /// history rather than an empty default. The caller is responsible for
    /// having persisted the underlying data to storage *before* calling this
    /// (storage is the source of truth). The mutation runs against the
    /// [`MinimizedSedimentree`] wrapper, which it marks dirty; callers
    /// re-minimize (e.g. via [`minimize_tree`](Self::minimize_tree)) afterward.
    async fn with_tree_hydrated<F: FnOnce(&mut MinimizedSedimentree) -> R, R>(
        &self,
        id: SedimentreeId,
        mutate: F,
    ) -> Result<R, IoError<Async, Store, Conn, Hdl::Message>> {
        let access = self.storage.hydration_access();
        self.sedimentrees
            .with_entry_hydrated(id, || ingest::load_tree::<Async, _>(&access, id), mutate)
            .await
            .map_err(IoError::Storage)
    }
}

// The spawning methods (`listen` + `full_sync_with_peer`) share their own
// `impl` block because both `Spawn` `'static` futures (`Async::dispatch_task`
// for inbound handlers, `Async::doc_sync_task` for per-document cold-start
// fan-out) and so require bounds the rest of the sync surface does not:
// `'a: 'static`, `Timer: Send + Sync`, `Sp: Send + Sync + 'static`, and
// `SpawnDocSync`. Every real deployment uses a `'static` `Subduction`, so this
// is not a practical restriction.
impl<
    Async: SubductionFutureForm<'static, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>
        + SpawnDocSync<'static, Store, Conn, Hdl::Message, Hdl, Auth, Sign, Metric, SHARDS>
        + 'static,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'static,
    Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone + Send + Sync + 'static,
    Sp: Spawn<Async> + Clone + Send + Sync + 'static,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'static, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
where
    Hdl::Message: From<SyncMessage>,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
    ManagedConnection<Conn, Async, Timer>: ManagedCall<
            Async,
            Hdl::Message,
            SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
        >,
{
    /// Listen for incoming messages and dispatch them through the handler.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// Each inbound message is dispatched as its own task via the configured
    /// [`Spawn`](crate::spawn::Spawn), so independent handlers
    /// (e.g. batch-sync requests for different sedimentrees) run in parallel
    /// across worker threads on `Sendable` and concurrently on `Local`. A
    /// completion channel reports each task's outcome back so a broken
    /// connection is still torn down on a handler error.
    ///
    /// The handler stored on this instance receives each decoded message
    /// and decides what to do with it. For the standard sync protocol,
    /// this is a [`SyncHandler`].
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a handler error signals a broken connection.
    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(name = "listen", level = "info", skip_all)]
    pub async fn listen(
        self: Arc<Self>,
    ) -> Result<(), ListenError<Async, Store, Conn, Hdl::Message>> {
        tracing::info!("starting Subduction listener with spawned dispatch");

        let handler = &self.handler;

        #[allow(clippy::type_complexity)]
        let (done_tx, done_rx): (
            async_channel::Sender<DispatchOutcome<Conn, Async, Hdl::HandlerError>>,
            async_channel::Receiver<DispatchOutcome<Conn, Async, Hdl::HandlerError>>,
        ) = async_channel::unbounded();

        // Dispatch admission is per-peer, enforced upstream in the connection
        // manager (a reader acquires its peer's permit before forwarding); the
        // listener just drains the queue and spawns.
        loop {
            futures::select_biased! {
                done = done_rx.recv().fuse() => {
                    if let Ok(outcome) = done {
                        // Every received outcome means one dispatch task ended.
                        #[cfg(feature = "metrics")]
                        crate::metrics::dispatch_inflight_dec();

                        match outcome {
                            DispatchOutcome::Completed { conn, result: Err(e) } => {
                                #[cfg(feature = "metrics")]
                                crate::metrics::dispatch_completed("err");

                                let peer_id = conn.peer_id();
                                tracing::error!(
                                    peer = %peer_id,
                                    error = %e,
                                    "error dispatching message"
                                );

                                if self.remove_connection(&conn).await == Some(true) {
                                    handler.on_peer_disconnect(peer_id).await;
                                }
                                tracing::debug!(peer = %peer_id, "removed failed connection");
                            }
                            DispatchOutcome::Completed { result: Ok(()), .. } => {
                                #[cfg(feature = "metrics")]
                                crate::metrics::dispatch_completed("ok");
                            }
                            DispatchOutcome::Aborted => {
                                #[cfg(feature = "metrics")]
                                crate::metrics::dispatch_completed("aborted");
                            }
                        }
                    }
                }

                resp_result = self.response_queue.recv().fuse() => {
                    if let Ok((conn, msg)) = resp_result {
                        let peer_id = conn.peer_id();
                        if let Some(resp) = msg.try_as_batch_sync_response() {
                            let muxes_for_peer = {
                                let multiplexers = self.multiplexers.lock().await;
                                multiplexers.get(&peer_id).cloned()
                            };

                            let mut consumed = false;
                            if let Some(muxes) = muxes_for_peer {
                                for mux in &muxes {
                                    if mux.resolve_pending(resp).await {
                                        tracing::debug!(peer = %peer_id, "routed BatchSyncResponse to pending caller");
                                        consumed = true;
                                        break;
                                    }
                                }
                            }

                            if !consumed {
                                tracing::warn!(
                                    peer = %peer_id,
                                    "BatchSyncResponse had no pending caller"
                                );
                            }
                        } else {
                            tracing::warn!(
                                peer = %peer_id,
                                "non-BatchSyncResponse message arrived on response_queue — dropping"
                            );
                        }
                    } else {
                        tracing::info!("response queue closed");
                        break;
                    }
                }

                received = self.msg_queue.recv().fuse() => {
                    // The per-peer permit was acquired upstream in the
                    // connection reader and rides the queue here; it's moved
                    // into the dispatch task and released on completion.
                    if let Ok((conn, msg, permit)) = received {
                        let peer_id = conn.peer_id();
                        // Don't Debug-format `msg` — it can embed commit/blob bytes.
                        tracing::trace!(peer = %peer_id, "listener received message");

                        if let Some(resp) = msg.try_as_batch_sync_response() {
                            tracing::debug!(
                                peer = %peer_id,
                                "BatchSyncResponse arrived via msg_queue (expected response_queue) — routing to multiplexer"
                            );
                            let muxes_for_peer = {
                                let multiplexers = self.multiplexers.lock().await;
                                multiplexers.get(&peer_id).cloned()
                            };
                            let mut consumed = false;
                            if let Some(muxes) = muxes_for_peer {
                                for mux in &muxes {
                                    if mux.resolve_pending(resp).await {
                                        consumed = true;
                                        break;
                                    }
                                }
                            }
                            if !consumed {
                                tracing::warn!(
                                    peer = %peer_id,
                                    "BatchSyncResponse via safety net had no pending caller"
                                );
                            }
                            // Not a dispatch — `permit` drops here, releasing the slot.
                            continue;
                        }

                        let propagate = msg
                            .try_as_subscribe_request()
                            .map(|sed_id| (sed_id, peer_id));

                        // Increment before spawning; the matching decrement
                        // happens in the `done_rx` arm when the task reports
                        // its outcome (guaranteed exactly once per task).
                        #[cfg(feature = "metrics")]
                        crate::metrics::dispatch_inflight_inc();

                        self.spawner.spawn(Async::dispatch_task(
                            Arc::clone(&self),
                            handler.clone(),
                            conn.clone(),
                            msg,
                            propagate,
                            done_tx.clone(),
                            permit,
                        ));
                    } else {
                        tracing::info!("SyncMessage queue closed");
                        break;
                    }
                }

                closed_result = self.connection_closed.recv().fuse() => {
                    if let Ok((conn_id, conn)) = closed_result {
                        let peer_id = conn.peer_id();
                        tracing::warn!(conn = %conn_id, peer = %peer_id, "connection closed, removing");
                        if self.remove_connection(&conn).await == Some(true) {
                            handler.on_peer_disconnect(peer_id).await;
                        }
                    }
                }
            }
        }

        drop(done_tx);

        while let Ok(outcome) = done_rx.recv().await {
            if let DispatchOutcome::Completed {
                conn,
                result: Err(e),
            } = outcome
            {
                tracing::error!(
                    peer = %conn.peer_id(),
                    error = %e,
                    "error dispatching message during shutdown"
                );
            }
        }

        Ok(())
    }

    /// Sync all known [`Sedimentree`]s with a single peer.
    ///
    /// This is the single-peer counterpart of [`full_sync_with_all_peers`](Self::full_sync_with_all_peers).
    /// Each document's sync is **spawned onto the runtime** (via the configured
    /// [`Spawn`](crate::spawn::Spawn)) so that — on a `Sendable`
    /// multi-threaded runtime — independent documents verify, ingest, and
    /// minimize in parallel across worker threads rather than all draining
    /// through one caller task. On `Local` (Wasm) the spawner runs tasks on the
    /// same thread, preserving today's concurrency-without-parallelism.
    ///
    /// All documents are spawned at once (no in-flight cap), matching the
    /// concurrency level of the pre-spawn single-task fan-out. A shared results
    /// channel collects each task's outcome and doubles as the join mechanism,
    /// since [`Spawn`](crate::spawn::Spawn) discards task output.
    ///
    /// Errors are collected rather than short-circuiting, so a failure on one
    /// sedimentree does not prevent the rest from syncing.
    #[allow(clippy::type_complexity)]
    #[tracing::instrument(
        name = "full_sync_with_peer",
        level = "debug",
        skip_all,
        fields(peer = %peer_id, subscribe)
    )]
    pub async fn full_sync_with_peer(
        self: &Arc<Self>,
        peer_id: &PeerId,
        subscribe: bool,
        timeout: CallTimeout,
    ) -> (
        bool,
        SyncStats,
        Vec<(
            Authenticated<Conn, Async>,
            CallError<<Conn as Connection<Async, Hdl::Message>>::SendError>,
        )>,
        Vec<(SedimentreeId, IoError<Async, Store, Conn, Hdl::Message>)>,
    ) {
        tracing::info!(peer = %peer_id, "requesting batch sync for all sedimentrees with peer");
        // Enumerate from storage (complete across evictions), mirroring
        // `full_sync_with_all_peers`. Using the resident (LRU) set here would
        // silently skip cold/evicted documents, contradicting "all known".
        // Each per-document `sync_with_peer` hydrates through the LRU cache, so
        // the resident set stays bounded even across a full sweep.
        let tree_ids = self.sedimentree_ids().await;

        // Shared results channel. Each spawned per-document task reports its
        // outcome here; the channel is the join mechanism (`Spawn` discards task
        // output). Unbounded so the fan-out concurrency level is unbounded: one
        // in-flight task per document.
        let (tx, rx) =
            async_channel::unbounded::<DocSyncResult<Async, Store, Conn, Hdl::Message>>();

        let mut had_success = false;
        let mut stats = SyncStats::new();
        let mut call_errs = Vec::new();
        let mut io_errs = Vec::new();

        // Abort any still-running per-document task if this future is dropped
        // before the drain below completes (e.g. a cancelled caller). Without
        // this, `spawn`'d tasks would detach and run to self-termination,
        // holding an `Arc` clone of the node, breaking structured concurrency.
        let mut guard = AbortOnDrop::new();
        let mut outstanding = 0usize;
        for id in tree_ids {
            guard.push(self.spawner.spawn(Async::doc_sync_task(
                Arc::clone(self),
                *peer_id,
                id,
                subscribe,
                timeout,
                tx.clone(),
            )));
            outstanding += 1;
        }

        // Drop our sender so the channel closes once every spawned task's
        // sender clone is gone, then drain results until close. We drain to
        // channel-closed (not to `outstanding == 0`) so that a task which exits
        // without sending — e.g. a panic before `tx.send` — cannot wedge the
        // loop or make it return early silently; instead the channel closes and
        // we surface the discrepancy below.
        drop(tx);
        while let Ok((done_id, result)) = rx.recv().await {
            outstanding -= 1;
            match result {
                Ok((success, step_stats, step_errs)) => {
                    had_success |= success;
                    stats.commits_received += step_stats.commits_received;
                    stats.fragments_received += step_stats.fragments_received;
                    stats.commits_sent += step_stats.commits_sent;
                    stats.fragments_sent += step_stats.fragments_sent;
                    call_errs.extend(step_errs);
                }
                Err(e) => {
                    tracing::error!(tree = ?done_id, peer = %peer_id, error = %e, "failed to sync sedimentree with peer");
                    io_errs.push((done_id, e));
                }
            }
        }

        if outstanding != 0 {
            tracing::warn!(
                outstanding,
                "full_sync_with_peer: per-document sync task(s) finished without reporting a result (likely panicked)"
            );
        }

        // `guard` drops here with every tracked task already finished, so it
        // aborts nothing; it only fires if this future is dropped mid-drain.
        (had_success, stats, call_errs, io_errs)
    }
}

// ---------------------------------------------------------------------------
// Test-only observability surface.
//
// Gated as one block so the test-introspection API stays in one place and
// out of the production `impl`s. Reads internal state to assert invariants.
// ---------------------------------------------------------------------------

#[cfg(any(feature = "test_utils", test))]
impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + Clone + 'static,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    /// Returns a reference to the sedimentrees map.
    #[must_use]
    pub const fn sedimentrees(
        &self,
    ) -> &Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>> {
        &self.sedimentrees
    }

    /// Number of multiplexers currently registered for `peer_id`.
    ///
    /// One multiplexer is created per connection, so at a quiescent point
    /// this equals [`connection_count`](Self::connection_count).
    pub async fn mux_count(&self, peer_id: &PeerId) -> usize {
        self.multiplexers
            .lock()
            .await
            .get(peer_id)
            .map_or(0, Vec::len)
    }

    /// Number of connections currently registered for `peer_id`.
    pub async fn connection_count(&self, peer_id: &PeerId) -> usize {
        self.connections
            .lock()
            .await
            .get(peer_id)
            .map_or(0, NonEmpty::len)
    }

    /// Whether the connection/multiplexer invariant holds for `peer_id`:
    /// a connected peer has at least one multiplexer, and a disconnected
    /// peer has none.
    ///
    /// This is the presence/absence form the `expect("multiplexer
    /// exists...")` sites depend on, not strict per-connection equality:
    /// the non-last [`remove_connection`](Self::remove_connection) path
    /// drops a connection without dropping its mux, so `mux_count` can
    /// exceed `connection_count` while the peer is still connected.
    pub async fn conn_mux_invariant_holds(&self, peer_id: &PeerId) -> bool {
        let conns = self.connection_count(peer_id).await;
        let muxes = self.mux_count(peer_id).await;
        if conns > 0 { muxes > 0 } else { muxes == 0 }
    }

    /// Current per-peer send-counter value without incrementing it, or
    /// `None` if the peer has no counter (never stamped, or cleared).
    ///
    /// Test-only observability for asserting that teardown does not reset
    /// a still-connected peer's monotonic counter.
    pub async fn send_counter_value(&self, peer_id: &PeerId) -> Option<u64> {
        self.send_counter.peek(peer_id).await
    }

    /// Advance the per-peer send counter once, returning the new value.
    ///
    /// Test-only: lets a test put a peer's counter into a known non-zero
    /// state so a later reset is observable.
    pub async fn stamp_send_counter(&self, peer_id: PeerId) -> u64 {
        self.send_counter.next(peer_id).await
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Drop for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    fn drop(&mut self) {
        self.abort_manager_handle.abort();
        self.abort_listener_handle.abort();
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> ConnectionPolicy<Async>
    for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    type ConnectionDisallowed = Auth::ConnectionDisallowed;

    fn authorize_connect(
        &self,
        peer_id: PeerId,
    ) -> Async::Future<'_, Result<(), Self::ConnectionDisallowed>> {
        self.storage.policy().authorize_connect(peer_id)
    }
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Sp: Spawn<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> StoragePolicy<Async>
    for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>
{
    type FetchDisallowed = Auth::FetchDisallowed;
    type PutDisallowed = Auth::PutDisallowed;

    fn authorize_fetch(
        &self,
        peer: PeerId,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::FetchDisallowed>> {
        self.storage.policy().authorize_fetch(peer, sedimentree_id)
    }

    fn authorize_put(
        &self,
        requestor: PeerId,
        author: VerifiedAuthor,
        sedimentree_id: SedimentreeId,
    ) -> Async::Future<'_, Result<(), Self::PutDisallowed>> {
        self.storage
            .policy()
            .authorize_put(requestor, author, sedimentree_id)
    }

    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> Async::Future<'_, Vec<SedimentreeId>> {
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
///
/// Note: [`StartListener`] is _not_ a supertrait here — it is only required
/// on the constructor methods that build the listener future, keeping the
/// handler type `Hdl` out of the main `Subduction` struct definition.
pub trait SubductionFutureForm<
    'a,
    Store: Storage<Self>,
    Conn: Connection<Self, WireMsg> + PartialEq + 'a,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    Auth: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sign: Signer<Self>,
    Metric: DepthMetric,
    const SHARDS: usize,
>: FutureForm + RunManager<Authenticated<Conn, Self>, WireMsg> + Sized
{
}

impl<
    'a,
    Store: Storage<Self>,
    Conn: Connection<Self, WireMsg> + PartialEq + 'a,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    Auth: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sign: Signer<Self>,
    Metric: DepthMetric,
    const SHARDS: usize,
    Async: FutureForm + RunManager<Authenticated<Conn, Async>, WireMsg> + Sized,
> SubductionFutureForm<'a, Store, Conn, WireMsg, Auth, Sign, Metric, SHARDS> for Async
{
}

/// A trait for starting the listener task for Subduction.
///
/// This lets us abstract over `Send` and `!Send` futures while keeping
/// the handler type `Hdl` available for the `#[future_form]` macro to
/// generate correct `Send` bounds.
///
/// `Hdl` is a trait-level (not method-level) generic so the macro can
/// emit the required `Hdl: Send + Sync` bounds for the `Sendable` impl.
pub trait StartListener<
    'a,
    Store: Storage<Self>,
    Conn: Connection<Self, WireMsg> + PartialEq + 'a,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    Hdl: Handler<Self, Conn, Message = WireMsg> + RemoteHeadsNotifier,
    Auth: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sign: Signer<Self>,
    Metric: DepthMetric,
    const SHARDS: usize,
>: FutureForm + RunManager<Authenticated<Conn, Self>, WireMsg> + Sized where
    Hdl::HandlerError: Into<ListenError<Self, Store, Conn, WireMsg>>,
{
    /// Start the listener task for Subduction.
    #[allow(clippy::type_complexity)]
    fn start_listener<
        Timer: Timeout<Self> + Clone + Send + Sync + 'a,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        Self: Sized,
        'a: 'static;
}

#[future_form(
    Sendable where
        Conn: Connection<Sendable, WireMsg> + PartialEq + Clone + Send + Sync + 'static,
        WireMsg: Encode + Decode + Clone + Send + Sync + core::fmt::Debug + 'static,
        Store: Storage<Sendable> + Send + Sync + 'a,
        Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'a,
        Auth::PutDisallowed: Send + 'static,
        Auth::FetchDisallowed: Send + 'static,
        Sign: Signer<Sendable> + Send + Sync + 'a,
        Metric: DepthMetric + Send + Sync + 'a,
        Hdl: Handler<Sendable, Conn, Message = WireMsg> + RemoteHeadsNotifier + Send + Sync + 'a,
        Hdl::HandlerError: Into<ListenError<Sendable, Store, Conn, WireMsg>> + Send + 'static,
        Store::Error: Send + 'static,
        Conn::DisconnectionError: Send + 'static,
        Conn::RecvError: Send + 'static,
        Conn::SendError: Send + 'static,
    Local where
        Conn: Connection<Local, WireMsg> + PartialEq + Clone + 'static,
        WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
        Store: Storage<Local> + 'a,
        Auth: ConnectionPolicy<Local> + StoragePolicy<Local> + 'a,
        Sign: Signer<Local> + 'a,
        Metric: DepthMetric + 'a,
        Hdl: Handler<Local, Conn, Message = WireMsg> + RemoteHeadsNotifier + 'a,
        Hdl::HandlerError: Into<ListenError<Local, Store, Conn, WireMsg>>
)]
impl<'a, Async: FutureForm, Conn, Store, WireMsg, Hdl, Auth, Sign, Metric, const SHARDS: usize>
    StartListener<'a, Store, Conn, WireMsg, Hdl, Auth, Sign, Metric, SHARDS> for Async
where
    Hdl: Handler<Async, Conn, Message = WireMsg> + RemoteHeadsNotifier,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, WireMsg>>,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + From<SyncMessage> + 'static,
{
    fn start_listener<
        Timer: Timeout<Self> + Clone + Send + Sync + 'a,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        'a: 'static,
    {
        Abortable::new(
            Async::from_future(async move {
                if let Err(e) = subduction.listen().await {
                    tracing::info!(error = %e, "subduction listener disconnected");
                }
            }),
            abort_reg,
        )
    }
}

/// Result of a single document's sync, as sent back through the fan-out
/// channel by a spawned [`SpawnDocSync`] task.
#[allow(clippy::type_complexity)]
pub type DocSyncResult<Async, Store, Conn, WireMsg> = (
    SedimentreeId,
    Result<
        (
            bool,
            SyncStats,
            Vec<(
                Authenticated<Conn, Async>,
                CallError<<Conn as Connection<Async, WireMsg>>::SendError>,
            )>,
        ),
        IoError<Async, Store, Conn, WireMsg>,
    >,
);

/// The completion outcome a spawned dispatch task reports to the listener,
/// specialized for a handler `Hdl`. A module-local alias (like [`DocSyncResult`])
/// so the `#[future_form]` macro can name it in the generated signatures.
pub type DispatchOutcomeFor<Conn, Async, Hdl> =
    DispatchOutcome<Conn, Async, <Hdl as Handler<Async, Conn>>::HandlerError>;

/// Builds the `'static` future that syncs one document and reports the result
/// back through a channel — the per-task unit of the cold-start fan-out in
/// [`Subduction::full_sync_with_peer`].
///
/// This exists as a `#[future_form]`-split trait (mirroring [`StartListener`])
/// solely so the per-task future can be constructed via `Async::from_future`
/// with the correct `Send` bounds on `Sendable` and no `Send` requirement on
/// `Local`. The bounded-channel driver loop itself stays in the generic method.
pub trait SpawnDocSync<
    'a,
    Store: Storage<Self>,
    Conn: Connection<Self, WireMsg> + PartialEq + 'a,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    Hdl: Handler<Self, Conn, Message = WireMsg> + RemoteHeadsNotifier,
    Auth: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sign: Signer<Self>,
    Metric: DepthMetric,
    const SHARDS: usize,
>: FutureForm + RunManager<Authenticated<Conn, Self>, WireMsg> + Sized
{
    /// Construct the spawnable future for syncing a single document.
    #[allow(clippy::type_complexity)]
    fn doc_sync_task<
        Timer: Timeout<Self> + Clone + Send + Sync + 'a,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        peer_id: PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
        sender: async_channel::Sender<DocSyncResult<Self, Store, Conn, WireMsg>>,
    ) -> Self::Future<'static, ()>
    where
        'a: 'static,
        ManagedConnection<Conn, Self, Timer>:
            ManagedCall<Self, WireMsg, SendError = <Conn as Connection<Self, WireMsg>>::SendError>;

    /// Construct the spawnable future for handling one inbound message in the
    /// listen loop. Runs `handler.handle`, then the subscription-propagation
    /// gate, then reports `(conn, result)` back through `sender` so the
    /// listener's cleanup arm can tear down a broken connection on error.
    #[allow(clippy::type_complexity)]
    fn dispatch_task<
        Timer: Timeout<Self> + Clone + Send + Sync + 'static,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'static, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        handler: Arc<Hdl>,
        conn: Authenticated<Conn, Self>,
        msg: WireMsg,
        propagate: Option<(SedimentreeId, PeerId)>,
        sender: async_channel::Sender<DispatchOutcomeFor<Conn, Self, Hdl>>,
        permit: SemaphoreGuardArc,
    ) -> Self::Future<'static, ()>;
}

#[future_form(
    Sendable where
        Conn: Connection<Sendable, WireMsg> + PartialEq + Clone + Send + Sync + 'static,
        WireMsg: Encode + Decode + Clone + Send + Sync + core::fmt::Debug + From<SyncMessage> + 'static,
        Store: Storage<Sendable> + Send + Sync + 'static,
        Store::Error: Send + 'static,
        Auth: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'static,
        Auth::PutDisallowed: Send + 'static,
        Auth::FetchDisallowed: Send + 'static,
        Sign: Signer<Sendable> + Send + Sync + 'static,
        Metric: DepthMetric + Send + Sync + 'static,
        Hdl: Handler<Sendable, Conn, Message = WireMsg> + RemoteHeadsNotifier + Send + Sync + 'static,
        Hdl::HandlerError: Into<ListenError<Sendable, Store, Conn, WireMsg>> + Send + 'static,
        Conn::DisconnectionError: Send + 'static,
        Conn::RecvError: Send + 'static,
        Conn::SendError: Send + 'static,
    Local where
        Conn: Connection<Local, WireMsg> + PartialEq + Clone + 'static,
        WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + From<SyncMessage> + 'static,
        Store: Storage<Local> + 'static,
        Auth: ConnectionPolicy<Local> + StoragePolicy<Local> + 'static,
        Sign: Signer<Local> + 'static,
        Metric: DepthMetric + 'static,
        Hdl: Handler<Local, Conn, Message = WireMsg> + RemoteHeadsNotifier + 'static,
        Hdl::HandlerError: Into<ListenError<Local, Store, Conn, WireMsg>>
)]
impl<'a, Async: FutureForm, Conn, Store, WireMsg, Hdl, Auth, Sign, Metric, const SHARDS: usize>
    SpawnDocSync<'a, Store, Conn, WireMsg, Hdl, Auth, Sign, Metric, SHARDS> for Async
where
    Hdl: Handler<Async, Conn, Message = WireMsg> + RemoteHeadsNotifier,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, WireMsg>>,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + From<SyncMessage> + 'static,
{
    fn doc_sync_task<
        Timer: Timeout<Self> + Clone + Send + Sync + 'a,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        peer_id: PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: CallTimeout,
        sender: async_channel::Sender<DocSyncResult<Self, Store, Conn, WireMsg>>,
    ) -> Self::Future<'static, ()>
    where
        'a: 'static,
        ManagedConnection<Conn, Self, Timer>:
            ManagedCall<Self, WireMsg, SendError = <Conn as Connection<Self, WireMsg>>::SendError>,
    {
        Async::from_future(async move {
            let result = subduction
                .sync_with_peer(&peer_id, id, subscribe, timeout)
                .await;
            // Always send a result (even an error) so the driver's drain count
            // stays exact and a per-document failure can't wedge the loop. If
            // the receiver is already gone (driver dropped), there's nothing to
            // report to — discard.
            sender.send((id, result)).await.ok();
        })
    }

    fn dispatch_task<
        Timer: Timeout<Self> + Clone + Send + Sync + 'static,
        Sp: Spawn<Self> + Clone + Send + Sync + 'static,
    >(
        subduction: Arc<
            Subduction<'static, Self, Store, Conn, Hdl, Auth, Sign, Timer, Sp, Metric, SHARDS>,
        >,
        handler: Arc<Hdl>,
        conn: Authenticated<Conn, Self>,
        msg: WireMsg,
        propagate: Option<(SedimentreeId, PeerId)>,
        sender: async_channel::Sender<DispatchOutcomeFor<Conn, Self, Hdl>>,
        permit: SemaphoreGuardArc,
    ) -> Self::Future<'static, ()> {
        Async::from_future(async move {
            let _permit = permit;

            let mut completion = DispatchCompletion::new(sender);

            let result = handler.handle(&conn, msg).await;
            if result.is_ok()
                && let Some((sed_id, originator)) = propagate
            {
                let sd = Arc::clone(&subduction);
                let _propagation = subduction.spawner.spawn(Async::from_future(async move {
                    if sd
                        .storage
                        .policy()
                        .authorize_fetch(originator, sed_id)
                        .await
                        .is_ok()
                    {
                        sd.propagate_subscription(sed_id, originator).await;
                    }
                }));
            }

            completion.complete(conn, result);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        collections::bounded_sharded_map::BoundedShardedMap,
        connection::test_utils::{
            FailingSendMockConnection, InstantTimeout, TestSpawn, test_signer,
        },
        handler::sync::SyncHandler,
        nonce_cache::NonceCache,
        policy::open::OpenPolicy,
        storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
    };
    use alloc::collections::BTreeSet;
    use async_lock::Mutex;
    use future_form::Sendable;
    use sedimentree_core::{
        blob::Blob,
        collections::Map,
        depth::CountLeadingZeroBytes,
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
    };
    use testresult::TestResult;

    fn make_commit_parts() -> (CommitId, BTreeSet<CommitId>, Blob) {
        let contents = vec![0u8; 32];
        let blob = Blob::new(contents);
        (CommitId::new([0xCC; 32]), BTreeSet::new(), blob)
    }

    #[allow(clippy::type_complexity)]
    fn make_fragment_parts() -> (CommitId, BTreeSet<CommitId>, Vec<CommitId>, Blob) {
        let contents = vec![0u8; 32];
        let blob = Blob::new(contents);
        let head = CommitId::new([1u8; 32]);
        let boundary = BTreeSet::from([CommitId::new([2u8; 32])]);
        let checkpoints = vec![CommitId::new([3u8; 32])];
        (head, boundary, checkpoints, blob)
    }

    /// Keystone invariant for the in-RAM sedimentree LRU cache: a tree must
    /// be fully reconstructable from durable storage alone, with no help
    /// from the in-RAM map.
    ///
    /// Storage is the source of truth; the in-RAM
    /// [`BoundedShardedMap`](crate::collections::bounded_sharded_map::BoundedShardedMap)
    /// is a cache. Every write persists to storage *before* (or independent
    /// of) the in-RAM mutation, so dropping a tree from RAM and reloading it
    /// from storage is lossless. This test pins that invariant — if a future
    /// change ever leaves data only in the in-RAM tree, eviction would
    /// silently lose it and this test fails.
    ///
    /// Procedure: write commits + fragments via the public store API,
    /// snapshot the resident (minimized) tree, evict it from the in-RAM map
    /// *only* (`BoundedShardedMap::remove` does not touch storage), rebuild
    /// from storage exactly as hydration does, and assert equality.
    #[tokio::test]
    async fn tree_is_reconstructable_from_storage_alone() -> TestResult {
        let sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree>> =
            Arc::new(BoundedShardedMap::with_key(0, 0));
        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
        let handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            storage.clone(),
            CountLeadingZeroBytes,
            TestSpawn,
        ));

        let (subduction, _listener_fut, _actor_fut) = Subduction::<
            '_,
            Sendable,
            _,
            FailingSendMockConnection,
            _,
            _,
            _,
            InstantTimeout,
            _,
        >::new(
            handler,
            None,
            test_signer(),
            sedimentrees.clone(),
            connections,
            subscriptions,
            storage.clone(),
            PeerCounter::default(),
            NonceCache::default(),
            InstantTimeout,
            Duration::from_secs(30),
            CountLeadingZeroBytes,
            TestSpawn,
        );

        let id = SedimentreeId::new([0x5A; 32]);

        // Write a commit and a fragment via the public (persist-only) API.
        let (_chead, cparents, cblob) = make_commit_parts();
        subduction
            .store_commit(id, CommitId::new([0xC0; 32]), cparents, cblob)
            .await?;

        let (fhead, fboundary, fcheckpoints, fblob) = make_fragment_parts();
        subduction
            .store_fragment(id, fhead, fboundary, &fcheckpoints, fblob)
            .await?;

        // Snapshot the resident tree, then evict it from the in-RAM map
        // ONLY (this does not delete anything from storage). The writes ran
        // `minimize_tree`, so the resident wrapper is clean; take its inner
        // (minimal) tree for comparison with the rehydrated minimal form.
        let resident = sedimentrees
            .get_cloned(&id)
            .await
            .ok_or("tree should be resident after writes")?
            .into_tree();
        let evicted = sedimentrees.remove(&id).await;
        assert!(evicted.is_some(), "tree should have been in the map");
        assert!(
            sedimentrees.get_cloned(&id).await.is_none(),
            "tree must be gone from the in-RAM map after eviction"
        );

        // Reconstruct purely from storage, exactly as hydration does.
        let access = storage.hydration_access();
        let loaded_commits: Vec<LooseCommit> = access
            .load_loose_commits::<Sendable>(id)
            .await?
            .into_iter()
            .map(|vm| vm.payload().clone())
            .collect();
        let loaded_fragments: Vec<Fragment> = access
            .load_fragments::<Sendable>(id)
            .await?
            .into_iter()
            .map(|vm| vm.payload().clone())
            .collect();

        assert!(
            !loaded_commits.is_empty() || !loaded_fragments.is_empty(),
            "storage must hold the persisted data after in-RAM eviction"
        );

        let rehydrated =
            Sedimentree::new(loaded_fragments, loaded_commits).minimize(&CountLeadingZeroBytes);

        assert_eq!(
            rehydrated, resident,
            "tree rebuilt from storage must equal the evicted resident tree \
             (storage is the source of truth; eviction must be lossless)"
        );

        Ok(())
    }
}
