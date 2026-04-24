//! The main synchronization logic and bookkeeping for [`Sedimentree`].
//!
//! # API Guide
//!
//! ## Connection Management
//!
//! | Method | Description |
//! |--------|-------------|
//! | [`add_connection`] | Add a connection (no automatic sync) |
//! | [`remove_connection`] | Remove a connection from tracking |
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
//! [`remove_connection`]: Subduction::remove_connection
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
pub mod pending_blob_requests;
pub mod request;

pub(crate) mod ingest;
pub(crate) mod peers;

use crate::{
    authenticated::Authenticated,
    connection::{
        Connection,
        backoff::Backoff,
        id::ConnectionId,
        managed::{ManagedCall, ManagedConnection},
        manager::{Command, ConnectionManager, RunManager, Spawn},
        message::{
            BatchSyncRequest, BatchSyncResponse, DataRequestRejected, RequestedData, SyncDiff,
            SyncMessage, SyncResult, TryAsBatchSyncResponse,
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
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
    sync_session::{DynSyncSessionObserver, SyncSession, SyncSessionKind},
    timeout::Timeout,
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
    AddConnectionError, IoError, ListenError, SendRequestedDataError, Unauthorized, WriteError,
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
    codec::{decode::Decode, encode::Encode},
    collections::{
        Entry, Map, Set,
        nonempty_ext::{NonEmptyExt, RemoveResult},
    },
    commit::CountLeadingZeroBytes,
    crypto::{digest::Digest, fingerprint::FingerprintSeed},
    depth::{Depth, DepthMetric},
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintResolver, Sedimentree},
};
use subduction_crypto::{
    signed::Signed, signer::Signer, verified_author::VerifiedAuthor, verified_meta::VerifiedMeta,
};

use pending_blob_requests::PendingBlobRequests;

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
    Metric: DepthMetric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> {
    handler: Arc<Hdl>,
    signer: Sign,
    discovery_id: Option<DiscoveryId>,

    timer: Timer,
    depth_metric: Metric,
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, SHARDS>>,
    storage: StoragePowerbox<Store, Auth>,

    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,

    /// Per-connection multiplexers for request-response correlation.
    ///
    /// Keyed by peer ID, with one [`Multiplexer`] per connection (matching
    /// the order in [`connections`](Self::connections)). Used by
    /// [`sync_with_peer`](Self::sync_with_peer) to make roundtrip calls
    /// and by the listen loop to route [`BatchSyncResponse`] messages.
    multiplexers: Arc<Mutex<Map<PeerId, Vec<Arc<Multiplexer>>>>>,

    /// Default timeout for roundtrip calls (`BatchSyncRequest` → `BatchSyncResponse`).
    default_call_timeout: Duration,

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

    /// Shared monotonic counter for outgoing messages, per peer.
    ///
    /// Shared with all handlers so that every message to a given peer
    /// draws from the same monotonic sequence regardless of which handler
    /// produced it.
    send_counter: PeerCounter,
    sync_session_observer: Arc<Mutex<Option<DynSyncSessionObserver>>>,

    manager_channel: Sender<Command<Authenticated<Conn, Async>>>,
    msg_queue: async_channel::Receiver<(Authenticated<Conn, Async>, Hdl::Message)>,
    response_queue: async_channel::Receiver<(Authenticated<Conn, Async>, Hdl::Message)>,
    connection_closed: async_channel::Receiver<(ConnectionId, Authenticated<Conn, Async>)>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a Async>,
}

/// A single fragment for [`Subduction::add_fragments_batch`].
#[derive(Debug, Clone)]
pub struct FragmentBatchItem {
    /// The head commit of the fragment.
    pub head: CommitId,
    /// The boundary commits (fragment edges).
    pub boundary: BTreeSet<CommitId>,
    /// Checkpoint digests within the fragment.
    pub checkpoints: Vec<CommitId>,
    /// The blob containing the fragment's data.
    pub blob: Blob,
}

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS> + 'static,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
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
    /// `subscriptions`, `storage`, `pending_blob_requests`) and the `handler`
    /// externally, then passes them in. This lets the handler hold its own
    /// `Arc` clones of whatever shared state it needs.
    ///
    /// For the standard sync protocol, pass a [`SyncHandler`] constructed
    /// from the same `Arc`s.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sedimentrees = Arc::new(ShardedMap::new());
    /// let connections = Arc::new(Mutex::new(Map::new()));
    /// let subscriptions = Arc::new(Mutex::new(Map::new()));
    /// let storage = StoragePowerbox::new(storage, Arc::new(policy));
    /// let pending = Arc::new(Mutex::new(PendingBlobRequests::new(1024)));
    ///
    /// let handler = Arc::new(SyncHandler::new(
    ///     sedimentrees.clone(),
    ///     connections.clone(),
    ///     subscriptions.clone(),
    ///     storage.clone(),
    ///     pending.clone(),
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
    ///     pending,
    ///     nonce_cache,
    ///     depth_metric,
    ///     spawner,
    /// );
    /// ```
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn new<Sp: Spawn<Async> + Send + Sync + 'static>(
        handler: Arc<Hdl>,
        discovery_id: Option<DiscoveryId>,
        signer: Sign,
        sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, SHARDS>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<Store, Auth>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        send_counter: PeerCounter,
        nonce_cache: NonceCache,
        timer: Timer,
        default_call_timeout: Duration,
        depth_metric: Metric,
        spawner: Sp,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>,
        crate::connection::manager::ManagerFuture<Async>,
    )
    where
        Async: StartListener<'a, Store, Conn, Hdl::Message, Hdl, Auth, Sign, Metric, SHARDS>,
        Timer: Send + Sync + 'a,
    {
        tracing::info!("initializing Subduction instance");

        let (manager_sender, manager_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(2048);
        let (response_sender, response_receiver) = async_channel::bounded(8192);
        let (closed_sender, closed_receiver) = async_channel::bounded(32);
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
            default_call_timeout,
            depth_metric,
            sedimentrees,
            connections,
            multiplexers: Arc::new(Mutex::new(Map::new())),
            subscriptions,
            storage,
            nonce_tracker: Arc::new(nonce_cache),
            reconnect_backoff: Arc::new(Mutex::new(Map::new())),
            outgoing_subscriptions: Arc::new(Mutex::new(Map::new())),
            pending_blob_requests,
            send_counter,
            sync_session_observer: Arc::new(Mutex::new(None)),
            manager_channel: manager_sender,
            msg_queue: queue_receiver,
            response_queue: response_receiver,
            connection_closed: closed_receiver,
            abort_manager_handle,
            abort_listener_handle,
            _phantom: PhantomData,
        });

        let manager_fut = manager.run();
        let abortable_manager = Abortable::new(manager_fut, abort_manager_reg);

        (
            sd.clone(),
            ListenerFuture::<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>::new(
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

    /// Returns a reference to the sedimentrees map.
    ///
    /// This is only available with the `test_utils` feature or in tests.
    #[cfg(any(feature = "test_utils", test))]
    #[must_use]
    pub const fn sedimentrees(&self) -> &Arc<ShardedMap<SedimentreeId, Sedimentree, SHARDS>> {
        &self.sedimentrees
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

    /// Listen for incoming messages and dispatch them through the handler.
    ///
    /// This method runs indefinitely, processing messages as they arrive.
    /// If no peers are connected, it will wait until a peer connects.
    ///
    /// Dispatches messages concurrently using [`FuturesUnordered`], which
    /// significantly improves throughput when handling many independent
    /// requests (e.g., batch sync requests for different sedimentrees).
    ///
    /// The handler stored on this instance receives each decoded message
    /// and decides what to do with it. For the standard sync protocol,
    /// this is a [`SyncHandler`].
    ///
    /// # Errors
    ///
    /// * Returns `ListenError` if a handler error signals a broken connection.
    #[allow(clippy::too_many_lines)]
    pub async fn listen(
        self: Arc<Self>,
    ) -> Result<(), ListenError<Async, Store, Conn, Hdl::Message>> {
        tracing::info!("starting Subduction listener with concurrent dispatch");

        let handler = &self.handler;
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

        loop {
            futures::select_biased! {
                // 1st priority: drain completed handler tasks
                result = in_flight.select_next_some() => {
                    #[allow(clippy::type_complexity)]
                    let (conn, dispatch_result): (Authenticated<Conn, Async>, Result<(), Hdl::HandlerError>) = result;
                    if let Err(e) = dispatch_result {
                        let peer_id = conn.peer_id();
                        tracing::error!(
                            peer = %peer_id,
                            "error dispatching message: {e}"
                        );
                        // Connection is broken — remove from conns map.
                        if self.remove_connection(&conn).await == Some(true) {
                            handler.on_peer_disconnect(peer_id).await;
                            self.send_counter.clear_peer(&peer_id).await;
                        }
                        tracing::warn!("removed failed connection from peer {}", peer_id);
                    }
                }

                // 2nd priority: route responses to complete our pending sync
                // calls. Prioritized over incoming requests because completing
                // round trips frees resources and unblocks callers. Response
                // volume is self-limiting (bounded by our in-flight request count).
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
                                        tracing::debug!(
                                            "routed BatchSyncResponse to pending caller for peer {}",
                                            peer_id
                                        );
                                        consumed = true;
                                        break;
                                    }
                                }
                            }

                            if !consumed {
                                tracing::warn!(
                                    "BatchSyncResponse from peer {peer_id} had no pending caller"
                                );
                            }
                        } else {
                            tracing::warn!(
                                "non-BatchSyncResponse message from peer {peer_id} \
                                 arrived on response_queue — dropping"
                            );
                        }
                    } else {
                        tracing::info!("response queue closed");
                        break;
                    }
                }

                // 3rd priority: accept new requests from peers. Lower priority
                // than responses because incoming requests can wait (bounded by
                // msg_queue backpressure) while our callers are actively blocked
                // on response routing.
                msg_result = self.msg_queue.recv().fuse() => {
                    if let Ok((conn, msg)) = msg_result {
                        let peer_id = conn.peer_id();
                        tracing::debug!(
                            "Subduction listener received message from peer {}: {:?}",
                            peer_id,
                            msg
                        );

                        // Safety net: if a BatchSyncResponse ended up in the
                        // request queue (should go through response_queue),
                        // route it to the multiplexer rather than the handler.
                        if let Some(resp) = msg.try_as_batch_sync_response() {
                            tracing::debug!(
                                "BatchSyncResponse from peer {peer_id} arrived via msg_queue \
                                 (expected response_queue) — routing to multiplexer"
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
                                    "BatchSyncResponse from peer {peer_id} via safety net \
                                     had no pending caller"
                                );
                            }
                            continue;
                        }

                        // Normal path: dispatch to handler
                        let h = handler.clone();
                        let conn_clone = conn.clone();

                        in_flight.push(async move {
                            let result = h.handle(&conn_clone, msg).await;
                            (conn_clone, result)
                        });
                    } else {
                        tracing::info!("SyncMessage queue closed");
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

                // 4th priority: handle closed connections
                closed_result = self.connection_closed.recv().fuse() => {
                    if let Ok((conn_id, conn)) = closed_result {
                        let peer_id = conn.peer_id();
                        tracing::warn!(
                            "Connection {conn_id} from peer {peer_id} closed, removing"
                        );
                        if self.remove_connection(&conn).await == Some(true) {
                            handler.on_peer_disconnect(peer_id).await;
                            self.send_counter.clear_peer(&peer_id).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /***************
     * CONNECTIONS *
     ***************/

    /// Gracefully shut down the manager and listener loops by closing
    /// the channels they read from. Unlike [`Drop`] (which aborts mid-
    /// await), the listener drains its in-flight `Handler::handle`
    /// `FuturesUnordered` before exiting. Idempotent.
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
        tracing::info!("Disconnecting connection from peer {}", peer_id);

        let mut connections = self.connections.lock().await;
        if let Some(peer_conns) = connections.remove(&peer_id) {
            match peer_conns.remove_item(conn) {
                RemoveResult::Removed(remaining) => {
                    // Put the remaining connections back
                    connections.insert(peer_id, remaining);

                    #[cfg(feature = "metrics")]
                    crate::metrics::connection_closed();

                    conn.disconnect().await.map(|()| true)
                }
                RemoveResult::WasLast(_) => {
                    // Don't put anything back, peer entry stays removed
                    self.send_counter.clear_peer(&peer_id).await;

                    #[cfg(feature = "metrics")]
                    crate::metrics::connection_closed();

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
    /// * Returns [`Conn::DisconnectionError`] if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_all(&self) -> Result<(), Conn::DisconnectionError> {
        let all_conns: Vec<Authenticated<Conn, Async>> = {
            let mut guard = self.connections.lock().await;
            core::mem::take(&mut *guard)
                .into_values()
                .flat_map(NonEmpty::into_iter)
                .collect()
        };
        self.multiplexers.lock().await.clear();

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
        let peer_conns = { self.connections.lock().await.remove(peer_id) };
        self.multiplexers.lock().await.remove(peer_id);

        if let Some(conns) = peer_conns {
            self.send_counter.clear_peer(peer_id).await;

            #[cfg(feature = "metrics")]
            for _ in &conns {
                crate::metrics::connection_closed();
            }

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
        tracing::info!("adding connection from peer {}", peer_id);

        self.storage
            .policy()
            .authorize_connect(peer_id)
            .await
            .map_err(AddConnectionError::ConnectionDisallowed)?;

        {
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
        }

        // Create a multiplexer for request-response correlation
        {
            let mux = Arc::new(Multiplexer::new(peer_id, self.default_call_timeout));
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

    /// Remove a connection from tracking (low-level).
    ///
    /// Does _not_ close the transport. Use [`disconnect`](Self::disconnect)
    /// to gracefully shut down a live connection.
    ///
    /// Uses `NonEmptyExt::remove_item` to handle the three cases:
    /// - Connection not found
    /// - Connection removed, peer still has other connections
    /// - Connection removed, was the last connection for this peer
    ///
    /// Returns `Some(true)` if this was the last connection for the peer,
    /// `Some(false)` if the peer still has connections,
    /// `None` if the connection wasn't found.
    pub async fn remove_connection(&self, conn: &Authenticated<Conn, Async>) -> Option<bool> {
        peers::remove_connection(&self.connections, &self.subscriptions, conn).await
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
        tracing::debug!("Getting local blobs for sedimentree with id {:?}", id);
        let tree = self.sedimentrees.get_cloned(&id).await;
        if tree.is_none() {
            return Ok(None);
        }

        tracing::debug!("Found sedimentree with id {:?}", id);
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
        timeout: Option<Duration>,
    ) -> Result<Option<NonEmpty<Blob>>, IoError<Async, Store, Conn, Hdl::Message>> {
        tracing::debug!("Fetching blobs for sedimentree with id {:?}", id);
        if let Some(maybe_blobs) = self.get_blobs(id).await.map_err(IoError::Storage)? {
            Ok(Some(maybe_blobs))
        } else {
            let tree = self.sedimentrees.get_cloned(&id).await;
            if let Some(tree) = tree {
                let conns = self.all_connections().await;
                for conn in conns {
                    let peer_id = conn.peer_id();
                    let seed = FingerprintSeed::random();
                    let resolver = tree.fingerprint_resolver(&seed);

                    #[allow(clippy::expect_used)]
                    // Invariant: add_connection creates a Multiplexer for every peer
                    let mux = {
                        let muxes = self.multiplexers.lock().await;
                        muxes
                            .get(&peer_id)
                            .and_then(|v| v.first())
                            .cloned()
                            .expect("multiplexer exists for every connected peer")
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
                            .send_requested_data(&conn, id, &resolver, &diff.requesting)
                            .await
                    {
                        if matches!(e, SendRequestedDataError::Unauthorized(_)) {
                            let msg: Hdl::Message =
                                SyncMessage::from(DataRequestRejected { id }).into();
                            if let Err(send_err) = conn.send(&msg).await {
                                tracing::info!(
                                    "peer {} disconnected while sending DataRequestRejected: {send_err}",
                                    conn.peer_id()
                                );
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

    /// Remove a sedimentree locally and delete all associated data from storage.
    ///
    /// # Errors
    ///
    /// * [`IoError`] if a storage or network error occurs.
    pub async fn remove_sedimentree(
        &self,
        id: SedimentreeId,
    ) -> Result<(), IoError<Async, Store, Conn, Hdl::Message>> {
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
        tracing::debug!("adding commit {:?} to sedimentree {:?}", commit_head, id);

        let signed_for_wire = verified_meta.signed().clone();
        let blob = verified_meta.blob().clone();

        self.insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(|e| WriteError::Io(IoError::Storage(e)))?;

        self.minimize_tree(id).await;

        let heads = {
            let shard = self.sedimentrees.get_shard_containing(&id).lock().await;
            shard
                .get(&id)
                .map(|s| s.heads(&self.depth_metric))
                .unwrap_or_default()
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
                tracing::debug!("Propagating commit for sedimentree {:?} to {}", id, peer_id);

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
                        "peer {} disconnected: {}",
                        peer_id,
                        IoError::<Async, Store, Conn, Hdl::Message>::ConnSend(e)
                    );
                    if self.remove_connection(&conn).await == Some(true) {
                        self.send_counter.clear_peer(&peer_id).await;
                    }
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

        self.minimize_tree(id).await;

        let heads = {
            let shard = self.sedimentrees.get_shard_containing(&id).lock().await;
            shard
                .get(&id)
                .map(|s| s.heads(&self.depth_metric))
                .unwrap_or_default()
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
                    "peer {} disconnected: {}",
                    peer_id,
                    IoError::<Async, Store, Conn, Hdl::Message>::ConnSend(e)
                );
                if self.remove_connection(&conn).await == Some(true) {
                    self.send_counter.clear_peer(&peer_id).await;
                }
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
    pub async fn add_commits_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(CommitId, BTreeSet<CommitId>, Blob)>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if commits.is_empty() {
            return Ok(());
        }

        let putter = self.storage.local_putter::<Async>(id);
        let count = commits.len();
        tracing::info!("bulk-inserting {count} commits into sedimentree {id:?}");

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

        self.sedimentrees
            .with_entry_or_default(id, |tree| {
                for commit in commit_payloads {
                    tree.add_commit(commit);
                }
                *tree = tree.minimize(&self.depth_metric);
            })
            .await;

        tracing::info!("bulk-insert of {count} commits complete, tree minimized");
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
    pub async fn add_fragments_batch(
        &self,
        id: SedimentreeId,
        fragments: Vec<FragmentBatchItem>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if fragments.is_empty() {
            return Ok(());
        }

        let putter = self.storage.local_putter::<Async>(id);
        let count = fragments.len();
        tracing::info!("bulk-inserting {count} fragments into sedimentree {id:?}");

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

        self.sedimentrees
            .with_entry_or_default(id, |tree| {
                for fragment in fragment_payloads {
                    tree.add_fragment(fragment);
                }
                *tree = tree.minimize(&self.depth_metric);
            })
            .await;
        tracing::info!("bulk-insert of {count} fragments complete, tree minimized");
        Ok(())
    }

    /// Bulk-insert already-built [`LooseCommit`] and [`Fragment`] payloads
    /// alongside their blobs into local storage and the in-memory tree only.
    ///
    /// This is the local-only counterpart to
    /// [`add_built_batch`](Self::add_built_batch): it signs each payload,
    /// flushes to storage in a single `save_batch` call, applies the in-memory
    /// updates under one shard lock, and minimizes once — but does *not*
    /// follow up with [`sync_with_all_peers`](Self::sync_with_all_peers). Use
    /// when the caller will trigger sync explicitly (or never), e.g. from a
    /// background ingestion path.
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
    pub async fn add_built_batch_locally(
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
            "bulk-inserting {commit_count} commits and {fragment_count} fragments \
             into sedimentree {id:?} (no broadcast)"
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

        self.sedimentrees
            .with_entry_or_default(id, |tree| {
                for commit in commit_payloads {
                    tree.add_commit(commit);
                }
                for fragment in fragment_payloads {
                    tree.add_fragment(fragment);
                }
                *tree = tree.minimize(&self.depth_metric);
            })
            .await;

        tracing::info!(
            "bulk-insert of {commit_count} commits and {fragment_count} fragments \
             complete, tree minimized"
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

    /// Find blobs from connected peers for a specific sedimentree.
    pub async fn request_blobs(&self, id: SedimentreeId, digests: Vec<Digest<Blob>>) {
        {
            let mut pending = self.pending_blob_requests.lock().await;
            for digest in &digests {
                pending.insert(id, *digest);
            }
        }

        let msg: Hdl::Message = SyncMessage::BlobsRequest { id, digests }.into();
        let conns = self.all_connections().await;
        for conn in conns {
            let peer_id = conn.peer_id();
            if let Err(e) = conn.send(&msg).await {
                tracing::warn!("peer {peer_id} disconnected: {e}");
                if self.remove_connection(&conn).await == Some(true) {
                    self.send_counter.clear_peer(&peer_id).await;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sync methods — require the handler to implement `RemoteHeadsNotifier`
// so that `responder_heads` from `BatchSyncResponse` can be surfaced.
// ---------------------------------------------------------------------------

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS> + 'static,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn> + RemoteHeadsNotifier,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Hdl::Message: From<SyncMessage>,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
    ManagedConnection<Conn, Async, Timer>: ManagedCall<
            Async,
            Hdl::Message,
            SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
        >,
{
    /// Add a new sedimentree locally and propagate it to all connected peers.
    ///
    /// # Errors
    ///
    /// * [`WriteError::Io`] if a storage or network error occurs.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_sedimentree(
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

        self.sync_with_all_peers(id, true, None).await?;
        Ok(())
    }

    /// Bulk-insert already-built [`LooseCommit`] and [`Fragment`] payloads
    /// alongside their blobs, signing each, then minimize once and broadcast.
    ///
    /// This is the underlying "do everything in one round-trip" entrypoint used
    /// by callers that already have payloads in their canonical, post-truncation
    /// form (notably the Wasm bindings, which carry [`Fragment`] values whose
    /// checkpoints are already truncated). Unlike
    /// [`add_commits_batch`](Self::add_commits_batch) and
    /// [`add_fragments_batch`](Self::add_fragments_batch), this method also
    /// performs a single [`sync_with_all_peers`](Self::sync_with_all_peers) at
    /// the end so callers don't need to follow up with a separate broadcast.
    ///
    /// Commits are inserted before fragments. Passing two empty vectors is a
    /// no-op (no minimize, no broadcast).
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
    /// Per-peer transport failures during the trailing broadcast are *not*
    /// surfaced as `Err`. [`sync_with_all_peers`](Self::sync_with_all_peers)
    /// reports those out-of-band in its `Ok` value (a per-peer map of
    /// successes and per-connection [`CallError`](crate::connection::managed::CallError)s);
    /// this method discards that map. If a caller needs to observe peer-level
    /// sync outcomes, prefer driving the local insert via
    /// [`add_built_batch_locally`](Self::add_built_batch_locally) and calling
    /// [`sync_with_all_peers`](Self::sync_with_all_peers) directly.
    ///
    /// Note: `WriteError::PutDisallowed` is unreachable for local writes
    /// (the node trusts itself via [`local_putter`](crate::storage::powerbox::StoragePowerbox::local_putter)).
    pub async fn add_built_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(LooseCommit, Blob)>,
        fragments: Vec<(Fragment, Blob)>,
    ) -> Result<(), WriteError<Async, Store, Conn, Hdl::Message, Auth::PutDisallowed>> {
        if commits.is_empty() && fragments.is_empty() {
            return Ok(());
        }

        // Storage write first (cancel-safety: storage is the source of truth;
        // a cancel between this and the broadcast self-heals on rehydrate).
        self.add_built_batch_locally(id, commits, fragments).await?;
        self.sync_with_all_peers(id, true, None).await?;
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
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    #[allow(clippy::too_many_lines)]
    pub async fn sync_with_peer(
        &self,
        to_ask: &PeerId,
        id: SedimentreeId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> Result<
        (
            bool,
            SyncStats,
            Vec<(
                Authenticated<Conn, Async>,
                crate::connection::managed::CallError<
                    <Conn as Connection<Async, Hdl::Message>>::SendError,
                >,
            )>,
        ),
        IoError<Async, Store, Conn, Hdl::Message>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from peer {:?}",
            id,
            to_ask
        );

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
            tracing::info!("Using connection to peer {}", to_ask);
            let mut session = SyncSession::new(id, conn.peer_id(), SyncSessionKind::OutboundBatch);
            let seed = FingerprintSeed::random();
            let resolver = self.sedimentrees.get_cloned(&id).await.map_or_else(
                || Sedimentree::default().fingerprint_resolver(&seed),
                |t| t.fingerprint_resolver(&seed),
            );

            tracing::debug!(
                "Sending fingerprint summary for {:?}: {} commit fps, {} fragment fps",
                id,
                resolver.summary().commit_fingerprints().len(),
                resolver.summary().fragment_fingerprints().len()
            );

            #[allow(clippy::expect_used)]
            // Invariant: add_connection creates a Multiplexer for every peer
            let mux = {
                let muxes = self.multiplexers.lock().await;
                muxes
                    .get(to_ask)
                    .and_then(|v| v.first())
                    .cloned()
                    .expect("multiplexer exists for every connected peer")
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
                    // Track counts for stats
                    let commits_to_receive = missing_commits.len();
                    let fragments_to_receive = missing_fragments.len();

                    // Cache putters by author to avoid redundant policy checks.
                    let mut putter_cache: Map<PeerId, Putter<Async, Store>> = Map::new();

                    for (signed_commit, blob) in missing_commits {
                        let verified = match signed_commit.try_verify() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("sync commit signature verification failed: {e}");
                                continue;
                            }
                        };
                        let commit_id = verified.payload().head();
                        let verified_meta = match VerifiedMeta::new(verified, blob) {
                            Ok(vm) => vm,
                            Err(e) => {
                                tracing::warn!("sync commit blob mismatch: {e}");
                                continue;
                            }
                        };
                        let author = verified_meta.verified_author();
                        let author_id = PeerId::from(*author.verifying_key());
                        let putter = match putter_cache.entry(author_id) {
                            Entry::Occupied(e) => e.into_mut(),
                            Entry::Vacant(e) => {
                                match self.storage.get_putter::<Async>(*to_ask, author, id).await {
                                    Ok(p) => e.insert(p),
                                    Err(err) => {
                                        tracing::warn!(
                                            "policy rejected sync commit from peer {:?} (author {:?}) for sedimentree {:?}: {err}",
                                            to_ask,
                                            author,
                                            id
                                        );
                                        continue;
                                    }
                                }
                            }
                        };
                        let was_new = self
                            .insert_commit_locally(putter, verified_meta)
                            .await
                            .map_err(IoError::Storage)?;
                        if was_new {
                            session.received_commit_ids.push(commit_id);
                        }
                    }

                    for (signed_fragment, blob) in missing_fragments {
                        let verified = match signed_fragment.try_verify() {
                            Ok(v) => v,
                            Err(e) => {
                                tracing::warn!("sync fragment signature verification failed: {e}");
                                continue;
                            }
                        };
                        let fragment_id = verified.payload().head();
                        let verified_meta = match VerifiedMeta::new(verified, blob) {
                            Ok(vm) => vm,
                            Err(e) => {
                                tracing::warn!("sync fragment blob mismatch: {e}");
                                continue;
                            }
                        };
                        let author = verified_meta.verified_author();
                        let author_id = PeerId::from(*author.verifying_key());
                        let putter = match putter_cache.entry(author_id) {
                            Entry::Occupied(e) => e.into_mut(),
                            Entry::Vacant(e) => {
                                match self.storage.get_putter::<Async>(*to_ask, author, id).await {
                                    Ok(p) => e.insert(p),
                                    Err(err) => {
                                        tracing::warn!(
                                            "policy rejected sync fragment from peer {:?} (author {:?}) for sedimentree {:?}: {err}",
                                            to_ask,
                                            author,
                                            id
                                        );
                                        continue;
                                    }
                                }
                            }
                        };
                        let was_new = self
                            .insert_fragment_locally(putter, verified_meta)
                            .await
                            .map_err(IoError::Storage)?;
                        if was_new {
                            session.received_fragment_ids.push(fragment_id);
                        }
                    }

                    self.minimize_tree(id).await;

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
                            .send_requested_data(&conn, id, &resolver, &requesting)
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
                                session.sent_commit_ids = sent.commit_ids;
                                session.sent_fragment_ids = sent.fragment_ids;
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

                    self.emit_sync_session(session).await;

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
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    #[allow(clippy::too_many_lines)]
    pub async fn sync_with_all_peers(
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
                Vec<(
                    Authenticated<Conn, Async>,
                    crate::connection::managed::CallError<
                        <Conn as Connection<Async, Hdl::Message>>::SendError,
                    >,
                )>,
            ),
        >,
        IoError<Async, Store, Conn, Hdl::Message>,
    > {
        tracing::info!(
            "Requesting batch sync for sedimentree {:?} from all peers",
            id
        );
        let peers: Map<PeerId, Vec<Authenticated<Conn, Async>>> = {
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
                        let mut session = SyncSession::new(id, conn.peer_id(), SyncSessionKind::OutboundBatch);
                        let seed = FingerprintSeed::random();
                        let resolver = self
                            .sedimentrees
                            .get_cloned(&id)
                            .await
                            .map_or_else(
                                || Sedimentree::default().fingerprint_resolver(&seed),
                                |t| t.fingerprint_resolver(&seed),
                            );

                        #[allow(clippy::expect_used)] // Invariant: add_connection creates a Multiplexer for every peer
                        let mux = {
                            let muxes = self.multiplexers.lock().await;
                            muxes.get(peer_id)
                                .and_then(|v| v.first())
                                .cloned()
                                .expect("multiplexer exists for every connected peer")
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

                                // Cache putters by author to avoid redundant policy checks.
                                let mut putter_cache: Map<PeerId, Putter<Async, Store>> = Map::new();

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
                                    let commit_id = verified.payload().head();
                                    let verified_meta = match VerifiedMeta::new(verified, blob) {
                                        Ok(vm) => vm,
                                        Err(e) => {
                                            tracing::warn!("full sync commit blob mismatch: {e}");
                                            continue;
                                        }
                                    };
                                    let author = verified_meta.verified_author();
                                    let author_id = PeerId::from(*author.verifying_key());
                                    let putter = match putter_cache.entry(author_id) {
                                        Entry::Occupied(e) => e.into_mut(),
                                        Entry::Vacant(e) => {
                                            match self.storage.get_putter::<Async>(*peer_id, author, id).await {
                                                Ok(p) => e.insert(p),
                                                Err(err) => {
                                                    tracing::warn!(
                                                        "policy rejected full sync commit from peer {:?} (author {:?}) for sedimentree {:?}: {err}",
                                                        peer_id, author, id
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                    };
                                    let was_new = self
                                        .insert_commit_locally(putter, verified_meta)
                                        .await
                                        .map_err(IoError::Storage)?;
                                    if was_new {
                                        session.received_commit_ids.push(commit_id);
                                    }
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
                                    let fragment_id = verified.payload().head();
                                    let verified_meta = match VerifiedMeta::new(verified, blob) {
                                        Ok(vm) => vm,
                                        Err(e) => {
                                            tracing::warn!("full sync fragment blob mismatch: {e}");
                                            continue;
                                        }
                                    };
                                    let author = verified_meta.verified_author();
                                    let author_id = PeerId::from(*author.verifying_key());
                                    let putter = match putter_cache.entry(author_id) {
                                        Entry::Occupied(e) => e.into_mut(),
                                        Entry::Vacant(e) => {
                                            match self.storage.get_putter::<Async>(*peer_id, author, id).await {
                                                Ok(p) => e.insert(p),
                                                Err(err) => {
                                                    tracing::warn!(
                                                        "policy rejected full sync fragment from peer {:?} (author {:?}) for sedimentree {:?}: {err}",
                                                        peer_id, author, id
                                                    );
                                                    continue;
                                                }
                                            }
                                        }
                                    };
                                    let was_new = self
                                        .insert_fragment_locally(putter, verified_meta)
                                        .await
                                        .map_err(IoError::Storage)?;
                                    if was_new {
                                        session.received_fragment_ids.push(fragment_id);
                                    }
                                }

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
                                                tracing::warn!("peer {peer_id} disconnected while sending DataRequestRejected: {send_err}");
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
                    tracing::error!("{e}");
                }
                Ok((peer_id, success, stats, errs)) => {
                    out.insert(peer_id, (success, stats, errs));
                }
            }
        }
        Ok(out)
    }

    /// Sync all known [`Sedimentree`]s with a single peer.
    ///
    /// This is the single-peer counterpart of [`full_sync_with_all_peers`](Self::full_sync_with_all_peers).
    /// All sedimentrees are synced **concurrently** using [`FuturesUnordered`],
    /// avoiding head-of-line blocking where a slow document stalls the rest.
    /// The multiplexer supports multiple in-flight requests per connection,
    /// each keyed by a unique [`crate::connection::message::RequestId`].
    ///
    /// Errors are collected rather than short-circuiting, so a failure on one
    /// sedimentree does not prevent the rest from syncing.
    pub async fn full_sync_with_peer(
        &self,
        peer_id: &PeerId,
        subscribe: bool,
        timeout: Option<Duration>,
    ) -> (
        bool,
        SyncStats,
        Vec<(
            Authenticated<Conn, Async>,
            crate::connection::managed::CallError<
                <Conn as Connection<Async, Hdl::Message>>::SendError,
            >,
        )>,
        Vec<(SedimentreeId, IoError<Async, Store, Conn, Hdl::Message>)>,
    ) {
        tracing::info!(
            "Requesting batch sync for all sedimentrees with peer {}",
            peer_id
        );
        let tree_ids = self.sedimentrees.into_keys().await;

        let mut sync_futures: FuturesUnordered<_> = tree_ids
            .into_iter()
            .map(|id| async move {
                let result = self.sync_with_peer(peer_id, id, subscribe, timeout).await;
                (id, result)
            })
            .collect();

        let mut had_success = false;
        let mut stats = SyncStats::new();
        let mut call_errs = Vec::new();
        let mut io_errs = Vec::new();

        while let Some((id, result)) = sync_futures.next().await {
            match result {
                Ok((success, step_stats, step_errs)) => {
                    if success {
                        had_success = true;
                    }
                    stats.commits_received += step_stats.commits_received;
                    stats.fragments_received += step_stats.fragments_received;
                    stats.commits_sent += step_stats.commits_sent;
                    stats.fragments_sent += step_stats.fragments_sent;
                    call_errs.extend(step_errs);
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to sync sedimentree {:?} with peer {}: {}",
                        id,
                        peer_id,
                        e
                    );
                    io_errs.push((id, e));
                }
            }
        }

        (had_success, stats, call_errs, io_errs)
    }

    /// Sync all known [`Sedimentree`]s with all connected peers.
    pub async fn full_sync_with_all_peers(
        &self,
        timeout: Option<Duration>,
    ) -> (
        bool,
        SyncStats,
        Vec<(
            Authenticated<Conn, Async>,
            crate::connection::managed::CallError<
                <Conn as Connection<Async, Hdl::Message>>::SendError,
            >,
        )>,
        Vec<(SedimentreeId, IoError<Async, Store, Conn, Hdl::Message>)>,
    ) {
        tracing::info!("Requesting batch sync for all sedimentrees from all peers");
        let tree_ids = self.sedimentrees.into_keys().await;

        let mut sync_futures: FuturesUnordered<_> = tree_ids
            .into_iter()
            .map(|id| async move {
                tracing::debug!("Requesting batch sync for sedimentree {:?}", id);
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
                    tracing::error!("Failed to sync sedimentree {:?}: {}", id, e);
                    io_errs.push((id, e));
                }
            }
        }

        (had_success, stats, call_errs, io_errs)
    }
}

// ---------------------------------------------------------------------------
// Public utilities and private helpers — no `RemoteHeadsNotifier` bound.
// ---------------------------------------------------------------------------

impl<
    'a,
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS> + 'static,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
where
    Hdl::Message: From<SyncMessage>,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, Hdl::Message>>,
    ManagedConnection<Conn, Async, Timer>: ManagedCall<
            Async,
            Hdl::Message,
            SendError = <Conn as Connection<Async, Hdl::Message>>::SendError,
        >,
{
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

    /// Get the current heads for every locally known sedimentree.
    ///
    /// Walks each shard exactly once, computing heads while holding the
    /// shard's lock (matching the existing pattern in `SyncHandler`).
    /// An inner empty `Vec<CommitId>` means the sedimentree exists but has
    /// no heads yet.
    pub async fn get_all_heads(&self) -> Vec<(SedimentreeId, Vec<CommitId>)> {
        let mut out = Vec::new();
        for idx in self.sedimentrees.shard_indices() {
            if let Some(shard) = self.sedimentrees.shard_at(idx) {
                let guard = shard.lock().await;
                out.reserve(guard.len());
                for (id, tree) in guard.iter() {
                    out.push((*id, tree.heads(&self.depth_metric)));
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
        tracing::debug!("adding sedimentree with id {:?}", id);

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
        self.sedimentrees
            .with_entry_or_default(id, |tree| tree.merge(sedimentree))
            .await;

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
            "sending {} requested commits and {} requested fragments to peer {:?}",
            requesting.commit_fingerprints.len(),
            requesting.fragment_fingerprints.len(),
            peer_id
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
        // so minimize cannot invalidate these lookups.
        let heads = self
            .sedimentrees
            .get_cloned(&id)
            .await
            .unwrap_or_default()
            .heads(&self.depth_metric);

        let requested_commit_ids: Vec<CommitId> = requesting
            .commit_fingerprints
            .iter()
            .filter_map(|fp| {
                let id = resolver.resolve_commit(fp);
                if id.is_none() {
                    // Coverage-only fingerprints (fragment head/boundary) are in
                    // the summary but intentionally not in the resolver. The remote
                    // echoing them back is expected when it doesn't have that CommitId.
                    tracing::debug!("requested commit fingerprint {fp} not found in resolver");
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
                    tracing::warn!("requested fragment fingerprint {fp} not found in resolver");
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
                    tracing::warn!("failed to send requested data: {}", e);
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
    Metric: DepthMetric,
    const SHARDS: usize,
> Drop for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
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
    Metric: DepthMetric,
    const SHARDS: usize,
> ConnectionPolicy<Async>
    for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
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
    Metric: DepthMetric,
    const SHARDS: usize,
> StoragePolicy<Async>
    for Subduction<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
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
    Hdl: Handler<Self, Conn, Message = WireMsg>,
    Auth: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sign: Signer<Self>,
    Metric: DepthMetric,
    const SHARDS: usize,
>: FutureForm + RunManager<Authenticated<Conn, Self>, WireMsg> + Sized where
    Hdl::HandlerError: Into<ListenError<Self, Store, Conn, WireMsg>>,
{
    /// Start the listener task for Subduction.
    #[allow(clippy::type_complexity)]
    fn start_listener<Timer: Timeout<Self> + Clone + Send + Sync + 'a>(
        subduction: Arc<Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        Self: Sized;
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
        Hdl: Handler<Sendable, Conn, Message = WireMsg> + Send + Sync + 'a,
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
        Hdl: Handler<Local, Conn, Message = WireMsg> + 'a,
        Hdl::HandlerError: Into<ListenError<Local, Store, Conn, WireMsg>>
)]
impl<'a, Async: FutureForm, Conn, Store, WireMsg, Hdl, Auth, Sign, Metric, const SHARDS: usize>
    StartListener<'a, Store, Conn, WireMsg, Hdl, Auth, Sign, Metric, SHARDS> for Async
where
    Hdl: Handler<Async, Conn, Message = WireMsg>,
    Hdl::HandlerError: Into<ListenError<Async, Store, Conn, WireMsg>>,
    WireMsg: Encode + Decode + Clone + Send + core::fmt::Debug + From<SyncMessage> + 'static,
{
    fn start_listener<Timer: Timeout<Self> + Clone + Send + Sync + 'a>(
        subduction: Arc<Subduction<'a, Self, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            Async::from_future(async move {
                if let Err(e) = subduction.listen().await {
                    tracing::info!("Subduction listener disconnected: {}", e.to_string());
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
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Metric: DepthMetric = CountLeadingZeroBytes,
    const SHARDS: usize = 256,
> {
    fut: Pin<Box<Abortable<Async::Future<'a, ()>>>>,
    _phantom: PhantomData<(Store, Conn, Hdl, Auth, Sign, Timer, Metric)>,
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
    Metric: DepthMetric,
    const SHARDS: usize,
> ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
{
    /// Create a new [`ListenerFuture`] wrapping the given abortable future.
    pub(crate) fn new(fut: Abortable<Async::Future<'a, ()>>) -> Self {
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
    Async: SubductionFutureForm<'a, Store, Conn, Hdl::Message, Auth, Sign, Metric, SHARDS>,
    Store: Storage<Async>,
    Conn: Connection<Async, Hdl::Message> + PartialEq + 'a,
    Hdl: Handler<Async, Conn>,
    Auth: ConnectionPolicy<Async> + StoragePolicy<Async>,
    Sign: Signer<Async>,
    Timer: Timeout<Async> + Clone,
    Metric: DepthMetric,
    const SHARDS: usize,
> Deref for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
{
    type Target = Abortable<Async::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
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
    Metric: DepthMetric,
    const SHARDS: usize,
> Future for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
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
    Metric: DepthMetric,
    const SHARDS: usize,
> Unpin for ListenerFuture<'a, Async, Store, Conn, Hdl, Auth, Sign, Timer, Metric, SHARDS>
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        connection::test_utils::{
            FailingSendMockConnection, InstantTimeout, TestSpawn, test_signer,
        },
        handler::sync::SyncHandler,
        nonce_cache::NonceCache,
        policy::open::OpenPolicy,
        sharded_map::ShardedMap,
        storage::{memory::MemoryStorage, powerbox::StoragePowerbox},
        subduction::pending_blob_requests::{
            DEFAULT_MAX_PENDING_BLOB_REQUESTS, PendingBlobRequests,
        },
    };
    use alloc::collections::BTreeSet;
    use async_lock::Mutex;
    use future_form::Sendable;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        collections::Map,
        commit::CountLeadingZeroBytes,
        fragment::Fragment,
        id::SedimentreeId,
        loose_commit::{LooseCommit, id::CommitId},
    };
    use subduction_crypto::signed::Signed;
    use testresult::TestResult;

    fn make_commit_parts() -> (CommitId, BTreeSet<CommitId>, Blob) {
        let contents = vec![0u8; 32];
        let blob = Blob::new(contents);
        (CommitId::new([0xCC; 32]), BTreeSet::new(), blob)
    }

    async fn make_signed_test_commit(id: &SedimentreeId) -> (Signed<LooseCommit>, Blob) {
        let (head, parents, blob) = make_commit_parts();
        let blob_meta = BlobMeta::new(&blob);
        let commit = LooseCommit::new(*id, head, parents, blob_meta);
        let verified = Signed::seal::<Sendable, _>(&test_signer(), commit).await;
        (verified.into_signed(), blob)
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

    async fn make_signed_test_fragment(id: &SedimentreeId) -> (Signed<Fragment>, Blob) {
        let (head, boundary, checkpoints, blob) = make_fragment_parts();
        let blob_meta = BlobMeta::new(&blob);
        let fragment = Fragment::new(*id, head, boundary, &checkpoints, blob_meta);
        let verified = Signed::seal::<Sendable, _>(&test_signer(), fragment).await;
        (verified.into_signed(), blob)
    }

    #[tokio::test]
    async fn test_recv_commit_unregisters_connection_on_send_failure() -> TestResult {
        let sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
        let pending = Arc::new(Mutex::new(PendingBlobRequests::new(
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        )));

        let handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            storage.clone(),
            pending.clone(),
            CountLeadingZeroBytes,
        ));

        let (subduction, _listener_fut, _actor_fut) =
            Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
                handler.clone(),
                None,
                test_signer(),
                sedimentrees,
                connections,
                subscriptions,
                storage,
                pending,
                PeerCounter::default(),
                NonceCache::default(),
                InstantTimeout,
                Duration::from_secs(30),
                CountLeadingZeroBytes,
                TestSpawn,
            );

        // Add a failing connection with a different peer ID than the sender
        let sender_peer_id = PeerId::new([1u8; 32]);
        let other_peer_id = PeerId::new([2u8; 32]);
        let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
        let _fresh = subduction.add_connection(conn.authenticated()).await?;
        assert_eq!(subduction.connected_peer_ids().await.len(), 1);

        // Subscribe other_peer to the sedimentree so forwarding will be attempted
        let id = SedimentreeId::new([1u8; 32]);
        subduction.add_subscription(other_peer_id, id).await;

        // Dispatch a commit via the handler from a different peer
        let (signed_commit, blob) = make_signed_test_commit(&id).await;
        let sender_conn = FailingSendMockConnection::with_peer_id(sender_peer_id).authenticated();
        let msg = SyncMessage::LooseCommit {
            id,
            commit: signed_commit,
            blob,
            sender_heads: RemoteHeads::default(),
        };
        let _ = handler.handle(&sender_conn, msg).await;

        // Connection should be removed after send failure during propagation
        assert_eq!(
            subduction.connected_peer_ids().await.len(),
            0,
            "Connection should be removed after send failure"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_recv_fragment_removes_connection_on_send_failure() -> TestResult {
        let sedimentrees = Arc::new(ShardedMap::with_key(0, 0));
        let connections = Arc::new(Mutex::new(Map::new()));
        let subscriptions = Arc::new(Mutex::new(Map::new()));
        let storage = StoragePowerbox::new(MemoryStorage::new(), Arc::new(OpenPolicy));
        let pending = Arc::new(Mutex::new(PendingBlobRequests::new(
            DEFAULT_MAX_PENDING_BLOB_REQUESTS,
        )));

        let handler = Arc::new(SyncHandler::new(
            sedimentrees.clone(),
            connections.clone(),
            subscriptions.clone(),
            storage.clone(),
            pending.clone(),
            CountLeadingZeroBytes,
        ));

        let (subduction, _listener_fut, _actor_fut) =
            Subduction::<'_, Sendable, _, FailingSendMockConnection, _, _, _, InstantTimeout>::new(
                handler.clone(),
                None,
                test_signer(),
                sedimentrees,
                connections,
                subscriptions,
                storage,
                pending,
                PeerCounter::default(),
                NonceCache::default(),
                InstantTimeout,
                Duration::from_secs(30),
                CountLeadingZeroBytes,
                TestSpawn,
            );

        // Add a failing connection with a different peer ID than the sender
        let sender_peer_id = PeerId::new([1u8; 32]);
        let other_peer_id = PeerId::new([2u8; 32]);
        let conn = FailingSendMockConnection::with_peer_id(other_peer_id);
        let _fresh = subduction.add_connection(conn.authenticated()).await?;
        assert_eq!(subduction.connected_peer_ids().await.len(), 1);

        // Subscribe other_peer to the sedimentree so forwarding will be attempted
        let id = SedimentreeId::new([1u8; 32]);
        subduction.add_subscription(other_peer_id, id).await;

        // Dispatch a fragment via the handler from a different peer
        let (signed_fragment, blob) = make_signed_test_fragment(&id).await;
        let sender_conn = FailingSendMockConnection::with_peer_id(sender_peer_id).authenticated();
        let msg = SyncMessage::Fragment {
            id,
            fragment: signed_fragment,
            blob,
            sender_heads: RemoteHeads::default(),
        };
        let _ = handler.handle(&sender_conn, msg).await;

        // Connection should be removed after send failure during propagation
        assert_eq!(
            subduction.connected_peer_ids().await.len(),
            0,
            "Connection should be removed after send failure"
        );

        Ok(())
    }
}
