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
            SyncMessage, SyncResult,
        },
        stats::{SendCount, SyncStats},
    },
    handler::Handler,
    handshake::audience::DiscoveryId,
    multiplexer::Multiplexer,
    nonce_cache::NonceCache,
    peer::id::PeerId,
    policy::{connection::ConnectionPolicy, storage::StoragePolicy},
    remote_heads::{RemoteHeads, RemoteHeadsNotifier},
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
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
        Map, Set,
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + Clone + 'static,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric = CountLeadingZeroBytes,
    const N: usize = 256,
> {
    handler: Arc<H>,
    signer: Sig,
    discovery_id: Option<DiscoveryId>,

    timer: O,
    depth_metric: M,
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    storage: StoragePowerbox<S, P>,

    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,

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
    /// Shared with [`SyncHandler`] so all outgoing heads (from both
    /// `Subduction` methods and the handler's dispatch) use the same
    /// counter sequence.
    send_counter: Arc<Mutex<Map<PeerId, u64>>>,

    manager_channel: Sender<Command<Authenticated<C, F>>>,
    msg_queue: async_channel::Receiver<(Authenticated<C, F>, H::Message)>,
    connection_closed: async_channel::Receiver<(ConnectionId, Authenticated<C, F>)>,

    abort_manager_handle: AbortHandle,
    abort_listener_handle: AbortHandle,

    _phantom: core::marker::PhantomData<&'a F>,
}

/// A single fragment for [`Subduction::add_fragments_batch`].
#[derive(Debug, Clone)]
pub struct FragmentBatchItem {
    /// The head commit of the fragment.
    pub head: Digest<LooseCommit>,
    /// The boundary commits (fragment edges).
    pub boundary: BTreeSet<Digest<LooseCommit>>,
    /// Checkpoint digests within the fragment.
    pub checkpoints: Vec<Digest<LooseCommit>>,
    /// The blob containing the fragment's data.
    pub blob: Blob,
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N> + 'static,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Subduction<'a, F, S, C, H, P, Sig, O, M, N>
where
    H::Message: From<SyncMessage>,
    H::HandlerError: Into<ListenError<F, S, C, H::Message>>,
    ManagedConnection<C, F, O>:
        ManagedCall<F, H::Message, SendError = <C as Connection<F, H::Message>>::SendError>,
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
    pub fn new<Sp: Spawn<F> + Send + Sync + 'static>(
        handler: Arc<H>,
        discovery_id: Option<DiscoveryId>,
        signer: Sig,
        sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<S, P>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        send_counter: Arc<Mutex<Map<PeerId, u64>>>,
        nonce_cache: NonceCache,
        timer: O,
        default_call_timeout: Duration,
        depth_metric: M,
        spawner: Sp,
    ) -> (
        Arc<Self>,
        ListenerFuture<'a, F, S, C, H, P, Sig, O, M, N>,
        crate::connection::manager::ManagerFuture<F>,
    )
    where
        F: StartListener<'a, S, C, H::Message, H, P, Sig, M, N>,
        O: Send + Sync + 'a,
    {
        tracing::info!("initializing Subduction instance");

        let (manager_sender, manager_receiver) = bounded(256);
        let (queue_sender, queue_receiver) = async_channel::bounded(256);
        let (closed_sender, closed_receiver) = async_channel::bounded(32);
        let manager = ConnectionManager::<F, Authenticated<C, F>, H::Message, Sp>::new(
            spawner,
            manager_receiver,
            queue_sender,
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
            ListenerFuture::<'a, F, S, C, H, P, Sig, O, M, N>::new(F::start_listener(
                sd,
                abort_listener_reg,
            )),
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
    pub const fn sedimentrees(&self) -> &Arc<ShardedMap<SedimentreeId, Sedimentree, N>> {
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
    pub async fn listen(self: Arc<Self>) -> Result<(), ListenError<F, S, C, H::Message>> {
        tracing::info!("starting Subduction listener with concurrent dispatch");

        let handler = &self.handler;
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

        loop {
            futures::select_biased! {
                // First priority: handle completed dispatch tasks
                result = in_flight.select_next_some() => {
                    #[allow(clippy::type_complexity)]
                    let (conn, dispatch_result): (Authenticated<C, F>, Result<(), H::HandlerError>) = result;
                    if let Err(e) = dispatch_result {
                        let peer_id = conn.peer_id();
                        tracing::error!(
                            peer = %peer_id,
                            "error dispatching message: {e}"
                        );
                        // Connection is broken — remove from conns map.
                        if self.remove_connection(&conn).await == Some(true) {
                            handler.on_peer_disconnect(peer_id).await;
                        }
                        tracing::info!("removed failed connection from peer {}", peer_id);
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

                        // Route BatchSyncResponse to pending callers before handler dispatch.
                        if let Some(resp) = H::as_batch_sync_response(&msg) {
                            let mut consumed = false;
                            let multiplexers = self.multiplexers.lock().await;
                            if let Some(muxes) = multiplexers.get(&peer_id) {
                                for mux in muxes {
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
                            drop(multiplexers);
                            if consumed {
                                continue;
                            }
                            // Not consumed — fall through to handler dispatch
                        }

                        // Dispatch via handler
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
                // Third: handle closed connections
                closed_result = self.connection_closed.recv().fuse() => {
                    if let Ok((conn_id, conn)) = closed_result {
                        let peer_id = conn.peer_id();
                        tracing::info!(
                            "Connection {conn_id} from peer {peer_id} closed, removing"
                        );
                        if self.remove_connection(&conn).await == Some(true) {
                            handler.on_peer_disconnect(peer_id).await;
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

                    #[cfg(feature = "metrics")]
                    crate::metrics::connection_closed();

                    conn.disconnect().await.map(|()| true)
                }
                RemoveResult::WasLast(_) => {
                    // Don't put anything back, peer entry stays removed

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
    /// * Returns [`C::DisconnectionError`] if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_all(&self) -> Result<(), C::DisconnectionError> {
        let all_conns: Vec<Authenticated<C, F>> = {
            let mut guard = self.connections.lock().await;
            core::mem::take(&mut *guard)
                .into_values()
                .flat_map(NonEmpty::into_iter)
                .collect()
        };
        self.multiplexers.lock().await.clear();

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
    /// * Returns `C::DisconnectionError` if disconnect fails or it occurs ungracefully.
    pub async fn disconnect_from_peer(
        &self,
        peer_id: &PeerId,
    ) -> Result<bool, C::DisconnectionError> {
        let peer_conns = { self.connections.lock().await.remove(peer_id) };
        self.multiplexers.lock().await.remove(peer_id);

        if let Some(conns) = peer_conns {
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
        conn: Authenticated<C, F>,
    ) -> Result<bool, AddConnectionError<P::ConnectionDisallowed>> {
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
    pub async fn remove_connection(&self, conn: &Authenticated<C, F>) -> Option<bool> {
        peers::remove_connection(&self.connections, &self.subscriptions, conn).await
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
    ) -> Vec<Authenticated<C, F>> {
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
    /// * Returns `S::Error` if the storage backend encounters an error.
    pub async fn get_blob(
        &self,
        id: SedimentreeId,
        digest: Digest<Blob>,
    ) -> Result<Option<Blob>, S::Error> {
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
    ///
    /// # Panics
    ///
    /// Panics if a connected peer has no corresponding multiplexer
    /// (internal invariant: `add_connection` always creates one).
    pub async fn fetch_blobs(
        &self,
        id: SedimentreeId,
        timeout: Option<Duration>,
    ) -> Result<Option<NonEmpty<Blob>>, IoError<F, S, C, H::Message>> {
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
                    let summary = tree.fingerprint_summarize(&seed);

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
                        responder_heads: _,
                    } = ManagedCall::<F, H::Message>::call(
                        &managed,
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
                            let msg: H::Message =
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
    ) -> Result<(), IoError<F, S, C, H::Message>> {
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

    /// Get the next send counter value for a given peer, incrementing it atomically.
    async fn next_send_counter(&self, peer: PeerId) -> u64 {
        let mut counters = self.send_counter.lock().await;
        let counter = counters.entry(peer).or_insert(0);
        *counter += 1;
        *counter
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
    ) -> Result<Option<FragmentRequested>, WriteError<F, S, C, H::Message, P::PutDisallowed>> {
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

        self.minimize_tree(id).await;

        let heads = self
            .sedimentrees
            .get_cloned(&id)
            .await
            .map(|s| s.heads(&self.depth_metric))
            .unwrap_or_default();
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

                let msg: H::Message = SyncMessage::LooseCommit {
                    id,
                    commit: signed_for_wire.clone(),
                    blob: blob.clone(),
                    sender_heads: RemoteHeads {
                        counter: self.next_send_counter(peer_id).await,
                        heads: heads.clone(),
                    },
                }
                .into();

                if let Err(e) = conn.send(&msg).await {
                    tracing::info!(
                        "peer {} disconnected: {}",
                        peer_id,
                        IoError::<F, S, C, H::Message>::ConnSend(e)
                    );
                    self.remove_connection(&conn).await;
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
    ) -> Result<(), WriteError<F, S, C, H::Message, P::PutDisallowed>> {
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

        let heads = self
            .sedimentrees
            .get_cloned(&id)
            .await
            .map(|s| s.heads(&self.depth_metric))
            .unwrap_or_default();

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

            let msg: H::Message = SyncMessage::Fragment {
                id,
                fragment: signed_for_wire.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads {
                    counter: self.next_send_counter(peer_id).await,
                    heads: heads.clone(),
                },
            }
            .into();

            if let Err(e) = conn.send(&msg).await {
                tracing::info!(
                    "peer {} disconnected: {}",
                    peer_id,
                    IoError::<F, S, C, H::Message>::ConnSend(e)
                );
                self.remove_connection(&conn).await;
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
    /// * [`WriteError::Io`] if a storage error occurs.
    /// * [`WriteError::PutDisallowed`] if the storage policy rejects the write.
    pub async fn add_commits_batch(
        &self,
        id: SedimentreeId,
        commits: Vec<(BTreeSet<Digest<LooseCommit>>, Blob)>,
    ) -> Result<(), WriteError<F, S, C, H::Message, P::PutDisallowed>> {
        if commits.is_empty() {
            return Ok(());
        }

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        let count = commits.len();
        tracing::info!("bulk-inserting {count} commits into sedimentree {id:?}");

        for (parents, blob) in commits {
            let verified_blob = VerifiedBlobMeta::new(blob);
            let verified_meta: VerifiedMeta<LooseCommit> =
                VerifiedMeta::seal::<F, _>(&self.signer, (id, parents), verified_blob).await;

            self.insert_commit_locally(&putter, verified_meta)
                .await
                .map_err(|e| WriteError::Io(IoError::Storage(e)))?;
        }

        self.minimize_tree(id).await;
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
    /// * [`WriteError::Io`] if a storage error occurs.
    /// * [`WriteError::PutDisallowed`] if the storage policy rejects the write.
    pub async fn add_fragments_batch(
        &self,
        id: SedimentreeId,
        fragments: Vec<FragmentBatchItem>,
    ) -> Result<(), WriteError<F, S, C, H::Message, P::PutDisallowed>> {
        if fragments.is_empty() {
            return Ok(());
        }

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

        let count = fragments.len();
        tracing::info!("bulk-inserting {count} fragments into sedimentree {id:?}");

        for item in fragments {
            let FragmentBatchItem {
                head,
                boundary,
                checkpoints,
                blob,
            } = item;
            let verified_blob = VerifiedBlobMeta::new(blob);
            let verified_meta: VerifiedMeta<Fragment> = VerifiedMeta::seal::<F, _>(
                &self.signer,
                (id, head, boundary, checkpoints),
                verified_blob,
            )
            .await;

            self.insert_fragment_locally(&putter, verified_meta)
                .await
                .map_err(|e| WriteError::Io(IoError::Storage(e)))?;
        }

        self.minimize_tree(id).await;
        tracing::info!("bulk-insert of {count} fragments complete, tree minimized");
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
    ) -> Result<(), IoError<F, S, C, H::Message>> {
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

        let msg: H::Message = SyncMessage::BlobsRequest { id, digests }.into();
        let conns = self.all_connections().await;
        for conn in conns {
            let peer_id = conn.peer_id();
            if let Err(e) = conn.send(&msg).await {
                tracing::info!("peer {peer_id} disconnected: {e}");
                self.remove_connection(&conn).await;
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N> + 'static,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C> + RemoteHeadsNotifier,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Subduction<'a, F, S, C, H, P, Sig, O, M, N>
where
    H::Message: From<SyncMessage>,
    H::HandlerError: Into<ListenError<F, S, C, H::Message>>,
    ManagedConnection<C, F, O>:
        ManagedCall<F, H::Message, SendError = <C as Connection<F, H::Message>>::SendError>,
{
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
    ) -> Result<(), WriteError<F, S, C, H::Message, P::PutDisallowed>> {
        use sedimentree_core::collections::Map;

        let self_id = self.peer_id();
        let putter = self
            .storage
            .get_putter::<F>(self_id, self_id, id)
            .await
            .map_err(WriteError::PutDisallowed)?;

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
                Authenticated<C, F>,
                crate::connection::managed::CallError<<C as Connection<F, H::Message>>::SendError>,
            )>,
        ),
        IoError<F, S, C, H::Message>,
    > {
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

            let result = ManagedCall::<F, H::Message>::call(
                &managed,
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
                    Authenticated<C, F>,
                    crate::connection::managed::CallError<
                        <C as Connection<F, H::Message>>::SendError,
                    >,
                )>,
            ),
        >,
        IoError<F, S, C, H::Message>,
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

                        let result = ManagedCall::<F, H::Message>::call(
                                &managed,
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
                            Ok(BatchSyncResponse {
                                result,
                                responder_heads,
                                ..
                            }) => {
                                if !responder_heads.is_empty() {
                                    self.handler.notify_remote_heads(
                                        id,
                                        *peer_id,
                                        responder_heads.clone(),
                                    );
                                }
                                stats.remote_heads = responder_heads;
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
                                    "sync_with_all_peers: response received"
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

                                self.minimize_tree(id).await;

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
                                            let msg: H::Message = SyncMessage::from(DataRequestRejected { id }).into();
                                            if let Err(send_err) = conn.send(&msg).await {
                                                tracing::info!("peer {peer_id} disconnected while sending DataRequestRejected: {send_err}");
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

                    Ok::<(PeerId, bool, SyncStats, Vec<(Authenticated<C, F>, _)>), IoError<F, S, C, H::Message>>((
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
            Authenticated<C, F>,
            crate::connection::managed::CallError<<C as Connection<F, H::Message>>::SendError>,
        )>,
        Vec<(SedimentreeId, IoError<F, S, C, H::Message>)>,
    ) {
        tracing::info!(
            "Requesting batch sync for all sedimentrees with peer {}",
            peer_id
        );
        let tree_ids = self.sedimentrees.into_keys().await;

        let mut had_success = false;
        let mut stats = SyncStats::new();
        let mut call_errs = Vec::new();
        let mut io_errs = Vec::new();

        for id in tree_ids {
            match self.sync_with_peer(peer_id, id, subscribe, timeout).await {
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
            Authenticated<C, F>,
            crate::connection::managed::CallError<<C as Connection<F, H::Message>>::SendError>,
        )>,
        Vec<(SedimentreeId, IoError<F, S, C, H::Message>)>,
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N> + 'static,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Subduction<'a, F, S, C, H, P, Sig, O, M, N>
where
    H::Message: From<SyncMessage>,
    H::HandlerError: Into<ListenError<F, S, C, H::Message>>,
    ManagedConnection<C, F, O>:
        ManagedCall<F, H::Message, SendError = <C as Connection<F, H::Message>>::SendError>,
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
    ) -> Result<SendCount, SendRequestedDataError<F, S, C, H::Message>> {
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
        let (requested_commit_digests, requested_fragment_digests, sender_heads) = {
            let sedimentree = self.sedimentrees.get_cloned(&id).await.unwrap_or_default();
            let sender_heads = RemoteHeads {
                counter: self.next_send_counter(conn.peer_id()).await,
                heads: sedimentree.heads(&self.depth_metric),
            };

            let commit_fp_to_digest: Map<Fingerprint<CommitId>, Digest<LooseCommit>> = sedimentree
                .commit_entries()
                .map(|(digest, c)| (Fingerprint::new(seed, &c.commit_id()), *digest))
                .collect();

            let fragment_fp_to_digest: Map<Fingerprint<FragmentId>, Digest<Fragment>> = sedimentree
                .fragment_entries()
                .map(|(digest, f)| (Fingerprint::new(seed, &f.fragment_id()), *digest))
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

            (commit_digests, fragment_digests, sender_heads)
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
                        .map(|vm| (Digest::hash(vm.payload()), vm))
                        .collect()
                };

            let commit_msgs: Vec<(bool, H::Message)> = requested_commit_digests
                .iter()
                .filter_map(|commit_digest| {
                    let verified = commit_by_digest.get(commit_digest)?;
                    let msg: H::Message = SyncMessage::LooseCommit {
                        id,
                        commit: verified.signed().clone(),
                        blob: verified.blob().clone(),
                        sender_heads: sender_heads.clone(),
                    }
                    .into();
                    Some((true, msg))
                })
                .collect();

            let fragment_msgs: Vec<(bool, H::Message)> = requested_fragment_digests
                .iter()
                .filter_map(|fragment_digest| {
                    let verified = fragment_by_digest.get(fragment_digest)?;
                    let msg: H::Message = SyncMessage::Fragment {
                        id,
                        fragment: verified.signed().clone(),
                        blob: verified.blob().clone(),
                        sender_heads: sender_heads.clone(),
                    }
                    .into();
                    Some((false, msg))
                })
                .collect();

            (commit_msgs, fragment_msgs)
        };

        // Send all messages concurrently using FuturesUnordered
        let mut send_futures: FuturesUnordered<_> = commit_messages
            .into_iter()
            .chain(fragment_messages.into_iter())
            .map(|(is_commit, msg)| async move {
                let result = conn.send(&msg).await;
                (is_commit, result)
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
        putter: &Putter<F, S>,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<bool, S::Error> {
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Drop for Subduction<'a, F, S, C, H, P, Sig, O, M, N>
{
    fn drop(&mut self) {
        self.abort_manager_handle.abort();
        self.abort_listener_handle.abort();
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> ConnectionPolicy<F> for Subduction<'a, F, S, C, H, P, Sig, O, M, N>
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> StoragePolicy<F> for Subduction<'a, F, S, C, H, P, Sig, O, M, N>
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
///
/// Note: [`StartListener`] is _not_ a supertrait here — it is only required
/// on the constructor methods that build the listener future, keeping the
/// handler type `H` out of the main `Subduction` struct definition.
pub trait SubductionFutureForm<
    'a,
    S: Storage<Self>,
    C: Connection<Self, W> + PartialEq + 'a,
    W: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
>: FutureForm + RunManager<Authenticated<C, Self>, W> + Sized
{
}

impl<
    'a,
    S: Storage<Self>,
    C: Connection<Self, W> + PartialEq + 'a,
    W: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
    U: FutureForm + RunManager<Authenticated<C, U>, W> + Sized,
> SubductionFutureForm<'a, S, C, W, P, Sig, M, N> for U
{
}

/// A trait for starting the listener task for Subduction.
///
/// This lets us abstract over `Send` and `!Send` futures while keeping
/// the handler type `H` available for the `#[future_form]` macro to
/// generate correct `Send` bounds.
///
/// `H` is a trait-level (not method-level) generic so the macro can
/// emit the required `H: Send + Sync` bounds for the `Sendable` impl.
pub trait StartListener<
    'a,
    S: Storage<Self>,
    C: Connection<Self, W> + PartialEq + 'a,
    W: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
    H: Handler<Self, C, Message = W>,
    P: ConnectionPolicy<Self> + StoragePolicy<Self>,
    Sig: Signer<Self>,
    M: DepthMetric,
    const N: usize,
>: FutureForm + RunManager<Authenticated<C, Self>, W> + Sized where
    H::HandlerError: Into<ListenError<Self, S, C, W>>,
{
    /// Start the listener task for Subduction.
    #[allow(clippy::type_complexity)]
    fn start_listener<O: Timeout<Self> + Clone + Send + Sync + 'a>(
        subduction: Arc<Subduction<'a, Self, S, C, H, P, Sig, O, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>>
    where
        Self: Sized;
}

#[future_form(
    Sendable where
        C: Connection<Sendable, W> + PartialEq + Clone + Send + Sync + 'static,
        W: Encode + Decode + Clone + Send + Sync + core::fmt::Debug + 'static,
        S: Storage<Sendable> + Send + Sync + 'a,
        P: ConnectionPolicy<Sendable> + StoragePolicy<Sendable> + Send + Sync + 'a,
        P::PutDisallowed: Send + 'static,
        P::FetchDisallowed: Send + 'static,
        Sig: Signer<Sendable> + Send + Sync + 'a,
        M: DepthMetric + Send + Sync + 'a,
        H: Handler<Sendable, C, Message = W> + Send + Sync + 'a,
        H::HandlerError: Into<ListenError<Sendable, S, C, W>> + Send + 'static,
        S::Error: Send + 'static,
        C::DisconnectionError: Send + 'static,
        C::RecvError: Send + 'static,
        C::SendError: Send + 'static,
    Local where
        C: Connection<Local, W> + PartialEq + Clone + 'static,
        W: Encode + Decode + Clone + Send + core::fmt::Debug + 'static,
        S: Storage<Local> + 'a,
        P: ConnectionPolicy<Local> + StoragePolicy<Local> + 'a,
        Sig: Signer<Local> + 'a,
        M: DepthMetric + 'a,
        H: Handler<Local, C, Message = W> + 'a,
        H::HandlerError: Into<ListenError<Local, S, C, W>>
)]
impl<'a, K: FutureForm, C, S, W, H, P, Sig, M, const N: usize>
    StartListener<'a, S, C, W, H, P, Sig, M, N> for K
where
    H: Handler<K, C, Message = W>,
    H::HandlerError: Into<ListenError<K, S, C, W>>,
    W: Encode + Decode + Clone + Send + core::fmt::Debug + From<SyncMessage> + 'static,
{
    fn start_listener<O: Timeout<Self> + Clone + Send + Sync + 'a>(
        subduction: Arc<Subduction<'a, Self, S, C, H, P, Sig, O, M, N>>,
        abort_reg: AbortRegistration,
    ) -> Abortable<Self::Future<'a, ()>> {
        Abortable::new(
            K::from_future(async move {
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric = CountLeadingZeroBytes,
    const N: usize = 256,
> {
    fut: Pin<Box<Abortable<F::Future<'a, ()>>>>,
    _phantom: PhantomData<(S, C, H, P, Sig, O, M)>,
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> ListenerFuture<'a, F, S, C, H, P, Sig, O, M, N>
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
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Deref for ListenerFuture<'a, F, S, C, H, P, Sig, O, M, N>
{
    type Target = Abortable<F::Future<'a, ()>>;

    fn deref(&self) -> &Self::Target {
        &self.fut
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Future for ListenerFuture<'a, F, S, C, H, P, Sig, O, M, N>
{
    type Output = Result<(), Aborted>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

impl<
    'a,
    F: SubductionFutureForm<'a, S, C, H::Message, P, Sig, M, N>,
    S: Storage<F>,
    C: Connection<F, H::Message> + PartialEq + 'a,
    H: Handler<F, C>,
    P: ConnectionPolicy<F> + StoragePolicy<F>,
    Sig: Signer<F>,
    O: Timeout<F> + Clone,
    M: DepthMetric,
    const N: usize,
> Unpin for ListenerFuture<'a, F, S, C, H, P, Sig, O, M, N>
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
                Arc::new(Mutex::new(Map::new())),
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
                Arc::new(Mutex::new(Map::new())),
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
