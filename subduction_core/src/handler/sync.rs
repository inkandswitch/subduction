//! Default sync protocol handler for Subduction.
//!
//! [`SyncHandler`] implements the [`Handler`] trait for the standard
//! Subduction sync protocol. It processes [`SyncMessage`] variants
//! (commits, fragments, batch sync, blobs, subscriptions) using
//! shared state passed at construction time.
//!
//! This handler is self-contained: it holds its own [`Arc`] references
//! to the shared data structures and duplicates the helper methods it
//! needs from [`Subduction`]. This allows custom handlers to replace
//! it entirely without inheriting any `Subduction` internals.
//!
//! [`Handler`]: super::Handler
//! [`Subduction`]: crate::subduction::Subduction
//! [`SyncMessage`]: crate::connection::message::SyncMessage
//! [`Arc`]: alloc::sync::Arc

use alloc::{sync::Arc, vec::Vec};
use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use nonempty::NonEmpty;
use sedimentree_core::{
    blob::Blob,
    collections::{Map, Set},
    crypto::digest::Digest,
    depth::DepthMetric,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree, minimized::MinimizedSedimentree},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};
use tracing::Instrument;

use crate::{
    authenticated::Authenticated,
    collections::bounded_sharded_map::BoundedShardedMap,
    connection::{
        Connection,
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
    },
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
    subduction::{
        error::{BlobRequestErr, IoError, ListenError},
        ingest, peers,
        pending_blob_requests::PendingBlobRequests,
    },
};

use super::Handler;
use crate::{
    peer::counter::PeerCounter,
    remote_heads::{
        FilteredHeadsNotifier, NoRemoteHeadsObserver, RemoteHeads, RemoteHeadsNotifier,
        RemoteHeadsObserver,
    },
};

/// The default sync protocol handler for Subduction.
///
/// Processes the standard [`SyncMessage`] protocol: commits, fragments,
/// batch sync requests/responses, blob requests/responses, and
/// subscription management.
///
/// # Construction
///
/// Built automatically by [`SubductionBuilder::build`], or manually
/// via [`SyncHandler::new`] for custom setups. Holds `Arc` clones of
/// the shared state that [`Subduction`] also references.
///
/// [`SubductionBuilder::build`]: crate::subduction::builder::SubductionBuilder::build
/// [`Subduction`]: crate::subduction::Subduction
#[allow(clippy::type_complexity)]
pub struct SyncHandler<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    const SHARDS: usize = 256,
    R: RemoteHeadsObserver = NoRemoteHeadsObserver,
> {
    sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    storage: StoragePowerbox<Store, Auth>,
    pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
    depth_metric: Metric,
    heads_notifier: FilteredHeadsNotifier<R>,
    send_counter: PeerCounter,
}

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    R: RemoteHeadsObserver,
    const SHARDS: usize,
> core::fmt::Debug for SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SyncHandler").finish_non_exhaustive()
    }
}

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric + Clone,
    R: RemoteHeadsObserver + Clone,
    const SHARDS: usize,
> Clone for SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    fn clone(&self) -> Self {
        Self {
            sedimentrees: self.sedimentrees.clone(),
            connections: self.connections.clone(),
            subscriptions: self.subscriptions.clone(),
            storage: self.storage.clone(),
            pending_blob_requests: self.pending_blob_requests.clone(),
            depth_metric: self.depth_metric.clone(),
            heads_notifier: self.heads_notifier.clone(),
            send_counter: self.send_counter.clone(),
        }
    }
}

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    const SHARDS: usize,
> SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, NoRemoteHeadsObserver>
{
    /// Create a new `SyncHandler` from shared state.
    ///
    /// The `Arc`s should be clones of the same references passed to
    /// [`Subduction::new`], so mutations through the handler are visible
    /// to `Subduction` and vice versa.
    ///
    /// [`Subduction::new`]: crate::subduction::Subduction::new
    #[allow(clippy::type_complexity)]
    pub fn new(
        sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<Store, Auth>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        depth_metric: Metric,
    ) -> Self {
        Self {
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending_blob_requests,
            depth_metric,
            heads_notifier: FilteredHeadsNotifier::new(NoRemoteHeadsObserver),
            send_counter: PeerCounter::default(),
        }
    }
}

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    R: RemoteHeadsObserver,
    const SHARDS: usize,
> SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    /// Create a new `SyncHandler` with a custom remote heads observer.
    #[allow(clippy::type_complexity)]
    pub fn with_remote_heads_observer(
        sedimentrees: Arc<BoundedShardedMap<SedimentreeId, MinimizedSedimentree, SHARDS>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<Store, Auth>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        depth_metric: Metric,
        remote_heads_observer: R,
    ) -> Self {
        Self {
            sedimentrees,
            connections,
            subscriptions,
            storage,
            pending_blob_requests,
            depth_metric,
            heads_notifier: FilteredHeadsNotifier::new(remote_heads_observer),
            send_counter: PeerCounter::default(),
        }
    }

    /// Returns the shared connections map.
    ///
    /// Use this to share the connection map with other handlers (e.g.,
    /// [`EphemeralHandler`]) that need to send messages to connected peers.
    ///
    /// [`EphemeralHandler`]: subduction_ephemeral::handler::EphemeralHandler
    #[allow(clippy::type_complexity)]
    pub fn connections(&self) -> Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<Conn, Async>>>>> {
        self.connections.clone()
    }

    /// Returns a shared reference to the per-peer send counter.
    ///
    /// Pass a clone to [`Subduction`] so that outgoing messages stamped
    /// by either `SyncHandler` or `Subduction` share the same monotonic
    /// counter per peer.
    ///
    /// [`Subduction`]: crate::subduction::Subduction
    pub const fn send_counter(&self) -> &PeerCounter {
        &self.send_counter
    }
}

// ---------------------------------------------------------------------------
// Handler implementation
// ---------------------------------------------------------------------------

#[future_form(
    Sendable where
        Store: Storage<Sendable> + Send + Sync + core::fmt::Debug,
        Conn: Connection<Sendable, SyncMessage> + PartialEq + Clone + Send + Sync + core::fmt::Debug + 'static,
        Auth: StoragePolicy<Sendable> + Send + Sync,
        Auth::FetchDisallowed: Send + 'static,
        Auth::PutDisallowed: Send + 'static,
        Metric: DepthMetric + Send + Sync,
        Store::Error: Send + 'static,
        Conn::SendError: Send + 'static,
        Conn::RecvError: Send + 'static,
        Conn::DisconnectionError: Send + 'static,
        R: RemoteHeadsObserver + Send + Sync,
    Local where
        Store: Storage<Local> + core::fmt::Debug,
        Conn: Connection<Local, SyncMessage> + PartialEq + Clone + core::fmt::Debug + 'static,
        Auth: StoragePolicy<Local>,
        Metric: DepthMetric,
        R: RemoteHeadsObserver
)]
impl<Async: FutureForm, Store, Conn, Auth, Metric, R, const SHARDS: usize> Handler<Async, Conn>
    for SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    type Message = SyncMessage;
    type HandlerError = ListenError<Async, Store, Conn, SyncMessage>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Async>,
        message: Self::Message,
    ) -> Async::Future<'a, Result<(), Self::HandlerError>> {
        Async::from_future(async move { self.dispatch(conn, message).await })
    }

    fn on_peer_disconnect(&self, _peer: PeerId) -> Async::Future<'_, ()> {
        // Sync subscriptions are already cleaned by `peers::remove_connection`,
        // so there is nothing extra to do here.
        Async::from_future(async {})
    }
}

// ---------------------------------------------------------------------------
// RemoteHeadsNotifier implementation
// ---------------------------------------------------------------------------

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    R: RemoteHeadsObserver,
    const SHARDS: usize,
> RemoteHeadsNotifier for SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.heads_notifier.notify(id, peer, heads);
    }
}

// ---------------------------------------------------------------------------
// Dispatch + recv_* methods (self-contained copies from Subduction)
// ---------------------------------------------------------------------------

impl<
    Async: FutureForm,
    Store: Storage<Async>,
    Conn: Connection<Async, SyncMessage> + PartialEq + Clone + 'static,
    Auth: StoragePolicy<Async>,
    Metric: DepthMetric,
    R: RemoteHeadsObserver,
    const SHARDS: usize,
> SyncHandler<Async, Store, Conn, Auth, Metric, SHARDS, R>
{
    #[allow(clippy::too_many_lines)]
    async fn dispatch(
        &self,
        conn: &Authenticated<Conn, Async>,
        message: SyncMessage,
    ) -> Result<(), ListenError<Async, Store, Conn, SyncMessage>> {
        let from = conn.peer_id();

        let span = tracing::debug_span!(
            "dispatch",
            peer = %from,
            msg = message.variant_name(),
            tree = ?message.sedimentree_id(),
            req = ?message.request_id(),
        );

        #[cfg(feature = "metrics")]
        crate::metrics::message_dispatched(message.variant_name());

        #[cfg(feature = "metrics")]
        let _timer = crate::metrics::DispatchTimer::new(message.variant_name());

        self.dispatch_inner(from, message, conn)
            .instrument(span)
            .await
    }

    /// Inner dispatch body, run inside the per-message correlation span.
    async fn dispatch_inner(
        &self,
        from: PeerId,
        message: SyncMessage,
        conn: &Authenticated<Conn, Async>,
    ) -> Result<(), ListenError<Async, Store, Conn, SyncMessage>> {
        // Note: remote heads arrive via three paths:
        //
        // 1. `responder_heads` in `BatchSyncResponse` — handled by
        //    `Subduction::sync_with_all_peers` via `RemoteHeadsNotifier`.
        //
        // 2. `sender_heads` on subscription-push `LooseCommit`/`Fragment`
        //    messages — handled here in dispatch.
        //
        // 3. `HeadsUpdate` messages (post-ingestion ack from the 1.5 RTT
        //    second half) — handled here in dispatch.
        match message {
            SyncMessage::LooseCommit {
                id,
                commit,
                blob,
                sender_heads,
            } => {
                self.heads_notifier.notify(id, from, sender_heads);
                self.recv_commit(&from, id, &commit, blob, conn).await?;
            }
            SyncMessage::Fragment {
                id,
                fragment,
                blob,
                sender_heads,
            } => {
                self.heads_notifier.notify(id, from, sender_heads);
                self.recv_fragment(&from, id, &fragment, blob, conn).await?;
            }
            SyncMessage::BatchSyncRequest(BatchSyncRequest {
                id,
                fingerprint_summary,
                req_id,
                subscribe,
            }) => {
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_request();

                if subscribe {
                    self.add_subscription(from, id).await;
                    tracing::debug!(peer = %from, tree = ?id, "added subscription");
                }

                self.recv_batch_sync_request(id, &fingerprint_summary, req_id, conn)
                    .await?;
            }
            SyncMessage::BatchSyncResponse(BatchSyncResponse { id, result, .. }) => {
                #[cfg(feature = "metrics")]
                crate::metrics::batch_sync_response();

                match result {
                    SyncResult::Ok(diff) => {
                        self.recv_batch_sync_response(&from, id, diff).await?;
                    }
                    SyncResult::NotFound => {
                        tracing::debug!(peer = %from, tree = ?id, "peer reports sedimentree not found");
                    }
                    SyncResult::Unauthorized => {
                        tracing::debug!(peer = %from, tree = ?id, "peer reports we are unauthorized for sedimentree");
                    }
                }
            }
            SyncMessage::BlobsRequest { id, digests } => {
                match self.recv_blob_request(conn, id, &digests).await {
                    Ok(()) => {
                        tracing::debug!(peer = %from, tree = ?id, "handled blob request");
                    }
                    Err(BlobRequestErr::IoError(e)) => Err(e)?,
                    Err(BlobRequestErr::MissingBlobs(missing)) => {
                        tracing::warn!(peer = %from, tree = ?id, missing = ?missing, "missing blobs for request");
                    }
                }
            }
            SyncMessage::BlobsResponse { id, blobs } => {
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
                    peer = %from,
                    tree = ?id,
                    accepted = accepted_count,
                    total = blobs.len(),
                    "blob response acknowledged (compound storage)"
                );
            }
            SyncMessage::RemoveSubscriptions(crate::connection::message::RemoveSubscriptions {
                ids,
            }) => {
                self.remove_subscriptions(from, &ids).await;
                tracing::debug!(peer = %from, trees = ?ids, "removed subscriptions");
            }
            SyncMessage::DataRequestRejected(crate::connection::message::DataRequestRejected {
                id,
            }) => {
                tracing::debug!(peer = %from, tree = ?id, "peer rejected our data request");
            }
            SyncMessage::HeadsUpdate { id, heads } => {
                tracing::debug!(peer = %from, tree = ?id, heads = heads.heads.len(), "peer reports heads");
                self.heads_notifier.notify(id, from, heads);
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // recv_* methods
    // -----------------------------------------------------------------------

    async fn recv_commit(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        signed_commit: &Signed<LooseCommit>,
        blob: Blob,
        conn: &Authenticated<Conn, Async>,
    ) -> Result<bool, IoError<Async, Store, Conn, SyncMessage>> {
        let verified = match signed_commit.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(peer = %from, error = %e, "commit signature verification failed");
                return Ok(false);
            }
        };

        let author = verified.verified_author();
        tracing::debug!(
            digest = ?Digest::hash(verified.payload()),
            tree = ?id,
            peer = %from,
            author = ?author,
            "receiving commit"
        );

        let putter = match self.storage.get_putter::<Async>(*from, author, id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    peer = %from,
                    author = ?author,
                    tree = ?id,
                    error = %e,
                    "policy rejected commit"
                );
                return Ok(false);
            }
        };

        let signed_for_wire = verified.signed().clone();

        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!(peer = %from, error = %e, "blob mismatch");
                return Err(IoError::BlobMismatch(e));
            }
        };

        let was_new = self
            .insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        self.minimize_tree(id).await;

        if was_new {
            let heads = self.heads_for(id).await;

            // Send HeadsUpdate back to the originating peer
            let heads_msg = SyncMessage::HeadsUpdate {
                id,
                heads: RemoteHeads {
                    counter: self.send_counter.next(*from).await,
                    heads: heads.clone(),
                },
            };
            if let Err(e) = conn.send(&heads_msg).await {
                tracing::warn!(peer = %from, error = %e, "peer disconnected while sending HeadsUpdate");
            }

            // Broadcast to subscribers (excluding sender)
            let conns = self.get_authorized_subscriber_conns(id, from).await;
            #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
            let pushed = self
                .broadcast_loose_commit(id, &signed_for_wire, &blob, &heads, conns)
                .await;
            #[cfg(feature = "metrics")]
            crate::metrics::subscription_pushes(pushed);
        }

        Ok(was_new)
    }

    /// Fan a newly-received loose commit out to its authorized subscribers.
    ///
    /// Per-peer send counters are allocated sequentially (cheap, and counter
    /// order per peer must be preserved), then the wire sends run concurrently
    /// so a slow or backpressured subscriber can't stall delivery to the rest.
    /// Connections whose send failed are pruned afterwards. Returns the number
    /// of successful pushes.
    async fn broadcast_loose_commit(
        &self,
        id: SedimentreeId,
        commit: &Signed<LooseCommit>,
        blob: &Blob,
        heads: &[CommitId],
        conns: Vec<Authenticated<Conn, Async>>,
    ) -> u64 {
        // Allocate counters + build messages sequentially.
        let mut pending = Vec::with_capacity(conns.len());
        for conn in conns {
            let peer_id = conn.peer_id();
            let msg = SyncMessage::LooseCommit {
                id,
                commit: commit.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads {
                    counter: self.send_counter.next(peer_id).await,
                    heads: heads.to_vec(),
                },
            };
            pending.push((conn, msg));
        }

        // Run the sends concurrently; collect failures to prune after.
        let results = futures::future::join_all(
            pending
                .iter()
                .map(|(conn, msg)| async move { (conn, conn.send(msg).await) }),
        )
        .await;

        let mut pushed: u64 = 0;
        let mut failed = Vec::new();
        for (conn, result) in results {
            match result {
                Ok(()) => pushed += 1,
                Err(e) => {
                    tracing::warn!(peer = %conn.peer_id(), error = %e, "peer disconnected");
                    failed.push(conn);
                }
            }
        }
        for conn in failed {
            self.remove_connection(conn).await;
        }

        pushed
    }

    /// Fan a newly-received fragment out to its authorized subscribers.
    ///
    /// See [`broadcast_loose_commit`](Self::broadcast_loose_commit) for the
    /// sequential-counter / concurrent-send rationale.
    async fn broadcast_fragment(
        &self,
        id: SedimentreeId,
        fragment: &Signed<Fragment>,
        blob: &Blob,
        heads: &[CommitId],
        conns: Vec<Authenticated<Conn, Async>>,
    ) -> u64 {
        let mut pending = Vec::with_capacity(conns.len());
        for conn in conns {
            let peer_id = conn.peer_id();
            let msg = SyncMessage::Fragment {
                id,
                fragment: fragment.clone(),
                blob: blob.clone(),
                sender_heads: RemoteHeads {
                    counter: self.send_counter.next(peer_id).await,
                    heads: heads.to_vec(),
                },
            };
            pending.push((conn, msg));
        }

        let results = futures::future::join_all(
            pending
                .iter()
                .map(|(conn, msg)| async move { (conn, conn.send(msg).await) }),
        )
        .await;

        let mut pushed: u64 = 0;
        let mut failed = Vec::new();
        for (conn, result) in results {
            match result {
                Ok(()) => pushed += 1,
                Err(e) => {
                    tracing::warn!(peer = %conn.peer_id(), error = %e, "peer disconnected");
                    failed.push(conn);
                }
            }
        }
        for conn in failed {
            self.remove_connection(conn).await;
        }

        pushed
    }

    async fn recv_fragment(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        signed_fragment: &Signed<Fragment>,
        blob: Blob,
        conn: &Authenticated<Conn, Async>,
    ) -> Result<bool, IoError<Async, Store, Conn, SyncMessage>> {
        let verified = match signed_fragment.try_verify() {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(peer = %from, error = %e, "fragment signature verification failed");
                return Ok(false);
            }
        };

        let author = verified.verified_author();
        tracing::debug!(
            digest = ?Digest::hash(verified.payload()),
            tree = ?id,
            peer = %from,
            author = ?author,
            "receiving fragment"
        );

        let putter = match self.storage.get_putter::<Async>(*from, author, id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    peer = %from,
                    author = ?author,
                    tree = ?id,
                    error = %e,
                    "policy rejected fragment"
                );
                return Ok(false);
            }
        };

        let signed_for_wire = verified.signed().clone();

        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!(peer = %from, error = %e, "blob mismatch");
                return Err(IoError::BlobMismatch(e));
            }
        };

        let was_new = self
            .insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        self.minimize_tree(id).await;

        if was_new {
            let heads = self.heads_for(id).await;

            // Send HeadsUpdate back to the originating peer
            let heads_msg = SyncMessage::HeadsUpdate {
                id,
                heads: RemoteHeads {
                    counter: self.send_counter.next(*from).await,
                    heads: heads.clone(),
                },
            };
            if let Err(e) = conn.send(&heads_msg).await {
                tracing::warn!(peer = %from, error = %e, "peer disconnected while sending HeadsUpdate");
            }

            // Broadcast to subscribers (excluding sender)
            let conns = self.get_authorized_subscriber_conns(id, from).await;
            #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
            let pushed = self
                .broadcast_fragment(id, &signed_for_wire, &blob, &heads, conns)
                .await;
            #[cfg(feature = "metrics")]
            crate::metrics::subscription_pushes(pushed);
        }

        Ok(was_new)
    }

    #[allow(clippy::too_many_lines)]
    async fn recv_batch_sync_request(
        &self,
        id: SedimentreeId,
        their_fingerprints: &FingerprintSummary,
        req_id: RequestId,
        conn: &Authenticated<Conn, Async>,
    ) -> Result<(), ListenError<Async, Store, Conn, SyncMessage>> {
        let peer_id = conn.peer_id();
        tracing::info!(peer = %peer_id, tree = ?id, "recv_batch_sync_request");

        let fetcher = match self.storage.get_fetcher::<Async>(peer_id, id).await {
            Ok(f) => f,
            Err(e) => {
                tracing::debug!(
                    %peer_id,
                    ?id,
                    error = %e,
                    "policy rejected fetch request"
                );
                let msg: SyncMessage = BatchSyncResponse {
                    id,
                    req_id,
                    result: SyncResult::Unauthorized,
                    responder_heads: RemoteHeads::default(),
                }
                .into();
                if let Err(e) = conn.send(&msg).await {
                    tracing::info!(
                        peer = %conn.peer_id(),
                        error = %e,
                        "peer disconnected while sending unauthorized response"
                    );
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

        let mut commit_by_id: Map<CommitId, VerifiedMeta<LooseCommit>> = Map::new();
        for vm in verified_commits {
            commit_by_id.entry(vm.payload().head()).or_insert(vm);
        }
        let fragment_by_id: Map<CommitId, VerifiedMeta<Fragment>> = verified_fragments
            .into_iter()
            .map(|vm| (vm.payload().head(), vm))
            .collect();

        // Build the resident tree from the data we just loaded (reusing the
        // fetcher reads above — no second storage round-trip).
        let loose_commits: Vec<_> = commit_by_id
            .values()
            .map(|vm| vm.payload().clone())
            .collect();
        let fragments: Vec<_> = fragment_by_id
            .values()
            .map(|vm| vm.payload().clone())
            .collect();

        // Only install into the cache when there is actual stored data.
        //
        // Caching an empty tree for a never-stored id would make a later
        // `get_or_hydrate(id)` return `Some(empty)` instead of `None`,
        // corrupting the exists-vs-nonexistent contract eviction relies on —
        // and would let any authorized peer pollute the cache by requesting
        // arbitrary ids (`get_fetcher` gates on policy, not existence). When
        // there is no data we diff against an ephemeral empty tree and cache
        // nothing; the response correctly advertises no local items.
        //
        // `get_or_insert_with` adopts a concurrently-installed value if one
        // appeared and records an LRU access; the built tree is already
        // minimal, so it is wrapped clean.
        let mut sedimentree = if loose_commits.is_empty() && fragments.is_empty() {
            MinimizedSedimentree::already_minimal(Sedimentree::default())
        } else {
            let built = Sedimentree::new(fragments, loose_commits).minimize(&self.depth_metric);
            self.sedimentrees
                .get_or_insert_with(id, || MinimizedSedimentree::already_minimal(built))
                .await
        };

        tracing::debug!(
            tree = ?id,
            req = ?req_id,
            commit_fps = their_fingerprints.commit_fingerprints().len(),
            fragment_fps = their_fingerprints.fragment_fingerprints().len(),
            "received batch sync request"
        );

        // The wire diff requires the minimal form. A freshly built value is
        // already minimal, but an adopted concurrently-installed value may be
        // dirty, so minimize once here. `heads_assuming_minimal` then reads the
        // heads off the now-minimal tree without re-minimizing (which
        // `Sedimentree::heads` would otherwise do internally).
        let minimal = sedimentree.minimized(&self.depth_metric);
        let raw_heads = minimal.heads_assuming_minimal();
        let diff = minimal.diff_remote_fingerprints(their_fingerprints);
        let local_commit_ids: Vec<_> = diff.local_only_commits.iter().map(|(id, _)| **id).collect();
        let local_fragment_ids: Vec<_> = diff
            .local_only_fragments
            .iter()
            .map(|(id, _)| **id)
            .collect();
        let our_missing_commit_fingerprints = diff.remote_only_commit_fingerprints;
        let our_missing_fragment_fingerprints = diff.remote_only_fragment_fingerprints;

        let responder_heads = RemoteHeads {
            counter: self.send_counter.next(conn.peer_id()).await,
            heads: raw_heads,
        };

        for commit_id in local_commit_ids {
            if let Some(verified) = commit_by_id.get(&commit_id) {
                their_missing_commits.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        for frag_id in local_fragment_ids {
            if let Some(verified) = fragment_by_id.get(&frag_id) {
                their_missing_fragments.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        tracing::info!(
            peer = %peer_id,
            tree = ?id,
            req = ?req_id,
            missing_commits = their_missing_commits.len(),
            missing_fragments = their_missing_fragments.len(),
            requesting_commits = our_missing_commit_fingerprints.len(),
            requesting_fragments = our_missing_fragment_fingerprints.len(),
            "sending batch sync response"
        );

        let sync_diff = SyncDiff {
            missing_commits: their_missing_commits,
            missing_fragments: their_missing_fragments,
            requesting: RequestedData {
                commit_fingerprints: our_missing_commit_fingerprints,
                fragment_fingerprints: our_missing_fragment_fingerprints,
            },
        };

        let msg: SyncMessage = BatchSyncResponse {
            id,
            req_id,
            result: SyncResult::Ok(sync_diff),
            responder_heads,
        }
        .into();
        if let Err(e) = conn.send(&msg).await {
            tracing::warn!(peer = %conn.peer_id(), error = %e, "peer disconnected");
        }

        Ok(())
    }

    async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: SyncDiff,
    ) -> Result<(), IoError<Async, Store, Conn, SyncMessage>> {
        ingest::recv_batch_sync_response(&self.sedimentrees, &self.storage, from, id, diff).await?;
        self.minimize_tree(id).await;
        Ok(())
    }

    async fn recv_blob_request(
        &self,
        conn: &Authenticated<Conn, Async>,
        id: SedimentreeId,
        digests: &[Digest<Blob>],
    ) -> Result<(), BlobRequestErr<Async, Store, Conn, SyncMessage>> {
        let mut blobs = Vec::new();
        let mut missing = Vec::new();
        for digest in digests {
            if let Some(blob) = self.get_blob(id, *digest).await.map_err(IoError::Storage)? {
                blobs.push(blob);
            } else {
                missing.push(*digest);
            }
        }

        conn.send(&SyncMessage::BlobsResponse { id, blobs })
            .await
            .map_err(IoError::ConnSend)?;

        if missing.is_empty() {
            Ok(())
        } else {
            Err(BlobRequestErr::MissingBlobs(missing))
        }
    }

    // -----------------------------------------------------------------------
    // Delegating helpers — logic lives in `ingest` and `peers` modules
    // -----------------------------------------------------------------------

    async fn get_blob(
        &self,
        id: SedimentreeId,
        digest: Digest<Blob>,
    ) -> Result<Option<Blob>, Store::Error> {
        ingest::get_blob(&self.storage, id, digest).await
    }

    async fn insert_commit_locally(
        &self,
        putter: &Putter<Async, Store>,
        verified_meta: VerifiedMeta<LooseCommit>,
    ) -> Result<bool, Store::Error> {
        ingest::insert_commit_locally(&self.sedimentrees, putter, verified_meta).await
    }

    async fn insert_fragment_locally(
        &self,
        putter: &Putter<Async, Store>,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<bool, Store::Error> {
        ingest::insert_fragment_locally(&self.sedimentrees, putter, verified_meta).await
    }

    async fn minimize_tree(&self, id: SedimentreeId) {
        ingest::minimize_tree(&self.sedimentrees, &self.depth_metric, id).await;
    }

    /// Compute the current heads for a sedimentree (without a counter).
    ///
    /// Routes through [`ingest::heads_or_hydrate`] so an evicted (or
    /// never-resident) tree reports its real heads from storage rather than
    /// empty, while a resident hit computes heads in place — no tree clone and
    /// a single dirty-gated minimize. A nonexistent tree (or a storage error)
    /// yields empty heads (best-effort; the heads field is advisory).
    async fn heads_for(&self, id: SedimentreeId) -> Vec<CommitId> {
        match ingest::heads_or_hydrate(
            &self.sedimentrees,
            &self.storage,
            &self.depth_metric,
            id,
        )
        .await
        {
            Ok(heads) => heads,
            Err(e) => {
                tracing::warn!(tree = ?id, error = %e, "heads_for: hydration failed");
                Vec::new()
            }
        }
    }

    async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
        peers::add_subscription(&self.subscriptions, peer_id, sedimentree_id).await;
        #[cfg(feature = "metrics")]
        crate::metrics::set_subscribed_sedimentrees(self.subscriptions.lock().await.len());
    }

    async fn remove_subscriptions(&self, peer_id: PeerId, ids: &[SedimentreeId]) {
        let mut subscriptions = self.subscriptions.lock().await;
        for id in ids {
            if let Some(peer_set) = subscriptions.get_mut(id) {
                peer_set.remove(&peer_id);
                if peer_set.is_empty() {
                    subscriptions.remove(id);
                }
            }
        }
        #[cfg(feature = "metrics")]
        crate::metrics::set_subscribed_sedimentrees(subscriptions.len());
    }

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

    async fn remove_connection(&self, conn: &Authenticated<Conn, Async>) -> Option<bool> {
        peers::remove_connection(&self.connections, &self.subscriptions, conn).await
    }
}
