//! Default sync protocol handler for Subduction.
//!
//! [`SyncHandler`] implements the [`Handler`] trait for the standard
//! Subduction sync protocol. It processes [`SyncMessage`] variants
//! (commits, fragments, batch sync, heads updates, subscriptions) using
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
    crypto::{digest::Digest, fingerprint::Fingerprint},
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
        error::{IoError, ListenError},
        ingest, peers,
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
/// batch sync requests/responses, heads updates, and subscription
/// management.
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
        depth_metric: Metric,
    ) -> Self {
        Self {
            sedimentrees,
            connections,
            subscriptions,
            storage,
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
        depth_metric: Metric,
        remote_heads_observer: R,
    ) -> Self {
        Self {
            sedimentrees,
            connections,
            subscriptions,
            storage,
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

        tracing::debug!(
            tree = ?id,
            req = ?req_id,
            commit_fps = their_fingerprints.commit_fingerprints().len(),
            fragment_fps = their_fingerprints.fragment_fingerprints().len(),
            "received batch sync request"
        );

        // Fast path: diff against the resident minimized tree (recording an
        // LRU touch), then fetch only the items the peer is actually missing
        // as targeted point reads. For a cache-resident tree this avoids a
        // full commit+fragment storage scan. The cached tree holds payload
        // metadata only, so the wire data (signed bytes + blobs) still comes
        // from storage — but only for the (typically small) local-only set.
        //
        // Coherence note: writes persist to storage *before* updating the
        // resident tree, so a commit that is durable but not yet cached is
        // omitted from this response. That brief lag is benign — the next
        // sync round picks it up, and the protocol tolerates stale views.
        let cached = self
            .sedimentrees
            .with_entry(&id, |tree| {
                ResponderDiff::new(tree.minimized(&self.depth_metric), their_fingerprints)
            })
            .await;

        let (diff, their_missing_commits, their_missing_fragments) = if let Some(diff) = cached {
            #[cfg(feature = "metrics")]
            crate::metrics::sedimentree_cache_hit();

            let missing = diff.local_commit_ids.len() + diff.local_fragment_ids.len();
            let crossover =
                (diff.total_items / SCAN_FRACTION_DENOMINATOR).max(SCAN_CROSSOVER_FLOOR);
            if missing > crossover {
                // Cold-clone-sized diff (e.g. a fresh peer with an empty
                // fingerprint summary): one bulk scan beats N point-read
                // round-trips. The cache stays authoritative for the diff
                // itself; the scan only supplies the wire payloads.
                let wanted_commits: Set<CommitId> = diff.local_commit_ids.iter().copied().collect();
                let wanted_fragments: Set<CommitId> =
                    diff.local_fragment_ids.iter().copied().collect();

                // Each table is scanned only when the diff actually wants
                // something from it — a commit-only tree shouldn't pay a
                // fragments scan (a directory walk on the fs backend).
                let mut commit_by_id: Map<CommitId, VerifiedMeta<LooseCommit>> = Map::new();
                if !wanted_commits.is_empty() {
                    for vm in fetcher
                        .load_loose_commits()
                        .await
                        .map_err(IoError::Storage)?
                    {
                        let head = vm.payload().head();
                        if wanted_commits.contains(&head) {
                            commit_by_id.entry(head).or_insert(vm);
                        }
                    }
                }

                let mut fragment_by_id: Map<CommitId, VerifiedMeta<Fragment>> = Map::new();
                if !wanted_fragments.is_empty() {
                    for vm in fetcher.load_fragments().await.map_err(IoError::Storage)? {
                        let head = vm.payload().head();
                        if wanted_fragments.contains(&head) {
                            fragment_by_id.entry(head).or_insert(vm);
                        }
                    }
                }

                let commits = commit_by_id
                    .into_values()
                    .map(|vm| {
                        let (signed, _, blob) = vm.into_full_parts();
                        (signed, blob)
                    })
                    .collect();
                let fragments = fragment_by_id
                    .into_values()
                    .map(|vm| {
                        let (signed, _, blob) = vm.into_full_parts();
                        (signed, blob)
                    })
                    .collect();

                (diff, commits, fragments)
            } else {
                // Incremental sync: targeted point reads, run concurrently
                // in bounded chunks so a single request can't monopolize
                // the blocking pool.
                let mut commits = Vec::with_capacity(diff.local_commit_ids.len());
                for chunk in diff.local_commit_ids.chunks(POINT_READ_CHUNK) {
                    let results = futures::future::join_all(
                        chunk
                            .iter()
                            .map(|commit_id| fetcher.load_loose_commit(*commit_id)),
                    )
                    .await;

                    for (commit_id, result) in chunk.iter().zip(results) {
                        if let Some(verified) = result.map_err(IoError::Storage)? {
                            let (signed, _, blob) = verified.into_full_parts();
                            commits.push((signed, blob));
                        } else {
                            // Cache-ahead-of-storage window: the resident
                            // tree claims an item storage no longer holds
                            // (e.g. a racing delete). The item is silently
                            // omitted — the next sync round self-corrects —
                            // but the condition should be observable.
                            tracing::debug!(
                                tree = ?id,
                                ?commit_id,
                                "cached commit missing from storage; omitting from sync response"
                            );
                        }
                    }
                }

                let mut fragments = Vec::with_capacity(diff.local_fragment_ids.len());
                for chunk in diff.local_fragment_ids.chunks(POINT_READ_CHUNK) {
                    let results = futures::future::join_all(
                        chunk
                            .iter()
                            .map(|fragment_id| fetcher.load_fragment(*fragment_id)),
                    )
                    .await;

                    for (fragment_id, result) in chunk.iter().zip(results) {
                        if let Some(verified) = result.map_err(IoError::Storage)? {
                            let (signed, _, blob) = verified.into_full_parts();
                            fragments.push((signed, blob));
                        } else {
                            // Same cache-ahead-of-storage window as the
                            // commit loop above.
                            tracing::debug!(
                                tree = ?id,
                                ?fragment_id,
                                "cached fragment missing from storage; omitting from sync response"
                            );
                        }
                    }
                }

                (diff, commits, fragments)
            }
        } else {
            #[cfg(feature = "metrics")]
            crate::metrics::sedimentree_cache_miss();

            // Slow path (cache miss): full storage scan, which also warms
            // the cache so subsequent requests for this tree take the
            // fast path.
            let verified_commits = fetcher
                .load_loose_commits()
                .await
                .map_err(IoError::Storage)?;
            let verified_fragments = fetcher.load_fragments().await.map_err(IoError::Storage)?;

            // Byzantine duplicates (multiple payloads per id) resolve
            // first-loaded-wins — the same policy for commits and
            // fragments, in both the crossover and slow paths.
            let mut commit_by_id: Map<CommitId, VerifiedMeta<LooseCommit>> = Map::new();
            for vm in verified_commits {
                commit_by_id.entry(vm.payload().head()).or_insert(vm);
            }
            let mut fragment_by_id: Map<CommitId, VerifiedMeta<Fragment>> = Map::new();
            for vm in verified_fragments {
                fragment_by_id.entry(vm.payload().head()).or_insert(vm);
            }

            // Build the resident tree from the data we just loaded
            // (reusing the fetcher reads above — no second storage
            // round-trip).
            let loose_commits: Vec<_> = commit_by_id
                .values()
                .map(|vm| vm.payload().clone())
                .collect();
            let fragments: Vec<_> = fragment_by_id
                .values()
                .map(|vm| vm.payload().clone())
                .collect();

            // Only install into the cache when there is actual stored
            // data.
            //
            // Caching an empty tree for a never-stored id would make a
            // later `get_or_hydrate(id)` return `Some(empty)` instead of
            // `None`, corrupting the exists-vs-nonexistent contract
            // eviction relies on — and would let any authorized peer
            // pollute the cache by requesting arbitrary ids
            // (`get_fetcher` gates on policy, not existence). When there
            // is no data we diff against an ephemeral empty tree and
            // cache nothing; the response correctly advertises no local
            // items.
            //
            // `get_or_insert_with` adopts a concurrently-installed value
            // if one appeared and records an LRU access; the built tree
            // is already minimal, so it is wrapped clean.
            let mut sedimentree = if loose_commits.is_empty() && fragments.is_empty() {
                MinimizedSedimentree::already_minimal(Sedimentree::default())
            } else {
                let built = Sedimentree::new(fragments, loose_commits).minimize(&self.depth_metric);
                self.sedimentrees
                    .get_or_insert_with(id, || MinimizedSedimentree::already_minimal(built))
                    .await
            };

            // The wire diff requires the minimal form. A freshly built
            // value is already minimal, but an adopted
            // concurrently-installed value may be dirty, so minimize once
            // here.
            let diff = ResponderDiff::new(
                sedimentree.minimized(&self.depth_metric),
                their_fingerprints,
            );

            let commits = diff
                .local_commit_ids
                .iter()
                .filter_map(|commit_id| commit_by_id.get(commit_id))
                .map(|vm| (vm.signed().clone(), vm.blob().clone()))
                .collect();
            let fragments = diff
                .local_fragment_ids
                .iter()
                .filter_map(|fragment_id| fragment_by_id.get(fragment_id))
                .map(|vm| (vm.signed().clone(), vm.blob().clone()))
                .collect();

            (diff, commits, fragments)
        };

        let ResponderDiff {
            heads,
            requesting_commit_fingerprints,
            requesting_fragment_fingerprints,
            ..
        } = diff;

        let responder_heads = RemoteHeads {
            counter: self.send_counter.next(conn.peer_id()).await,
            heads,
        };

        tracing::info!(
            peer = %peer_id,
            tree = ?id,
            req = ?req_id,
            missing_commits = their_missing_commits.len(),
            missing_fragments = their_missing_fragments.len(),
            requesting_commits = requesting_commit_fingerprints.len(),
            requesting_fragments = requesting_fragment_fingerprints.len(),
            "sending batch sync response"
        );

        let sync_diff = SyncDiff {
            missing_commits: their_missing_commits,
            missing_fragments: their_missing_fragments,
            requesting: RequestedData {
                commit_fingerprints: requesting_commit_fingerprints,
                fragment_fingerprints: requesting_fragment_fingerprints,
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

    // -----------------------------------------------------------------------
    // Delegating helpers — logic lives in `ingest` and `peers` modules
    // -----------------------------------------------------------------------

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
        match ingest::heads_or_hydrate(&self.sedimentrees, &self.storage, &self.depth_metric, id)
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

/// Number of responder fast-path point reads issued concurrently per
/// batch. Bounds blocking-pool pressure from a single request while still
/// overlapping storage round-trips for peers missing many items.
const POINT_READ_CHUNK: usize = 32;

/// Scan-fallback fraction denominator: the responder fast path switches
/// from point reads to one bulk scan when the requestor is missing more
/// than `total_items / 4` of the tree.
///
/// A cold clone (fresh peer, empty fingerprint summary) of a *popular* —
/// therefore cache-resident — tree would otherwise hit the point-read path
/// with the entire tree as its missing set: thousands of storage
/// round-trips where one bulk scan does the same work.
///
/// The crossover is a *fraction* of tree size, not a constant, because the
/// two costs scale differently: a bulk scan grows with the whole tree, a
/// point-read pass with the missing set. Benchmarking put the break-even
/// between roughly a quarter and a third of tree size; `1/4` sits at the
/// conservative edge, favouring point reads.
const SCAN_FRACTION_DENOMINATOR: usize = 4;

/// Absolute floor for the scan fallback: below this many missing items,
/// point reads are always used regardless of the fraction rule (a scan of
/// a tiny tree and a handful of point reads are both trivial; the floor
/// keeps micro-diffs on the zero-scan path).
const SCAN_CROSSOVER_FLOOR: usize = 32;

/// The responder-side wire diff, copied out to owned values.
///
/// [`Sedimentree::diff_remote_fingerprints`] returns a
/// [`FingerprintDiff`](sedimentree_core::sedimentree::FingerprintDiff) that
/// *borrows* from the tree, which cannot escape the sedimentree cache's
/// shard lock (`with_entry` runs its closure under the lock). This type
/// extracts everything `recv_batch_sync_request` needs as owned data: the
/// ids to fetch from storage, the fingerprints to echo back, and the heads.
struct ResponderDiff {
    /// Total items (loose commits + fragments) in the minimal tree, used
    /// by the point-read/bulk-scan crossover.
    total_items: usize,

    /// The responder's heads, read off the minimal tree.
    heads: Vec<CommitId>,

    /// Ids of commits we hold that the requestor is missing.
    local_commit_ids: Vec<CommitId>,

    /// Ids of fragments we hold that the requestor is missing.
    local_fragment_ids: Vec<CommitId>,

    /// Requestor commit fingerprints we don't recognize (echoed back so the
    /// requestor can reverse-lookup and send the data).
    requesting_commit_fingerprints: Vec<Fingerprint<CommitId>>,

    /// Requestor fragment fingerprints we don't recognize (echoed back).
    requesting_fragment_fingerprints: Vec<Fingerprint<CommitId>>,
}

impl ResponderDiff {
    /// Diff `minimal` against the requestor's fingerprint summary.
    ///
    /// `minimal` must already be in minimal form (the wire diff and
    /// `heads_assuming_minimal` both rely on it).
    fn new(minimal: &Sedimentree, their_fingerprints: &FingerprintSummary) -> Self {
        let diff = minimal.diff_remote_fingerprints(their_fingerprints);

        Self {
            total_items: minimal.loose_commits().count() + minimal.fragments().count(),
            heads: minimal.heads_assuming_minimal(),
            local_commit_ids: diff.local_only_commits.iter().map(|(id, _)| **id).collect(),
            local_fragment_ids: diff
                .local_only_fragments
                .iter()
                .map(|(id, _)| **id)
                .collect(),
            requesting_commit_fingerprints: diff.remote_only_commit_fingerprints,
            requesting_fragment_fingerprints: diff.remote_only_fragment_fingerprints,
        }
    }
}
