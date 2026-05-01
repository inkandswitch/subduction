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
    collections::{Entry, Map, Set},
    crypto::digest::Digest,
    depth::DepthMetric,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

use crate::{
    authenticated::Authenticated,
    connection::{
        Connection,
        message::{
            BatchSyncRequest, BatchSyncResponse, RequestId, RequestedData, SyncDiff, SyncMessage,
            SyncResult,
        },
    },
    peer::id::PeerId,
    policy::storage::StoragePolicy,
    sharded_map::ShardedMap,
    storage::{powerbox::StoragePowerbox, putter::Putter, traits::Storage},
    subduction::{
        error::{BlobRequestErr, IoError, ListenError},
        ingest, peers,
        pending_blob_requests::PendingBlobRequests,
    },
    sync_session::{DynSyncSessionObserver, SyncSession, SyncSessionKind},
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
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize = 256,
    R: RemoteHeadsObserver = NoRemoteHeadsObserver,
> {
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    storage: StoragePowerbox<S, P>,
    pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
    depth_metric: M,
    heads_notifier: FilteredHeadsNotifier<R>,
    send_counter: PeerCounter,
    sync_session_observer: Arc<Mutex<Option<DynSyncSessionObserver>>>,
    pending_push_sessions: Arc<Mutex<Map<(PeerId, SedimentreeId), SyncSession>>>,
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    R: RemoteHeadsObserver,
    const N: usize,
> core::fmt::Debug for SyncHandler<F, S, C, P, M, N, R>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SyncHandler").finish_non_exhaustive()
    }
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric + Clone,
    R: RemoteHeadsObserver + Clone,
    const N: usize,
> Clone for SyncHandler<F, S, C, P, M, N, R>
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
            sync_session_observer: self.sync_session_observer.clone(),
            pending_push_sessions: self.pending_push_sessions.clone(),
        }
    }
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize,
> SyncHandler<F, S, C, P, M, N, NoRemoteHeadsObserver>
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
        sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<S, P>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        depth_metric: M,
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
            sync_session_observer: Arc::new(Mutex::new(None)),
            pending_push_sessions: Arc::new(Mutex::new(Map::new())),
        }
    }
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    R: RemoteHeadsObserver,
    const N: usize,
> SyncHandler<F, S, C, P, M, N, R>
{
    /// Create a new `SyncHandler` with a custom remote heads observer.
    #[allow(clippy::type_complexity)]
    pub fn with_remote_heads_observer(
        sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
        connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
        subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
        storage: StoragePowerbox<S, P>,
        pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
        depth_metric: M,
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
            sync_session_observer: Arc::new(Mutex::new(None)),
            pending_push_sessions: Arc::new(Mutex::new(Map::new())),
        }
    }

    /// Set a [`SyncSessionObserver`] implementation.
    ///
    /// This should just be another parameter to `new` but that
    /// would be an invasive change just for a sketch.
    ///
    /// [`SyncSessionObserver`]: crate::sync_session::SyncSessionObserver
    /// [`new`]: Self::new
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

    /// Returns the shared connections map.
    ///
    /// Use this to share the connection map with other handlers (e.g.,
    /// [`EphemeralHandler`]) that need to send messages to connected peers.
    ///
    /// [`EphemeralHandler`]: subduction_ephemeral::handler::EphemeralHandler
    #[allow(clippy::type_complexity)]
    pub fn connections(&self) -> Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>> {
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
        S: Storage<Sendable> + Send + Sync + core::fmt::Debug,
        C: Connection<Sendable, SyncMessage> + PartialEq + Clone + Send + Sync + core::fmt::Debug + 'static,
        P: StoragePolicy<Sendable> + Send + Sync,
        P::FetchDisallowed: Send + 'static,
        P::PutDisallowed: Send + 'static,
        M: DepthMetric + Send + Sync,
        S::Error: Send + 'static,
        C::SendError: Send + 'static,
        C::RecvError: Send + 'static,
        C::DisconnectionError: Send + 'static,
        R: RemoteHeadsObserver + Send + Sync,
    Local where
        S: Storage<Local> + core::fmt::Debug,
        C: Connection<Local, SyncMessage> + PartialEq + Clone + core::fmt::Debug + 'static,
        P: StoragePolicy<Local>,
        M: DepthMetric,
        R: RemoteHeadsObserver
)]
impl<K: FutureForm, S, C, P, M, R, const N: usize> Handler<K, C>
    for SyncHandler<K, S, C, P, M, N, R>
{
    type Message = SyncMessage;
    type HandlerError = ListenError<K, S, C, SyncMessage>;

    fn as_batch_sync_response(msg: &Self::Message) -> Option<&BatchSyncResponse> {
        match msg {
            SyncMessage::BatchSyncResponse(resp) => Some(resp),
            SyncMessage::BatchSyncRequest(_)
            | SyncMessage::BlobsRequest { .. }
            | SyncMessage::BlobsResponse { .. }
            | SyncMessage::DataRequestRejected(_)
            | SyncMessage::Fragment { .. }
            | SyncMessage::LooseCommit { .. }
            | SyncMessage::RemoveSubscriptions(_)
            | SyncMessage::HeadsUpdate { .. } => None,
        }
    }

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, K>,
        message: Self::Message,
    ) -> K::Future<'a, Result<(), Self::HandlerError>> {
        K::from_future(async move { self.dispatch(conn, message).await })
    }

    fn on_peer_disconnect(&self, _peer: PeerId) -> K::Future<'_, ()> {
        // Sync subscriptions are already cleaned by `peers::remove_connection`,
        // so there is nothing extra to do here.
        K::from_future(async {})
    }
}

// ---------------------------------------------------------------------------
// RemoteHeadsNotifier implementation
// ---------------------------------------------------------------------------

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    R: RemoteHeadsObserver,
    const N: usize,
> RemoteHeadsNotifier for SyncHandler<F, S, C, P, M, N, R>
{
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        self.heads_notifier.notify(id, peer, heads);
    }
}

// ---------------------------------------------------------------------------
// Dispatch + recv_* methods (self-contained copies from Subduction)
// ---------------------------------------------------------------------------

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F, SyncMessage> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    R: RemoteHeadsObserver,
    const N: usize,
> SyncHandler<F, S, C, P, M, N, R>
{
    #[allow(clippy::too_many_lines)]
    async fn dispatch(
        &self,
        conn: &Authenticated<C, F>,
        message: SyncMessage,
    ) -> Result<(), ListenError<F, S, C, SyncMessage>> {
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
                    tracing::debug!("added subscription for peer {from} to sedimentree {id:?}");
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
                        tracing::info!("peer {from} reports sedimentree {id:?} not found");
                    }
                    SyncResult::Unauthorized => {
                        tracing::info!(
                            "peer {from} reports we are unauthorized to access sedimentree {id:?}"
                        );
                    }
                }
            }
            SyncMessage::BlobsRequest { id, digests } => {
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
                    "blob response from peer {from} for {id:?}: {accepted_count}/{} blobs acknowledged (compound storage - blobs stored with commits)",
                    blobs.len()
                );
            }
            SyncMessage::RemoveSubscriptions(crate::connection::message::RemoveSubscriptions {
                ids,
            }) => {
                self.remove_subscriptions(from, &ids).await;
                tracing::debug!("removed subscriptions for peer {from}: {ids:?}");
            }
            SyncMessage::DataRequestRejected(crate::connection::message::DataRequestRejected {
                id,
            }) => {
                tracing::info!("peer {from} rejected our data request for sedimentree {id:?}");
            }
            SyncMessage::HeadsUpdate { id, heads } => {
                tracing::debug!(
                    "peer {from} reports heads for sedimentree {id:?}: {} heads",
                    heads.heads.len()
                );
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
        conn: &Authenticated<C, F>,
    ) -> Result<bool, IoError<F, S, C, SyncMessage>> {
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

        let author = verified.verified_author();
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

        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("blob mismatch from peer {:?}: {e}", from);
                return Err(IoError::BlobMismatch(e));
            }
        };
        let commit_id = verified_meta.payload().head();

        let was_new = self
            .insert_commit_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        self.minimize_tree(id).await;

        if was_new {
            let session_key = (*from, id);
            {
                let mut sessions = self.pending_push_sessions.lock().await;
                let session = sessions
                    .entry(session_key)
                    .or_insert_with(|| SyncSession::new(id, *from, SyncSessionKind::InboundPush));
                session.received_commit_ids.push(commit_id);
            }
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
                tracing::warn!("peer {} disconnected while sending HeadsUpdate: {e}", from);
            }

            let maybe_session = self.pending_push_sessions.lock().await.remove(&session_key);
            if let Some(session) = maybe_session {
                self.emit_sync_session(session).await;
            }

            // Broadcast to subscribers (excluding sender)
            let conns = self.get_authorized_subscriber_conns(id, from).await;
            for conn in conns {
                let peer_id = conn.peer_id();
                let msg = SyncMessage::LooseCommit {
                    id,
                    commit: signed_for_wire.clone(),
                    blob: blob.clone(),
                    sender_heads: RemoteHeads {
                        counter: self.send_counter.next(peer_id).await,
                        heads: heads.clone(),
                    },
                };
                if let Err(e) = conn.send(&msg).await {
                    tracing::warn!("peer {peer_id} disconnected: {e}");
                    self.remove_connection(&conn).await;
                }
            }
        }

        Ok(was_new)
    }

    async fn recv_fragment(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        signed_fragment: &Signed<Fragment>,
        blob: Blob,
        conn: &Authenticated<C, F>,
    ) -> Result<bool, IoError<F, S, C, SyncMessage>> {
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

        let author = verified.verified_author();
        tracing::debug!(
            "receiving fragment {:?} for sedimentree {:?} from peer {:?} (author {:?})",
            Digest::hash(verified.payload()),
            id,
            from,
            author
        );

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

        let signed_for_wire = verified.signed().clone();

        let verified_meta = match VerifiedMeta::new(verified, blob.clone()) {
            Ok(vm) => vm,
            Err(e) => {
                tracing::warn!("blob mismatch from peer {:?}: {e}", from);
                return Err(IoError::BlobMismatch(e));
            }
        };
        let fragment_id = verified_meta.payload().head();

        let was_new = self
            .insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        self.minimize_tree(id).await;

        if was_new {
            let session_key = (*from, id);
            {
                let mut sessions = self.pending_push_sessions.lock().await;
                let session = sessions
                    .entry(session_key)
                    .or_insert_with(|| SyncSession::new(id, *from, SyncSessionKind::InboundPush));
                session.received_fragment_ids.push(fragment_id);
            }
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
                tracing::warn!("peer {} disconnected while sending HeadsUpdate: {e}", from);
            }

            let maybe_session = self.pending_push_sessions.lock().await.remove(&session_key);
            if let Some(session) = maybe_session {
                self.emit_sync_session(session).await;
            }

            // Broadcast to subscribers (excluding sender)
            let conns = self.get_authorized_subscriber_conns(id, from).await;
            for conn in conns {
                let peer_id = conn.peer_id();
                let msg = SyncMessage::Fragment {
                    id,
                    fragment: signed_for_wire.clone(),
                    blob: blob.clone(),
                    sender_heads: RemoteHeads {
                        counter: self.send_counter.next(peer_id).await,
                        heads: heads.clone(),
                    },
                };
                if let Err(e) = conn.send(&msg).await {
                    tracing::warn!("peer {peer_id} disconnected: {e}");
                    self.remove_connection(&conn).await;
                }
            }
        }

        Ok(was_new)
    }

    #[allow(clippy::too_many_lines)]
    async fn recv_batch_sync_request(
        &self,
        id: SedimentreeId,
        their_fingerprints: &FingerprintSummary,
        req_id: RequestId,
        conn: &Authenticated<C, F>,
    ) -> Result<(), ListenError<F, S, C, SyncMessage>> {
        tracing::info!("recv_batch_sync_request for sedimentree {:?}", id);

        let peer_id = conn.peer_id();
        let mut session = SyncSession::new(id, peer_id, SyncSessionKind::InboundBatch);
        let fetcher = match self.storage.get_fetcher::<F>(peer_id, id).await {
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
                        "peer {} disconnected while sending unauthorized response: {e}",
                        conn.peer_id()
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

        let (
            local_commit_ids,
            local_fragment_ids,
            our_missing_commit_fingerprints,
            our_missing_fragment_fingerprints,
            raw_heads,
        ) = {
            let mut locked = self.sedimentrees.get_shard_containing(&id).lock().await;

            if let Entry::Vacant(entry) = locked.entry(id) {
                let loose_commits: Vec<_> = commit_by_id
                    .values()
                    .map(|vm| vm.payload().clone())
                    .collect();
                let fragments: Vec<_> = fragment_by_id
                    .values()
                    .map(|vm| vm.payload().clone())
                    .collect();

                if !loose_commits.is_empty() || !fragments.is_empty() {
                    let sedimentree =
                        Sedimentree::new(fragments, loose_commits).minimize(&self.depth_metric);
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

            let heads = sedimentree.heads(&self.depth_metric);
            let diff = sedimentree.diff_remote_fingerprints(their_fingerprints);
            (
                diff.local_only_commits
                    .iter()
                    .map(|(id, _)| **id)
                    .collect::<Vec<_>>(),
                diff.local_only_fragments
                    .iter()
                    .map(|(id, _)| **id)
                    .collect::<Vec<_>>(),
                diff.remote_only_commit_fingerprints,
                diff.remote_only_fragment_fingerprints,
                heads,
            )
        };

        let responder_heads = RemoteHeads {
            counter: self.send_counter.next(conn.peer_id()).await,
            heads: raw_heads,
        };

        for commit_id in local_commit_ids {
            if let Some(verified) = commit_by_id.get(&commit_id) {
                session.sent_commit_ids.push(commit_id);
                their_missing_commits.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        for frag_id in local_fragment_ids {
            if let Some(verified) = fragment_by_id.get(&frag_id) {
                session.sent_fragment_ids.push(frag_id);
                their_missing_fragments.push((verified.signed().clone(), verified.blob().clone()));
            }
        }

        session.remote_heads = Some(responder_heads.clone());

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

        let msg: SyncMessage = BatchSyncResponse {
            id,
            req_id,
            result: SyncResult::Ok(sync_diff),
            responder_heads,
        }
        .into();
        if let Err(e) = conn.send(&msg).await {
            tracing::warn!("peer {} disconnected: {e}", conn.peer_id());
        }

        self.emit_sync_session(session).await;

        Ok(())
    }

    async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: SyncDiff,
    ) -> Result<(), IoError<F, S, C, SyncMessage>> {
        ingest::recv_batch_sync_response(&self.sedimentrees, &self.storage, from, id, diff).await?;
        self.minimize_tree(id).await;
        Ok(())
    }

    async fn recv_blob_request(
        &self,
        conn: &Authenticated<C, F>,
        id: SedimentreeId,
        digests: &[Digest<Blob>],
    ) -> Result<(), BlobRequestErr<F, S, C, SyncMessage>> {
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
    ) -> Result<Option<Blob>, S::Error> {
        ingest::get_blob(&self.storage, id, digest).await
    }

    async fn insert_commit_locally(
        &self,
        putter: &Putter<F, S>,
        verified_meta: VerifiedMeta<LooseCommit>,
    ) -> Result<bool, S::Error> {
        ingest::insert_commit_locally(&self.sedimentrees, putter, verified_meta).await
    }

    async fn insert_fragment_locally(
        &self,
        putter: &Putter<F, S>,
        verified_meta: VerifiedMeta<Fragment>,
    ) -> Result<bool, S::Error> {
        ingest::insert_fragment_locally(&self.sedimentrees, putter, verified_meta).await
    }

    async fn minimize_tree(&self, id: SedimentreeId) {
        ingest::minimize_tree(&self.sedimentrees, &self.depth_metric, id).await;
    }

    /// Compute the current heads for a sedimentree (without a counter).
    async fn heads_for(&self, id: SedimentreeId) -> Vec<CommitId> {
        let locked = self.sedimentrees.get_shard_containing(&id).lock().await;
        locked
            .get(&id)
            .map(|s| s.heads(&self.depth_metric))
            .unwrap_or_default()
    }

    async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
        peers::add_subscription(&self.subscriptions, peer_id, sedimentree_id).await;
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
    }

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

    async fn remove_connection(&self, conn: &Authenticated<C, F>) -> Option<bool> {
        peers::remove_connection(&self.connections, &self.subscriptions, conn).await
    }
}
