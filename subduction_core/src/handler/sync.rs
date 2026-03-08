//! Default sync protocol handler for Subduction.
//!
//! [`SyncHandler`] implements the [`Handler`] trait for the standard
//! Subduction sync protocol. It processes [`Message`] variants
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
//! [`Message`]: crate::connection::message::Message
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
    loose_commit::LooseCommit,
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_crypto::{signed::Signed, verified_meta::VerifiedMeta};

use crate::{
    connection::{
        Connection,
        authenticated::Authenticated,
        message::{
            BatchSyncRequest, BatchSyncResponse, Message, RequestId, RequestedData, SyncDiff,
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
};

use super::Handler;

/// The default sync protocol handler for Subduction.
///
/// Processes the standard [`Message`] protocol: commits, fragments,
/// batch sync requests/responses, blob requests/responses, and
/// subscription management.
///
/// # Construction
///
/// Built internally by [`Subduction::new`] / [`Subduction::hydrate`]
/// with clones of the shared `Arc` state. Not intended to be
/// constructed directly by users.
///
/// [`Subduction::new`]: crate::subduction::Subduction::new
/// [`Subduction::hydrate`]: crate::subduction::Subduction::hydrate
pub struct SyncHandler<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize = 256,
> {
    sedimentrees: Arc<ShardedMap<SedimentreeId, Sedimentree, N>>,
    connections: Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>,
    subscriptions: Arc<Mutex<Map<SedimentreeId, Set<PeerId>>>>,
    storage: StoragePowerbox<S, P>,
    pending_blob_requests: Arc<Mutex<PendingBlobRequests>>,
    depth_metric: M,
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize,
> core::fmt::Debug for SyncHandler<F, S, C, P, M, N>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SyncHandler").finish_non_exhaustive()
    }
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric + Clone,
    const N: usize,
> Clone for SyncHandler<F, S, C, P, M, N>
{
    fn clone(&self) -> Self {
        Self {
            sedimentrees: self.sedimentrees.clone(),
            connections: self.connections.clone(),
            subscriptions: self.subscriptions.clone(),
            storage: self.storage.clone(),
            pending_blob_requests: self.pending_blob_requests.clone(),
            depth_metric: self.depth_metric.clone(),
        }
    }
}

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize,
> SyncHandler<F, S, C, P, M, N>
{
    /// Create a new `SyncHandler` from shared state.
    ///
    /// The `Arc`s should be clones of the same references passed to
    /// [`Subduction::new`], so mutations through the handler are visible
    /// to `Subduction` and vice versa.
    ///
    /// [`Subduction::new`]: crate::subduction::Subduction::new
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
        }
    }
}

// ---------------------------------------------------------------------------
// Handler implementation
// ---------------------------------------------------------------------------

#[future_form(
    Sendable where
        S: Storage<Sendable> + Send + Sync + core::fmt::Debug,
        C: Connection<Sendable> + PartialEq + Clone + Send + Sync + core::fmt::Debug + 'static,
        P: StoragePolicy<Sendable> + Send + Sync,
        P::FetchDisallowed: Send + 'static,
        P::PutDisallowed: Send + 'static,
        M: DepthMetric + Send + Sync,
        S::Error: Send + 'static,
        C::SendError: Send + 'static,
        C::RecvError: Send + 'static,
        C::CallError: Send + 'static,
        C::DisconnectionError: Send + 'static,
    Local where
        S: Storage<Local> + core::fmt::Debug,
        C: Connection<Local> + PartialEq + Clone + core::fmt::Debug + 'static,
        P: StoragePolicy<Local>,
        M: DepthMetric
)]
impl<K: FutureForm, S, C, P, M, const N: usize> Handler<K, C>
    for SyncHandler<K, S, C, P, M, N>
{
    type Message = Message;
    type HandlerError = ListenError<K, S, C>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<C, K>,
        message: Self::Message,
    ) -> K::Future<'a, Result<(), Self::HandlerError>> {
        K::from_future(async move { self.dispatch(conn, message).await })
    }
}

// ---------------------------------------------------------------------------
// Dispatch + recv_* methods (self-contained copies from Subduction)
// ---------------------------------------------------------------------------

impl<
    F: FutureForm,
    S: Storage<F>,
    C: Connection<F> + PartialEq + Clone + 'static,
    P: StoragePolicy<F>,
    M: DepthMetric,
    const N: usize,
> SyncHandler<F, S, C, P, M, N>
{
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

                if subscribe {
                    self.add_subscription(from, id).await;
                    tracing::debug!("added subscription for peer {from} to sedimentree {id:?}");
                }

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
            Message::RemoveSubscriptions(crate::connection::message::RemoveSubscriptions {
                ids,
            }) => {
                self.remove_subscriptions(from, &ids).await;
                tracing::debug!("removed subscriptions for peer {from}: {ids:?}");
            }
            Message::DataRequestRejected(crate::connection::message::DataRequestRejected {
                id,
            }) => {
                tracing::info!("peer {from} rejected our data request for sedimentree {id:?}");
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

        self.minimize_tree(id).await;

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
                    tracing::info!("peer {peer_id} disconnected: {e}");
                    self.unregister(&conn).await;
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
    ) -> Result<bool, IoError<F, S, C>> {
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

        let was_new = self
            .insert_fragment_locally(&putter, verified_meta)
            .await
            .map_err(IoError::Storage)?;

        self.minimize_tree(id).await;

        if was_new {
            let msg = Message::Fragment {
                id,
                fragment: signed_for_wire,
                blob,
            };

            let conns = self.get_authorized_subscriber_conns(id, from).await;
            for conn in conns {
                let peer_id = conn.peer_id();
                if let Err(e) = conn.send(&msg).await {
                    tracing::info!("peer {peer_id} disconnected: {e}");
                    self.unregister(&conn).await;
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
                let msg: Message = BatchSyncResponse {
                    id,
                    req_id,
                    result: SyncResult::Unauthorized,
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

        let commit_by_digest: Map<Digest<LooseCommit>, VerifiedMeta<LooseCommit>> =
            verified_commits
                .into_iter()
                .map(|vm| (Digest::hash(vm.payload()), vm))
                .collect();
        let fragment_by_digest: Map<Digest<Fragment>, VerifiedMeta<Fragment>> = verified_fragments
            .into_iter()
            .map(|vm| (vm.payload().digest(), vm))
            .collect();

        let (
            local_commit_digests,
            local_fragment_digests,
            our_missing_commit_fingerprints,
            our_missing_fragment_fingerprints,
        ) = {
            let mut locked = self.sedimentrees.get_shard_containing(&id).lock().await;

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
        };

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
            tracing::info!("peer {} disconnected: {e}", conn.peer_id());
        }

        Ok(())
    }

    async fn recv_batch_sync_response(
        &self,
        from: &PeerId,
        id: SedimentreeId,
        diff: SyncDiff,
    ) -> Result<(), IoError<F, S, C>> {
        ingest::recv_batch_sync_response(&self.sedimentrees, &self.storage, from, id, diff).await
    }

    async fn recv_blob_request(
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

    async fn unregister(&self, conn: &Authenticated<C, F>) -> Option<bool> {
        peers::unregister(&self.connections, &self.subscriptions, conn).await
    }
}
