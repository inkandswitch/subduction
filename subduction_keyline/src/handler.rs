//! [`KeylineHandler`]: moves the keyline tree between peers.
//!
//! One sedimentree, [`KEYLINE_TREE_ID`], holds every certificate as batch
//! commits. The handler keeps that tree, its own storage, and the
//! [`Keyline`] store it fills.
//!
//! # Protocol
//!
//! ```text
//!  A                                              B
//!  │── SyncRequest { A's fingerprints } ─────────►│  B tracks A as a peer
//!  │                                              │
//!  │◄── SyncResponse { commits A lacks,  ─────────│
//!  │                   fingerprints B lacks }     │
//!  │                                              │
//!  │── Commit (each one B asked for) ────────────►│
//!  │                                              │
//!  │◄─────────────── Commit (new batches) ───────►│  pushed both ways
//! ```
//!
//! Either side starting a sync makes the two peers push new batches to each
//! other. Anyone may fetch the tree, so the only gate is on the way in.
//!
//! # Admission
//!
//! Every incoming commit is checked before it is stored:
//!
//! 1. The signature verifies, and the blob matches its metadata.
//! 2. It is a canonical batch on the keyline tree, with the right commit ID
//!    ([`Batch::from_commit`]).
//! 3. [`admit`] lets its certificates into the [`Keyline`] store.
//!
//! A commit is stored, added to the tree, and forwarded to the other peers
//! only after admission succeeds. One that is not admissible _yet_ is
//! dropped quietly: whoever holds it offers it again on the next sync, once
//! the certificates it depends on have arrived. Commits in one message are
//! admitted together, so proofs may arrive in any order within it.
//!
//! The tree is the source of truth. [`KeylineHandler::hydrate`] rebuilds the
//! Keyline store from storage, so the store must be filled only through this
//! handler ([`KeylineHandler::publish`] and sync).

use alloc::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    vec,
    vec::Vec,
};

use async_lock::Mutex;
use future_form::{FutureForm, Local, Sendable, future_form};
use keyline::contract::Keyline;
use sedimentree_core::{
    blob::Blob,
    crypto::fingerprint::FingerprintSeed,
    depth::DepthMetric,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::Sedimentree,
};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        Connection,
        message::{RequestId, RequestedData, SyncDiff},
    },
    handler::Handler,
    peer::id::PeerId,
    storage::traits::Storage,
};
use subduction_crypto::{signed::Signed, signer::Signer, verified_meta::VerifiedMeta};

use crate::{
    batch::{Batch, KEYLINE_TREE_ID},
    ingest::{AdmitError, admit},
    message::KeylineMessage,
};

/// Syncs the keyline tree and fills a [`Keyline`] store from it.
///
/// See the [module documentation](self) for the protocol.
pub struct KeylineHandler<Async: FutureForm, K, Store, Conn: Clone, Metric> {
    local_peer: PeerId,
    keyline: Arc<K>,
    storage: Store,
    depth_metric: Metric,

    /// The keyline tree. Held for the whole of an ingest or publish, which
    /// serializes admission.
    tree: Mutex<Sedimentree>,

    /// Peers that receive new batches.
    peers: Mutex<BTreeMap<PeerId, Authenticated<Conn, Async>>>,

    requests: Mutex<Requests>,
}

/// This node's open sync requests.
#[derive(Debug, Default)]
struct Requests {
    next_nonce: u64,

    /// The peer each request went to, and the seed of its fingerprints.
    open: BTreeMap<RequestId, (PeerId, FingerprintSeed)>,
}

impl<Async: FutureForm, K, Store, Conn: Clone, Metric> core::fmt::Debug
    for KeylineHandler<Async, K, Store, Conn, Metric>
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeylineHandler")
            .field("local_peer", &self.local_peer)
            .finish_non_exhaustive()
    }
}

impl<Async, K, Store, Conn, Metric> KeylineHandler<Async, K, Store, Conn, Metric>
where
    Async: FutureForm,
    K: Keyline<Async>,
    Store: Storage<Async>,
    Conn: Connection<Async, KeylineMessage> + Clone,
    Metric: DepthMetric,
{
    /// Load the keyline tree from `storage` and admit it into `keyline`.
    ///
    /// Stored commits were admitted before they were stored, so they are
    /// admitted again here. A stored commit that no longer decodes, or no
    /// longer admits (because the store was once filled from elsewhere), is
    /// kept in the tree and logged.
    ///
    /// # Errors
    ///
    /// Returns [`KeylineSyncError`] if storage or the Keyline store fails.
    pub async fn hydrate(
        local_peer: PeerId,
        keyline: Arc<K>,
        storage: Store,
        depth_metric: Metric,
    ) -> Result<Self, KeylineSyncError<K::Error, Store::Error>> {
        let stored = storage
            .load_loose_commits(KEYLINE_TREE_ID)
            .await
            .map_err(KeylineSyncError::Storage)?;

        let mut tree = Sedimentree::default();
        let mut pending = Vec::with_capacity(stored.len());
        for verified in stored {
            tree.add_commit(verified.payload().clone());
            match Batch::from_commit(verified.payload(), verified.blob()) {
                Ok(batch) => pending.push(batch),
                Err(error) => {
                    tracing::error!(commit = ?verified.payload().head(), %error, "stored keyline commit is not a batch");
                }
            }
        }

        let (_, unadmitted) = admit_all(keyline.as_ref(), pending).await?;
        if !unadmitted.is_empty() {
            tracing::warn!(
                count = unadmitted.len(),
                "stored keyline batches did not admit; was the store filled outside this handler?"
            );
        }

        Ok(Self {
            local_peer,
            keyline,
            storage,
            depth_metric,
            tree: Mutex::new(tree),
            peers: Mutex::new(BTreeMap::new()),
            requests: Mutex::new(Requests::default()),
        })
    }

    /// The Keyline store this handler fills.
    #[must_use]
    pub const fn keyline(&self) -> &Arc<K> {
        &self.keyline
    }

    /// The keyline tree's current heads.
    pub async fn heads(&self) -> Vec<CommitId> {
        self.tree.lock().await.heads(&self.depth_metric)
    }

    /// Admit, store, and push a batch issued by this node.
    ///
    /// The commit's parents are the tree's current heads. `signer` signs the
    /// commit; the certificates inside are already signed by their issuers.
    ///
    /// # Errors
    ///
    /// Returns [`PublishError::Admit`] if the batch does not admit here (for
    /// example, an issuer with no standing yet), so nothing unadmissible is
    /// published. Returns [`PublishError::Storage`] if storage fails.
    pub async fn publish<S: Signer<Async>>(
        &self,
        signer: &S,
        batch: Batch<K::RetentionWatermark>,
    ) -> Result<CommitId, PublishError<K::Error, Store::Error>> {
        let (head, sealed) = {
            let mut tree = self.tree.lock().await;
            let parents: BTreeSet<CommitId> = tree.heads(&self.depth_metric).into_iter().collect();
            let (commit, blob) = batch.into_commit(parents);

            admit(self.keyline.as_ref(), &batch).await?;

            let verified = Signed::seal::<Async, _>(signer, commit).await;
            let verified =
                VerifiedMeta::new(verified, blob).map_err(|_| PublishError::BlobMismatch)?;
            self.storage
                .save_loose_commit(KEYLINE_TREE_ID, verified.clone())
                .await
                .map_err(PublishError::Storage)?;
            tree.add_commit(verified.payload().clone());

            let head = verified.payload().head();
            let (wire_commit, _, blob) = verified.into_full_parts();
            (head, (wire_commit, blob))
        };

        self.push(None, &[sealed]).await;
        Ok(head)
    }

    /// Start a sync with the peer on `conn`, and track it for pushes.
    ///
    /// `seed` keys this request's fingerprints. Use a fresh random seed per
    /// request, so a peer cannot precompute colliding fingerprints.
    ///
    /// The response is handled by [`Handler::handle`] when it arrives.
    ///
    /// # Errors
    ///
    /// Returns the connection's send error.
    pub async fn sync_with(
        &self,
        conn: &Authenticated<Conn, Async>,
        seed: FingerprintSeed,
    ) -> Result<(), Conn::SendError> {
        self.track(conn).await;

        let req_id = {
            let mut requests = self.requests.lock().await;
            let req_id = RequestId {
                requestor: self.local_peer,
                nonce: requests.next_nonce,
            };
            requests.next_nonce = requests.next_nonce.wrapping_add(1);
            requests.open.insert(req_id, (conn.peer_id(), seed));
            req_id
        };

        let summary = self.tree.lock().await.fingerprint_summarize(&seed);
        conn.send(&KeylineMessage::SyncRequest { req_id, summary })
            .await
    }

    async fn dispatch(
        &self,
        conn: &Authenticated<Conn, Async>,
        message: KeylineMessage,
    ) -> Result<(), KeylineSyncError<K::Error, Store::Error>> {
        match message {
            KeylineMessage::SyncRequest { req_id, summary } => {
                self.on_request(conn, req_id, &summary).await
            }
            KeylineMessage::SyncResponse { req_id, diff } => {
                self.on_response(conn, req_id, diff).await
            }
            KeylineMessage::Commit { commit, blob } => {
                let accepted = self.ingest(conn.peer_id(), vec![(commit, blob)]).await?;
                self.push(Some(conn.peer_id()), &accepted).await;
                Ok(())
            }
        }
    }

    async fn on_request(
        &self,
        conn: &Authenticated<Conn, Async>,
        req_id: RequestId,
        summary: &sedimentree_core::sedimentree::FingerprintSummary,
    ) -> Result<(), KeylineSyncError<K::Error, Store::Error>> {
        self.track(conn).await;

        let (theirs_missing, ours_missing) = {
            let tree = self.tree.lock().await;
            let diff = tree.diff_remote_fingerprints(summary);
            let theirs_missing: Vec<CommitId> =
                diff.local_only_commits.iter().map(|(id, _)| **id).collect();
            (theirs_missing, diff.remote_only_commit_fingerprints)
        };

        let missing_commits = self.load(&theirs_missing).await?;
        let response = KeylineMessage::SyncResponse {
            req_id,
            diff: SyncDiff {
                missing_commits,
                missing_fragments: Vec::new(),
                requesting: RequestedData {
                    commit_fingerprints: ours_missing,
                    fragment_fingerprints: Vec::new(),
                },
            },
        };

        if let Err(error) = conn.send(&response).await {
            tracing::debug!(peer = %conn.peer_id(), %error, "failed to send keyline sync response");
        }
        Ok(())
    }

    async fn on_response(
        &self,
        conn: &Authenticated<Conn, Async>,
        req_id: RequestId,
        diff: SyncDiff,
    ) -> Result<(), KeylineSyncError<K::Error, Store::Error>> {
        let peer = conn.peer_id();
        let seed = {
            let mut requests = self.requests.lock().await;
            match requests.open.remove(&req_id) {
                Some((to, seed)) if to == peer => Some(seed),
                Some(other) => {
                    // Not this peer's request: put it back for its peer.
                    requests.open.insert(req_id, other);
                    None
                }
                None => None,
            }
        };

        let accepted = self.ingest(peer, diff.missing_commits).await?;
        self.push(Some(peer), &accepted).await;

        // Without our seed we cannot tell which of our commits they mean.
        let Some(seed) = seed else {
            tracing::debug!(%peer, ?req_id, "keyline sync response to no open request of theirs");
            return Ok(());
        };

        let wanted: Vec<CommitId> = {
            let tree = self.tree.lock().await;
            let resolver = tree.fingerprint_resolver(&seed);
            diff.requesting
                .commit_fingerprints
                .iter()
                .filter_map(|fp| resolver.resolve_commit(fp))
                .collect()
        };

        for (commit, blob) in self.load(&wanted).await? {
            if let Err(error) = conn.send(&KeylineMessage::Commit { commit, blob }).await {
                tracing::debug!(%peer, %error, "failed to send a requested keyline commit");
                break;
            }
        }
        Ok(())
    }

    /// Admit, store, and add to the tree whichever of `commits` admit.
    /// Returns those, for forwarding.
    async fn ingest(
        &self,
        from: PeerId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
    ) -> Result<Vec<(Signed<LooseCommit>, Blob)>, KeylineSyncError<K::Error, Store::Error>> {
        let mut tree = self.tree.lock().await;

        let mut candidates = Vec::with_capacity(commits.len());
        for (signed, blob) in commits {
            let verified = match signed.try_verify() {
                Ok(verified) => verified,
                Err(error) => {
                    tracing::warn!(peer = %from, %error, "keyline commit signature did not verify");
                    continue;
                }
            };
            let Ok(verified) = VerifiedMeta::new(verified, blob) else {
                tracing::warn!(peer = %from, "keyline commit blob does not match its metadata");
                continue;
            };
            if tree.has_loose_commit(verified.payload().head()) {
                continue;
            }
            match Batch::from_commit(verified.payload(), verified.blob()) {
                Ok(batch) => candidates.push((verified, batch)),
                Err(error) => {
                    tracing::warn!(peer = %from, %error, "keyline commit is not a valid batch commit");
                }
            }
        }

        let mut accepted = Vec::new();
        loop {
            let mut waiting = Vec::new();
            let before = accepted.len();
            for (verified, batch) in candidates {
                match admit(self.keyline.as_ref(), &batch).await {
                    Ok(_) => {
                        self.storage
                            .save_loose_commit(KEYLINE_TREE_ID, verified.clone())
                            .await
                            .map_err(KeylineSyncError::Storage)?;
                        tree.add_commit(verified.payload().clone());
                        let (signed, _, blob) = verified.into_full_parts();
                        accepted.push((signed, blob));
                    }
                    Err(AdmitError::NotYet { .. }) => waiting.push((verified, batch)),
                    Err(AdmitError::Storage(error)) => {
                        return Err(KeylineSyncError::Keyline(error));
                    }
                    Err(
                        error @ (AdmitError::InvalidCertificate { .. }
                        | AdmitError::CorruptStore(_)),
                    ) => {
                        tracing::warn!(peer = %from, commit = ?verified.payload().head(), %error, "keyline batch refused");
                    }
                }
            }

            candidates = waiting;
            if candidates.is_empty() || accepted.len() == before {
                break;
            }
        }

        if !candidates.is_empty() {
            tracing::debug!(
                peer = %from,
                count = candidates.len(),
                "keyline batches not admissible yet; awaiting their proofs"
            );
        }
        Ok(accepted)
    }

    /// Load commits with their blobs. Missing ones are skipped.
    async fn load(
        &self,
        ids: &[CommitId],
    ) -> Result<Vec<(Signed<LooseCommit>, Blob)>, KeylineSyncError<K::Error, Store::Error>> {
        let mut loaded = Vec::with_capacity(ids.len());
        for id in ids {
            let stored = self
                .storage
                .load_loose_commit(KEYLINE_TREE_ID, *id)
                .await
                .map_err(KeylineSyncError::Storage)?;
            if let Some(verified) = stored {
                let (signed, _, blob) = verified.into_full_parts();
                loaded.push((signed, blob));
            } else {
                tracing::warn!(commit = ?id, "keyline commit in the tree is missing from storage");
            }
        }
        Ok(loaded)
    }

    /// Send `commits` to every tracked peer except `except`.
    async fn push(&self, except: Option<PeerId>, commits: &[(Signed<LooseCommit>, Blob)]) {
        if commits.is_empty() {
            return;
        }

        let peers: Vec<Authenticated<Conn, Async>> = self
            .peers
            .lock()
            .await
            .iter()
            .filter(|(id, _)| Some(**id) != except)
            .map(|(_, conn)| conn.clone())
            .collect();

        for conn in peers {
            for (commit, blob) in commits {
                let message = KeylineMessage::Commit {
                    commit: commit.clone(),
                    blob: blob.clone(),
                };
                if let Err(error) = conn.send(&message).await {
                    tracing::debug!(peer = %conn.peer_id(), %error, "failed to push a keyline commit");
                    break;
                }
            }
        }
    }

    async fn track(&self, conn: &Authenticated<Conn, Async>) {
        self.peers.lock().await.insert(conn.peer_id(), conn.clone());
    }

    async fn forget(&self, peer: PeerId) {
        self.peers.lock().await.remove(&peer);
        self.requests
            .lock()
            .await
            .open
            .retain(|_, (to, _)| *to != peer);
    }
}

#[future_form(
    Sendable where
        K: Send + Sync,
        K::Error: Send,
        K::RetentionWatermark: Send + Sync,
        Store: Send + Sync,
        Store::Error: Send,
        Conn: Send + Sync,
        Conn::SendError: Send,
        Metric: Send + Sync,
    Local
)]
impl<Async, K, Store, Conn, Metric> Handler<Async, Conn>
    for KeylineHandler<Async, K, Store, Conn, Metric>
where
    Async: FutureForm,
    K: Keyline<Async>,
    K::Error: 'static,
    K::RetentionWatermark: 'static,
    Store: Storage<Async>,
    Store::Error: 'static,
    Conn: Connection<Async, KeylineMessage> + Clone,
    Conn::SendError: 'static,
    Metric: DepthMetric,
{
    type Message = KeylineMessage;
    type HandlerError = KeylineSyncError<K::Error, Store::Error>;

    fn handle<'a>(
        &'a self,
        conn: &'a Authenticated<Conn, Async>,
        message: KeylineMessage,
    ) -> Async::Future<'a, Result<(), Self::HandlerError>> {
        Async::from_future(self.dispatch(conn, message))
    }

    fn on_peer_disconnect(&self, peer: PeerId) -> Async::Future<'_, ()> {
        Async::from_future(self.forget(peer))
    }
}

/// Admit `batches` in rounds until no more admit. Returns the admitted
/// count and the batches still waiting.
async fn admit_all<F: FutureForm, K: Keyline<F>, SE>(
    keyline: &K,
    mut pending: Vec<Batch<K::RetentionWatermark>>,
) -> Result<(usize, Vec<Batch<K::RetentionWatermark>>), KeylineSyncError<K::Error, SE>> {
    let mut admitted = 0;
    loop {
        let before = admitted;
        let mut waiting = Vec::new();
        for batch in pending {
            match admit(keyline, &batch).await {
                Ok(_) => admitted += 1,
                Err(AdmitError::NotYet { .. }) => waiting.push(batch),
                Err(AdmitError::Storage(error)) => return Err(KeylineSyncError::Keyline(error)),
                Err(
                    error @ (AdmitError::InvalidCertificate { .. } | AdmitError::CorruptStore(_)),
                ) => {
                    tracing::error!(%error, "stored keyline batch refused");
                }
            }
        }
        pending = waiting;
        if pending.is_empty() || admitted == before {
            return Ok((admitted, pending));
        }
    }
}

/// A failure that should drop the connection: local storage or the Keyline
/// store failed. Problems with what a peer sent are logged, not returned.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeylineSyncError<KE, SE> {
    /// The Keyline store failed.
    #[error("keyline store failed")]
    Keyline(#[source] KE),

    /// The keyline tree's storage failed.
    #[error("keyline tree storage failed")]
    Storage(#[source] SE),
}

/// Why a batch was not published.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublishError<KE, SE> {
    /// The batch does not admit here.
    #[error(transparent)]
    Admit(#[from] AdmitError<KE>),

    /// The keyline tree's storage failed.
    #[error("keyline tree storage failed")]
    Storage(#[source] SE),

    /// The sealed commit did not match its own blob. Unreachable unless
    /// signing or encoding is broken.
    #[error("sealed commit does not match its blob")]
    BlobMismatch,
}
