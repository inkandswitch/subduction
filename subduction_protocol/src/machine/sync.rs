//! Batch sync sessions: the 1.5-RTT fingerprint sync, sans-io.
//!
//! Ported from `legacy/subduction_core/src/{handler/sync.rs, subduction.rs}`;
//! the pure diff logic (`ResponderDiff`) copies verbatim, the async
//! orchestration inverts into per-connection pending state.
//!
//! ```text
//!  Requester                                   Responder
//!  ─────────                                   ─────────
//!  Command::SyncTree
//!    summary = fingerprints(resident tree)
//!    ── BatchSyncRequest{summary} ──────────▶  diff vs resident tree (pure)
//!    requests[nonce] = {tree, seed, deadline}    ├─ nothing to send:
//!                                                │    respond immediately
//!                                                └─ FetchItems (blobs)
//!                                                     └▶ pending BuildResponse
//!  ◀── BatchSyncResponse{diff, req_heads} ──── on Fetched: respond
//!    ├─ Ingest(missing items)  ──▶ pending IngestRemote
//!    │    on Ingested: merge metadata into resident tree
//!    └─ resolve requested fingerprints (pure, via stored seed)
//!         FetchItems ──▶ pending ReturnRequested
//!           on Fetched:
//!    ── LooseCommit / Fragment (fire-and-forget) ──▶ per-item ingest
//! ```
//!
//! The legacy `Multiplexer` (pending-response map + oneshot channels)
//! dissolves into `ConnEntry::requests`, keyed by request nonce, with
//! deadlines folded into `poll_timeout`. Subscriptions and broadcast
//! fan-out are the next chunk; `subscribe` flags are accepted but not yet
//! acted on.

use alloc::{vec, vec::Vec};

use sedimentree_core::{
    blob::Blob,
    crypto::fingerprint::FingerprintSeed,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{id::CommitId, LooseCommit},
    sedimentree::{FingerprintSummary, Sedimentree},
};
use subduction_crypto::signed::Signed;

use super::{try_decode_payload, Machine, Now};
use crate::{
    effect::{AppEvent, Effect, SyncStatus},
    id::ConnId,
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    remote_heads::RemoteHeads,
    storage::{Provenance, StorageFailure, StorageOp, StorageResult},
    timestamp::Timestamp,
    wire::{
        BatchSyncRequest, BatchSyncResponse, DataRequestRejected, RequestId, RequestedData,
        SyncDiff, SyncMessage, SyncResult,
    },
};

use sedimentree_core::depth::CountLeadingZeroBytes;

/// A post-handshake driver op in flight on a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ConnPending {
    /// Responder: awaiting `FetchItems` for the diff's blobs.
    BuildResponse {
        /// The request being answered.
        req_id: RequestId,
        /// The tree being synced.
        tree: SedimentreeId,
        /// Fingerprints we did not recognize (echoed back).
        requesting: RequestedData,
        /// Our heads at diff time.
        heads: Vec<CommitId>,
    },

    /// Requester: awaiting `FetchItems` for items the responder asked for.
    ReturnRequested {
        /// The tree being synced.
        tree: SedimentreeId,
    },

    /// Awaiting `Ingest` durability for received items. The items are
    /// held (transit-only) so the completion can both merge metadata into
    /// the resident tree and forward to other subscribers.
    IngestRemote {
        /// The tree being appended to.
        tree: SedimentreeId,
        /// The signed commits with blobs, in op order.
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        /// The signed fragments with blobs, in op order.
        fragments: Vec<(Signed<Fragment>, Blob)>,
        /// Whether to ack with a `HeadsUpdate` after ingesting — true for
        /// individual commit/fragment messages (the 1.5-RTT second half /
        /// subscription pushes; legacy's `FanOut.ack_msg`), false for
        /// batch-response ingests (the requester does not ack).
        ack: bool,
    },
}

/// An in-flight batch sync request we initiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OutboundRequest {
    /// The tree being synced.
    pub(super) tree: SedimentreeId,

    /// The fingerprint seed we summarized with (needed to resolve the
    /// responder's echoed fingerprints).
    pub(super) seed: FingerprintSeed,

    /// Whether we asked to subscribe (mutual-subscription bookkeeping —
    /// next chunk).
    pub(super) subscribe: bool,

    /// When this request expires.
    pub(super) deadline: Timestamp,
}

impl Machine {
    // ── requester ──────────────────────────────────────────────────

    /// `Command::SyncTree`: send a fingerprint summary, remember the
    /// request.
    pub(super) fn start_sync(
        &mut self,
        now: Now,
        conn: ConnId,
        tree: SedimentreeId,
        subscribe: bool,
    ) -> Outcome {
        let seed = self.next_fingerprint_seed();
        // A tree we don't hold syncs as empty: we advertise nothing and
        // the peer sends us everything (as legacy).
        let summary = self.summarize(tree, &seed);

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        if !matches!(entry.state, super::HandshakeState::Authenticated) {
            return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
        }

        let nonce = self.request_nonce;
        self.request_nonce = self.request_nonce.saturating_add(1);
        let req_id = RequestId {
            requestor: self.config.local_peer,
            nonce,
        };
        entry.requests.insert(
            nonce,
            OutboundRequest {
                tree,
                seed,
                subscribe,
                deadline: now.monotonic.saturating_add(self.config.sync_timeout),
            },
        );

        let msg = SyncMessage::BatchSyncRequest(BatchSyncRequest {
            id: tree,
            req_id,
            fingerprint_summary: summary,
            subscribe,
        });
        self.effects.push_back(Effect::SendMessage {
            conn,
            bytes: msg.encode(),
        });
        self.stats.sync_requests_sent = self.stats.sync_requests_sent.saturating_add(1);
        Outcome::Progressed
    }

    /// Expire timed-out requests. Returns whether any fired.
    pub(super) fn expire_sync_requests(&mut self, now: Timestamp) -> bool {
        let mut expired: Vec<(ConnId, u64, SedimentreeId)> = Vec::new();
        for (conn, entry) in &self.conns {
            for (nonce, request) in &entry.requests {
                if request.deadline.is_due(now) {
                    expired.push((*conn, *nonce, request.tree));
                }
            }
        }
        for (conn, nonce, tree) in &expired {
            if let Some(entry) = self.conns.get_mut(conn) {
                let _request = entry.requests.remove(nonce);
            }
            self.effects.push_back(Effect::App(AppEvent::SyncFinished {
                conn: *conn,
                tree: *tree,
                status: SyncStatus::TimedOut,
            }));
        }
        !expired.is_empty()
    }

    // ── message dispatch ───────────────────────────────────────────

    /// One decoded sync-protocol message on an authenticated connection.
    pub(super) fn on_sync_message(
        &mut self,
        now: Now,
        conn: ConnId,
        peer: PeerId,
        msg: SyncMessage,
    ) -> Outcome {
        match msg {
            SyncMessage::BatchSyncRequest(request) => {
                self.on_batch_sync_request(conn, peer, &request)
            }
            SyncMessage::BatchSyncResponse(response) => {
                self.on_batch_sync_response(now, conn, peer, response)
            }
            SyncMessage::LooseCommit {
                id,
                commit,
                blob,
                sender_heads,
            } => {
                self.notify_heads(id, peer, sender_heads);
                self.ingest_remote_items(conn, peer, id, vec![(commit, blob)], vec![], true)
            }
            SyncMessage::Fragment {
                id,
                fragment,
                blob,
                sender_heads,
            } => {
                self.notify_heads(id, peer, sender_heads);
                self.ingest_remote_items(conn, peer, id, vec![], vec![(fragment, blob)], true)
            }
            SyncMessage::HeadsUpdate { id, heads } => {
                self.notify_heads(id, peer, heads);
                Outcome::Progressed
            }
            SyncMessage::RemoveSubscriptions(unsub) => {
                for tree in &unsub.ids {
                    if let Some(conns) = self.subscriptions.get_mut(tree) {
                        let _removed = conns.remove(&conn);
                        if conns.is_empty() {
                            let _entry = self.subscriptions.remove(tree);
                        }
                    }
                }
                Outcome::Progressed
            }
            // Informational: the peer could not fulfil our data request.
            SyncMessage::DataRequestRejected(_) => Outcome::Progressed,
        }
    }

    // ── responder ──────────────────────────────────────────────────

    fn on_batch_sync_request(
        &mut self,
        conn: ConnId,
        peer: PeerId,
        request: &BatchSyncRequest,
    ) -> Outcome {
        self.stats.sync_requests_received = self.stats.sync_requests_received.saturating_add(1);
        let tree = request.id;

        if request.subscribe {
            let _new = self.subscriptions.entry(tree).or_default().insert(conn);
        }

        let Some(resident) = self.trees.get_mut(&tree) else {
            let heads = self.next_sender_heads(peer, vec![]);
            self.respond(conn, request.req_id, tree, SyncResult::NotFound, heads);
            return Outcome::Progressed;
        };

        let minimal = resident.minimized(&CountLeadingZeroBytes);
        let diff = ResponderDiff::new(minimal, &request.fingerprint_summary);
        let requesting = RequestedData {
            commit_fingerprints: diff.requesting_commit_fingerprints,
            fragment_fingerprints: diff.requesting_fragment_fingerprints,
        };

        if diff.local_commit_ids.is_empty() && diff.local_fragment_ids.is_empty() {
            // Nothing to load: respond in this turn.
            let heads = self.next_sender_heads(peer, diff.heads);
            let result = SyncResult::Ok(SyncDiff {
                missing_commits: vec![],
                missing_fragments: vec![],
                requesting,
            });
            self.respond(conn, request.req_id, tree, result, heads);
            return Outcome::Progressed;
        }

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let ticket = entry.issue_storage_ticket(conn);
        entry.pending.insert(
            ticket.seq,
            ConnPending::BuildResponse {
                req_id: request.req_id,
                tree,
                requesting,
                heads: diff.heads,
            },
        );
        self.effects.push_back(Effect::Storage {
            ticket,
            op: StorageOp::FetchItems {
                tree,
                provenance: Provenance::Remote(peer),
                commit_ids: diff.local_commit_ids,
                fragment_heads: diff.local_fragment_ids,
            },
        });
        Outcome::Progressed
    }

    // ── requester: response processing ─────────────────────────────

    fn on_batch_sync_response(
        &mut self,
        _now: Now,
        conn: ConnId,
        peer: PeerId,
        response: BatchSyncResponse,
    ) -> Outcome {
        // Only responses to requests we actually made can land.
        if response.req_id.requestor != self.config.local_peer {
            return Outcome::Ignored(IgnoreReason::UnknownRequest);
        }
        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let Some(request) = entry.requests.remove(&response.req_id.nonce) else {
            return Outcome::Ignored(IgnoreReason::UnknownRequest);
        };
        self.stats.sync_responses_received = self.stats.sync_responses_received.saturating_add(1);

        let tree = response.id;
        self.notify_heads(tree, peer, response.responder_heads);

        let diff = match response.result {
            SyncResult::NotFound => {
                self.finish_sync(conn, tree, SyncStatus::NotFound);
                return Outcome::Progressed;
            }
            SyncResult::Unauthorized => {
                self.finish_sync(conn, tree, SyncStatus::Unauthorized);
                return Outcome::Progressed;
            }
            SyncResult::Ok(diff) => diff,
        };

        // Mutual subscription: we subscribed to them, so their pushes for
        // this tree should flow back to us and ours to them (as legacy).
        if request.subscribe {
            let _new = self.subscriptions.entry(tree).or_default().insert(conn);
        }

        // Ingest what we were missing.
        let _ingest_outcome = self.ingest_remote_items(
            conn,
            peer,
            tree,
            diff.missing_commits,
            diff.missing_fragments,
            false, // batch-response ingests are not acked (legacy parity)
        );

        // Send back what the responder asked for (bidirectional half).
        if !diff.requesting.is_empty() {
            self.return_requested(conn, peer, tree, &request.seed, &diff.requesting);
        }

        self.finish_sync(conn, tree, SyncStatus::Completed);
        Outcome::Progressed
    }

    /// Resolve the responder's echoed fingerprints against our tree and
    /// fetch the corresponding items for sending.
    fn return_requested(
        &mut self,
        conn: ConnId,
        peer: PeerId,
        tree: SedimentreeId,
        seed: &FingerprintSeed,
        requesting: &RequestedData,
    ) {
        let (commit_ids, fragment_heads) = {
            let Some(resident) = self.trees.get_mut(&tree) else {
                return; // tree vanished locally; nothing to send
            };
            let minimal = resident.minimized(&CountLeadingZeroBytes);
            let resolver = minimal.fingerprint_resolver(seed);
            let commit_ids: Vec<CommitId> = requesting
                .commit_fingerprints
                .iter()
                .filter_map(|fp| resolver.resolve_commit(fp))
                .collect();
            let fragment_heads: Vec<CommitId> = requesting
                .fragment_fingerprints
                .iter()
                .filter_map(|fp| resolver.resolve_fragment(fp))
                .collect();
            (commit_ids, fragment_heads)
        };
        if commit_ids.is_empty() && fragment_heads.is_empty() {
            return;
        }

        let Some(entry) = self.conns.get_mut(&conn) else {
            return;
        };
        let ticket = entry.issue_storage_ticket(conn);
        entry
            .pending
            .insert(ticket.seq, ConnPending::ReturnRequested { tree });
        self.effects.push_back(Effect::Storage {
            ticket,
            op: StorageOp::FetchItems {
                tree,
                provenance: Provenance::Remote(peer),
                commit_ids,
                fragment_heads,
            },
        });
    }

    // ── storage completions ────────────────────────────────────────

    /// A connection-entity storage op completed (pending entry already
    /// popped and generation-validated by the caller).
    pub(super) fn on_sync_storage_done(
        &mut self,
        conn: ConnId,
        pending: ConnPending,
        result: StorageResult,
    ) -> Outcome {
        match pending {
            ConnPending::BuildResponse {
                req_id,
                tree,
                requesting,
                heads,
            } => self.on_response_fetch_done(conn, req_id, tree, requesting, heads, result),
            ConnPending::ReturnRequested { tree } => self.on_return_fetch_done(conn, tree, result),
            ConnPending::IngestRemote {
                tree,
                commits,
                fragments,
                ack,
            } => self.on_remote_ingest_done(conn, tree, &commits, &fragments, ack, result),
        }
    }

    fn on_response_fetch_done(
        &mut self,
        conn: ConnId,
        req_id: RequestId,
        tree: SedimentreeId,
        requesting: RequestedData,
        heads: Vec<CommitId>,
        result: StorageResult,
    ) -> Outcome {
        let Some(peer) = self.conns.get(&conn).and_then(|e| e.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::Fetched { commits, fragments } => {
                let heads = self.next_sender_heads(peer, heads);
                let result = SyncResult::Ok(SyncDiff {
                    missing_commits: commits,
                    missing_fragments: fragments,
                    requesting,
                });
                self.respond(conn, req_id, tree, result, heads);
                Outcome::Progressed
            }
            StorageResult::Unauthorized => {
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond(conn, req_id, tree, SyncResult::Unauthorized, heads);
                Outcome::Progressed
            }
            StorageResult::UnknownTree => {
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond(conn, req_id, tree, SyncResult::NotFound, heads);
                Outcome::Progressed
            }
            StorageResult::Failed(failure) => {
                // Degrade to NotFound on the wire; surface locally.
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond(conn, req_id, tree, SyncResult::NotFound, heads);
                self.effects
                    .push_back(Effect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }
            StorageResult::Ingested { .. }
            | StorageResult::TreeDeleted
            | StorageResult::LocallyIngested { .. } => self.driver_result_mismatch(tree),
        }
    }

    fn on_return_fetch_done(
        &mut self,
        conn: ConnId,
        tree: SedimentreeId,
        result: StorageResult,
    ) -> Outcome {
        let Some(peer) = self.conns.get(&conn).and_then(|e| e.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::Fetched { commits, fragments } => {
                let heads: Vec<CommitId> = self
                    .trees
                    .get_mut(&tree)
                    .map(|t| t.heads(&CountLeadingZeroBytes))
                    .unwrap_or_default();
                for (commit, blob) in commits {
                    let sender_heads = self.next_sender_heads(peer, heads.clone());
                    let msg = SyncMessage::LooseCommit {
                        id: tree,
                        commit,
                        blob,
                        sender_heads,
                    };
                    self.effects.push_back(Effect::SendMessage {
                        conn,
                        bytes: msg.encode(),
                    });
                }
                for (fragment, blob) in fragments {
                    let sender_heads = self.next_sender_heads(peer, heads.clone());
                    let msg = SyncMessage::Fragment {
                        id: tree,
                        fragment,
                        blob,
                        sender_heads,
                    };
                    self.effects.push_back(Effect::SendMessage {
                        conn,
                        bytes: msg.encode(),
                    });
                }
                Outcome::Progressed
            }
            StorageResult::Unauthorized => {
                // We may not read the data the peer asked for: tell them.
                let msg = SyncMessage::DataRequestRejected(DataRequestRejected { id: tree });
                self.effects.push_back(Effect::SendMessage {
                    conn,
                    bytes: msg.encode(),
                });
                Outcome::Progressed
            }
            StorageResult::UnknownTree => Outcome::Progressed, // nothing to send
            StorageResult::Failed(failure) => {
                self.effects
                    .push_back(Effect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }
            StorageResult::Ingested { .. }
            | StorageResult::TreeDeleted
            | StorageResult::LocallyIngested { .. } => self.driver_result_mismatch(tree),
        }
    }

    fn on_remote_ingest_done(
        &mut self,
        conn: ConnId,
        tree: SedimentreeId,
        commits: &[(Signed<LooseCommit>, Blob)],
        fragments: &[(Signed<Fragment>, Blob)],
        ack: bool,
        result: StorageResult,
    ) -> Outcome {
        let Some(peer) = self.conns.get(&conn).and_then(|e| e.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::Ingested { rejected, .. } => {
                let is_rejected = |kind: crate::storage::ItemKind, index: usize| {
                    rejected
                        .iter()
                        .any(|(k, i, _)| *k == kind && *i as usize == index)
                };

                // Merge accepted metadata into the resident tree; collect
                // pairs for the subscriber forward — but ONLY items that
                // were new to us. Forwarding already-known items echoes
                // between mutually-subscribed peers forever (the legacy
                // thrash-loop bug class, #281): every hop re-forwards, and
                // the cycle never damps. Freshness is the damping factor.
                let mut fresh_items: Vec<(Signed<LooseCommit>, Blob)> = Vec::new();
                let mut fresh_fragments: Vec<(Signed<Fragment>, Blob)> = Vec::new();
                {
                    let entry = self.trees.entry(tree).or_default();
                    for (index, (signed, blob)) in commits.iter().enumerate() {
                        if is_rejected(crate::storage::ItemKind::Commit, index) {
                            continue;
                        }
                        if let Ok((commit, _)) = try_decode_payload::<LooseCommit>(signed)
                            && entry.add_commit(commit)
                        {
                            fresh_items.push((signed.clone(), blob.clone()));
                        }
                    }
                    for (index, (signed, blob)) in fragments.iter().enumerate() {
                        if is_rejected(crate::storage::ItemKind::Fragment, index) {
                            continue;
                        }
                        if let Ok((fragment, _)) = try_decode_payload::<Fragment>(signed)
                            && entry.add_fragment(fragment)
                        {
                            fresh_fragments.push((signed.clone(), blob.clone()));
                        }
                    }
                }
                self.effects
                    .push_back(Effect::App(AppEvent::TreeUpdated { tree, peer }));

                // Ack the sender with our updated heads (1.5-RTT second
                // half; legacy sends this after every individual-message
                // ingest).
                if ack {
                    let heads: Vec<CommitId> = self
                        .trees
                        .get_mut(&tree)
                        .map(|t| t.heads(&CountLeadingZeroBytes))
                        .unwrap_or_default();
                    let sender_heads = self.next_sender_heads(peer, heads);
                    let msg = SyncMessage::HeadsUpdate {
                        id: tree,
                        heads: sender_heads,
                    };
                    self.effects.push_back(Effect::SendMessage {
                        conn,
                        bytes: msg.encode(),
                    });
                }

                // Forward to other subscribers (never the source).
                self.broadcast_items(tree, &fresh_items, &fresh_fragments, Some(conn));
                Outcome::Progressed
            }
            StorageResult::Unauthorized | StorageResult::UnknownTree => Outcome::Progressed,
            StorageResult::Failed(failure) => {
                self.effects
                    .push_back(Effect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }
            StorageResult::Fetched { .. }
            | StorageResult::TreeDeleted
            | StorageResult::LocallyIngested { .. } => self.driver_result_mismatch(tree),
        }
    }

    // ── shared helpers ─────────────────────────────────────────────

    /// Queue an ingest of received items: decode metadata for the
    /// resident-tree merge, hand signature/blob verification and
    /// persistence to the driver (ADR-012).
    fn ingest_remote_items(
        &mut self,
        conn: ConnId,
        peer: PeerId,
        tree: SedimentreeId,
        commits: Vec<(Signed<LooseCommit>, Blob)>,
        fragments: Vec<(Signed<Fragment>, Blob)>,
        ack: bool,
    ) -> Outcome {
        if commits.is_empty() && fragments.is_empty() {
            return Outcome::Progressed;
        }

        let Some(entry) = self.conns.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let ticket = entry.issue_storage_ticket(conn);
        entry.pending.insert(
            ticket.seq,
            ConnPending::IngestRemote {
                tree,
                commits: commits.clone(),
                fragments: fragments.clone(),
                ack,
            },
        );
        self.effects.push_back(Effect::Storage {
            ticket,
            op: StorageOp::Ingest {
                tree,
                provenance: Provenance::Remote(peer),
                commits,
                fragments,
            },
        });
        Outcome::Progressed
    }

    fn respond(
        &mut self,
        conn: ConnId,
        req_id: RequestId,
        tree: SedimentreeId,
        result: SyncResult,
        responder_heads: RemoteHeads,
    ) {
        let msg = SyncMessage::BatchSyncResponse(BatchSyncResponse {
            req_id,
            id: tree,
            result,
            responder_heads,
        });
        self.effects.push_back(Effect::SendMessage {
            conn,
            bytes: msg.encode(),
        });
    }

    fn finish_sync(&mut self, conn: ConnId, tree: SedimentreeId, status: SyncStatus) {
        self.effects
            .push_back(Effect::App(AppEvent::SyncFinished { conn, tree, status }));
    }

    /// Per-peer received-heads staleness filter (legacy
    /// `FilteredHeadsNotifier`, minus the mutex).
    fn notify_heads(&mut self, tree: SedimentreeId, peer: PeerId, heads: RemoteHeads) {
        if heads.is_empty() && heads.counter == 0 {
            return; // never-sent sentinel
        }
        let last = self.heads_recv.entry(peer).or_insert(0);
        if heads.counter <= *last {
            self.stats.stale_heads_dropped = self.stats.stale_heads_dropped.saturating_add(1);
            return;
        }
        *last = heads.counter;
        self.effects
            .push_back(Effect::App(AppEvent::RemoteHeadsUpdated {
                tree,
                peer,
                heads,
            }));
    }

    /// Bump the per-peer sent-heads counter and wrap `heads` for the wire.
    fn next_sender_heads(&mut self, peer: PeerId, heads: Vec<CommitId>) -> RemoteHeads {
        let counter = self.heads_sent.entry(peer).or_insert(0);
        *counter = counter.saturating_add(1);
        RemoteHeads {
            counter: *counter,
            heads,
        }
    }

    /// Fingerprint summary of a resident tree, or of the empty tree.
    fn summarize(&mut self, tree: SedimentreeId, seed: &FingerprintSeed) -> FingerprintSummary {
        self.trees.get_mut(&tree).map_or_else(
            || Sedimentree::default().fingerprint_summarize(seed),
            |resident| {
                resident
                    .minimized(&CountLeadingZeroBytes)
                    .fingerprint_summarize(seed)
            },
        )
    }

    /// A deterministic-but-unpredictable fingerprint seed from the
    /// machine's entropy stream (drivers have no RNG hook; ADR-006a
    /// permits inline small hashing).
    fn next_fingerprint_seed(&mut self) -> FingerprintSeed {
        let nonce = self.next_nonce();
        let bytes = nonce.as_bytes();
        let mut key0 = [0u8; 8];
        let mut key1 = [0u8; 8];
        key0.copy_from_slice(&bytes[..8]);
        key1.copy_from_slice(&bytes[8..]);
        FingerprintSeed::new(u64::from_be_bytes(key0), u64::from_be_bytes(key1))
    }

    /// The driver answered a pending op with the wrong result shape.
    fn driver_result_mismatch(&mut self, tree: SedimentreeId) -> Outcome {
        self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
        self.effects.push_back(Effect::App(AppEvent::StorageError {
            tree,
            failure: StorageFailure::Permanent,
        }));
        Outcome::Ignored(IgnoreReason::UnknownTicket)
    }

    /// Push items to every subscribed, authenticated connection (except
    /// `exclude`, the source), with fresh sender-heads.
    pub(super) fn broadcast_items(
        &mut self,
        tree: SedimentreeId,
        items: &[(Signed<LooseCommit>, Blob)],
        fragment_items: &[(Signed<Fragment>, Blob)],
        exclude: Option<ConnId>,
    ) {
        if items.is_empty() && fragment_items.is_empty() {
            return;
        }
        let Some(subscribers) = self.subscriptions.get(&tree) else {
            return;
        };
        let targets: Vec<(ConnId, PeerId)> = subscribers
            .iter()
            .filter(|subscriber| Some(**subscriber) != exclude)
            .filter_map(|subscriber| {
                self.conns.get(subscriber).and_then(|entry| {
                    matches!(entry.state, super::HandshakeState::Authenticated)
                        .then_some(entry.peer.map(|p| (*subscriber, p)))
                        .flatten()
                })
            })
            .collect();
        if targets.is_empty() {
            return;
        }

        let heads: Vec<CommitId> = self
            .trees
            .get_mut(&tree)
            .map(|t| t.heads(&CountLeadingZeroBytes))
            .unwrap_or_default();

        for (subscriber, peer) in targets {
            for (signed, blob) in items {
                let sender_heads = self.next_sender_heads(peer, heads.clone());
                let msg = SyncMessage::LooseCommit {
                    id: tree,
                    commit: signed.clone(),
                    blob: blob.clone(),
                    sender_heads,
                };
                self.effects.push_back(Effect::SendMessage {
                    conn: subscriber,
                    bytes: msg.encode(),
                });
                self.stats.subscription_pushes =
                    self.stats.subscription_pushes.saturating_add(1);
            }
            for (signed, blob) in fragment_items {
                let sender_heads = self.next_sender_heads(peer, heads.clone());
                let msg = SyncMessage::Fragment {
                    id: tree,
                    fragment: signed.clone(),
                    blob: blob.clone(),
                    sender_heads,
                };
                self.effects.push_back(Effect::SendMessage {
                    conn: subscriber,
                    bytes: msg.encode(),
                });
                self.stats.subscription_pushes =
                    self.stats.subscription_pushes.saturating_add(1);
            }
        }
    }

    /// Test/introspection: number of in-flight sync requests.
    #[must_use]
    pub fn pending_sync_requests(&self) -> usize {
        self.conns.values().map(|c| c.requests.len()).sum()
    }
}

/// The responder-side wire diff, copied out to owned values.
///
/// Ported verbatim from `legacy/subduction_core/src/handler/sync.rs`
/// (minus the point-read/bulk-scan crossover fields, which are a driver
/// concern now — the machine always issues targeted `FetchItems`).
struct ResponderDiff {
    /// The responder's heads, read off the minimal tree.
    heads: Vec<CommitId>,

    /// Ids of commits we hold that the requestor is missing.
    local_commit_ids: Vec<CommitId>,

    /// Ids of fragments we hold that the requestor is missing.
    local_fragment_ids: Vec<CommitId>,

    /// Requestor commit fingerprints we don't recognize (echoed back so
    /// the requestor can reverse-lookup and send the data).
    requesting_commit_fingerprints:
        Vec<sedimentree_core::crypto::fingerprint::Fingerprint<CommitId>>,

    /// Requestor fragment fingerprints we don't recognize (echoed back).
    requesting_fragment_fingerprints:
        Vec<sedimentree_core::crypto::fingerprint::Fingerprint<CommitId>>,
}

impl ResponderDiff {
    /// Diff `minimal` against the requestor's fingerprint summary.
    ///
    /// `minimal` must already be in minimal form (the wire diff and
    /// `heads_assuming_minimal` both rely on it).
    fn new(minimal: &Sedimentree, their_fingerprints: &FingerprintSummary) -> Self {
        let diff = minimal.diff_remote_fingerprints(their_fingerprints);

        Self {
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
