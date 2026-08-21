//! Sync sessions over sealed edges.
//!
//! Everything here operates on already-verified data (the connection
//! machines are the forgery gate); the core's jobs are the _joins_:
//! resident-tree diffing, session matching, subscription fan-out, and
//! heads staleness — each entry touching exactly one tree (the
//! no-cross-tree invariant; the per-peer heads counters are the
//! documented exception).
//!
//! Diff/summarize run inline in the turn for now — bench-gated decision,
//! see the 🚨 banner in TODO.md.

use alloc::{vec, vec::Vec};

use sedimentree_core::{
    codec::decode::DecodeFields,
    crypto::fingerprint::FingerprintSeed,
    depth::CountLeadingZeroBytes,
    fragment::Fragment,
    id::SedimentreeId,
    loose_commit::{LooseCommit, id::CommitId},
    sedimentree::{FingerprintSummary, Sedimentree},
};

use subduction_crypto::signed::Signed;

use super::{CoreEffect, CoreMachine, Now};
use crate::{
    blob_ref::{BlobRef, Part},
    edge::{ForwardStatus, SyncForward, VerifiedCommit, VerifiedFragment},
    effect::{AppEvent, SyncStatus},
    id::ConnId,
    outcome::{IgnoreReason, Outcome},
    peer_id::PeerId,
    remote_heads::RemoteHeads,
    storage::{Provenance, StorageFailure, StorageOp, StorageResult},
    timestamp::Timestamp,
    wire,
};

/// An in-flight batch sync request we initiated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OutboundRequest {
    /// The (single) tree being synced.
    pub(super) tree: SedimentreeId,

    /// The fingerprint seed we summarized with.
    pub(super) seed: FingerprintSeed,

    /// Whether we asked to subscribe (mutual subscription on `Ok`).
    pub(super) subscribe: bool,

    /// When this request expires.
    pub(super) deadline: Timestamp,

    /// Resident heads (sorted) when the summary was taken. If they
    /// differ at response time, local writes landed inside the
    /// snapshot→subscription window — items no diff and no push will
    /// ever carry — and the requester must immediately re-sync.
    pub(super) issued_heads: Vec<CommitId>,
}

/// A driver storage op in flight for an edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CorePending {
    /// Responder: awaiting `FetchItemRefs` for the diff's blobs.
    BuildResponse {
        req_id: wire::RequestId,
        tree: SedimentreeId,
        requesting: wire::RequestedData,
        heads: Vec<CommitId>,
    },

    /// Requester: awaiting `FetchItemRefs` for items the responder asked
    /// back for.
    ReturnRequested { tree: SedimentreeId },

    /// Awaiting `PersistItems` durability; items held for the resident
    /// merge and the subscriber forward.
    Persist {
        tree: SedimentreeId,
        commits: Vec<VerifiedCommit>,
        fragments: Vec<VerifiedFragment>,
        /// Ack the sender with a `HeadsUpdate` (individual pushes only).
        ack: bool,
    },
}

impl CoreMachine {
    // ── requester ──────────────────────────────────────────────────

    /// `Command::SyncTree`: summarize (inline; 🚨 bench-gated), send the
    /// request, remember the session.
    pub(super) fn start_sync(
        &mut self,
        now: Now,
        conn: ConnId,
        tree: SedimentreeId,
        subscribe: bool,
    ) -> Outcome {
        let seed = self.next_seed();
        let summary = self.summarize(tree, &seed);
        let issued_heads = self.sorted_heads(tree);

        let Some(entry) = self.edges.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        if entry.peer.is_none() {
            return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
        }

        let nonce = self.request_nonce;
        self.request_nonce = self.request_nonce.saturating_add(1);
        let req_id = wire::RequestId {
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
                issued_heads,
            },
        );

        let msg = wire::SyncMessage::BatchSyncRequest(wire::BatchSyncRequest {
            id: tree,
            req_id,
            fingerprint_summary: summary,
            subscribe,
        });
        self.effects.push_back(CoreEffect::Send {
            conn,
            parts: vec![Part::Bytes(msg.encode())],
        });
        self.stats.sync_requests_sent = self.stats.sync_requests_sent.saturating_add(1);
        Outcome::Progressed
    }

    /// Expire timed-out requests. Returns whether any fired.
    pub(super) fn expire_requests(&mut self, now: Timestamp) -> bool {
        let mut expired: Vec<(ConnId, u64, SedimentreeId)> = Vec::new();
        for (conn, entry) in &self.edges {
            for (nonce, request) in &entry.requests {
                if request.deadline.is_due(now) {
                    expired.push((*conn, *nonce, request.tree));
                }
            }
        }
        for (conn, nonce, tree) in &expired {
            if let Some(entry) = self.edges.get_mut(conn) {
                let _request = entry.requests.remove(nonce);
            }
            self.effects
                .push_back(CoreEffect::App(AppEvent::SyncFinished {
                    conn: *conn,
                    tree: *tree,
                    status: SyncStatus::TimedOut,
                }));
        }
        !expired.is_empty()
    }

    // ── forwarded traffic dispatch ─────────────────────────────────

    pub(super) fn on_sync_forward(
        &mut self,
        now: Now,
        conn: ConnId,
        forward: SyncForward,
    ) -> Outcome {
        match forward {
            SyncForward::Request(request) => self.on_request(conn, &request),
            SyncForward::Response {
                req_id,
                tree,
                commits,
                fragments,
                requesting,
                responder_heads,
                status,
                ..
            } => self.on_response(
                now,
                conn,
                req_id,
                tree,
                commits,
                fragments,
                &requesting,
                responder_heads,
                status,
            ),
            SyncForward::Commit {
                tree,
                item,
                sender_heads,
            } => {
                self.notify_heads(tree, conn, sender_heads);
                self.persist_remote(conn, tree, vec![item], vec![], true)
            }
            SyncForward::Fragment {
                tree,
                item,
                sender_heads,
            } => {
                self.notify_heads(tree, conn, sender_heads);
                self.persist_remote(conn, tree, vec![], vec![item], true)
            }
            SyncForward::HeadsUpdate { tree, heads } => {
                // Each individual push is acked by one HeadsUpdate
                // (1.5-RTT): drain the lagging gauge.
                if let Some(entry) = self.edges.get_mut(&conn) {
                    entry.outstanding_pushes = entry.outstanding_pushes.saturating_sub(1);
                }
                self.notify_heads(tree, conn, heads);
                Outcome::Progressed
            }
            SyncForward::RemoveSubscriptions(trees) => {
                for tree in &trees {
                    if let Some(conns) = self.subscriptions.get_mut(tree) {
                        let _removed = conns.remove(&conn);
                        if conns.is_empty() {
                            let _entry = self.subscriptions.remove(tree);
                        }
                    }
                }
                Outcome::Progressed
            }
            SyncForward::DataRequestRejected(_) => Outcome::Progressed,
        }
    }

    // ── responder ──────────────────────────────────────────────────

    fn on_request(&mut self, conn: ConnId, request: &wire::BatchSyncRequest) -> Outcome {
        self.stats.sync_requests_received = self.stats.sync_requests_received.saturating_add(1);
        let tree = request.id;
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
        };

        if request.subscribe {
            let _new = self.subscriptions.entry(tree).or_default().insert(conn);
        }
        // A fresh full-diff request supersedes any unacked pushes: the
        // response carries everything they lack, so the lagging gauge
        // resets (the recovery point). Re-breach is bounded per
        // cycle, and each cycle costs the peer a full sync request.
        if let Some(entry) = self.edges.get_mut(&conn) {
            entry.outstanding_pushes = 0;
        }

        let Some(resident) = self.trees.get_mut(&tree) else {
            let heads = self.next_sender_heads(peer, vec![]);
            self.respond_plain(
                conn,
                request.req_id,
                tree,
                wire::SyncResult::NotFound,
                heads,
            );
            return Outcome::Progressed;
        };

        // Inline diff (🚨 bench-gated; see TODO.md banner).
        let minimal = resident.minimized(&CountLeadingZeroBytes);
        let diff = minimal.diff_remote_fingerprints(&request.fingerprint_summary);
        let heads = minimal.heads_assuming_minimal();
        let local_commit_ids: Vec<CommitId> =
            diff.local_only_commits.iter().map(|(id, _)| **id).collect();
        let local_fragment_ids: Vec<CommitId> = diff
            .local_only_fragments
            .iter()
            .map(|(id, _)| **id)
            .collect();
        let requesting = wire::RequestedData {
            commit_fingerprints: diff.remote_only_commit_fingerprints,
            fragment_fingerprints: diff.remote_only_fragment_fingerprints,
        };

        if local_commit_ids.is_empty() && local_fragment_ids.is_empty() {
            let heads = self.next_sender_heads(peer, heads);
            self.respond_plain(
                conn,
                request.req_id,
                tree,
                wire::SyncResult::Ok(wire::SyncDiff {
                    missing_commits: vec![],
                    missing_fragments: vec![],
                    requesting,
                }),
                heads,
            );
            return Outcome::Progressed;
        }

        let Some(entry) = self.edges.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let ticket = entry.issue_ticket();
        entry.pending.insert(
            ticket.seq,
            CorePending::BuildResponse {
                req_id: request.req_id,
                tree,
                requesting,
                heads,
            },
        );
        self.effects.push_back(CoreEffect::Storage {
            ticket,
            op: StorageOp::FetchItemRefs {
                tree,
                provenance: Provenance::Remote(peer),
                commit_ids: local_commit_ids,
                fragment_heads: local_fragment_ids,
            },
        });
        Outcome::Progressed
    }

    // ── requester: response processing ─────────────────────────────

    #[allow(clippy::too_many_arguments)] // mirrors the forwarded wire shape
    fn on_response(
        &mut self,
        now: Now,
        conn: ConnId,
        req_id: wire::RequestId,
        tree: SedimentreeId,
        commits: Vec<VerifiedCommit>,
        fragments: Vec<VerifiedFragment>,
        requesting: &wire::RequestedData,
        responder_heads: RemoteHeads,
        status: ForwardStatus,
    ) -> Outcome {
        if req_id.requestor != self.config.local_peer {
            return Outcome::Ignored(IgnoreReason::UnknownRequest);
        }
        let Some(entry) = self.edges.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let Some(request) = entry.requests.remove(&req_id.nonce) else {
            return Outcome::Ignored(IgnoreReason::UnknownRequest);
        };
        self.stats.sync_responses_received = self.stats.sync_responses_received.saturating_add(1);

        self.notify_heads(tree, conn, responder_heads);

        match status {
            ForwardStatus::NotFound => {
                // The peer lacks the tree entirely. With subscribe:true
                // the sync relationship still forms and the diff
                // degenerates to "send everything". Without it, our
                // data would never cross: no diff carries it (they
                // can't summarize a tree they lack) and no push does
                // (no subscription).
                if request.subscribe {
                    let _new = self.subscriptions.entry(tree).or_default().insert(conn);
                    self.push_all_resident(conn, tree);
                }
                self.finish_sync(conn, tree, SyncStatus::NotFound);
                return Outcome::Progressed;
            }
            ForwardStatus::Unauthorized => {
                self.finish_sync(conn, tree, SyncStatus::Unauthorized);
                return Outcome::Progressed;
            }
            ForwardStatus::Ok => {}
        }

        // Mutual subscription on success.
        if request.subscribe {
            let _new = self.subscriptions.entry(tree).or_default().insert(conn);
        }

        // Persist what we were missing (already verified upstream).
        let _outcome = self.persist_remote(conn, tree, commits, fragments, false);

        // Return what the responder asked for.
        if !requesting.is_empty() {
            self.return_requested(conn, tree, &request.seed, requesting);
        }

        self.finish_sync(conn, tree, SyncStatus::Completed);

        // Local writes that landed between the summary snapshot and this
        // response fell into a window no diff and no push covers: the
        // summary predates them and the mutual subscription postdates
        // them. Re-sync immediately — the fresh summary includes them.
        // Terminates: re-triggers only while writes keep landing
        // mid-flight.
        if self.sorted_heads(tree) != request.issued_heads {
            let _outcome = self.start_sync(now, conn, tree, request.subscribe);
        }
        Outcome::Progressed
    }

    fn return_requested(
        &mut self,
        conn: ConnId,
        tree: SedimentreeId,
        seed: &FingerprintSeed,
        requesting: &wire::RequestedData,
    ) {
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return;
        };
        let (commit_ids, fragment_heads) = {
            let Some(resident) = self.trees.get_mut(&tree) else {
                return;
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
        let Some(entry) = self.edges.get_mut(&conn) else {
            return;
        };
        let ticket = entry.issue_ticket();
        entry
            .pending
            .insert(ticket.seq, CorePending::ReturnRequested { tree });
        self.effects.push_back(CoreEffect::Storage {
            ticket,
            op: StorageOp::FetchItemRefs {
                tree,
                provenance: Provenance::Remote(peer),
                commit_ids,
                fragment_heads,
            },
        });
    }

    /// Push every resident item for `tree` to `conn` as individual
    /// item messages (the degenerate diff against a peer with nothing).
    fn push_all_resident(&mut self, conn: ConnId, tree: SedimentreeId) {
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return;
        };
        let (commit_ids, fragment_heads) = {
            let Some(resident) = self.trees.get_mut(&tree) else {
                return;
            };
            let minimal = resident.minimized(&CountLeadingZeroBytes);
            let commit_ids: Vec<CommitId> = minimal.commit_entries().map(|(id, _)| *id).collect();
            let fragment_heads: Vec<CommitId> =
                minimal.fragment_entries().map(|(id, _)| *id).collect();
            (commit_ids, fragment_heads)
        };
        if commit_ids.is_empty() && fragment_heads.is_empty() {
            return;
        }
        let Some(entry) = self.edges.get_mut(&conn) else {
            return;
        };
        let ticket = entry.issue_ticket();
        entry
            .pending
            .insert(ticket.seq, CorePending::ReturnRequested { tree });
        self.effects.push_back(CoreEffect::Storage {
            ticket,
            op: StorageOp::FetchItemRefs {
                tree,
                provenance: Provenance::Remote(peer),
                commit_ids,
                fragment_heads,
            },
        });
    }

    // ── storage completions ────────────────────────────────────────

    pub(super) fn on_sync_storage_done(
        &mut self,
        conn: ConnId,
        pending: CorePending,
        result: StorageResult,
    ) -> Outcome {
        match pending {
            CorePending::BuildResponse {
                req_id,
                tree,
                requesting,
                heads,
            } => self.on_response_fetch_done(conn, req_id, tree, &requesting, heads, result),
            CorePending::ReturnRequested { tree } => self.on_return_fetch_done(conn, tree, result),
            CorePending::Persist {
                tree,
                commits,
                fragments,
                ack,
            } => self.on_persist_done(conn, tree, &commits, &fragments, ack, &result),
        }
    }

    fn on_response_fetch_done(
        &mut self,
        conn: ConnId,
        req_id: wire::RequestId,
        tree: SedimentreeId,
        requesting: &wire::RequestedData,
        heads: Vec<CommitId>,
        result: StorageResult,
    ) -> Outcome {
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::FetchedRefs { commits, fragments } => {
                let responder_heads = self.next_sender_heads(peer, heads);
                let parts = wire::batch_sync_response_parts(
                    req_id,
                    tree,
                    &commits,
                    &fragments,
                    requesting,
                    &responder_heads,
                );
                self.effects.push_back(CoreEffect::Send { conn, parts });
                self.release_refs(commits.iter().map(|(_, r)| *r));
                self.release_refs(fragments.iter().map(|(_, r)| *r));
                Outcome::Progressed
            }
            StorageResult::Unauthorized => {
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond_plain(conn, req_id, tree, wire::SyncResult::Unauthorized, heads);
                Outcome::Progressed
            }
            StorageResult::UnknownTree => {
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond_plain(conn, req_id, tree, wire::SyncResult::NotFound, heads);
                Outcome::Progressed
            }
            StorageResult::Failed(failure) => {
                let heads = self.next_sender_heads(peer, vec![]);
                self.respond_plain(conn, req_id, tree, wire::SyncResult::NotFound, heads);
                self.effects
                    .push_back(CoreEffect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }
            StorageResult::Persisted { .. }
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
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::FetchedRefs { commits, fragments } => {
                let heads: Vec<CommitId> = self
                    .trees
                    .get_mut(&tree)
                    .map(|t| t.heads(&CountLeadingZeroBytes))
                    .unwrap_or_default();
                for (signed, blob) in &commits {
                    let sender_heads = self.next_sender_heads(peer, heads.clone());
                    let parts =
                        wire::loose_commit_parts(tree, signed, &sender_heads, Part::Ref(*blob));
                    self.effects.push_back(CoreEffect::Send { conn, parts });
                }
                for (signed, blob) in &fragments {
                    let sender_heads = self.next_sender_heads(peer, heads.clone());
                    let parts = wire::fragment_parts(tree, signed, &sender_heads, Part::Ref(*blob));
                    self.effects.push_back(CoreEffect::Send { conn, parts });
                }
                self.release_refs(commits.iter().map(|(_, r)| *r));
                self.release_refs(fragments.iter().map(|(_, r)| *r));
                Outcome::Progressed
            }
            StorageResult::Unauthorized => {
                let msg =
                    wire::SyncMessage::DataRequestRejected(wire::DataRequestRejected { id: tree });
                self.effects.push_back(CoreEffect::Send {
                    conn,
                    parts: vec![Part::Bytes(msg.encode())],
                });
                Outcome::Progressed
            }
            StorageResult::UnknownTree => Outcome::Progressed,
            StorageResult::Failed(failure) => {
                self.effects
                    .push_back(CoreEffect::App(AppEvent::StorageError { tree, failure }));
                Outcome::Progressed
            }
            StorageResult::Persisted { .. }
            | StorageResult::TreeDeleted
            | StorageResult::LocallyIngested { .. } => self.driver_result_mismatch(tree),
        }
    }

    fn on_persist_done(
        &mut self,
        conn: ConnId,
        tree: SedimentreeId,
        commits: &[VerifiedCommit],
        fragments: &[VerifiedFragment],
        ack: bool,
        result: &StorageResult,
    ) -> Outcome {
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        match result {
            StorageResult::Persisted { .. } => {
                // Merge metadata (parse WITHOUT re-verifying: the conn
                // machine already verified) and keep the fresh items for
                // the subscriber forward (freshness = the damping factor).
                let mut fresh_commits: Vec<(&Signed<LooseCommit>, Part)> = Vec::new();
                let mut fresh_fragments: Vec<(&Signed<Fragment>, Part)> = Vec::new();
                {
                    let entry = self.trees.entry(tree).or_default();
                    for item in commits {
                        if let Ok((commit, _)) =
                            LooseCommit::try_decode_fields(item.commit.fields_bytes())
                            && entry.add_commit(commit)
                        {
                            fresh_commits.push((&item.commit, Part::Ref(item.blob)));
                        }
                    }
                    for item in fragments {
                        if let Ok((fragment, _)) =
                            Fragment::try_decode_fields(item.fragment.fields_bytes())
                            && entry.add_fragment(fragment)
                        {
                            fresh_fragments.push((&item.fragment, Part::Ref(item.blob)));
                        }
                    }
                }
                self.effects
                    .push_back(CoreEffect::App(AppEvent::TreeUpdated { tree, peer }));

                // Ack individual pushes with our updated heads (1.5-RTT
                // second half).
                if ack {
                    let heads: Vec<CommitId> = self
                        .trees
                        .get_mut(&tree)
                        .map(|t| t.heads(&CountLeadingZeroBytes))
                        .unwrap_or_default();
                    let sender_heads = self.next_sender_heads(peer, heads);
                    let msg = wire::SyncMessage::HeadsUpdate {
                        id: tree,
                        heads: sender_heads,
                    };
                    self.effects.push_back(CoreEffect::Send {
                        conn,
                        parts: vec![Part::Bytes(msg.encode())],
                    });
                }

                // Forward fresh items to other subscribers (never the
                // source), then release every ref this pending held.
                self.broadcast_items(tree, &fresh_commits, &fresh_fragments, Some(conn));
                self.release_refs(commits.iter().map(|item| item.blob));
                self.release_refs(fragments.iter().map(|item| item.blob));
                Outcome::Progressed
            }
            StorageResult::Unauthorized | StorageResult::UnknownTree => {
                self.release_refs(commits.iter().map(|item| item.blob));
                self.release_refs(fragments.iter().map(|item| item.blob));
                Outcome::Progressed
            }
            StorageResult::Failed(failure) => {
                self.release_refs(commits.iter().map(|item| item.blob));
                self.release_refs(fragments.iter().map(|item| item.blob));
                self.effects
                    .push_back(CoreEffect::App(AppEvent::StorageError {
                        tree,
                        failure: *failure,
                    }));
                Outcome::Progressed
            }
            StorageResult::FetchedRefs { .. }
            | StorageResult::TreeDeleted
            | StorageResult::LocallyIngested { .. } => self.driver_result_mismatch(tree),
        }
    }

    // ── shared helpers ─────────────────────────────────────────────

    /// Queue a persist of verified items and remember them for the
    /// post-durability merge + forward.
    fn persist_remote(
        &mut self,
        conn: ConnId,
        tree: SedimentreeId,
        commits: Vec<VerifiedCommit>,
        fragments: Vec<VerifiedFragment>,
        ack: bool,
    ) -> Outcome {
        if commits.is_empty() && fragments.is_empty() {
            return Outcome::Progressed;
        }
        let Some(entry) = self.edges.get_mut(&conn) else {
            return Outcome::Ignored(IgnoreReason::UnknownConnection(conn));
        };
        let Some(peer) = entry.peer else {
            return Outcome::Ignored(IgnoreReason::NotAuthenticated(conn));
        };
        let ticket = entry.issue_ticket();
        entry.pending.insert(
            ticket.seq,
            CorePending::Persist {
                tree,
                commits: commits.clone(),
                fragments: fragments.clone(),
                ack,
            },
        );
        self.effects.push_back(CoreEffect::Storage {
            ticket,
            op: StorageOp::PersistItems {
                tree,
                provenance: Provenance::Remote(peer),
                commits: commits
                    .into_iter()
                    .map(|item| (item.commit, item.blob))
                    .collect(),
                fragments: fragments
                    .into_iter()
                    .map(|item| (item.fragment, item.blob))
                    .collect(),
            },
        });
        Outcome::Progressed
    }

    /// Push fresh items to every subscribed, authenticated edge except
    /// the source, with fresh sender-heads, blobs by ref.
    /// Push items to a tree's subscribers. Blobs arrive as [`Part`]s:
    /// refs for remote-sourced items (zero-copy fan-out), inline bytes
    /// for local writes.
    pub(super) fn broadcast_items(
        &mut self,
        tree: SedimentreeId,
        commits: &[(&Signed<LooseCommit>, Part)],
        fragments: &[(&Signed<Fragment>, Part)],
        exclude: Option<ConnId>,
    ) {
        if commits.is_empty() && fragments.is_empty() {
            return;
        }
        let Some(subscribers) = self.subscriptions.get(&tree) else {
            return;
        };
        let targets: Vec<(ConnId, PeerId)> = subscribers
            .iter()
            .filter(|subscriber| Some(**subscriber) != exclude)
            .filter_map(|subscriber| {
                self.edges
                    .get(subscriber)
                    .and_then(|entry| entry.peer.map(|peer| (*subscriber, peer)))
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
            for (signed, blob) in commits {
                if !self.try_push_credit(subscriber, tree, peer, &heads) {
                    break;
                }
                let sender_heads = self.next_sender_heads(peer, heads.clone());
                let parts = wire::loose_commit_parts(tree, signed, &sender_heads, blob.clone());
                self.effects.push_back(CoreEffect::Send {
                    conn: subscriber,
                    parts,
                });
                self.stats.subscription_pushes = self.stats.subscription_pushes.saturating_add(1);
            }
            for (signed, blob) in fragments {
                if !self.try_push_credit(subscriber, tree, peer, &heads) {
                    break;
                }
                let sender_heads = self.next_sender_heads(peer, heads.clone());
                let parts = wire::fragment_parts(tree, signed, &sender_heads, blob.clone());
                self.effects.push_back(CoreEffect::Send {
                    conn: subscriber,
                    parts,
                });
                self.stats.subscription_pushes = self.stats.subscription_pushes.saturating_add(1);
            }
        }
    }

    /// Spend one push credit for `subscriber`, or pause its subscription
    /// to `tree` (pause + resync, never unbounded queues).
    ///
    /// A paused subscriber gets a `HeadsUpdate` nudge: if it is alive,
    /// it sees heads it does not hold and re-syncs (which re-subscribes
    /// and computes a full diff, covering the skipped pushes). Liveness
    /// of DEAD peers is the transport/supervision layer's job — no
    /// protocol timeout here, by design.
    fn try_push_credit(
        &mut self,
        subscriber: ConnId,
        tree: SedimentreeId,
        peer: PeerId,
        heads: &[CommitId],
    ) -> bool {
        let limit = self.config.max_outstanding_pushes;
        let Some(entry) = self.edges.get_mut(&subscriber) else {
            return false;
        };
        if entry.outstanding_pushes < limit {
            entry.outstanding_pushes += 1;
            return true;
        }

        // Pause: drop the subscription; their next sync re-forms it.
        // The nudge fires only when this call actually removes the
        // subscription, so a breach mid-broadcast (commits loop) is not
        // re-announced by the fragments loop — one nudge per breach
        //.
        let mut removed = false;
        if let Some(conns) = self.subscriptions.get_mut(&tree) {
            removed = conns.remove(&subscriber);
            if conns.is_empty() {
                let _entry = self.subscriptions.remove(&tree);
            }
        }
        if !removed {
            return false;
        }
        self.stats.subscribers_paused = self.stats.subscribers_paused.saturating_add(1);
        let sender_heads = self.next_sender_heads(peer, heads.to_vec());
        let msg = wire::SyncMessage::HeadsUpdate {
            id: tree,
            heads: sender_heads,
        };
        self.effects.push_back(CoreEffect::Send {
            conn: subscriber,
            parts: vec![Part::Bytes(msg.encode())],
        });
        self.effects
            .push_back(CoreEffect::App(AppEvent::SubscriberLagging {
                conn: subscriber,
                tree,
            }));
        false
    }

    fn respond_plain(
        &mut self,
        conn: ConnId,
        req_id: wire::RequestId,
        tree: SedimentreeId,
        result: wire::SyncResult,
        responder_heads: RemoteHeads,
    ) {
        let msg = wire::SyncMessage::BatchSyncResponse(wire::BatchSyncResponse {
            req_id,
            id: tree,
            result,
            responder_heads,
        });
        self.effects.push_back(CoreEffect::Send {
            conn,
            parts: vec![Part::Bytes(msg.encode())],
        });
    }

    fn finish_sync(&mut self, conn: ConnId, tree: SedimentreeId, status: SyncStatus) {
        self.effects
            .push_back(CoreEffect::App(AppEvent::SyncFinished {
                conn,
                tree,
                status,
            }));
    }

    /// Per-peer received-heads staleness filter.
    fn notify_heads(&mut self, tree: SedimentreeId, conn: ConnId, heads: RemoteHeads) {
        let Some(peer) = self.edges.get(&conn).and_then(|entry| entry.peer) else {
            return;
        };
        if heads.is_empty() && heads.counter == 0 {
            return;
        }
        let last = self.heads_recv.entry(peer).or_insert(0);
        if heads.counter <= *last {
            self.stats.stale_heads_dropped = self.stats.stale_heads_dropped.saturating_add(1);
            return;
        }
        *last = heads.counter;
        self.effects
            .push_back(CoreEffect::App(AppEvent::RemoteHeadsUpdated {
                tree,
                peer,
                heads,
            }));
    }

    /// Bump the per-peer sent-heads counter and wrap `heads`.
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

    /// Deterministic-but-unpredictable fingerprint seed from entropy.
    /// Resident heads for `tree`, sorted (∅ when not resident).
    fn sorted_heads(&mut self, tree: SedimentreeId) -> Vec<CommitId> {
        let mut heads: Vec<CommitId> = self
            .trees
            .get_mut(&tree)
            .map(|t| t.heads(&CountLeadingZeroBytes))
            .unwrap_or_default();
        heads.sort_unstable();
        heads
    }

    fn next_seed(&mut self) -> FingerprintSeed {
        let hash = blake3::keyed_hash(&self.config.entropy, &self.seed_counter.to_be_bytes());
        self.seed_counter = self.seed_counter.saturating_add(1);
        let bytes = hash.as_bytes();
        let mut key0 = [0u8; 8];
        let mut key1 = [0u8; 8];
        // blake3 output is 32 bytes; taking 16 cannot fail.
        #[allow(clippy::indexing_slicing)]
        {
            key0.copy_from_slice(&bytes[..8]);
            key1.copy_from_slice(&bytes[8..16]);
        }
        FingerprintSeed::new(u64::from_be_bytes(key0), u64::from_be_bytes(key1))
    }

    fn release_refs(&mut self, refs: impl Iterator<Item = BlobRef>) {
        for blob in refs {
            self.effects.push_back(CoreEffect::ReleaseBlob(blob));
        }
    }

    /// The driver answered a pending op with the wrong result shape.
    fn driver_result_mismatch(&mut self, tree: SedimentreeId) -> Outcome {
        self.stats.unknown_tickets = self.stats.unknown_tickets.saturating_add(1);
        self.effects
            .push_back(CoreEffect::App(AppEvent::StorageError {
                tree,
                failure: StorageFailure::Permanent,
            }));
        Outcome::Ignored(IgnoreReason::UnknownTicket)
    }

    /// Test/introspection: in-flight sync requests.
    #[must_use]
    pub fn pending_sync_requests(&self) -> usize {
        self.edges.values().map(|entry| entry.requests.len()).sum()
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::{
        blob_ref::FrameId,
        command::Command,
        core_machine::{CoreConfig, CoreEvent},
        edge::{ConnToCore, EdgeId, Sealed},
        event::Direction,
        id::{Generation, Seq},
        timestamp::Now,
        wall_clock::TimestampSeconds,
    };
    use alloc::collections::BTreeSet;
    use ed25519_dalek::SigningKey;
    use sedimentree_core::{
        blob::{Blob, BlobMeta},
        collections::Set,
    };
    use subduction_crypto::signed::Signed;
    use testresult::TestResult;

    const fn now_at(ms: u64) -> Now {
        Now {
            monotonic: crate::timestamp::Timestamp::from_millis(ms),
            wall: TimestampSeconds::new(1_700_000_000),
        }
    }

    /// A core with one authenticated edge (conn 1, peer from `seed`).
    fn core_with_peer(seed: u8) -> (CoreMachine, EdgeId, Seq, PeerId) {
        let mut core = CoreMachine::new(CoreConfig::new(PeerId::new([0xCC; 32]), [1u8; 32]));
        let edge = EdgeId {
            conn: ConnId::new(1),
            generation: Generation::FIRST,
        };
        let peer = PeerId::new([seed; 32]);
        let mut seq = Seq::FIRST;
        for msg in [
            ConnToCore::Opened {
                direction: Direction::Inbound,
            },
            ConnToCore::Authenticated { peer },
        ] {
            let _outcome =
                core.handle(now_at(0), CoreEvent::FromConn(Sealed::mint(edge, seq, msg)));
            seq = seq.next();
        }
        (core, edge, seq, peer)
    }

    /// A verified commit (sealed with a real signer) plus its blob bytes.
    fn verified_commit(tree: SedimentreeId, head: u8) -> (VerifiedCommit, Vec<u8>) {
        let signing_key = SigningKey::from_bytes(&[head; 32]);
        let blob = Blob::new(alloc::vec![head; 16]);
        let commit = LooseCommit::new(
            tree,
            CommitId::new([head; 32]),
            BTreeSet::new(),
            BlobMeta::new(&blob),
        );
        let sealed = Signed::seal_sync(&signing_key, commit).into_signed();
        let item = VerifiedCommit {
            commit: sealed,
            blob: BlobRef {
                frame: FrameId::new(u64::from(head)),
                offset: 0,
                len: 16,
            },
        };
        (item, blob.as_slice().to_vec())
    }

    fn drain(core: &mut CoreMachine) -> Vec<CoreEffect> {
        let mut out = Vec::new();
        while let Some(effect) = core.poll_effect() {
            out.push(effect);
        }
        out
    }

    #[test]
    fn push_persists_merges_acks_and_forwards() -> TestResult {
        let (mut core, edge, seq, _peer) = core_with_peer(0xA1);
        let tree = SedimentreeId::new([7u8; 32]);

        // A second authenticated edge subscribes to the tree.
        let edge2 = EdgeId {
            conn: ConnId::new(2),
            generation: Generation::FIRST,
        };
        let mut seq2 = Seq::FIRST;
        for msg in [
            ConnToCore::Opened {
                direction: Direction::Inbound,
            },
            ConnToCore::Authenticated {
                peer: PeerId::new([0xB2; 32]),
            },
        ] {
            let _o = core.handle(
                now_at(0),
                CoreEvent::FromConn(Sealed::mint(edge2, seq2, msg)),
            );
            seq2 = seq2.next();
        }
        let _new = core
            .subscriptions
            .entry(tree)
            .or_default()
            .insert(edge2.conn);

        // A verified commit push arrives on edge 1.
        let (item, _blob) = verified_commit(tree, 0xA1);
        let blob_ref = item.blob;
        let forward = SyncForward::Commit {
            tree,
            item,
            sender_heads: RemoteHeads {
                counter: 1,
                heads: alloc::vec![CommitId::new([0xA1; 32])],
            },
        };
        let outcome = core.handle(
            now_at(1),
            CoreEvent::FromConn(Sealed::mint(
                edge,
                seq,
                ConnToCore::Inbound(alloc::boxed::Box::new(forward)),
            )),
        );
        assert_eq!(outcome, Outcome::Progressed);

        // Effects: heads app event + PersistItems.
        let effects = drain(&mut core);
        let Some(CoreEffect::Storage { ticket, op }) = effects
            .iter()
            .find(|e| matches!(e, CoreEffect::Storage { .. }))
            .cloned()
        else {
            return Err("expected PersistItems".into());
        };
        assert!(matches!(op, StorageOp::PersistItems { .. }));

        // Durability confirmed.
        let outcome = core.handle(
            now_at(2),
            CoreEvent::StorageDone {
                ticket,
                result: StorageResult::Persisted { stored: 1 },
            },
        );
        assert_eq!(outcome, Outcome::Progressed);
        let effects = drain(&mut core);

        // Merged into resident state.
        assert_eq!(
            core.tree_heads(tree),
            Some(alloc::vec![CommitId::new([0xA1; 32])])
        );
        // TreeUpdated + HeadsUpdate ack to the source + forward to the
        // subscriber + the ref released.
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, CoreEffect::App(AppEvent::TreeUpdated { .. })))
        );
        let sends: Vec<&ConnId> = effects
            .iter()
            .filter_map(|e| match e {
                CoreEffect::Send { conn, .. } => Some(conn),
                CoreEffect::ToConn(_)
                | CoreEffect::Storage { .. }
                | CoreEffect::Disconnect { .. }
                | CoreEffect::ReleaseBlob(_)
                | CoreEffect::App(_) => None,
            })
            .collect();
        assert!(sends.contains(&&edge.conn), "HeadsUpdate ack to source");
        assert!(sends.contains(&&edge2.conn), "forward to subscriber");
        assert!(
            effects.contains(&CoreEffect::ReleaseBlob(blob_ref)),
            "ref released after last use"
        );
        Ok(())
    }

    #[test]
    fn responder_flow_fetches_and_responds_with_parts() -> TestResult {
        let (mut core, edge, seq, _peer) = core_with_peer(0xA3);
        let tree = SedimentreeId::new([8u8; 32]);

        // Seed resident state via hydration.
        let blob = Blob::new(alloc::vec![0xA3; 16]);
        let commit = LooseCommit::new(
            tree,
            CommitId::new([0xA3; 32]),
            BTreeSet::new(),
            BlobMeta::new(&blob),
        );
        let _outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::HydrateTree {
                tree,
                commits: alloc::vec![commit],
                fragments: alloc::vec![],
            }),
        );

        // An empty-summary request arrives (requester has nothing).
        let seed = FingerprintSeed::new(1, 2);
        let request = wire::BatchSyncRequest {
            id: tree,
            req_id: wire::RequestId {
                requestor: PeerId::new([0xEE; 32]),
                nonce: 5,
            },
            fingerprint_summary: Sedimentree::default().fingerprint_summarize(&seed),
            subscribe: false,
        };
        let outcome = core.handle(
            now_at(1),
            CoreEvent::FromConn(Sealed::mint(
                edge,
                seq,
                ConnToCore::Inbound(alloc::boxed::Box::new(SyncForward::Request(request))),
            )),
        );
        assert_eq!(outcome, Outcome::Progressed);

        let effects = drain(&mut core);
        let Some(CoreEffect::Storage { ticket, op }) = effects
            .iter()
            .find(|e| matches!(e, CoreEffect::Storage { .. }))
            .cloned()
        else {
            return Err("expected FetchItemRefs".into());
        };
        let StorageOp::FetchItemRefs { commit_ids, .. } = op else {
            return Err("expected FetchItemRefs".into());
        };
        assert_eq!(commit_ids, alloc::vec![CommitId::new([0xA3; 32])]);

        // Storage answers with a ref; the response goes out scatter-gather.
        let (item, _bytes) = verified_commit(tree, 0xA3);
        let outcome = core.handle(
            now_at(2),
            CoreEvent::StorageDone {
                ticket,
                result: StorageResult::FetchedRefs {
                    commits: alloc::vec![(item.commit, item.blob)],
                    fragments: alloc::vec![],
                },
            },
        );
        assert_eq!(outcome, Outcome::Progressed);
        let effects = drain(&mut core);
        let has_ref_send = effects.iter().any(|e| {
            matches!(e, CoreEffect::Send { parts, .. }
                if parts.iter().any(|p| matches!(p, Part::Ref(_))))
        });
        assert!(has_ref_send, "response uses scatter-gather blob refs");
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, CoreEffect::ReleaseBlob(_))),
            "fetched refs released after send enqueued"
        );
        Ok(())
    }

    #[test]
    fn requester_flow_completes_and_times_out() {
        let (mut core, _edge, _seq, _peer) = core_with_peer(0xA5);
        let tree = SedimentreeId::new([9u8; 32]);

        let outcome = core.handle(
            now_at(0),
            CoreEvent::Command(Command::SyncTree {
                conn: ConnId::new(1),
                tree,
                subscribe: false,
            }),
        );
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(core.pending_sync_requests(), 1);
        assert!(core.poll_timeout().is_some(), "request deadline armed");
        let effects = drain(&mut core);
        assert!(effects.iter().any(|e| matches!(e, CoreEffect::Send { .. })));

        // No response: past the deadline.
        let outcome = core.handle(now_at(31_000), CoreEvent::Wake);
        assert_eq!(outcome, Outcome::Progressed);
        assert_eq!(core.pending_sync_requests(), 0);
        let effects = drain(&mut core);
        assert!(effects.iter().any(|e| matches!(
            e,
            CoreEffect::App(AppEvent::SyncFinished {
                status: SyncStatus::TimedOut,
                ..
            })
        )));
    }

    /// A credit breach mid-broadcast fires exactly one nudge —
    /// the fragments loop must not re-announce the pause the commits
    /// loop already reported.
    #[test]
    fn breach_mid_broadcast_nudges_exactly_once() {
        let (mut core, edge, _seq, _peer) = core_with_peer(0xAB);
        let tree = SedimentreeId::new([9u8; 32]);

        // Subscribe conn 1 with its credit already exhausted.
        let mut subscribers = Set::new();
        let _inserted = subscribers.insert(edge.conn);
        let _prev = core.subscriptions.insert(tree, subscribers);
        if let Some(entry) = core.edges.get_mut(&edge.conn) {
            entry.outstanding_pushes = core.config.max_outstanding_pushes;
        }

        // One commit AND one fragment in a single broadcast.
        let (commit_item, commit_blob) = verified_commit(tree, 0xC1);
        let signing_key = SigningKey::from_bytes(&[0xF1; 32]);
        let frag_blob = Blob::new(alloc::vec![0xF1; 24]);
        let fragment = Fragment::new(
            tree,
            CommitId::new([0xF1; 32]),
            BTreeSet::new(),
            &[],
            BlobMeta::new(&frag_blob),
        );
        let signed_fragment = Signed::seal_sync(&signing_key, fragment).into_signed();

        core.broadcast_items(
            tree,
            &[(&commit_item.commit, Part::Bytes(commit_blob))],
            &[(&signed_fragment, Part::Bytes(frag_blob.as_slice().to_vec()))],
            None,
        );

        let effects = drain(&mut core);
        let nudges = effects
            .iter()
            .filter(|e| matches!(e, CoreEffect::Send { .. }))
            .count();
        let lagging_events = effects
            .iter()
            .filter(|e| matches!(e, CoreEffect::App(AppEvent::SubscriberLagging { .. })))
            .count();
        assert_eq!(nudges, 1, "exactly one HeadsUpdate nudge per breach");
        assert_eq!(lagging_events, 1, "exactly one SubscriberLagging per breach");
        assert_eq!(core.stats.subscribers_paused, 1);
    }
}
