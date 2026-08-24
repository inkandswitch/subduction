//! Tier-1 (boundary) and tier-3 (decision) telemetry, derived in the
//! driver from effect execution and machine outcomes.
//!
//! Three tiers (the `metrics` facade never appears in L1):
//!
//! | Tier | What | Mechanism |
//! |---|---|---|
//! | 1 — boundary | bytes/messages, durations, gauges | derived here while executing effects |
//! | 2 — internal counters | machine state snapshots | pull [`Node::stats`] via [`Handle::stats`] |
//! | 3 — decision events | faults, rejections, sync failures | [`Outcome`]s/[`AppEvent`]s matched here |
//!
//! Legacy metric _names_ are preserved verbatim (see [`names`], mirrored
//! from `legacy/subduction_core/src/metrics/names.rs`) so existing
//! dashboards keep working. Metrics emission is feature-gated
//! (`metrics`); tier-3 `tracing` events are always on.
//!
//! [`Node::stats`]: subduction_protocol::node::Node::stats
//! [`Handle::stats`]: crate::driver::handle::Handle::stats
//! [`Outcome`]: subduction_protocol::outcome::Outcome
//! [`AppEvent`]: subduction_protocol::effect::AppEvent

use sedimentree_core::{collections::Map, id::SedimentreeId};
#[cfg(feature = "metrics")]
use subduction_protocol::outcome::{Fault, IgnoreReason};
use subduction_protocol::{
    effect::{AppEvent, SyncStatus},
    id::ConnId,
    outcome::Outcome,
    storage::{StorageOp, StorageResult},
    timestamp::Timestamp,
};

/// Legacy metric names, preserved verbatim so dashboards keep working.
///
/// Mirrored from `legacy/subduction_core/src/metrics/names.rs`; only the
/// names the driver feeds appear here (tier-2 names are exported by
/// whatever scrapes [`Handle::stats`](crate::driver::handle::Handle::stats)).
pub mod names {
    /// Cumulative opened connections.
    pub const CONNECTIONS_TOTAL: &str = "subduction_connections_total";

    /// Cumulative closed connections.
    pub const CONNECTIONS_CLOSED: &str = "subduction_connections_closed";

    /// Handshake conclusions, labelled `outcome`.
    pub const HANDSHAKE_TOTAL: &str = "subduction_handshake_total";

    /// Connected → authenticated latency.
    pub const HANDSHAKE_DURATION_SECONDS: &str = "subduction_handshake_duration_seconds";

    /// Wire bytes, labelled `direction`.
    pub const NETWORK_FRAME_BYTES: &str = "subduction_network_frame_bytes";

    /// Wire messages, labelled `direction`.
    pub const MESSAGES_TOTAL: &str = "subduction_messages_total";

    /// Sync request → conclusion latency.
    pub const SYNC_DURATION_SECONDS: &str = "subduction_sync_duration_seconds";

    /// Syncs that concluded unsuccessfully, labelled `status`.
    pub const SYNC_CALL_FAILURES_TOTAL: &str = "subduction_sync_call_failures_total";

    /// Items that failed verification at the forgery gate.
    pub const SYNC_VERIFY_FAILURES_TOTAL: &str = "subduction_sync_verify_failures_total";

    /// Commits persisted from remote peers.
    pub const SYNC_COMMITS_RECEIVED_TOTAL: &str = "subduction_sync_commits_received_total";

    /// Fragments persisted from remote peers.
    pub const SYNC_FRAGMENTS_RECEIVED_TOTAL: &str = "subduction_sync_fragments_received_total";

    /// Completions that matched no live entity.
    pub const LATE_RESPONSES_TOTAL: &str = "subduction_late_responses_total";

    /// Commits written by local authorship.
    pub const STORAGE_COMMITS_WRITTEN_TOTAL: &str = "subduction_storage_commits_written_total";

    /// Fragments written by local authorship.
    pub const STORAGE_FRAGMENTS_WRITTEN_TOTAL: &str = "subduction_storage_fragments_written_total";

    /// Storage op latency, labelled `operation`.
    pub const STORAGE_OPERATION_DURATION_SECONDS: &str =
        "subduction_storage_operation_duration_seconds";

    /// Storage ops that failed, labelled `operation`.
    pub const STORAGE_OPERATION_ERRORS_TOTAL: &str = "subduction_storage_operation_errors_total";
}

/// Driver-side telemetry state: in-flight timers plus the emission
/// hooks. Plain fields, driver-owned — no locks, no globals beyond the
/// `metrics` recorder itself.
#[derive(Debug, Default)]
pub(crate) struct Telemetry {
    handshake_started: Map<ConnId, Timestamp>,
    sync_started: Map<(ConnId, SedimentreeId), Timestamp>,
}

#[cfg_attr(not(feature = "metrics"), allow(clippy::missing_const_for_fn))]
impl Telemetry {
    /// A connection was registered; the handshake clock starts.
    pub(crate) fn on_connected(&mut self, conn: ConnId, now: Timestamp) {
        let _previous = self.handshake_started.insert(conn, now);
        #[cfg(feature = "metrics")]
        metrics::counter!(names::CONNECTIONS_TOTAL).increment(1);
    }

    /// One complete inbound wire message.
    pub(crate) fn on_inbound(&mut self, len: usize) {
        let _ = self;
        let _ = len;
        #[cfg(feature = "metrics")]
        {
            metrics::counter!(names::MESSAGES_TOTAL, "direction" => "inbound").increment(1);
            metrics::counter!(names::NETWORK_FRAME_BYTES, "direction" => "inbound")
                .increment(len as u64);
        }
    }

    /// One assembled outbound wire message.
    pub(crate) fn on_outbound(&mut self, len: usize) {
        let _ = self;
        let _ = len;
        #[cfg(feature = "metrics")]
        {
            metrics::counter!(names::MESSAGES_TOTAL, "direction" => "outbound").increment(1);
            metrics::counter!(names::NETWORK_FRAME_BYTES, "direction" => "outbound")
                .increment(len as u64);
        }
    }

    /// A batch sync was requested; its clock starts.
    pub(crate) fn on_sync_requested(&mut self, conn: ConnId, tree: SedimentreeId, now: Timestamp) {
        let _previous = self.sync_started.insert((conn, tree), now);
    }

    /// Tier 3: pattern-match a machine outcome into telemetry.
    pub(crate) fn on_outcome(&mut self, outcome: &Outcome) {
        let _ = self;
        match outcome {
            Outcome::Progressed | Outcome::Idle => {}
            Outcome::Ignored(reason) => {
                tracing::debug!(?reason, "event ignored");
                #[cfg(feature = "metrics")]
                if matches!(
                    reason,
                    IgnoreReason::StaleTicket
                        | IgnoreReason::UnknownTicket
                        | IgnoreReason::UnknownRequest
                ) {
                    metrics::counter!(names::LATE_RESPONSES_TOTAL).increment(1);
                }
            }
            Outcome::ConnectionFault { conn, fault } => {
                tracing::warn!(?conn, ?fault, "connection fault");
                #[cfg(feature = "metrics")]
                {
                    metrics::counter!(names::HANDSHAKE_TOTAL, "outcome" => fault_label(*fault))
                        .increment(1);
                    if matches!(fault, Fault::HandshakeVerificationFailed) {
                        metrics::counter!(names::SYNC_VERIFY_FAILURES_TOTAL).increment(1);
                    }
                }
            }
        }
    }

    /// Tier 1 + 3: pattern-match an application event into telemetry.
    #[allow(clippy::wildcard_enum_match_arm)] // sampled events only; the rest are app concerns
    pub(crate) fn on_app_event(&mut self, event: &AppEvent, now: Timestamp) {
        match event {
            AppEvent::PeerAuthenticated { conn, peer } => {
                tracing::info!(?conn, %peer, "peer authenticated");
                if let Some(started) = self.handshake_started.remove(conn) {
                    let _ = (started, now);
                    #[cfg(feature = "metrics")]
                    {
                        metrics::histogram!(names::HANDSHAKE_DURATION_SECONDS)
                            .record(elapsed_secs(started, now));
                        metrics::counter!(names::HANDSHAKE_TOTAL, "outcome" => "completed")
                            .increment(1);
                    }
                }
            }
            AppEvent::ConnectionClosed { conn, peer } => {
                tracing::info!(?conn, ?peer, "connection closed");
                let _pending = self.handshake_started.remove(conn);
                self.sync_started.retain(|(c, _), _| c != conn);
                #[cfg(feature = "metrics")]
                metrics::counter!(names::CONNECTIONS_CLOSED).increment(1);
            }
            AppEvent::SyncFinished { conn, tree, status } => {
                tracing::debug!(?conn, ?tree, ?status, "sync finished");
                if let Some(started) = self.sync_started.remove(&(*conn, *tree)) {
                    let _ = (started, now);
                    #[cfg(feature = "metrics")]
                    metrics::histogram!(names::SYNC_DURATION_SECONDS)
                        .record(elapsed_secs(started, now));
                }
                if *status != SyncStatus::Completed {
                    tracing::warn!(?conn, ?tree, ?status, "sync did not complete");
                    #[cfg(feature = "metrics")]
                    metrics::counter!(
                        names::SYNC_CALL_FAILURES_TOTAL,
                        "status" => status_label(*status)
                    )
                    .increment(1);
                }
            }
            AppEvent::SubscriberLagging { conn, tree } => {
                tracing::warn!(?conn, ?tree, "subscriber lagging; paused");
            }
            AppEvent::StorageError { tree, failure } => {
                tracing::error!(?tree, ?failure, "storage error");
            }
            _ => {}
        }
    }

    /// Tier 1: one storage op executed, with its outcome and latency.
    /// `summary` is captured via [`OpSummary::of`] before execution
    /// consumes the op.
    pub(crate) fn on_storage_op(
        &mut self,
        summary: OpSummary,
        result: &StorageResult,
        started: Timestamp,
        now: Timestamp,
    ) {
        let _ = self;
        let operation = summary.label;
        let _ = (started, now);
        #[cfg(feature = "metrics")]
        metrics::histogram!(names::STORAGE_OPERATION_DURATION_SECONDS, "operation" => operation)
            .record(elapsed_secs(started, now));
        match result {
            StorageResult::Persisted { .. } => {
                #[cfg(feature = "metrics")]
                {
                    metrics::counter!(names::SYNC_COMMITS_RECEIVED_TOTAL)
                        .increment(summary.commits);
                    metrics::counter!(names::SYNC_FRAGMENTS_RECEIVED_TOTAL)
                        .increment(summary.fragments);
                }
            }
            StorageResult::LocallyIngested { commits, fragments } => {
                let _ = (commits, fragments);
                #[cfg(feature = "metrics")]
                {
                    metrics::counter!(names::STORAGE_COMMITS_WRITTEN_TOTAL)
                        .increment(commits.len() as u64);
                    metrics::counter!(names::STORAGE_FRAGMENTS_WRITTEN_TOTAL)
                        .increment(fragments.len() as u64);
                }
            }
            StorageResult::Failed(failure) => {
                tracing::error!(operation, ?failure, "storage op failed");
                #[cfg(feature = "metrics")]
                metrics::counter!(names::STORAGE_OPERATION_ERRORS_TOTAL, "operation" => operation)
                    .increment(1);
            }
            StorageResult::TreeDeleted
            | StorageResult::FetchedRefs { .. }
            | StorageResult::Unauthorized
            | StorageResult::UnknownTree => {}
        }
    }
}

/// The parts of a [`StorageOp`] telemetry needs after execution has
/// consumed the op itself.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
pub(crate) struct OpSummary {
    label: &'static str,
    commits: u64,
    fragments: u64,
}

impl OpSummary {
    /// Capture before executing `op`.
    pub(crate) const fn of(op: &StorageOp) -> Self {
        let (commits, fragments) = match op {
            StorageOp::PersistItems {
                commits, fragments, ..
            } => (commits.len() as u64, fragments.len() as u64),
            StorageOp::DeleteTree { .. }
            | StorageOp::FetchItemRefs { .. }
            | StorageOp::IngestLocal { .. } => (0, 0),
        };
        Self {
            label: op_label(op),
            commits,
            fragments,
        }
    }
}

/// Seconds between two monotonic timestamps (saturating).
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
fn elapsed_secs(started: Timestamp, now: Timestamp) -> f64 {
    #[allow(clippy::cast_precision_loss)] // sub-day durations, ms precision
    let ms = now.as_millis().saturating_sub(started.as_millis()) as f64;
    ms / 1000.0
}

/// A stable label for a fault (tier-3 `outcome` values).
#[cfg(feature = "metrics")]
const fn fault_label(fault: Fault) -> &'static str {
    match fault {
        Fault::MalformedMessage => "malformed_message",
        Fault::UnexpectedMessage => "unexpected_message",
        Fault::HandshakeVerificationFailed => "verification_failed",
        Fault::PeerMismatch => "peer_mismatch",
        Fault::HandshakeTimeout => "timeout",
        Fault::ReflectedChallenge => "reflected_challenge",
        Fault::ReflectionAttack => "reflection_attack",
        Fault::SimultaneousOpenPeerMismatch => "sim_open_peer_mismatch",
        Fault::MissingAudience => "missing_audience",
        Fault::HandshakeRejected(_) => "rejected",
        Fault::ChallengeRejected(_) => "challenge_rejected",
    }
}

/// A stable label for a sync conclusion.
#[cfg(feature = "metrics")]
const fn status_label(status: SyncStatus) -> &'static str {
    match status {
        SyncStatus::Completed => "completed",
        SyncStatus::NotFound => "not_found",
        SyncStatus::Unauthorized => "unauthorized",
        SyncStatus::TimedOut => "timed_out",
    }
}

/// A stable label for a storage op kind.
const fn op_label(op: &StorageOp) -> &'static str {
    match op {
        StorageOp::DeleteTree { .. } => "delete_tree",
        StorageOp::PersistItems { .. } => "persist_items",
        StorageOp::FetchItemRefs { .. } => "fetch_item_refs",
        StorageOp::IngestLocal { .. } => "ingest_local",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_duration_tracked_per_conn() {
        let mut telemetry = Telemetry::default();
        let conn = ConnId::new(1);
        telemetry.on_connected(conn, Timestamp::from_millis(100));
        assert!(telemetry.handshake_started.contains_key(&conn));

        telemetry.on_app_event(
            &AppEvent::PeerAuthenticated {
                conn,
                peer: subduction_protocol::peer_id::PeerId::new([1; 32]),
            },
            Timestamp::from_millis(350),
        );
        assert!(
            !telemetry.handshake_started.contains_key(&conn),
            "timer consumed on authentication"
        );
    }

    #[test]
    fn sync_timers_swept_on_connection_close() {
        let mut telemetry = Telemetry::default();
        let conn = ConnId::new(1);
        let tree = SedimentreeId::new([7; 32]);
        telemetry.on_sync_requested(conn, tree, Timestamp::from_millis(0));

        telemetry.on_app_event(
            &AppEvent::ConnectionClosed { conn, peer: None },
            Timestamp::from_millis(10),
        );
        assert!(
            telemetry.sync_started.is_empty(),
            "in-flight sync timers do not leak past connection death"
        );
    }

    #[test]
    fn elapsed_is_saturating() {
        // A regressed clock must not panic or produce negative durations.
        let elapsed = elapsed_secs(Timestamp::from_millis(500), Timestamp::from_millis(100));
        assert!(
            elapsed.abs() < f64::EPSILON,
            "regressed clock yields zero, got {elapsed}"
        );
    }
}
