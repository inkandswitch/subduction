//! Prometheus metrics instrumentation for Subduction.
//!
//! This module is only available when the `metrics` feature is enabled.
//!
//! # Label cardinality
//!
//! All label values must come from a small, fixed set of `&'static str`
//! known at compile time. Runtime-derived values (sedimentree IDs, peer
//! IDs, commit IDs, URLs) are forbidden: each unique value creates a
//! permanent series in the recorder registry and slows every scrape.
//! Expose per-entity data via on-demand endpoints instead.

/// Metric names used throughout the application.
pub mod names {
    /// Number of currently active connections.
    pub const CONNECTIONS_ACTIVE: &str = "subduction_connections_active";
    /// Total number of connections established.
    pub const CONNECTIONS_TOTAL: &str = "subduction_connections_total";
    /// Total number of connections closed.
    pub const CONNECTIONS_CLOSED: &str = "subduction_connections_closed";
    /// Total messages processed, labeled by type.
    pub const MESSAGES_TOTAL: &str = "subduction_messages_total";
    /// Message dispatch duration in seconds.
    pub const DISPATCH_DURATION_SECONDS: &str = "subduction_dispatch_duration_seconds";
    /// Inbound messages currently being dispatched (held semaphore permits).
    pub const DISPATCH_INFLIGHT: &str = "subduction_dispatch_inflight";
    /// Maximum concurrent in-flight dispatches (the semaphore bound).
    pub const DISPATCH_INFLIGHT_MAX: &str = "subduction_dispatch_inflight_max";
    /// Completed dispatch tasks, labeled by `outcome` (`ok`/`err`/`aborted`).
    pub const DISPATCH_COMPLETED_TOTAL: &str = "subduction_dispatch_completed_total";
    /// Total batch sync requests received.
    pub const BATCH_SYNC_REQUESTS_TOTAL: &str = "subduction_batch_sync_requests_total";
    /// Total batch sync responses received.
    pub const BATCH_SYNC_RESPONSES_TOTAL: &str = "subduction_batch_sync_responses_total";

    // Foreground sync health (`sync_with_peer` / `full_sync_with_peer`).
    /// Duration of a foreground sync round in seconds.
    pub const SYNC_DURATION_SECONDS: &str = "subduction_sync_duration_seconds";
    /// Cumulative commits received via foreground sync.
    pub const SYNC_COMMITS_RECEIVED_TOTAL: &str = "subduction_sync_commits_received_total";
    /// Cumulative fragments received via foreground sync.
    pub const SYNC_FRAGMENTS_RECEIVED_TOTAL: &str = "subduction_sync_fragments_received_total";
    /// Cumulative commits sent via foreground sync.
    pub const SYNC_COMMITS_SENT_TOTAL: &str = "subduction_sync_commits_sent_total";
    /// Cumulative fragments sent via foreground sync.
    pub const SYNC_FRAGMENTS_SENT_TOTAL: &str = "subduction_sync_fragments_sent_total";
    /// Per-connection sync call failures, labeled by `reason`.
    pub const SYNC_CALL_FAILURES_TOTAL: &str = "subduction_sync_call_failures_total";

    // Multiplexer (request/response correlation).
    /// Outstanding correlated requests awaiting a response (across all muxes).
    pub const MUX_PENDING: &str = "subduction_mux_pending";
    /// Cumulative correlated requests registered.
    pub const MUX_REQUESTS_TOTAL: &str = "subduction_mux_requests_total";
    /// Cumulative pending requests cancelled (timeout or disconnect teardown).
    pub const MUX_CANCELLED_TOTAL: &str = "subduction_mux_cancelled_total";

    // Subscriptions (live update fan-out).
    /// Number of sedimentrees with at least one subscriber.
    pub const SUBSCRIBED_SEDIMENTREES: &str = "subduction_subscribed_sedimentrees";
    /// Cumulative incremental updates pushed to subscribers.
    pub const SUBSCRIPTION_PUSHES_TOTAL: &str = "subduction_subscription_pushes_total";

    /// Current number of sedimentrees in storage.
    ///
    /// Refreshed cheaply from the storage backend's in-memory id cache (no
    /// directory scan).
    pub const STORAGE_SEDIMENTREES: &str = "subduction_storage_sedimentrees";
    /// Cumulative loose-commit write operations (CAS; includes idempotent
    /// no-ops). Maintained incrementally — never scanned.
    pub const STORAGE_COMMITS_WRITTEN_TOTAL: &str = "subduction_storage_commits_written_total";
    /// Cumulative fragment write operations (CAS; includes idempotent no-ops).
    pub const STORAGE_FRAGMENTS_WRITTEN_TOTAL: &str = "subduction_storage_fragments_written_total";
    /// Cumulative loose-commit delete operations.
    pub const STORAGE_COMMITS_DELETED_TOTAL: &str = "subduction_storage_commits_deleted_total";
    /// Cumulative fragment delete operations.
    pub const STORAGE_FRAGMENTS_DELETED_TOTAL: &str = "subduction_storage_fragments_deleted_total";
    /// Storage operation duration in seconds.
    pub const STORAGE_OPERATION_DURATION_SECONDS: &str =
        "subduction_storage_operation_duration_seconds";
    /// Cumulative storage operation errors, labeled by `operation`.
    pub const STORAGE_OPERATION_ERRORS_TOTAL: &str = "subduction_storage_operation_errors_total";

    // Background sync (iroh and similar periodic full-sync tasks).
    /// Duration of a background `full_sync_with_all_peers` round trip in seconds.
    pub const BACKGROUND_SYNC_DURATION_SECONDS: &str =
        "subduction_background_sync_duration_seconds";
    /// Cumulative count of call errors across background sync rounds (since process start).
    pub const BACKGROUND_SYNC_CALL_ERRORS_TOTAL: &str =
        "subduction_background_sync_call_errors_total";
    /// Cumulative count of I/O errors across background sync rounds (since process start).
    pub const BACKGROUND_SYNC_IO_ERRORS_TOTAL: &str = "subduction_background_sync_io_errors_total";
}

/// Record a new connection being established.
#[inline]
pub fn connection_opened() {
    metrics::gauge!(names::CONNECTIONS_ACTIVE).increment(1);
    metrics::counter!(names::CONNECTIONS_TOTAL).increment(1);
}

/// Record a connection being closed.
#[inline]
pub fn connection_closed() {
    metrics::gauge!(names::CONNECTIONS_ACTIVE).decrement(1);
    metrics::counter!(names::CONNECTIONS_CLOSED).increment(1);
}

/// Record a message being dispatched.
#[inline]
pub fn message_dispatched(message_type: &'static str) {
    metrics::counter!(names::MESSAGES_TOTAL, "type" => message_type).increment(1);
}

/// Record the duration of a dispatch operation.
#[inline]
pub fn dispatch_duration(duration_secs: f64) {
    metrics::histogram!(names::DISPATCH_DURATION_SECONDS).record(duration_secs);
}

/// A scope guard that records dispatch duration on drop.
///
/// This ensures the duration is recorded even if the function returns early
/// via `?` or other control flow, capturing both success and failure latencies.
#[derive(Debug)]
pub struct DispatchTimer {
    start: std::time::Instant,
}

impl DispatchTimer {
    /// Create a new dispatch timer, starting the clock now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

impl Default for DispatchTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DispatchTimer {
    fn drop(&mut self) {
        dispatch_duration(self.start.elapsed().as_secs_f64());
    }
}

/// Increment the in-flight dispatch gauge (a permit was acquired and a task spawned).
#[inline]
pub fn dispatch_inflight_inc() {
    metrics::gauge!(names::DISPATCH_INFLIGHT).increment(1.0);
}

/// Decrement the in-flight dispatch gauge (a dispatch task completed).
#[inline]
pub fn dispatch_inflight_dec() {
    metrics::gauge!(names::DISPATCH_INFLIGHT).decrement(1.0);
}

/// Publish the in-flight dispatch ceiling (set once at listener start).
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_dispatch_inflight_max(max: usize) {
    metrics::gauge!(names::DISPATCH_INFLIGHT_MAX).set(max as f64);
}

/// Record a completed dispatch task, labeled by outcome.
///
/// `outcome` must be a bounded `&'static str` (`"ok"`, `"err"`, `"aborted"`).
#[inline]
pub fn dispatch_completed(outcome: &'static str) {
    metrics::counter!(names::DISPATCH_COMPLETED_TOTAL, "outcome" => outcome).increment(1);
}

/// Record a batch sync request.
#[inline]
pub fn batch_sync_request() {
    metrics::counter!(names::BATCH_SYNC_REQUESTS_TOTAL).increment(1);
}

/// Record a batch sync response.
#[inline]
pub fn batch_sync_response() {
    metrics::counter!(names::BATCH_SYNC_RESPONSES_TOTAL).increment(1);
}

/// Record the duration of a foreground sync round.
#[inline]
pub fn sync_duration(duration_secs: f64) {
    metrics::histogram!(names::SYNC_DURATION_SECONDS).record(duration_secs);
}

/// Record the commit/fragment volume of a completed foreground sync.
#[inline]
#[allow(clippy::cast_possible_truncation)]
pub fn sync_data_exchanged(
    commits_received: usize,
    fragments_received: usize,
    commits_sent: usize,
    fragments_sent: usize,
) {
    metrics::counter!(names::SYNC_COMMITS_RECEIVED_TOTAL).increment(commits_received as u64);
    metrics::counter!(names::SYNC_FRAGMENTS_RECEIVED_TOTAL).increment(fragments_received as u64);
    metrics::counter!(names::SYNC_COMMITS_SENT_TOTAL).increment(commits_sent as u64);
    metrics::counter!(names::SYNC_FRAGMENTS_SENT_TOTAL).increment(fragments_sent as u64);
}

/// Record a per-connection sync call failure, labeled by a bounded reason.
///
/// `reason` must be a bounded `&'static str` (e.g. a `CallError` variant name).
#[inline]
pub fn sync_call_failure(reason: &'static str) {
    metrics::counter!(names::SYNC_CALL_FAILURES_TOTAL, "reason" => reason).increment(1);
}

/// A correlated request was registered (pending++).
#[inline]
pub fn mux_request_registered() {
    metrics::counter!(names::MUX_REQUESTS_TOTAL).increment(1);
    metrics::gauge!(names::MUX_PENDING).increment(1.0);
}

/// A pending request was resolved by a matching response (pending--).
#[inline]
pub fn mux_request_resolved() {
    metrics::gauge!(names::MUX_PENDING).decrement(1.0);
}

/// `n` pending requests were cancelled (timeout or disconnect teardown).
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn mux_requests_cancelled(n: usize) {
    if n == 0 {
        return;
    }
    metrics::counter!(names::MUX_CANCELLED_TOTAL).increment(n as u64);
    metrics::gauge!(names::MUX_PENDING).decrement(n as f64);
}

/// Set the number of sedimentrees with at least one subscriber.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_subscribed_sedimentrees(count: usize) {
    metrics::gauge!(names::SUBSCRIBED_SEDIMENTREES).set(count as f64);
}

/// Record `n` incremental updates pushed to subscribers.
#[inline]
pub fn subscription_pushes(n: u64) {
    if n == 0 {
        return;
    }
    metrics::counter!(names::SUBSCRIPTION_PUSHES_TOTAL).increment(n);
}

/// Set the current number of sedimentrees in storage.
///
/// Sourced from the backend's in-memory id cache; cheap and scan-free.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_sedimentrees(count: usize) {
    metrics::gauge!(names::STORAGE_SEDIMENTREES).set(count as f64);
}

/// Record a loose-commit write operation (one per `save_loose_commit`).
#[inline]
pub fn storage_commit_written() {
    metrics::counter!(names::STORAGE_COMMITS_WRITTEN_TOTAL).increment(1);
}

/// Record `n` loose-commit write operations (batch save).
#[inline]
pub fn storage_commits_written(n: u64) {
    metrics::counter!(names::STORAGE_COMMITS_WRITTEN_TOTAL).increment(n);
}

/// Record a fragment write operation (one per `save_fragment`).
#[inline]
pub fn storage_fragment_written() {
    metrics::counter!(names::STORAGE_FRAGMENTS_WRITTEN_TOTAL).increment(1);
}

/// Record `n` fragment write operations (batch save).
#[inline]
pub fn storage_fragments_written(n: u64) {
    metrics::counter!(names::STORAGE_FRAGMENTS_WRITTEN_TOTAL).increment(n);
}

/// Record a loose-commit delete operation.
#[inline]
pub fn storage_commit_deleted() {
    metrics::counter!(names::STORAGE_COMMITS_DELETED_TOTAL).increment(1);
}

/// Record a fragment delete operation.
#[inline]
pub fn storage_fragment_deleted() {
    metrics::counter!(names::STORAGE_FRAGMENTS_DELETED_TOTAL).increment(1);
}

/// Record the duration of a storage operation.
#[inline]
pub fn storage_operation_duration(operation: &'static str, duration_secs: f64) {
    metrics::histogram!(names::STORAGE_OPERATION_DURATION_SECONDS, "operation" => operation)
        .record(duration_secs);
}

/// Record a storage operation error, labeled by `operation`.
#[inline]
pub fn storage_operation_error(operation: &'static str) {
    metrics::counter!(names::STORAGE_OPERATION_ERRORS_TOTAL, "operation" => operation).increment(1);
}

/// Record the duration of a background `full_sync_with_all_peers` round.
#[inline]
pub fn background_sync_duration(duration_secs: f64) {
    metrics::histogram!(names::BACKGROUND_SYNC_DURATION_SECONDS).record(duration_secs);
}

/// Record `n` call errors observed in a background sync round.
#[inline]
pub fn background_sync_call_errors(n: u64) {
    metrics::counter!(names::BACKGROUND_SYNC_CALL_ERRORS_TOTAL).increment(n);
}

/// Record `n` I/O errors observed in a background sync round.
#[inline]
pub fn background_sync_io_errors(n: u64) {
    metrics::counter!(names::BACKGROUND_SYNC_IO_ERRORS_TOTAL).increment(n);
}

/// Register HELP/TYPE metadata for every metric this crate emits.
///
/// Call once at recorder init.
#[allow(clippy::too_many_lines)] // a flat list of describe! calls
pub fn describe_all() {
    metrics::describe_gauge!(
        names::CONNECTIONS_ACTIVE,
        "Number of currently active peer connections."
    );
    metrics::describe_counter!(
        names::CONNECTIONS_TOTAL,
        "Total number of peer connections established since process start."
    );
    metrics::describe_counter!(
        names::CONNECTIONS_CLOSED,
        "Total number of peer connections closed since process start."
    );
    metrics::describe_counter!(
        names::MESSAGES_TOTAL,
        "Total number of sync messages dispatched, labeled by `SyncMessage` variant."
    );
    metrics::describe_histogram!(
        names::DISPATCH_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of message dispatch (per-message handler runtime)."
    );
    metrics::describe_gauge!(
        names::DISPATCH_INFLIGHT,
        "Inbound messages currently being dispatched (held semaphore permits)."
    );
    metrics::describe_gauge!(
        names::DISPATCH_INFLIGHT_MAX,
        "Maximum concurrent in-flight dispatches (semaphore bound)."
    );
    metrics::describe_counter!(
        names::DISPATCH_COMPLETED_TOTAL,
        "Completed dispatch tasks, labeled by `outcome` (ok/err/aborted)."
    );
    metrics::describe_counter!(
        names::BATCH_SYNC_REQUESTS_TOTAL,
        "Total number of `BatchSyncRequest` messages received and processed."
    );
    metrics::describe_counter!(
        names::BATCH_SYNC_RESPONSES_TOTAL,
        "Total number of `BatchSyncResponse` messages received and routed."
    );
    metrics::describe_histogram!(
        names::SYNC_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of a foreground sync round (`sync_with_peer`/`full_sync_with_peer`)."
    );
    metrics::describe_counter!(
        names::SYNC_COMMITS_RECEIVED_TOTAL,
        "Cumulative commits received via foreground sync."
    );
    metrics::describe_counter!(
        names::SYNC_FRAGMENTS_RECEIVED_TOTAL,
        "Cumulative fragments received via foreground sync."
    );
    metrics::describe_counter!(
        names::SYNC_COMMITS_SENT_TOTAL,
        "Cumulative commits sent via foreground sync."
    );
    metrics::describe_counter!(
        names::SYNC_FRAGMENTS_SENT_TOTAL,
        "Cumulative fragments sent via foreground sync."
    );
    metrics::describe_counter!(
        names::SYNC_CALL_FAILURES_TOTAL,
        "Per-connection sync call failures, labeled by `reason`."
    );
    metrics::describe_gauge!(
        names::MUX_PENDING,
        "Outstanding correlated requests awaiting a response (across all multiplexers)."
    );
    metrics::describe_counter!(
        names::MUX_REQUESTS_TOTAL,
        "Cumulative correlated requests registered."
    );
    metrics::describe_counter!(
        names::MUX_CANCELLED_TOTAL,
        "Cumulative pending requests cancelled (timeout or disconnect teardown)."
    );
    metrics::describe_gauge!(
        names::SUBSCRIBED_SEDIMENTREES,
        "Number of sedimentrees with at least one subscriber."
    );
    metrics::describe_counter!(
        names::SUBSCRIPTION_PUSHES_TOTAL,
        "Cumulative incremental updates pushed to subscribers."
    );
    metrics::describe_gauge!(
        names::STORAGE_SEDIMENTREES,
        "Current number of sedimentrees in storage (from the in-memory id cache)."
    );
    metrics::describe_counter!(
        names::STORAGE_COMMITS_WRITTEN_TOTAL,
        "Cumulative loose-commit write operations (CAS; includes idempotent no-ops)."
    );
    metrics::describe_counter!(
        names::STORAGE_FRAGMENTS_WRITTEN_TOTAL,
        "Cumulative fragment write operations (CAS; includes idempotent no-ops)."
    );
    metrics::describe_counter!(
        names::STORAGE_COMMITS_DELETED_TOTAL,
        "Cumulative loose-commit delete operations."
    );
    metrics::describe_counter!(
        names::STORAGE_FRAGMENTS_DELETED_TOTAL,
        "Cumulative fragment delete operations."
    );
    metrics::describe_histogram!(
        names::STORAGE_OPERATION_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of individual storage operations, labeled by `operation`."
    );
    metrics::describe_counter!(
        names::STORAGE_OPERATION_ERRORS_TOTAL,
        "Cumulative storage operation errors, labeled by `operation`."
    );
    metrics::describe_histogram!(
        names::BACKGROUND_SYNC_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of a single background `full_sync_with_all_peers` round."
    );
    metrics::describe_counter!(
        names::BACKGROUND_SYNC_CALL_ERRORS_TOTAL,
        "Total call errors observed across background sync rounds."
    );
    metrics::describe_counter!(
        names::BACKGROUND_SYNC_IO_ERRORS_TOTAL,
        "Total I/O errors observed across background sync rounds."
    );
}
