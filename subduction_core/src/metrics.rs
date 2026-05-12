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
    /// Total batch sync requests received.
    pub const BATCH_SYNC_REQUESTS_TOTAL: &str = "subduction_batch_sync_requests_total";
    /// Total batch sync responses received.
    pub const BATCH_SYNC_RESPONSES_TOTAL: &str = "subduction_batch_sync_responses_total";

    // Storage metrics (gauges - refreshed periodically from actual state).
    /// Current number of sedimentrees in storage.
    pub const STORAGE_SEDIMENTREES: &str = "subduction_storage_sedimentrees";
    /// Total loose commits across all sedimentrees.
    pub const STORAGE_LOOSE_COMMITS_TOTAL: &str = "subduction_storage_loose_commits_total";
    /// Total fragments across all sedimentrees.
    pub const STORAGE_FRAGMENTS_TOTAL: &str = "subduction_storage_fragments_total";
    /// Storage operation duration in seconds.
    pub const STORAGE_OPERATION_DURATION_SECONDS: &str =
        "subduction_storage_operation_duration_seconds";

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

// Storage metrics (gauges - set from actual state)

/// Set the current number of sedimentrees in storage.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_sedimentrees(count: usize) {
    metrics::gauge!(names::STORAGE_SEDIMENTREES).set(count as f64);
}

/// Set the total number of loose commits across all sedimentrees.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_loose_commits_total(count: usize) {
    metrics::gauge!(names::STORAGE_LOOSE_COMMITS_TOTAL).set(count as f64);
}

/// Set the total number of fragments across all sedimentrees.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_fragments_total(count: usize) {
    metrics::gauge!(names::STORAGE_FRAGMENTS_TOTAL).set(count as f64);
}

/// Record the duration of a storage operation.
#[inline]
pub fn storage_operation_duration(operation: &'static str, duration_secs: f64) {
    metrics::histogram!(names::STORAGE_OPERATION_DURATION_SECONDS, "operation" => operation)
        .record(duration_secs);
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
    metrics::describe_counter!(
        names::BATCH_SYNC_REQUESTS_TOTAL,
        "Total number of `BatchSyncRequest` messages received and processed."
    );
    metrics::describe_counter!(
        names::BATCH_SYNC_RESPONSES_TOTAL,
        "Total number of `BatchSyncResponse` messages received and routed."
    );
    metrics::describe_gauge!(
        names::STORAGE_SEDIMENTREES,
        "Current number of sedimentrees in persistent storage."
    );
    metrics::describe_gauge!(
        names::STORAGE_LOOSE_COMMITS_TOTAL,
        "Total loose commits across all sedimentrees in storage."
    );
    metrics::describe_gauge!(
        names::STORAGE_FRAGMENTS_TOTAL,
        "Total fragments across all sedimentrees in storage."
    );
    metrics::describe_histogram!(
        names::STORAGE_OPERATION_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of individual storage operations, labeled by `operation`."
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
