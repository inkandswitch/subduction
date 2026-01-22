//! Prometheus metrics instrumentation for Subduction.
//!
//! This module is only available when the `metrics` feature is enabled.

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
    /// Number of active sedimentrees.
    pub const SEDIMENTREES_ACTIVE: &str = "subduction_sedimentrees_active";
    /// Total batch sync requests received.
    pub const BATCH_SYNC_REQUESTS_TOTAL: &str = "subduction_batch_sync_requests_total";
    /// Total batch sync responses received.
    pub const BATCH_SYNC_RESPONSES_TOTAL: &str = "subduction_batch_sync_responses_total";

    // Storage metrics (gauges - refreshed periodically from actual state)
    /// Current number of sedimentrees in storage.
    pub const STORAGE_SEDIMENTREES: &str = "subduction_storage_sedimentrees";
    /// Current number of loose commits in storage, labeled by `sedimentree_id`.
    pub const STORAGE_LOOSE_COMMITS: &str = "subduction_storage_loose_commits";
    /// Current number of fragments in storage, labeled by `sedimentree_id`.
    pub const STORAGE_FRAGMENTS: &str = "subduction_storage_fragments";
    /// Total loose commits across all sedimentrees.
    pub const STORAGE_LOOSE_COMMITS_TOTAL: &str = "subduction_storage_loose_commits_total";
    /// Total fragments across all sedimentrees.
    pub const STORAGE_FRAGMENTS_TOTAL: &str = "subduction_storage_fragments_total";
    /// Storage operation duration in seconds.
    pub const STORAGE_OPERATION_DURATION_SECONDS: &str =
        "subduction_storage_operation_duration_seconds";
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

/// Set the current number of loose commits for a sedimentree.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_loose_commits(sedimentree_id: String, count: usize) {
    metrics::gauge!(names::STORAGE_LOOSE_COMMITS, "sedimentree_id" => sedimentree_id).set(count as f64);
}

/// Set the current number of fragments for a sedimentree.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_fragments(sedimentree_id: String, count: usize) {
    metrics::gauge!(names::STORAGE_FRAGMENTS, "sedimentree_id" => sedimentree_id).set(count as f64);
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
