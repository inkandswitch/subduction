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

    // Storage metrics
    /// Total sedimentree save operations.
    pub const STORAGE_SEDIMENTREE_SAVES: &str = "subduction_storage_sedimentree_saves_total";
    /// Total sedimentree delete operations.
    pub const STORAGE_SEDIMENTREE_DELETES: &str = "subduction_storage_sedimentree_deletes_total";
    /// Total loose commits saved, labeled by `sedimentree_id`.
    pub const STORAGE_LOOSE_COMMITS_SAVED: &str = "subduction_storage_loose_commits_saved_total";
    /// Total fragments saved, labeled by `sedimentree_id`.
    pub const STORAGE_FRAGMENTS_SAVED: &str = "subduction_storage_fragments_saved_total";
    /// Total blob save operations.
    pub const STORAGE_BLOB_SAVES: &str = "subduction_storage_blob_saves_total";
    /// Total blob delete operations.
    pub const STORAGE_BLOB_DELETES: &str = "subduction_storage_blob_deletes_total";
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

// Storage metrics

/// Record a sedimentree save operation.
#[inline]
pub fn storage_sedimentree_saved() {
    metrics::counter!(names::STORAGE_SEDIMENTREE_SAVES).increment(1);
}

/// Record a sedimentree delete operation.
#[inline]
pub fn storage_sedimentree_deleted() {
    metrics::counter!(names::STORAGE_SEDIMENTREE_DELETES).increment(1);
}

/// Record a loose commit being saved.
#[inline]
pub fn storage_loose_commit_saved(sedimentree_id: String) {
    metrics::counter!(names::STORAGE_LOOSE_COMMITS_SAVED, "sedimentree_id" => sedimentree_id).increment(1);
}

/// Record a fragment being saved.
#[inline]
pub fn storage_fragment_saved(sedimentree_id: String) {
    metrics::counter!(names::STORAGE_FRAGMENTS_SAVED, "sedimentree_id" => sedimentree_id).increment(1);
}

/// Record a blob save operation.
#[inline]
pub fn storage_blob_saved() {
    metrics::counter!(names::STORAGE_BLOB_SAVES).increment(1);
}

/// Record a blob delete operation.
#[inline]
pub fn storage_blob_deleted() {
    metrics::counter!(names::STORAGE_BLOB_DELETES).increment(1);
}

/// Record the duration of a storage operation.
#[inline]
pub fn storage_operation_duration(operation: &'static str, duration_secs: f64) {
    metrics::histogram!(names::STORAGE_OPERATION_DURATION_SECONDS, "operation" => operation)
        .record(duration_secs);
}
