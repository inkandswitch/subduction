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
    /// Completed handshake attempts, labeled by `outcome`. Counts rejections
    /// (auth/clock-drift/decode) that never become a connection, which
    /// `CONNECTIONS_TOTAL` (successes only) can't show.
    pub const HANDSHAKE_TOTAL: &str = "subduction_handshake_total";
    /// Total messages processed, labeled by type.
    pub const MESSAGES_TOTAL: &str = "subduction_messages_total";
    /// Message dispatch duration in seconds.
    pub const DISPATCH_DURATION_SECONDS: &str = "subduction_dispatch_duration_seconds";
    /// Inbound messages currently being dispatched across all peers.
    pub const DISPATCH_INFLIGHT: &str = "subduction_dispatch_inflight";
    /// Completed dispatch tasks, labeled by `outcome` (`ok`/`err`/`aborted`).
    pub const DISPATCH_COMPLETED_TOTAL: &str = "subduction_dispatch_completed_total";
    /// Times an inbound message had to wait for a per-peer dispatch permit
    /// (the peer was at its concurrency cap — the rate limiter engaging).
    pub const DISPATCH_THROTTLED_TOTAL: &str = "subduction_dispatch_throttled_total";
    /// Time spent waiting to acquire a per-peer dispatch permit (0 on the
    /// fast path; grows as a peer saturates its cap).
    pub const DISPATCH_PERMIT_WAIT_SECONDS: &str = "subduction_dispatch_permit_wait_seconds";
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
    /// Time a correlated request stays pending until resolved by a response
    /// (successful round-trips): the full request→response wait at the
    /// correlation layer. Cancellations/timeouts are excluded (counted in
    /// [`MUX_CANCELLED_TOTAL`]).
    pub const MUX_PENDING_DURATION_SECONDS: &str = "subduction_mux_pending_duration_seconds";

    // Transport outbound queue (per-connection send buffer).
    /// Time an outbound message waits in the per-connection send queue before
    /// the peer grabs it, labeled by `transport` (`websocket`/`longpoll`/`iroh`).
    pub const OUTBOUND_QUEUE_DWELL_SECONDS: &str = "subduction_outbound_queue_dwell_seconds";
    /// Outbound send-queue depth sampled when a message is drained, labeled by
    /// `transport`. Rising depth signals a slow/absent peer backing up the
    /// bounded per-connection channel.
    pub const OUTBOUND_QUEUE_DEPTH: &str = "subduction_outbound_queue_depth";
    /// Times a send had to block because the bounded outbound channel was full
    /// (head-of-line backpressure from a slow peer), labeled by `transport`.
    pub const OUTBOUND_SEND_BLOCKED_TOTAL: &str = "subduction_outbound_send_blocked_total";

    // Subscriptions (live update fan-out).
    /// Number of sedimentrees with at least one subscriber.
    pub const SUBSCRIBED_SEDIMENTREES: &str = "subduction_subscribed_sedimentrees";
    /// Cumulative incremental updates pushed to subscribers.
    pub const SUBSCRIPTION_PUSHES_TOTAL: &str = "subduction_subscription_pushes_total";

    /// Current number of sedimentrees in storage.
    ///
    /// Refreshed from `load_all_sedimentree_ids`: an O(1) id-cache clone on the
    /// FS backend, or an O(trees) `trees` B+tree scan on redb (no per-tree
    /// contents read either way).
    pub const STORAGE_SEDIMENTREES: &str = "subduction_storage_sedimentrees";

    /// Cumulative resident-cache hits when resolving a sedimentree (the tree
    /// was already in the in-memory LRU; no storage hydration needed).
    pub const SEDIMENTREE_CACHE_HITS_TOTAL: &str = "subduction_sedimentree_cache_hits_total";
    /// Cumulative resident-cache misses (the tree had to be hydrated from
    /// durable storage). A high miss ratio means hydration — and the
    /// minimization it triggers — is on the hot path.
    pub const SEDIMENTREE_CACHE_MISSES_TOTAL: &str = "subduction_sedimentree_cache_misses_total";
    /// Sedimentrees currently resident in the in-memory LRU cache. Compare
    /// against the cache cap to see eviction pressure (which drives misses).
    pub const SEDIMENTREE_CACHE_RESIDENT: &str = "subduction_sedimentree_cache_resident";
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
    /// Storage operations currently executing on the blocking pool. A proxy for
    /// blocking-pool pressure (redb funnels every op through `spawn_blocking`):
    /// sustained high values mean storage ops are queueing for a thread.
    pub const STORAGE_BLOCKING_INFLIGHT: &str = "subduction_storage_blocking_inflight";

    // On-disk footprint (published from the metrics refresh loop).
    /// Free bytes on the filesystem holding the data directory.
    pub const DISK_FREE_BYTES: &str = "subduction_disk_free_bytes";
    /// Total bytes of the filesystem holding the data directory.
    pub const DISK_TOTAL_BYTES: &str = "subduction_disk_total_bytes";
    /// Size of the redb database file on disk.
    pub const REDB_FILE_BYTES: &str = "subduction_redb_file_bytes";
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

/// Record a completed handshake attempt, labeled by a bounded `outcome`
/// (`"ok"`, `"rejected"`, `"drift"`, `"decode"`, `"io"`, `"closed"`).
#[inline]
pub fn handshake_outcome(outcome: &'static str) {
    metrics::counter!(names::HANDSHAKE_TOTAL, "outcome" => outcome).increment(1);
}

/// Record a message being dispatched.
#[inline]
pub fn message_dispatched(message_type: &'static str) {
    metrics::counter!(names::MESSAGES_TOTAL, "type" => message_type).increment(1);
}

/// Record the duration of handling one inbound message of the given type.
///
/// The `message_type` is a bounded `&'static str` (the wire-message variant
/// name), keeping label cardinality fixed.
#[inline]
pub fn dispatch_duration(message_type: &'static str, duration_secs: f64) {
    metrics::histogram!(names::DISPATCH_DURATION_SECONDS, "type" => message_type)
        .record(duration_secs);
}

/// A scope guard that records message-handling duration on drop.
///
/// This ensures the duration is recorded even if the function returns early
/// via `?` or other control flow, capturing both success and failure latencies.
/// The recorded sample is labelled with the message type so per-type quantiles
/// don't blend cheap (e.g. `HeadsUpdate`) and heavy (e.g. `BatchSyncRequest`)
/// messages into a single misleading distribution.
#[derive(Debug)]
pub struct DispatchTimer {
    message_type: &'static str,
    start: std::time::Instant,
}

impl DispatchTimer {
    /// Create a new dispatch timer for `message_type`, starting the clock now.
    #[must_use]
    pub fn new(message_type: &'static str) -> Self {
        Self {
            message_type,
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for DispatchTimer {
    fn drop(&mut self) {
        dispatch_duration(self.message_type, self.start.elapsed().as_secs_f64());
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

/// Record a completed dispatch task, labeled by outcome.
///
/// `outcome` must be a bounded `&'static str` (`"ok"`, `"err"`, `"aborted"`).
#[inline]
pub fn dispatch_completed(outcome: &'static str) {
    metrics::counter!(names::DISPATCH_COMPLETED_TOTAL, "outcome" => outcome).increment(1);
}

/// Record that an inbound message had to wait for a per-peer dispatch permit
/// (the peer hit its concurrency cap), and how long the wait took. Called only
/// when the fast-path acquire fails, so the no-contention path stays free.
#[inline]
pub fn dispatch_permit_waited(wait_secs: f64) {
    metrics::counter!(names::DISPATCH_THROTTLED_TOTAL).increment(1);
    metrics::histogram!(names::DISPATCH_PERMIT_WAIT_SECONDS).record(wait_secs);
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

/// Record how long a correlated request stayed pending, from registration
/// until it was resolved by a response. Cancellations and timeouts are
/// excluded (they're counted in [`MUX_CANCELLED_TOTAL`](names::MUX_CANCELLED_TOTAL)).
#[inline]
pub fn mux_pending_duration(duration_secs: f64) {
    metrics::histogram!(names::MUX_PENDING_DURATION_SECONDS).record(duration_secs);
}

/// Record an outbound message's send-queue dwell (enqueue → drained by the
/// peer) and the queue depth observed at drain, labeled by `transport`.
///
/// `transport` must be a bounded `&'static str` (`"websocket"`, `"longpoll"`,
/// `"iroh"`) to keep label cardinality fixed.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn outbound_queue_dwell(transport: &'static str, dwell_secs: f64, depth: usize) {
    metrics::histogram!(names::OUTBOUND_QUEUE_DWELL_SECONDS, "transport" => transport)
        .record(dwell_secs);
    metrics::histogram!(names::OUTBOUND_QUEUE_DEPTH, "transport" => transport).record(depth as f64);
}

/// Record that an outbound send blocked on a full per-connection channel
/// (backpressure from a slow peer), labeled by `transport`.
#[inline]
pub fn outbound_send_blocked(transport: &'static str) {
    metrics::counter!(names::OUTBOUND_SEND_BLOCKED_TOTAL, "transport" => transport).increment(1);
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
/// Sourced from `load_all_sedimentree_ids` (O(1) id-cache clone on FS, O(trees)
/// `trees` B+tree scan on redb).
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_storage_sedimentrees(count: usize) {
    metrics::gauge!(names::STORAGE_SEDIMENTREES).set(count as f64);
}

/// Record a sedimentree resident-cache hit (resolved without hydration).
#[inline]
pub fn sedimentree_cache_hit() {
    metrics::counter!(names::SEDIMENTREE_CACHE_HITS_TOTAL).increment(1);
}

/// Record a sedimentree resident-cache miss (had to hydrate from storage).
#[inline]
pub fn sedimentree_cache_miss() {
    metrics::counter!(names::SEDIMENTREE_CACHE_MISSES_TOTAL).increment(1);
}

/// Publish the current number of sedimentrees resident in the in-memory cache.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_sedimentree_cache_resident(count: usize) {
    metrics::gauge!(names::SEDIMENTREE_CACHE_RESIDENT).set(count as f64);
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

/// Mark a storage operation entering the blocking pool.
#[inline]
pub fn storage_blocking_inc() {
    metrics::gauge!(names::STORAGE_BLOCKING_INFLIGHT).increment(1.0);
}

/// Mark a storage operation leaving the blocking pool.
#[inline]
pub fn storage_blocking_dec() {
    metrics::gauge!(names::STORAGE_BLOCKING_INFLIGHT).decrement(1.0);
}

/// Publish the on-disk footprint: filesystem free/total bytes for the data
/// directory and the redb database file size.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_disk_usage(free_bytes: u64, total_bytes: u64, redb_file_bytes: u64) {
    metrics::gauge!(names::DISK_FREE_BYTES).set(free_bytes as f64);
    metrics::gauge!(names::DISK_TOTAL_BYTES).set(total_bytes as f64);
    set_redb_file_bytes(redb_file_bytes);
}

/// Publish just the redb database file size, leaving the filesystem free/total
/// gauges untouched. For platforms without a portable `statvfs` (e.g. Windows),
/// where those gauges are skipped rather than reported as zero.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_redb_file_bytes(redb_file_bytes: u64) {
    metrics::gauge!(names::REDB_FILE_BYTES).set(redb_file_bytes as f64);
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
        names::HANDSHAKE_TOTAL,
        "Completed handshake attempts, labeled by `outcome` (ok/rejected/drift/decode/io/closed). Rejections never become connections, so `connections_total` can't show them."
    );
    metrics::describe_counter!(
        names::MESSAGES_TOTAL,
        "Total number of sync messages dispatched, labeled by `SyncMessage` variant."
    );
    metrics::describe_histogram!(
        names::DISPATCH_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of handling one inbound message (handler runtime), labeled by `type` (`SyncMessage` variant)."
    );
    metrics::describe_gauge!(
        names::DISPATCH_INFLIGHT,
        "Inbound messages currently being dispatched across all peers."
    );
    metrics::describe_counter!(
        names::DISPATCH_COMPLETED_TOTAL,
        "Completed dispatch tasks, labeled by `outcome` (ok/err/aborted)."
    );
    metrics::describe_counter!(
        names::DISPATCH_THROTTLED_TOTAL,
        "Times an inbound message waited for a per-peer dispatch permit (the peer hit its concurrency cap — the rate limiter engaging)."
    );
    metrics::describe_histogram!(
        names::DISPATCH_PERMIT_WAIT_SECONDS,
        metrics::Unit::Seconds,
        "Time spent waiting to acquire a per-peer dispatch permit (recorded only when the fast-path acquire fails)."
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
    metrics::describe_histogram!(
        names::MUX_PENDING_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Time a correlated request stays pending until resolved by a response (successful round-trips); the full request→response wait at the correlation layer. Cancellations/timeouts are excluded (counted in mux_cancelled_total)."
    );
    metrics::describe_histogram!(
        names::OUTBOUND_QUEUE_DWELL_SECONDS,
        metrics::Unit::Seconds,
        "Time an outbound message waits in the per-connection send queue before the peer grabs it, labeled by `transport`."
    );
    metrics::describe_histogram!(
        names::OUTBOUND_QUEUE_DEPTH,
        "Outbound send-queue depth sampled when a message is drained, labeled by `transport`."
    );
    metrics::describe_counter!(
        names::OUTBOUND_SEND_BLOCKED_TOTAL,
        "Times a send blocked on a full bounded outbound channel (slow-peer backpressure), labeled by `transport`."
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
        "Current number of sedimentrees in storage."
    );
    metrics::describe_counter!(
        names::SEDIMENTREE_CACHE_HITS_TOTAL,
        "Cumulative sedimentree resident-cache hits (resolved without hydration)."
    );
    metrics::describe_counter!(
        names::SEDIMENTREE_CACHE_MISSES_TOTAL,
        "Cumulative sedimentree resident-cache misses (hydrated from durable storage)."
    );
    metrics::describe_gauge!(
        names::SEDIMENTREE_CACHE_RESIDENT,
        "Sedimentrees currently resident in the in-memory LRU cache."
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
    metrics::describe_gauge!(
        names::STORAGE_BLOCKING_INFLIGHT,
        "Storage operations currently executing on the blocking pool (proxy for blocking-pool pressure; redb funnels every op through spawn_blocking)."
    );
    metrics::describe_gauge!(
        names::DISK_FREE_BYTES,
        "Free bytes on the filesystem holding the data directory."
    );
    metrics::describe_gauge!(
        names::DISK_TOTAL_BYTES,
        "Total bytes of the filesystem holding the data directory."
    );
    metrics::describe_gauge!(
        names::REDB_FILE_BYTES,
        "Size of the redb database file on disk."
    );
}
