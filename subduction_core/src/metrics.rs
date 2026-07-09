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

pub mod names;
pub mod requestor_tally;

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

/// Record a handshake's duration, labeled by `outcome` (`ok`/`err`).
#[inline]
pub fn handshake_duration(outcome: &'static str, secs: f64) {
    metrics::histogram!(names::HANDSHAKE_DURATION_SECONDS, "outcome" => outcome).record(secs);
}

/// Record one wire frame, labeled by `transport` and `direction`
/// (`sent`/`received`).
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn network_frame(transport: &'static str, direction: &'static str, bytes: usize) {
    metrics::histogram!(
        names::NETWORK_FRAME_BYTES,
        "transport" => transport,
        "direction" => direction,
    )
    .record(bytes as f64);
}

/// Publish the build identity once at startup: a constant `1` gauge whose
/// `version`/`git_sha` labels identify the running binary.
#[inline]
pub fn set_build_info(version: &'static str, git_sha: &'static str) {
    metrics::gauge!(
        names::BUILD_INFO,
        "version" => version,
        "git_sha" => git_sha,
    )
    .set(1.0);
}

/// Publish tokio runtime saturation gauges (values sampled by the host).
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn set_tokio_runtime(
    workers: usize,
    alive_tasks: usize,
    blocking_threads: usize,
    idle_blocking_threads: usize,
    blocking_queue_depth: usize,
    global_queue_depth: usize,
) {
    metrics::gauge!(names::TOKIO_WORKERS).set(workers as f64);
    metrics::gauge!(names::TOKIO_ALIVE_TASKS).set(alive_tasks as f64);
    metrics::gauge!(names::TOKIO_BLOCKING_THREADS).set(blocking_threads as f64);
    metrics::gauge!(names::TOKIO_IDLE_BLOCKING_THREADS).set(idle_blocking_threads as f64);
    metrics::gauge!(names::TOKIO_BLOCKING_QUEUE_DEPTH).set(blocking_queue_depth as f64);
    metrics::gauge!(names::TOKIO_GLOBAL_QUEUE_DEPTH).set(global_queue_depth as f64);
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

/// Record how long an inbound message dwelled in the shared message queue
/// before the listener picked it up for dispatch.
#[inline]
pub fn msg_queue_dwell(dwell_secs: f64) {
    metrics::histogram!(names::MSG_QUEUE_DWELL_SECONDS).record(dwell_secs);
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

/// Record a signature verification failure on a received sync item.
#[inline]
pub fn sync_verify_failure(kind: &'static str) {
    metrics::counter!(names::SYNC_VERIFY_FAILURES_TOTAL, "kind" => kind).increment(1);
}

/// Rank labels for [`set_top_requestors`] (bounded cardinality by
/// construction: exactly ten series, ever).
const TOP_REQUESTOR_RANKS: [&str; 10] = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];

/// Publish the per-window top-requestor gauges from counts sorted
/// descending. Ranks beyond `counts.len()` are zeroed so a quieter window
/// doesn't inherit stale values.
#[allow(clippy::cast_precision_loss)]
pub fn set_top_requestors(counts: &[u64]) {
    for (i, rank) in TOP_REQUESTOR_RANKS.iter().enumerate() {
        let value = counts.get(i).copied().unwrap_or(0);
        metrics::gauge!(names::TOP_REQUESTOR_REQUESTS, "rank" => *rank).set(value as f64);
    }
}

/// Record a failed send of requested data to a peer.
#[inline]
pub fn requested_data_send_failure() {
    metrics::counter!(names::REQUESTED_DATA_SEND_FAILURES_TOTAL).increment(1);
}

/// Record a `BatchSyncResponse` that found no pending caller.
#[inline]
pub fn late_response() {
    metrics::counter!(names::LATE_RESPONSES_TOTAL).increment(1);
}

/// Record a missed keepalive pong.
#[inline]
pub fn keepalive_pong_missed() {
    metrics::counter!(names::KEEPALIVE_PONGS_MISSED_TOTAL).increment(1);
}

/// Record a connection closed by keepalive (pong-miss threshold reached).
#[inline]
pub fn keepalive_close() {
    metrics::counter!(names::KEEPALIVE_CLOSES_TOTAL).increment(1);
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

/// Record incremental updates pushed to subscribers: `ok` delivered into
/// the outbound queue, `failed` rejected by a dead connection.
#[inline]
pub fn subscription_pushes(ok: u64, failed: u64) {
    if ok > 0 {
        metrics::counter!(names::SUBSCRIPTION_PUSHES_TOTAL, "outcome" => "ok").increment(ok);
    }
    if failed > 0 {
        metrics::counter!(names::SUBSCRIPTION_PUSHES_TOTAL, "outcome" => "failed")
            .increment(failed);
    }
}

/// Record an upstream subscription propagation attempt.
#[inline]
pub fn subscription_propagation(outcome: &'static str) {
    metrics::counter!(names::SUBSCRIPTION_PROPAGATIONS_TOTAL, "outcome" => outcome).increment(1);
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

/// Record how long a storage op waited on the blocking pool before executing.
#[inline]
pub fn storage_queue_wait(wait_secs: f64) {
    metrics::histogram!(names::STORAGE_BLOCKING_QUEUE_WAIT_SECONDS).record(wait_secs);
}

/// Record one redb group-commit drain that coalesced `batch_size` write jobs.
#[inline]
#[allow(clippy::cast_precision_loss)]
pub fn redb_drain(batch_size: usize) {
    metrics::counter!(names::REDB_DRAINS_TOTAL).increment(1);
    metrics::histogram!(names::REDB_DRAIN_BATCH_SIZE).record(batch_size as f64);
}

/// RAII guard for one sedimentree hydration: raises the in-flight gauge for
/// its lifetime and records the duration histogram on drop (any exit path,
/// including errors and cancellation).
#[derive(Debug)]
pub struct HydrationGuard {
    started: std::time::Instant,
}

impl HydrationGuard {
    /// Mark a hydration as started.
    #[must_use]
    pub fn new() -> Self {
        metrics::gauge!(names::HYDRATION_INFLIGHT).increment(1.0);
        Self {
            started: std::time::Instant::now(),
        }
    }
}

impl Default for HydrationGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for HydrationGuard {
    fn drop(&mut self) {
        metrics::gauge!(names::HYDRATION_INFLIGHT).decrement(1.0);
        metrics::histogram!(names::HYDRATION_DURATION_SECONDS)
            .record(self.started.elapsed().as_secs_f64());
    }
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
    metrics::describe_histogram!(
        names::HANDSHAKE_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Handshake duration from challenge receipt to accept/reject, labeled by `outcome` (ok/err)."
    );
    metrics::describe_histogram!(
        names::NETWORK_FRAME_BYTES,
        metrics::Unit::Bytes,
        "Wire frame sizes by `transport` and `direction` (sent/received); the _sum is total bandwidth."
    );
    metrics::describe_gauge!(
        names::BUILD_INFO,
        "Build identity: constant 1 with `version` and `git_sha` labels identifying the running binary."
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
    metrics::describe_histogram!(
        names::MSG_QUEUE_DWELL_SECONDS,
        metrics::Unit::Seconds,
        "Time an inbound message spent queued between the connection reader and the listener spawning its dispatch task."
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
    metrics::describe_counter!(
        names::SYNC_VERIFY_FAILURES_TOTAL,
        "Signature verification failures on received sync items, labeled by `kind` (commit/fragment)."
    );
    metrics::describe_gauge!(
        names::TOP_REQUESTOR_REQUESTS,
        "Batch-sync requests in the last refresh window from the rank-N most active peer (bounded rank label; peer ids are in the paired 'top requestors' log line)."
    );
    metrics::describe_counter!(
        names::REQUESTED_DATA_SEND_FAILURES_TOTAL,
        "Failed sends of requested data to a peer (connection closed or broke mid-push); wasted work the requestor will re-request."
    );
    metrics::describe_counter!(
        names::LATE_RESPONSES_TOTAL,
        "BatchSyncResponses that arrived after their pending caller was gone (timed out or cancelled); dead on arrival."
    );
    metrics::describe_counter!(
        names::KEEPALIVE_PONGS_MISSED_TOTAL,
        "Keepalive pongs missed (one per miss, before the close threshold)."
    );
    metrics::describe_counter!(
        names::KEEPALIVE_CLOSES_TOTAL,
        "Connections closed by keepalive after the pong-miss threshold (the server reaping an unresponsive peer)."
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
        "Incremental updates pushed to subscribers, labeled by `outcome` (ok/failed); failed pushes are sends into dead connections."
    );
    metrics::describe_counter!(
        names::SUBSCRIPTION_PROPAGATIONS_TOTAL,
        "Upstream subscription propagation attempts, labeled by `outcome` (established/rejected/failed); rejected/failed roll the claim back and retry on the next inbound subscribe."
    );
    metrics::describe_gauge!(
        names::TOKIO_WORKERS,
        "Async worker threads in the tokio runtime."
    );
    metrics::describe_gauge!(
        names::TOKIO_ALIVE_TASKS,
        "Tokio tasks currently alive (spawned and not yet completed)."
    );
    metrics::describe_gauge!(
        names::TOKIO_BLOCKING_THREADS,
        "Threads in the tokio blocking pool (busy + idle)."
    );
    metrics::describe_gauge!(
        names::TOKIO_IDLE_BLOCKING_THREADS,
        "Idle threads in the tokio blocking pool."
    );
    metrics::describe_gauge!(
        names::TOKIO_BLOCKING_QUEUE_DEPTH,
        "Tasks queued for the tokio blocking pool but not yet running (nonzero: the pool is at its thread cap)."
    );
    metrics::describe_gauge!(
        names::TOKIO_GLOBAL_QUEUE_DEPTH,
        "Tasks in the tokio global (injection) queue awaiting a worker."
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
        "Duration of individual storage operations, labeled by `operation`. Measures enqueue to completion, so it includes blocking-pool queue wait; see subduction_storage_blocking_queue_wait_seconds for the wait alone."
    );
    metrics::describe_counter!(
        names::STORAGE_OPERATION_ERRORS_TOTAL,
        "Cumulative storage operation errors, labeled by `operation`."
    );
    metrics::describe_gauge!(
        names::STORAGE_BLOCKING_INFLIGHT,
        "Storage operations currently executing on the blocking pool (proxy for blocking-pool pressure; redb funnels every op through spawn_blocking)."
    );
    metrics::describe_histogram!(
        names::STORAGE_BLOCKING_QUEUE_WAIT_SECONDS,
        metrics::Unit::Seconds,
        "Time a storage op spent queued on the blocking pool before executing (pool saturation, split out of the total operation duration)."
    );
    metrics::describe_histogram!(
        names::REDB_DRAIN_BATCH_SIZE,
        "Write jobs coalesced into one redb group-commit drain (a distribution stuck at 1 under write load means coalescing isn't engaging)."
    );
    metrics::describe_counter!(
        names::REDB_DRAINS_TOTAL,
        "Total redb group-commit drains (normally one fsync'd transaction each; a failed batch retries per-job)."
    );
    metrics::describe_gauge!(
        names::HYDRATION_INFLIGHT,
        "Sedimentree hydrations (cache-miss rebuilds from storage) currently in flight."
    );
    metrics::describe_histogram!(
        names::HYDRATION_DURATION_SECONDS,
        metrics::Unit::Seconds,
        "Duration of a sedimentree hydration: metadata loads plus rebuild and minimize."
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
