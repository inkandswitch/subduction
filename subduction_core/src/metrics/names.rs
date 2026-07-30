//! Metric names used throughout the application.

/// Build identity: constant `1` carrying `version` and `git_sha` labels
/// (one series per running binary), so dashboards can correlate behavior
/// changes with deploys.
pub const BUILD_INFO: &str = "subduction_build_info";
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
/// Accept-side WebSocket handshake duration, labeled by `outcome`
/// (`ok`/`err`). Spans from upgrade completion to accept/reject, so it
/// includes waiting for the client's challenge (client think-time and
/// network latency, not just server work). Other transports are not yet
/// instrumented.
pub const HANDSHAKE_DURATION_SECONDS: &str = "subduction_handshake_duration_seconds";
/// Wire frame sizes, labeled by `transport` (`websocket`/`longpoll`; iroh
/// not yet instrumented) and `direction` (`sent`/`received`). The `_sum`
/// is total bandwidth; the buckets resolve frames approaching the
/// message-size caps.
pub const NETWORK_FRAME_BYTES: &str = "subduction_network_frame_bytes";
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
/// Time an inbound message spent in the shared message queue between the
/// connection reader (permit already held) and the listener spawning its
/// dispatch task. Growth means the listen loop itself is the bottleneck.
pub const MSG_QUEUE_DWELL_SECONDS: &str = "subduction_msg_queue_dwell_seconds";
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
/// Signature verification failures on received sync items, labeled by
/// `kind` (`commit`/`fragment`). A sustained rate means a peer keeps
/// retrying data that can never be accepted.
pub const SYNC_VERIFY_FAILURES_TOTAL: &str = "subduction_sync_verify_failures_total";
/// Batch-sync requests in the last refresh window from the rank-N most
/// active peer (`rank` = `"1"`..`"10"`). Shows request skew — is one peer
/// dominating? — with the actual ids in the paired "top requestors" log
/// line (see [`requestor_tally`](crate::metrics::requestor_tally)).
pub const TOP_REQUESTOR_REQUESTS: &str = "subduction_top_requestor_requests";
/// Batch-sync requests in the last refresh window across *all* tracked
/// requestors — the honest denominator for rank-share comparisons (the
/// rank gauges alone cover only the top ten).
pub const REQUESTOR_WINDOW_REQUESTS: &str = "subduction_requestor_window_requests";
/// Failed deliveries of requested data to a peer: the connection closed
/// or broke mid-push, or reading the data for the send failed. Policy
/// rejections are excluded. Wasted work the requestor will re-request.
pub const REQUESTED_DATA_SEND_FAILURES_TOTAL: &str =
    "subduction_requested_data_send_failures_total";
/// `BatchSyncResponse`s that arrived after their pending caller was gone
/// (timed out or cancelled) — responses dead on arrival. Pairs with
/// outbound queue dwell: dwell above the sync timeout manufactures these.
pub const LATE_RESPONSES_TOTAL: &str = "subduction_late_responses_total";
/// Keepalive pongs missed (one per miss, before the close threshold).
/// Also counts cycles whose ping was dropped because the outbound queue
/// was full — a wedged connection misses without any ping reaching the
/// wire. Cycles forgiven by the write-progress gate do not count.
pub const KEEPALIVE_PONGS_MISSED_TOTAL: &str = "subduction_keepalive_pongs_missed_total";
/// Connections closed by keepalive — the server reaping an unresponsive
/// peer, either via the pong-miss threshold or via the
/// progress-but-no-pong hard ceiling.
pub const KEEPALIVE_CLOSES_TOTAL: &str = "subduction_keepalive_closes_total";
/// Keepalive cycles in which the ping could not be delivered at all: the
/// outbound queue was full at every attempt AND the sender task completed
/// no frame during the pong window (zero drainage — no injection point).
/// Undelivered pings accrue no liveness evidence against the peer, so
/// this counter is the honest measure of how often congestion blinds the
/// end-to-end (pong-based) check. Sustained nonzero rates mean liveness
/// is riding on the write-progress gate alone for those connections.
pub const KEEPALIVE_PINGS_UNDELIVERED_TOTAL: &str = "subduction_keepalive_pings_undelivered_total";

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
/// the peer grabs it, labeled by `transport` (`websocket`/`longpoll`; the
/// iroh transport is not yet instrumented).
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
/// Incremental updates pushed to subscribers, labeled by `outcome`
/// (`ok`/`failed`). Failed pushes are sends into dead connections — the
/// push-path twin of `subduction_requested_data_send_failures_total`.
pub const SUBSCRIPTION_PUSHES_TOTAL: &str = "subduction_subscription_pushes_total";
/// Upstream subscription propagation attempts, labeled by `outcome`
/// (`established`/`rejected`/`failed`). Rejected/failed attempts roll their
/// claim back and retry on the next inbound subscribe — a sustained rate is
/// the solicitation tax on peers that don't hold the tree.
pub const SUBSCRIPTION_PROPAGATIONS_TOTAL: &str = "subduction_subscription_propagations_total";

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
/// Time a storage op spent queued on the blocking pool before its closure
/// started executing. Splits pool saturation out of
/// [`STORAGE_OPERATION_DURATION_SECONDS`], which measures enqueue to
/// completion (queue wait *plus* execution).
pub const STORAGE_BLOCKING_QUEUE_WAIT_SECONDS: &str =
    "subduction_storage_blocking_queue_wait_seconds";
/// Jobs coalesced into one redb group-commit drain. A distribution stuck
/// at 1 under write load means coalescing isn't engaging.
pub const REDB_DRAIN_BATCH_SIZE: &str = "subduction_redb_drain_batch_size";
/// Total redb group-commit drains.
pub const REDB_DRAINS_TOTAL: &str = "subduction_redb_drains_total";
/// Full-tree metadata loads from storage (cache-miss reads and
/// write-path loads) currently in flight. Sustained high values mean a
/// hydration storm.
pub const HYDRATION_INFLIGHT: &str = "subduction_hydration_inflight";
/// Duration of a completed full-tree metadata load (cache-miss reads
/// and write-path loads); cancelled and tree-not-found probes are not
/// sampled.
pub const HYDRATION_DURATION_SECONDS: &str = "subduction_hydration_duration_seconds";

// Tokio runtime saturation (published by the host process's refresh loop;
// the unstable counters require `--cfg tokio_unstable`).
/// Async worker threads in the runtime.
pub const TOKIO_WORKERS: &str = "subduction_tokio_workers";
/// Tasks currently alive (spawned and not yet completed).
pub const TOKIO_ALIVE_TASKS: &str = "subduction_tokio_alive_tasks";
/// Threads in the blocking pool (busy + idle).
pub const TOKIO_BLOCKING_THREADS: &str = "subduction_tokio_blocking_threads";
/// Idle threads in the blocking pool.
pub const TOKIO_IDLE_BLOCKING_THREADS: &str = "subduction_tokio_idle_blocking_threads";
/// Tasks queued for the blocking pool but not yet running — nonzero means
/// the pool is at its thread cap and storage ops are queueing.
pub const TOKIO_BLOCKING_QUEUE_DEPTH: &str = "subduction_tokio_blocking_queue_depth";
/// Tasks in the runtime's global (injection) queue awaiting a worker.
pub const TOKIO_GLOBAL_QUEUE_DEPTH: &str = "subduction_tokio_global_queue_depth";

// On-disk footprint (published from the metrics refresh loop).
/// Free bytes on the filesystem holding the data directory.
pub const DISK_FREE_BYTES: &str = "subduction_disk_free_bytes";
/// Total bytes of the filesystem holding the data directory.
pub const DISK_TOTAL_BYTES: &str = "subduction_disk_total_bytes";
/// Size of the redb database file on disk.
pub const REDB_FILE_BYTES: &str = "subduction_redb_file_bytes";
