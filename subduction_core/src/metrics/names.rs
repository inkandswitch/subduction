//! Metric names used throughout the application.

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
/// active peer (`rank` = `"1"`..`"10"`, a bounded label). Shows request
/// skew — is one peer dominating? — without peer-id labels; the ids are
/// in the paired "top requestors" log line.
pub const TOP_REQUESTOR_REQUESTS: &str = "subduction_top_requestor_requests";
/// Failed sends of requested data to a peer (the connection closed or
/// broke mid-push) — wasted work the requestor will re-request.
pub const REQUESTED_DATA_SEND_FAILURES_TOTAL: &str =
    "subduction_requested_data_send_failures_total";
/// `BatchSyncResponse`s that arrived after their pending caller was gone
/// (timed out or cancelled) — responses dead on arrival. Pairs with
/// outbound queue dwell: dwell above the sync timeout manufactures these.
pub const LATE_RESPONSES_TOTAL: &str = "subduction_late_responses_total";
/// Keepalive pongs missed (one per miss, before the close threshold).
pub const KEEPALIVE_PONGS_MISSED_TOTAL: &str = "subduction_keepalive_pongs_missed_total";
/// Connections closed by keepalive (pong-miss threshold reached) — the
/// server reaping an unresponsive peer.
pub const KEEPALIVE_CLOSES_TOTAL: &str = "subduction_keepalive_closes_total";

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
/// Sedimentree hydrations (cache-miss rebuilds from storage) currently in
/// flight. Sustained high values mean a hydration storm.
pub const HYDRATION_INFLIGHT: &str = "subduction_hydration_inflight";
/// Duration of a sedimentree hydration: metadata loads from storage plus
/// rebuild and minimize.
pub const HYDRATION_DURATION_SECONDS: &str = "subduction_hydration_duration_seconds";

// On-disk footprint (published from the metrics refresh loop).
/// Free bytes on the filesystem holding the data directory.
pub const DISK_FREE_BYTES: &str = "subduction_disk_free_bytes";
/// Total bytes of the filesystem holding the data directory.
pub const DISK_TOTAL_BYTES: &str = "subduction_disk_total_bytes";
/// Size of the redb database file on disk.
pub const REDB_FILE_BYTES: &str = "subduction_redb_file_bytes";
