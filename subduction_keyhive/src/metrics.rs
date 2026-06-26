//! Prometheus metrics for the keyhive sync protocol.
//!
//! Only compiled when the `metrics` feature is enabled. Emits to the global
//! recorder the host binary installs (e.g. `subduction_cli`). Kept
//! self-contained in this crate so a subduction deployment that does not run
//! the keyhive handler never registers these series.

/// Total keyhive static events submitted for ingestion since process start.
///
/// Counts every event handed to keyhive, including any left pending on missing
/// dependencies and not yet applied to the op-graph. A persistent gap between
/// this and [`EVENTS_APPLIED_TOTAL`] means events are arriving whose
/// dependencies are not resolving.
pub const EVENTS_INGESTED_TOTAL: &str = "subduction_keyhive_events_ingested_total";

/// Total keyhive static events applied to the op-graph since process start.
///
/// The subset of [`EVENTS_INGESTED_TOTAL`] that keyhive accepted on the first
/// pass (not left pending on missing dependencies). A scraper's rate over this
/// counter is the effective ingestion throughput (events/sec).
pub const EVENTS_APPLIED_TOTAL: &str = "subduction_keyhive_events_applied_total";

/// Sync responses served via the per-peer diff path. High counts with large
/// [`SERVE_FOUND_EVENTS`] mean peers are not converging — the per-peer working
/// set is the memory sink.
pub const PER_PEER_SERVES_TOTAL: &str = "subduction_keyhive_per_peer_serves_total";

/// Distribution of the number of events sent in a per-peer `SyncResponse`
/// (`found` length). A converged peer's diff should be small (a handful of new
/// ops); a large tail here means peers are still missing most of the set.
pub const SERVE_FOUND_EVENTS: &str = "subduction_keyhive_serve_found_events";

/// Current size of the keyhive op-graph in distinct ops, as keyhive itself
/// reports via `total_ops`.
///
/// This is the real graph size. Unlike [`EVENTS_APPLIED_TOTAL`] — a cumulative
/// counter that re-counts the same op every time it is re-applied during a
/// client sync, and so can run orders of magnitude above the true graph — this
/// gauge reflects the actual number of distinct ops held. Watch this gauge, not
/// the `*_total` counters, to judge whether the graph is genuinely growing.
pub const OP_GRAPH_OPS: &str = "subduction_keyhive_op_graph_ops";

/// Number of keyhive events reachable to the public agent: the set a fresh
/// client downloads on connect (the cold-join cost). Grows with the number of
/// publicly-shared docs.
pub const PUBLIC_EVENTS: &str = "subduction_keyhive_public_events";

/// Number of distinct keyhive agents the server tracks (peers/groups/docs),
/// excluding the public agent which is counted separately.
pub const TRACKED_AGENTS: &str = "subduction_keyhive_tracked_agents";

/// Number of distinct keyhive events the server has serialized and retained in
/// its byte store (the events available to serve).
pub const KNOWN_EVENTS: &str = "subduction_keyhive_known_events";

/// Record `submitted` events handed to keyhive, of which `applied` were applied
/// to the op-graph (the rest left pending on missing dependencies).
#[inline]
pub fn events_ingested(submitted: u64, applied: u64) {
    metrics::counter!(EVENTS_INGESTED_TOTAL).increment(submitted);
    metrics::counter!(EVENTS_APPLIED_TOTAL).increment(applied);
}

/// Record one per-peer diff serve carrying `found_len` events.
#[inline]
pub fn per_peer_serve(found_len: u64) {
    metrics::counter!(PER_PEER_SERVES_TOTAL).increment(1);
    // The histogram API takes f64; a serve's event count is small, so the cast
    // is exact in practice.
    #[allow(clippy::cast_precision_loss)]
    metrics::histogram!(SERVE_FOUND_EVENTS).record(found_len as f64);
}

/// Record the current keyhive op-graph size and cache contents after a refresh.
///
/// `ops` is keyhive's `total_ops` (distinct graph size); the rest are the
/// cache's current set sizes. Counts are small relative to f64's exact-integer
/// range, so the gauge casts are exact in practice.
#[inline]
pub fn op_graph(ops: u64, public_events: u64, tracked_agents: u64, known_events: u64) {
    #[allow(clippy::cast_precision_loss)]
    {
        metrics::gauge!(OP_GRAPH_OPS).set(ops as f64);
        metrics::gauge!(PUBLIC_EVENTS).set(public_events as f64);
        metrics::gauge!(TRACKED_AGENTS).set(tracked_agents as f64);
        metrics::gauge!(KNOWN_EVENTS).set(known_events as f64);
    }
}

/// Register HELP/TYPE metadata for every metric this crate emits.
///
/// Call once at recorder init, after the recorder is installed.
pub fn describe_all() {
    metrics::describe_counter!(
        EVENTS_INGESTED_TOTAL,
        "Total keyhive static events submitted for ingestion since process start."
    );
    metrics::describe_counter!(
        EVENTS_APPLIED_TOTAL,
        "Total keyhive static events applied to the op-graph since process start."
    );
    metrics::describe_counter!(
        PER_PEER_SERVES_TOTAL,
        "Sync responses served via the per-peer diff path."
    );
    metrics::describe_histogram!(
        SERVE_FOUND_EVENTS,
        "Number of events sent in a per-peer SyncResponse (found length)."
    );
    metrics::describe_gauge!(
        OP_GRAPH_OPS,
        "Current keyhive op-graph size in distinct ops (keyhive total_ops). The real graph size, unlike the cumulative *_total apply counters which re-count re-applied ops."
    );
    metrics::describe_gauge!(
        PUBLIC_EVENTS,
        "Keyhive events reachable to the public agent (the set a fresh client downloads on connect)."
    );
    metrics::describe_gauge!(
        TRACKED_AGENTS,
        "Distinct keyhive agents tracked by the server, excluding the public agent."
    );
    metrics::describe_gauge!(
        KNOWN_EVENTS,
        "Distinct keyhive events serialized and retained in the server's byte store."
    );
}
