//! Prometheus metrics server for Subduction.
//!
//! Metrics are recorded by `subduction_core` when the `metrics` feature is enabled.
//! This module provides the HTTP server that exposes metrics to Prometheus.

use axum::{Router, routing::get};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use tokio::net::TcpListener;

// Histogram buckets must be configured, or `metrics-exporter-prometheus`
// renders histograms as summaries (quantile series, no `_bucket`), which the
// `histogram_quantile()` dashboard panels cannot use.
//
// Buckets are per-metric because the histograms live on very different time
// scales. A single coarse set boundary-snaps fast operations: e.g. a 700µs
// storage write with a lowest bucket of 0.5ms reports its p95 at the 1ms
// edge, making it look slower than it is at low sample counts.

/// Fine buckets (seconds) for sub-millisecond/low-millisecond operations:
/// per-message dispatch and individual storage operations. Resolves down to
/// 50µs so fast ops don't collapse into one bucket, and extends to 60s so a
/// stalled dispatch or write isn't clamped to a saturated top bucket
/// (which reads as "p99 = ceiling").
const FINE_BUCKETS_SECONDS: &[f64] = &[
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 1.5, 2.5, 3.5, 5.0, 10.0, 30.0, 60.0,
];

/// Coarse buckets (seconds) for whole-round operations measured in
/// milliseconds-to-seconds: foreground sync rounds. Sub-ms resolution would be
/// wasted series here. Extends to 300s so rounds queued behind a congested
/// outbound path stay observable.
const COARSE_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

/// Buckets (seconds) for outbound send-queue dwell — sub-millisecond
/// (WebSocket) to minutes (congested consumers), so both transports resolve.
const DWELL_BUCKETS_SECONDS: &[f64] = &[
    0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
    300.0,
];

/// Buckets (message count) for outbound send-queue depth sampled at drain, up
/// to the per-connection channel capacity (1024). The leading `0` matters: an
/// idle queue drains at depth 0, and without an `le="0"` bucket those samples
/// fall into `le="1"`, so `histogram_quantile` interpolates to ~0.95 for a
/// quiet queue instead of 0.
const DEPTH_BUCKETS: &[f64] = &[
    0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
];

/// Buckets (bytes) for wire frame sizes: ~2-octave steps from small sync
/// messages (64 B), then denser boundaries around the message-size caps —
/// 50 MiB (`DEFAULT_MAX_MESSAGE_SIZE`) and 100 MiB (the production
/// `maxMessageSize`) — so a distribution climbing toward a cap is
/// distinguishable from mid-size traffic. The boundary *at* each cap is the
/// alerting edge that matters.
#[allow(clippy::cast_precision_loss)]
const FRAME_BYTES_BUCKETS: &[f64] = &[
    64.0,
    256.0,
    1024.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    33_554_432.0,  // 32 MiB
    50_331_648.0,  // 48 MiB
    52_428_800.0,  // 50 MiB: DEFAULT_MAX_MESSAGE_SIZE
    67_108_864.0,  // 64 MiB
    104_857_600.0, // 100 MiB: production maxMessageSize
    134_217_728.0, // 128 MiB headroom
];

/// Initialize the metrics recorder and return a handle for the HTTP endpoint.
///
/// This must be called once at startup before any metrics are recorded.
/// Configures histogram buckets (so latency panels work), then registers
/// HELP/TYPE descriptions for all emitted metrics. There is no idle-series
/// eviction; see the builder comment for why.
///
/// # Panics
///
/// Panics if the recorder cannot be installed or buckets are invalid.
#[must_use]
pub fn init_metrics() -> PrometheusHandle {
    use subduction_core::metrics::names;

    // `Matcher::Full` overrides `Matcher::Suffix` for the same metric: the
    // exporter sorts overrides by `Matcher` `Ord` (Full < Prefix < Suffix) and
    // takes the first match. So the fine `Full(...)` rules below win over the
    // coarse `Suffix("_duration_seconds")` fallback for those specific metrics,
    // while whole-round sync durations fall through to the coarse set.
    #[allow(clippy::expect_used)]
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(names::STORAGE_OPERATION_DURATION_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        .set_buckets_for_metric(
            Matcher::Full(names::DISPATCH_DURATION_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        // Outbound-queue dwell ends in `_dwell_seconds` (not `_duration_seconds`)
        // and depth is a count, so neither matches the coarse suffix fallback —
        // they need explicit bucket sets or they'd render as summaries.
        .set_buckets_for_metric(
            Matcher::Full(names::OUTBOUND_QUEUE_DWELL_SECONDS.to_owned()),
            DWELL_BUCKETS_SECONDS,
        )
        .expect("dwell buckets are non-empty and sorted")
        .set_buckets_for_metric(
            Matcher::Full(names::OUTBOUND_QUEUE_DEPTH.to_owned()),
            DEPTH_BUCKETS,
        )
        .expect("depth buckets are non-empty and sorted")
        // Permit-wait is a contention wait (sub-ms..seconds); `_wait_seconds`
        // doesn't match the coarse suffix, so set it explicitly. (Mux
        // pending-duration ends in `_duration_seconds` → coarse via the suffix.)
        .set_buckets_for_metric(
            Matcher::Full(names::DISPATCH_PERMIT_WAIT_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        // Msg-queue dwell is µs on a healthy listener and seconds when the
        // listen loop is the bottleneck; fine buckets resolve both regimes.
        .set_buckets_for_metric(
            Matcher::Full(names::MSG_QUEUE_DWELL_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        // Blocking-pool queue wait: µs when the pool is healthy, seconds
        // when saturated; fine buckets resolve both regimes.
        .set_buckets_for_metric(
            Matcher::Full(names::STORAGE_BLOCKING_QUEUE_WAIT_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        // Drain batch size is a count bounded by the writer queue capacity
        // (1024), same shape as outbound queue depth.
        .set_buckets_for_metric(
            Matcher::Full(names::REDB_DRAIN_BATCH_SIZE.to_owned()),
            DEPTH_BUCKETS,
        )
        .expect("depth buckets are non-empty and sorted")
        // Frame sizes span 64 B sync messages to 128 MiB blob responses;
        // `_bytes` misses the duration-suffix fallback, so set explicitly.
        .set_buckets_for_metric(
            Matcher::Full(names::NETWORK_FRAME_BYTES.to_owned()),
            FRAME_BYTES_BUCKETS,
        )
        .expect("frame-bytes buckets are non-empty and sorted")
        // Hydrations of small page-cached trees are sub-millisecond, which
        // the coarse suffix fallback (1ms floor) would boundary-snap.
        .set_buckets_for_metric(
            Matcher::Full(names::HYDRATION_DURATION_SECONDS.to_owned()),
            FINE_BUCKETS_SECONDS,
        )
        .expect("fine buckets are non-empty and sorted")
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".to_owned()),
            COARSE_BUCKETS_SECONDS,
        )
        .expect("coarse buckets are non-empty and sorted")
        // No idle eviction: histogram counts are cumulative, so evicting an
        // idle series silently resets it and breaks `rate()`/`increase()`.
        // Label cardinality is bounded (`&'static str` values only), so
        // unbounded-series growth is not a risk.
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    subduction_core::metrics::describe_all();
    handle
}

/// Start the metrics HTTP server on the given address.
///
/// This spawns a background task that serves the `/metrics` endpoint.
///
/// # Errors
///
/// Returns an error if the server fails to bind to the address.
pub async fn start_metrics_server(addr: SocketAddr, handle: PrometheusHandle) -> eyre::Result<()> {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    );

    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %addr, "Metrics server listening");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "Metrics server error");
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression guard for the dead-latency-panel bug: a `*_duration_seconds`
    /// histogram must render as Prometheus `_bucket` series (not a summary), and
    /// fast-vs-slow metrics must get their respective fine/coarse bucket sets.
    /// Without `set_buckets_for_metric`, the exporter emits quantile summaries
    /// and `histogram_quantile(rate(..._bucket))` dashboard panels show no data.
    //
    // One long test by necessity: `install_recorder` installs a process-global
    // recorder, so all render assertions must share a single test.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn duration_histograms_render_as_buckets() {
        let handle = init_metrics();
        // Record through the installed recorder.
        subduction_core::metrics::dispatch_duration("LooseCommit", 0.000_3);
        subduction_core::metrics::storage_operation_duration("save_loose_commit", 0.000_8);
        subduction_core::metrics::sync_duration(2.0);
        subduction_core::metrics::outbound_queue_dwell("longpoll", 5.0, 3);
        subduction_core::metrics::dispatch_permit_waited(0.001);
        subduction_core::metrics::msg_queue_dwell(0.000_2);
        subduction_core::metrics::mux_pending_duration(0.5);

        // Cache counters: 2 hits + 1 miss must render with exactly those totals
        // (guards against the hit/miss being mis-wired or a miss double-counted).
        subduction_core::metrics::sedimentree_cache_hit();
        subduction_core::metrics::sedimentree_cache_hit();
        subduction_core::metrics::sedimentree_cache_miss();
        subduction_core::metrics::set_sedimentree_cache_resident(7);
        subduction_core::metrics::storage_blocking_queue_wait(0.001);
        subduction_core::metrics::redb_drain(3);
        subduction_core::metrics::HydrationGuard::new().complete();
        subduction_core::metrics::sync_verify_failure("commit");
        subduction_core::metrics::requested_data_send_failure();
        subduction_core::metrics::late_response();
        subduction_core::metrics::keepalive_pong_missed();
        subduction_core::metrics::keepalive_close();
        subduction_core::metrics::set_top_requestors(&[5, 3], 12);
        subduction_core::metrics::network_frame("websocket", "sent", 300);
        subduction_core::metrics::handshake_duration("ok", 0.05);
        subduction_core::metrics::subscription_pushes(2, 1);
        subduction_core::metrics::subscription_propagation("established");
        subduction_core::metrics::set_build_info("0.0.0-test", "deadbeef");

        let rendered = handle.render();

        // All three render as histograms (have `_bucket` series).
        for series in [
            "subduction_dispatch_duration_seconds_bucket",
            "subduction_storage_operation_duration_seconds_bucket",
            "subduction_sync_duration_seconds_bucket",
        ] {
            assert!(
                rendered.contains(series),
                "{series} should emit _bucket series:\n{rendered}"
            );
        }

        // Dispatch latency carries the bounded `type` label so per-type
        // quantiles don't blend cheap and heavy messages.
        assert!(
            rendered.contains("subduction_dispatch_duration_seconds_bucket")
                && rendered.contains("type=\"LooseCommit\""),
            "dispatch histogram should carry the `type` label:\n{rendered}"
        );

        // Fine metrics (storage op, dispatch) resolve sub-millisecond: the 50µs
        // boundary only exists in FINE_BUCKETS_SECONDS.
        let storage_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_storage_operation_duration_seconds_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            storage_lines.contains("le=\"0.00005\""),
            "storage op histogram should use the fine 50us bucket:\n{storage_lines}"
        );

        // The fine set extends past 1s so a dispatch or storage write that
        // stalls for seconds isn't clamped to a low top bucket — the tail
        // must stay observable up to 60s.
        assert!(
            storage_lines.contains("le=\"2.5\"") && storage_lines.contains("le=\"60\""),
            "storage op histogram should carry the >1s tail buckets:\n{storage_lines}"
        );

        // The coarse-only 300s boundary must NOT appear on the fine storage
        // metric (proving the per-metric override took effect).
        assert!(
            !storage_lines.contains("le=\"300\""),
            "storage op histogram should NOT carry the coarse 300s bucket:\n{storage_lines}"
        );

        // Sync rounds use the coarse set: the 300s boundary exists there.
        let sync_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_sync_duration_seconds_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            sync_lines.contains("le=\"300\""),
            "sync duration histogram should use the coarse 300s bucket:\n{sync_lines}"
        );

        // Outbound dwell/depth render as histograms (not summaries), carry the
        // `transport` label, and use their own bucket sets (depth up to the
        // 1024 channel capacity) — so the per-connection-type panels work.
        assert!(
            rendered.contains("subduction_outbound_queue_dwell_seconds_bucket")
                && rendered.contains("transport=\"longpoll\""),
            "dwell histogram should render with the transport label:\n{rendered}"
        );
        let depth_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_outbound_queue_depth_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            depth_lines.contains("le=\"1024\""),
            "depth histogram should render as buckets up to the 1024 capacity:\n{depth_lines}"
        );
        // The `le="0"` boundary makes an idle queue's depth p95 resolve to 0
        // rather than interpolating across [0, 1] up to ~0.95.
        assert!(
            depth_lines.contains("le=\"0\""),
            "depth histogram should carry the le=0 bucket so empty queues read 0:\n{depth_lines}"
        );

        // Permit-wait (fine, `_wait_seconds`) and mux pending-duration (coarse,
        // `_duration_seconds` suffix) must also render as `_bucket` series.
        assert!(
            rendered.contains("subduction_dispatch_permit_wait_seconds_bucket"),
            "permit-wait histogram should render as buckets:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_msg_queue_dwell_seconds_bucket"),
            "msg-queue dwell histogram should render as buckets:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_mux_pending_duration_seconds_bucket"),
            "mux pending-duration histogram should render as buckets:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_storage_blocking_queue_wait_seconds_bucket"),
            "storage queue-wait histogram should render as buckets:\n{rendered}"
        );

        // Drain batch size uses the depth (count) bucket set, not a summary.
        let drain_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_redb_drain_batch_size_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            drain_lines.contains("le=\"1024\""),
            "drain batch-size histogram should render count buckets up to the queue capacity:\n{drain_lines}"
        );
        // Presence only: other tests in this binary (e.g. `migrate`) drive
        // real `RedbStorage` writes into the same process-global recorder,
        // so an exact total would be a parallel-test flake.
        assert!(
            rendered.contains("subduction_redb_drains_total"),
            "drain counter should render:\n{rendered}"
        );

        // The hydration guard round-trips: `complete()` records one duration
        // sample, and the in-flight gauge returns to 0 once the guard drops.
        assert!(
            rendered.contains("subduction_hydration_duration_seconds_bucket"),
            "hydration duration histogram should render as buckets:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_hydration_inflight 0"),
            "hydration in-flight gauge should return to 0 after the guard drops:\n{rendered}"
        );

        // Incident-signal counters render with their labels.
        assert!(
            rendered.contains("subduction_sync_verify_failures_total{kind=\"commit\"} 1"),
            "verify-failure counter should render with kind label:\n{rendered}"
        );
        for counter in [
            "subduction_requested_data_send_failures_total 1",
            "subduction_late_responses_total 1",
            "subduction_keepalive_pongs_missed_total 1",
            "subduction_keepalive_closes_total 1",
        ] {
            assert!(
                rendered.contains(counter),
                "{counter} should render:\n{rendered}"
            );
        }

        // Top-requestor gauges: ranked counts land on their rank labels and
        // unfilled ranks are zeroed (no stale values from a busier window).
        assert!(
            rendered.contains("subduction_top_requestor_requests{rank=\"1\"} 5")
                && rendered.contains("subduction_top_requestor_requests{rank=\"2\"} 3")
                && rendered.contains("subduction_top_requestor_requests{rank=\"3\"} 0"),
            "top-requestor gauges should rank and zero-fill:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_requestor_window_requests 12"),
            "window-total gauge should carry the full (untruncated) sum:\n{rendered}"
        );

        // Frame sizes render as byte-bucketed histograms with both labels.
        let frame_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_network_frame_bytes_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            frame_lines.contains("transport=\"websocket\"")
                && frame_lines.contains("direction=\"sent\"")
                && frame_lines.contains("le=\"134217728\""),
            "frame-bytes histogram should carry transport/direction and the 128MiB top bucket:\n{frame_lines}"
        );
        // The 300 B frame lands in le="1024" but not le="256", pinning the
        // custom bucket boundaries.
        let bucket_count = |le: &str| {
            frame_lines
                .lines()
                .find(|l| l.contains(&format!("le=\"{le}\"")))
                .and_then(|l| l.rsplit(' ').next())
        };
        assert_eq!(
            bucket_count("256"),
            Some("0"),
            "300 B frame must not land in le=256:\n{frame_lines}"
        );
        assert_eq!(
            bucket_count("1024"),
            Some("1"),
            "300 B frame must land in le=1024:\n{frame_lines}"
        );

        // Handshake duration histogram renders with the outcome label.
        assert!(
            rendered.contains("subduction_handshake_duration_seconds_bucket{outcome=\"ok\"")
                || rendered.contains("outcome=\"ok\",le="),
            "handshake duration should render as an outcome-labeled histogram:\n{rendered}"
        );

        // Subscription outcomes: pushes split ok/failed; propagation labeled.
        assert!(
            rendered.contains("subduction_subscription_pushes_total{outcome=\"ok\"} 2")
                && rendered.contains("subduction_subscription_pushes_total{outcome=\"failed\"} 1"),
            "subscription pushes should split by outcome:\n{rendered}"
        );
        assert!(
            rendered
                .contains("subduction_subscription_propagations_total{outcome=\"established\"} 1"),
            "propagation counter should render with outcome:\n{rendered}"
        );

        // Build info: constant 1 carrying identity labels (order may vary).
        assert!(
            rendered.contains("subduction_build_info")
                && rendered.contains("version=\"0.0.0-test\"")
                && rendered.contains("git_sha=\"deadbeef\""),
            "build info gauge should carry version and git_sha labels:\n{rendered}"
        );

        // Cache counters render with their exact totals; the resident gauge too.
        assert!(
            rendered.contains("subduction_sedimentree_cache_hits_total 2"),
            "cache hits counter should render a total of 2:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_sedimentree_cache_misses_total 1"),
            "cache misses counter should render a total of 1:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_sedimentree_cache_resident 7"),
            "cache resident gauge should render 7:\n{rendered}"
        );
    }
}
