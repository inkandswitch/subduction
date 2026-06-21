//! Prometheus metrics server for Subduction.
//!
//! Metrics are recorded by `subduction_core` when the `metrics` feature is enabled.
//! This module provides the HTTP server that exposes metrics to Prometheus.

use axum::{Router, routing::get};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use std::{net::SocketAddr, time::Duration};
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
/// 50µs so fast ops don't collapse into one bucket, and extends to 10s so a
/// write that stalls for seconds under redb write contention isn't clamped to
/// the 1s bucket (which would hide the real p99).
const FINE_BUCKETS_SECONDS: &[f64] = &[
    0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.5, 5.0, 10.0,
];

/// Coarse buckets (seconds) for whole-round operations measured in
/// milliseconds-to-seconds: foreground sync rounds. Sub-ms resolution would be
/// wasted series here.
const COARSE_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Buckets (seconds) for outbound send-queue dwell — sub-millisecond
/// (WebSocket) to tens of seconds (long-poll), so both transports resolve.
const DWELL_BUCKETS_SECONDS: &[f64] = &[
    0.000_5, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Buckets (message count) for outbound send-queue depth sampled at drain, up
/// to the per-connection channel capacity (1024). The leading `0` matters: an
/// idle queue drains at depth 0, and without an `le="0"` bucket those samples
/// fall into `le="1"`, so `histogram_quantile` interpolates to ~0.95 for a
/// quiet queue instead of 0.
const DEPTH_BUCKETS: &[f64] = &[
    0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
];

/// Idle series are evicted after this long with no updates. A safety valve:
/// our label cardinality is bounded (`&'static str` only), but this bounds
/// memory if a label value stops being produced. Applied to histograms only —
/// counters/gauges are kept so `rate()`/`increase()` stay continuous.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60); // 1 hour

/// Initialize the metrics recorder and return a handle for the HTTP endpoint.
///
/// This must be called once at startup before any metrics are recorded.
/// Configures histogram buckets (so latency panels work) and an idle-eviction
/// safety valve, then registers HELP/TYPE descriptions for all emitted metrics.
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
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".to_owned()),
            COARSE_BUCKETS_SECONDS,
        )
        .expect("coarse buckets are non-empty and sorted")
        .idle_timeout(MetricKindMask::HISTOGRAM, Some(IDLE_TIMEOUT))
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
    #[test]
    fn duration_histograms_render_as_buckets() {
        let handle = init_metrics();
        // Record through the installed recorder.
        subduction_core::metrics::dispatch_duration("LooseCommit", 0.000_3);
        subduction_core::metrics::storage_operation_duration("save_loose_commit", 0.000_8);
        subduction_core::metrics::sync_duration(2.0);
        subduction_core::metrics::outbound_queue_dwell("longpoll", 5.0, 3);
        subduction_core::metrics::dispatch_permit_waited(0.001);
        subduction_core::metrics::mux_pending_duration(0.5);

        // Cache counters: 2 hits + 1 miss must render with exactly those totals
        // (guards against the hit/miss being mis-wired or a miss double-counted).
        subduction_core::metrics::sedimentree_cache_hit();
        subduction_core::metrics::sedimentree_cache_hit();
        subduction_core::metrics::sedimentree_cache_miss();
        subduction_core::metrics::set_sedimentree_cache_resident(7);

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

        // The fine set extends past 1s so a contended storage write (seconds)
        // isn't clamped to the 1s top bucket — p99 must stay observable.
        assert!(
            storage_lines.contains("le=\"2.5\"") && storage_lines.contains("le=\"10\""),
            "storage op histogram should carry the >1s tail buckets:\n{storage_lines}"
        );

        // The coarse-only 60s boundary must NOT appear on the fine storage
        // metric (proving the per-metric override took effect).
        assert!(
            !storage_lines.contains("le=\"60\""),
            "storage op histogram should NOT carry the coarse 60s bucket:\n{storage_lines}"
        );

        // Sync rounds use the coarse set: the 60s boundary exists there.
        let sync_lines: String = rendered
            .lines()
            .filter(|l| l.contains("subduction_sync_duration_seconds_bucket"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            sync_lines.contains("le=\"60\""),
            "sync duration histogram should use the coarse 60s bucket:\n{sync_lines}"
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
            rendered.contains("subduction_mux_pending_duration_seconds_bucket"),
            "mux pending-duration histogram should render as buckets:\n{rendered}"
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
