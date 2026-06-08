//! Prometheus metrics server for Subduction.
//!
//! Metrics are recorded by `subduction_core` when the `metrics` feature is enabled.
//! This module provides the HTTP server that exposes metrics to Prometheus.

use axum::{Router, routing::get};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use metrics_util::MetricKindMask;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpListener;

/// Explicit histogram buckets (seconds) for all `*_duration_seconds` metrics.
///
/// Without buckets, `metrics-exporter-prometheus` renders histograms as
/// summaries (quantile series, no `_bucket`), which `histogram_quantile()`
/// dashboard panels cannot use. These cover sub-millisecond handler runtimes
/// up to multi-second sync rounds.
const DURATION_BUCKETS_SECONDS: &[f64] = &[
    0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
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
    #[allow(clippy::expect_used)]
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Suffix("_duration_seconds".to_owned()),
            DURATION_BUCKETS_SECONDS,
        )
        .expect("duration buckets are non-empty and sorted")
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
    /// histogram must render as Prometheus `_bucket` series (not a summary).
    /// Without `set_buckets_for_metric`, the exporter emits quantile summaries
    /// and `histogram_quantile(rate(..._bucket))` dashboard panels show no data.
    #[test]
    fn duration_histograms_render_as_buckets() {
        let handle = init_metrics();
        // Record through the installed recorder.
        subduction_core::metrics::dispatch_duration(0.003);
        subduction_core::metrics::storage_operation_duration("save_loose_commit", 0.012);

        let rendered = handle.render();
        assert!(
            rendered.contains("subduction_dispatch_duration_seconds_bucket"),
            "dispatch duration histogram should emit _bucket series:\n{rendered}"
        );
        assert!(
            rendered.contains("subduction_storage_operation_duration_seconds_bucket"),
            "storage op duration histogram should emit _bucket series:\n{rendered}"
        );
        // A specific bucket boundary from DURATION_BUCKETS_SECONDS must appear.
        assert!(
            rendered.contains("le=\"0.005\""),
            "expected configured bucket boundary le=0.005:\n{rendered}"
        );
    }
}
