//! Prometheus metrics server for Subduction.

use axum::{routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use tokio::net::TcpListener;

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
    /// Total bytes sent.
    pub const BYTES_SENT_TOTAL: &str = "subduction_bytes_sent_total";
    /// Total bytes received.
    pub const BYTES_RECEIVED_TOTAL: &str = "subduction_bytes_received_total";
    /// Number of active sedimentrees.
    pub const SEDIMENTREES_ACTIVE: &str = "subduction_sedimentrees_active";
    /// Total batch sync requests.
    pub const BATCH_SYNC_REQUESTS_TOTAL: &str = "subduction_batch_sync_requests_total";
    /// Total batch sync responses.
    pub const BATCH_SYNC_RESPONSES_TOTAL: &str = "subduction_batch_sync_responses_total";
}

/// Initialize the metrics recorder and return a handle for the HTTP endpoint.
///
/// This must be called once at startup before any metrics are recorded.
///
/// # Panics
///
/// Panics if the recorder cannot be installed.
#[must_use]
pub fn init_metrics() -> PrometheusHandle {
    #[allow(clippy::expect_used)]
    PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder")
}

/// Start the metrics HTTP server on the given address.
///
/// This spawns a background task that serves the `/metrics` endpoint.
///
/// # Errors
///
/// Returns an error if the server fails to bind to the address.
pub async fn start_metrics_server(
    addr: SocketAddr,
    handle: PrometheusHandle,
) -> anyhow::Result<()> {
    let app = Router::new().route(
        "/metrics",
        get(move || {
            let handle = handle.clone();
            async move { handle.render() }
        }),
    );

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("Metrics server listening on {}", addr);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("Metrics server error: {}", e);
        }
    });

    Ok(())
}

/// Convenience functions for recording metrics.
pub mod record {
    use metrics::{counter, gauge, histogram};

    use super::names;

    /// Record a new connection being established.
    pub fn connection_opened() {
        gauge!(names::CONNECTIONS_ACTIVE).increment(1);
        counter!(names::CONNECTIONS_TOTAL).increment(1);
    }

    /// Record a connection being closed.
    pub fn connection_closed() {
        gauge!(names::CONNECTIONS_ACTIVE).decrement(1);
        counter!(names::CONNECTIONS_CLOSED).increment(1);
    }

    /// Record a message being dispatched.
    pub fn message_dispatched(message_type: &'static str) {
        counter!(names::MESSAGES_TOTAL, "type" => message_type).increment(1);
    }

    /// Record the duration of a dispatch operation.
    pub fn dispatch_duration(duration_secs: f64) {
        histogram!(names::DISPATCH_DURATION_SECONDS).record(duration_secs);
    }

    /// Record bytes sent.
    pub fn bytes_sent(bytes: u64) {
        counter!(names::BYTES_SENT_TOTAL).increment(bytes);
    }

    /// Record bytes received.
    pub fn bytes_received(bytes: u64) {
        counter!(names::BYTES_RECEIVED_TOTAL).increment(bytes);
    }

    /// Set the number of active sedimentrees.
    pub fn set_sedimentrees_active(count: usize) {
        #[allow(clippy::cast_precision_loss)]
        gauge!(names::SEDIMENTREES_ACTIVE).set(count as f64);
    }

    /// Record a batch sync request.
    pub fn batch_sync_request() {
        counter!(names::BATCH_SYNC_REQUESTS_TOTAL).increment(1);
    }

    /// Record a batch sync response.
    pub fn batch_sync_response() {
        counter!(names::BATCH_SYNC_RESPONSES_TOTAL).increment(1);
    }
}
