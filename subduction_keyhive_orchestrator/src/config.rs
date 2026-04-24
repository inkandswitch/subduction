//! Configuration for the keyhive orchestrator.

use core::time::Duration;

/// Configuration options for [`SubductionKeyhiveOrchestrator`].
#[derive(Debug, Clone, Copy)]
pub struct OrchestratorConfig {
    /// Interval between periodic cache refreshes.
    pub cache_refresh_interval: Duration,

    // /// Minimum interval between outbound sync requests to the same peer.
    // ///
    // TODO: Implement
    // pub min_sync_request_interval: Duration,

    // /// Minimum interval between outbound sync responses to the same peer.
    // ///
    // TODO: Implement
    // pub min_sync_response_interval: Duration,

    // /// Optional batching interval for inbound messages.
    // ///
    // /// `None` disables batching. `Some(d)` buffers inbound messages
    // /// for up to `d` before handing them to the protocol.
    // ///
    // TODO: Implement
    // pub batch_interval: Option<Duration>,
    /// Threshold above which a full archive is written instead of
    /// individual events during ingestion.
    ///
    /// `None` disables archive writes.
    pub archive_threshold: Option<usize>,
    // /// Whether to attempt storage recovery when events remain pending
    // /// after ingestion.
    // ///
    // TODO: Implement
    // pub attempt_storage_recovery: bool,

    // /// Whether to suppress individual event writes during bulk ingest.
    // ///
    // /// When `true`, the orchestrator (or archive write) takes over
    // /// durability; the protocol does not persist events itself.
    // ///
    // TODO: Implement
    // pub suppress_writes_during_ingest: bool,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            cache_refresh_interval: Duration::from_secs(2),
            // min_sync_request_interval: Duration::from_secs(1),
            // min_sync_response_interval: Duration::from_secs(1),
            // batch_interval: None,
            archive_threshold: None,
            // attempt_storage_recovery: true,
            // suppress_writes_during_ingest: false,
        }
    }
}
