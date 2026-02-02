//! Sync statistics for wasm bindings.

use subduction_core::connection::message::SyncStats;
use wasm_bindgen::prelude::*;

/// Statistics from a sync operation.
///
/// The "sent" counts reflect items that were _successfully_ sent over the wire,
/// not just items that were requested.
#[wasm_bindgen(js_name = SyncStats)]
#[derive(Debug, Clone, Copy, Default)]
pub struct WasmSyncStats {
    commits_received: usize,
    fragments_received: usize,
    commits_sent: usize,
    fragments_sent: usize,
}

#[wasm_bindgen(js_class = SyncStats)]
impl WasmSyncStats {
    /// Number of commits received from the peer.
    #[must_use]
    #[wasm_bindgen(getter, js_name = commitsReceived)]
    pub fn commits_received(&self) -> usize {
        self.commits_received
    }

    /// Number of fragments received from the peer.
    #[must_use]
    #[wasm_bindgen(getter, js_name = fragmentsReceived)]
    pub fn fragments_received(&self) -> usize {
        self.fragments_received
    }

    /// Number of commits successfully sent to the peer.
    #[must_use]
    #[wasm_bindgen(getter, js_name = commitsSent)]
    pub fn commits_sent(&self) -> usize {
        self.commits_sent
    }

    /// Number of fragments successfully sent to the peer.
    #[must_use]
    #[wasm_bindgen(getter, js_name = fragmentsSent)]
    pub fn fragments_sent(&self) -> usize {
        self.fragments_sent
    }

    /// Total items received (commits + fragments).
    #[must_use]
    #[wasm_bindgen(getter, js_name = totalReceived)]
    pub fn total_received(&self) -> usize {
        self.commits_received + self.fragments_received
    }

    /// Total items sent (commits + fragments).
    #[must_use]
    #[wasm_bindgen(getter, js_name = totalSent)]
    pub fn total_sent(&self) -> usize {
        self.commits_sent + self.fragments_sent
    }

    /// Returns true if no data was exchanged.
    #[must_use]
    #[wasm_bindgen(getter, js_name = isEmpty)]
    pub fn is_empty(&self) -> bool {
        self.commits_received == 0
            && self.fragments_received == 0
            && self.commits_sent == 0
            && self.fragments_sent == 0
    }
}

impl From<SyncStats> for WasmSyncStats {
    fn from(stats: SyncStats) -> Self {
        Self {
            commits_received: stats.commits_received,
            fragments_received: stats.fragments_received,
            commits_sent: stats.commits_sent,
            fragments_sent: stats.fragments_sent,
        }
    }
}
