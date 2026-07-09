//! Windowed per-peer counts of inbound sync requests.
//!
//! Peer ids are unbounded cardinality, so they can never appear as
//! Prometheus labels (see the parent [`metrics`](crate::metrics) module
//! doc). This tally is the sanctioned alternative for "who is hammering
//! us?": bounded in-memory counts keyed by [`PeerId`], drained
//! periodically by an operator loop that publishes rank-shaped gauges
//! (bounded labels) and logs the actual ids.

use alloc::vec::Vec;

use async_lock::Mutex;
use sedimentree_core::collections::Map;

use crate::peer::id::PeerId;

/// Maximum peers tracked per window. When full, a new peer evicts the
/// current minimum count (Space-Saving style), so heavy hitters survive
/// even under a flood of one-shot ids. Windows are drained on a timer
/// (~once a minute), so the cap rarely binds in practice.
const MAX_TRACKED_REQUESTORS: usize = 1024;

/// Per-peer counts of inbound `BatchSyncRequest`s since the last drain.
#[derive(Debug, Default)]
pub struct RequestorTally {
    counts: Mutex<Map<PeerId, u64>>,
}

impl RequestorTally {
    /// Create an empty tally.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one inbound sync request from `peer`.
    pub async fn record(&self, peer: PeerId) {
        let mut counts = self.counts.lock().await;
        if let Some(count) = counts.get_mut(&peer) {
            *count += 1;
            return;
        }
        if counts.len() >= MAX_TRACKED_REQUESTORS {
            // Evict the current minimum so a genuinely hot new peer can
            // still enter. O(n) scan, but only on insert at the cap.
            if let Some(min_peer) = counts
                .iter()
                .min_by_key(|(_, count)| **count)
                .map(|(peer, _)| *peer)
            {
                counts.remove(&min_peer);
            }
        }
        counts.insert(peer, 1);
    }

    /// Drain the window: return all `(peer, count)` pairs sorted by count
    /// descending, resetting the tally for the next window.
    pub async fn take_window(&self) -> Vec<(PeerId, u64)> {
        let counts = core::mem::take(&mut *self.counts.lock().await);
        let mut ranked: Vec<(PeerId, u64)> = counts.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1));
        ranked
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(byte: u8) -> PeerId {
        PeerId::new([byte; 32])
    }

    #[test]
    fn ranks_by_count_descending_and_resets() {
        futures::executor::block_on(async {
            let tally = RequestorTally::new();
            for _ in 0..3 {
                tally.record(peer(1)).await;
            }
            tally.record(peer(2)).await;

            let ranked = tally.take_window().await;
            assert_eq!(ranked, vec![(peer(1), 3), (peer(2), 1)]);

            assert!(tally.take_window().await.is_empty(), "drain must reset");
        });
    }

    #[test]
    fn cap_evicts_minimum_not_heavy_hitters() {
        futures::executor::block_on(async {
            let tally = RequestorTally::new();
            // A heavy hitter, then a flood of one-shot peers past the cap.
            for _ in 0..10 {
                tally.record(peer(0)).await;
            }
            for i in 0..MAX_TRACKED_REQUESTORS {
                #[allow(clippy::cast_possible_truncation)]
                let mut id = [0u8; 32];
                id[0] = (i % 256) as u8;
                id[1] = (i / 256) as u8;
                id[31] = 1; // distinct namespace from `peer(0)`
                tally.record(PeerId::new(id)).await;
            }

            let ranked = tally.take_window().await;
            assert_eq!(
                ranked.first(),
                Some(&(peer(0), 10)),
                "heavy hitter must survive the flood"
            );
            assert!(ranked.len() <= MAX_TRACKED_REQUESTORS);
        });
    }
}
