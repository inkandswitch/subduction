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

/// Per-peer counts of inbound `BatchSyncRequest`s since the last drain.
#[derive(Debug, Default)]
pub struct RequestorTally {
    counts: Mutex<Map<PeerId, u64>>,
}

/// Maximum peers tracked per window; see [`RequestorTally::record`] for the
/// at-cap eviction policy. Windows are drained on a timer (~once a minute),
/// so the cap rarely binds in practice.
const MAX_TRACKED_REQUESTORS: usize = 1024;

impl RequestorTally {
    /// Create an empty tally.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one inbound sync request from `peer`.
    ///
    /// At the cap this is Space-Saving eviction: the new peer replaces the
    /// current minimum *and inherits its count*, so a newly-hot peer climbs
    /// from the floor instead of being evicted by the next one-shot id.
    /// Counts near the cap are therefore overestimates (by at most the
    /// evicted minimum), never underestimates.
    pub async fn record(&self, peer: PeerId) {
        let mut counts = self.counts.lock().await;
        if let Some(count) = counts.get_mut(&peer) {
            *count += 1;
            return;
        }

        let mut initial = 1;
        if counts.len() >= MAX_TRACKED_REQUESTORS {
            // O(n) scan, but only on insert at the cap.
            if let Some((min_peer, min_count)) = counts
                .iter()
                .min_by_key(|(_, count)| **count)
                .map(|(peer, count)| (*peer, *count))
            {
                counts.remove(&min_peer);
                initial = min_count + 1;
            }
        }
        counts.insert(peer, initial);
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

    /// Distinct ids within a numbered namespace, disjoint from `peer(_)`.
    #[allow(clippy::cast_possible_truncation)]
    fn ns_peer(namespace: u8, i: usize) -> PeerId {
        let mut id = [0u8; 32];
        id[0] = (i % 256) as u8;
        id[1] = (i / 256) as u8;
        id[31] = namespace;
        PeerId::new(id)
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
    fn established_heavy_hitter_survives_flood() {
        futures::executor::block_on(async {
            let tally = RequestorTally::new();
            // A heavy hitter, then a flood of one-shot peers past the cap.
            for _ in 0..10 {
                tally.record(peer(0)).await;
            }
            for i in 0..MAX_TRACKED_REQUESTORS {
                tally.record(ns_peer(1, i)).await;
            }

            let ranked = tally.take_window().await;
            assert_eq!(
                ranked.first(),
                Some(&(peer(0), 10)),
                "heavy hitter must survive the flood"
            );
            assert_eq!(ranked.len(), MAX_TRACKED_REQUESTORS);
        });
    }

    /// The case plain evict-min gets wrong: a peer that becomes hot only
    /// *after* the map is full. Without count inheritance it re-enters at 1
    /// and is deterministically evicted by every subsequent fresh id (it is
    /// the unique minimum among the count-2 residents), so it never
    /// accumulates; with inheritance it climbs above the floor and stays.
    #[test]
    fn new_heavy_hitter_survives_interleaved_flood() {
        futures::executor::block_on(async {
            let tally = RequestorTally::new();
            // Fill to cap with established count-2 residents.
            for i in 0..MAX_TRACKED_REQUESTORS {
                tally.record(ns_peer(1, i)).await;
                tally.record(ns_peer(1, i)).await;
            }

            // A newly-hot peer arrives, interleaved with fresh one-shot ids.
            let hot = peer(0xEE);
            let rounds = 32;
            for i in 0..rounds {
                tally.record(hot).await;
                tally.record(ns_peer(2, i)).await;
            }

            let ranked = tally.take_window().await;
            let hot_count = ranked
                .iter()
                .find(|(peer, _)| *peer == hot)
                .map(|(_, count)| *count);
            assert!(
                hot_count.is_some_and(|count| count >= rounds as u64),
                "count inheritance must let a late-arriving heavy hitter climb; got {hot_count:?}"
            );
        });
    }
}
