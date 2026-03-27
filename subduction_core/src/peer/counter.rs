//! Per-peer monotonic message counter.
//!
//! [`PeerCounter`] provides a shared, lock-free counter per connected peer.
//! It is the canonical way to stamp outgoing messages with a monotonic
//! sequence number, ensuring receivers can detect out-of-order or stale
//! messages on non-TCP transports.
//!
//! The counter is shared across all handlers ([`SyncHandler`], ephemeral,
//! keyhive, etc.) and [`Subduction`] itself, so that every message to a
//! given peer draws from the same monotonic sequence.
//!
//! [`SyncHandler`]: crate::handler::sync::SyncHandler
//! [`Subduction`]: crate::subduction::Subduction

use alloc::sync::Arc;
use async_lock::Mutex;
use core::sync::atomic::{AtomicU64, Ordering};

use sedimentree_core::collections::Map;

use crate::peer::id::PeerId;

/// A shared, per-peer monotonic message counter.
///
/// Each peer's counter is an [`AtomicU64`] behind an [`Arc`], so counter
/// increments are lock-free once the per-peer entry exists. The outer
/// [`Mutex`] is only held briefly to insert new peers.
///
/// ```ignore
/// let counter = PeerCounter::default();
/// let seq = counter.next(peer_id).await;
/// ```
#[derive(Debug, Default, Clone)]
pub struct PeerCounter(Arc<Mutex<Map<PeerId, Arc<AtomicU64>>>>);

impl PeerCounter {
    /// Get the next counter value for a peer, incrementing atomically.
    ///
    /// The first call for a given peer returns 1. Subsequent calls return
    /// strictly increasing values. The counter is lock-free after the
    /// first call (only the map insertion requires the mutex).
    pub async fn next(&self, peer: PeerId) -> u64 {
        let counter = {
            let mut map = self.0.lock().await;
            map.entry(peer).or_default().clone()
        };
        counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}
