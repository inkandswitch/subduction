//! Per-peer syncpoint tracking.
//!
//! A syncpoint is the last confirmed per-pair operation total with a
//! given peer. When present, the orchestrator can send a lightweight
//! [`Message::SyncCheck`] instead of a full sync request. When absent
//! (or invalidated by new ingestions) it falls back to a full sync.
//!
//! This module provides the storage. Update rules live in [`crate::orchestrator`].
//!
//! [`Message::SyncCheck`]: subduction_keyhive::Message::SyncCheck

use alloc::collections::BTreeMap;

use subduction_keyhive::KeyhivePeerId;

/// Per-peer syncpoint map.
///
/// Values are the remote peer's reported total at the moment of the
/// last successful sync exchange. Entries are removed on peer
/// disconnect and cleared globally when local state advances.
#[derive(Debug, Default)]
pub struct SyncpointMap {
    inner: BTreeMap<KeyhivePeerId, u64>,
}

impl SyncpointMap {
    /// Create an empty [`SyncpointMap`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Get the syncpoint for a peer, if any.
    #[must_use]
    pub fn get(&self, peer: &KeyhivePeerId) -> Option<u64> {
        self.inner.get(peer).copied()
    }

    /// Set the syncpoint for a peer.
    pub fn set(&mut self, peer: KeyhivePeerId, total: u64) {
        self.inner.insert(peer, total);
    }

    /// Drop the syncpoint for a single peer.
    pub fn remove(&mut self, peer: &KeyhivePeerId) {
        self.inner.remove(peer);
    }

    /// Invalidate syncpoints for every peer.
    ///
    /// Called when new ops are ingested locally, since any cached
    /// totals from prior exchanges are now potentially stale.
    pub fn invalidate_all(&mut self) {
        self.inner.clear();
    }

    /// Number of peers with a recorded syncpoint.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no peer has a recorded syncpoint.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn peer(seed: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([seed; 32])
    }

    #[test]
    fn set_and_get_roundtrip() {
        let mut map = SyncpointMap::new();
        map.set(peer(1), 42);
        map.set(peer(2), 7);

        assert_eq!(map.get(&peer(1)), Some(42));
        assert_eq!(map.get(&peer(2)), Some(7));
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn set_overwrites() {
        let mut map = SyncpointMap::new();
        map.set(peer(1), 42);
        map.set(peer(1), 100);
        assert_eq!(map.get(&peer(1)), Some(100));
    }

    #[test]
    fn remove_drops_single_entry() {
        let mut map = SyncpointMap::new();
        map.set(peer(1), 42);
        map.set(peer(2), 7);
        map.remove(&peer(1));
        assert!(map.get(&peer(1)).is_none());
        assert_eq!(map.get(&peer(2)), Some(7));
    }

    #[test]
    fn invalidate_all_clears_everything() {
        let mut map = SyncpointMap::new();
        map.set(peer(1), 42);
        map.set(peer(2), 7);
        map.invalidate_all();
        assert!(map.is_empty());
    }
}
