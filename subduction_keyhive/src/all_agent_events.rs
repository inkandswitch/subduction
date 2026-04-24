//! Bulk per-agent event snapshot for the periodic event cache.

#![cfg(all(feature = "serde", feature = "std"))]

use alloc::collections::{BTreeMap, BTreeSet};

use crate::{
    message::{CborBytes, EventBytes, EventHash},
    peer_id::KeyhivePeerId,
};

/// Per-agent event snapshot, deduplicated by hash.
#[derive(Debug, Clone, Default)]
pub struct AllAgentEvents {
    /// Per-agent reachable hash sets.
    pub agent_hashes: BTreeMap<KeyhivePeerId, BTreeSet<EventHash>>,
    /// Bincode-serialized `StaticEvent` bytes paired with pre-encoded CBOR
    /// byte-string framing.
    pub event_data: BTreeMap<EventHash, (EventBytes, CborBytes)>,
}

impl AllAgentEvents {
    /// Hash set for `peer`, or `None` if unknown.
    #[must_use]
    pub fn hashes_for(&self, peer: &KeyhivePeerId) -> Option<&BTreeSet<EventHash>> {
        self.agent_hashes.get(peer)
    }

    /// Number of distinct events.
    #[must_use]
    pub fn event_count(&self) -> usize {
        self.event_data.len()
    }

    /// Number of agents in the snapshot.
    #[must_use]
    pub fn agent_count(&self) -> usize {
        self.agent_hashes.len()
    }
}
