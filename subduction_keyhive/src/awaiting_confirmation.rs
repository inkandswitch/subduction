//! Per-peer tracking of sync rounds awaiting confirmation.
//!
//! A round is recorded when this side sends a peer events in it, as
//! responder in [`Message::SyncResponse`] or as requester in
//! [`Message::SyncOps`]. The peer shows it has ingested the events by
//! answering a `SyncOps` with a [`Message::SyncConfirmation`], and a
//! `SyncResponse` with a `SyncConfirmation` or, if it has events to send
//! back, with `SyncOps`. A peer also confirms rounds in which this side sent
//! nothing, so a confirmation only means the peer received this side's
//! events if a round was awaiting one.
//!
//! [`Message::SyncResponse`]: crate::Message
//! [`Message::SyncOps`]: crate::Message
//! [`Message::SyncConfirmation`]: crate::Message

use alloc::collections::BTreeMap;

use crate::peer_id::KeyhivePeerId;

/// Per-peer count of rounds in which this side sent events that the peer
/// has not confirmed yet.
///
/// Entries are removed when their count reaches zero and on peer
/// disconnect.
#[derive(Debug, Default)]
pub(crate) struct AwaitingConfirmation {
    inner: BTreeMap<KeyhivePeerId, usize>,
}

impl AwaitingConfirmation {
    /// Create an empty [`AwaitingConfirmation`].
    pub(crate) const fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Record a round in which this side sent `peer` events.
    pub(crate) fn record_round(&mut self, peer: KeyhivePeerId) {
        *self.inner.entry(peer).or_default() += 1;
    }

    /// End one of `peer`'s rounds, returning whether one was awaiting
    /// confirmation.
    pub(crate) fn end_round(&mut self, peer: &KeyhivePeerId) -> bool {
        let Some(count) = self.inner.get_mut(peer) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            self.inner.remove(peer);
        }
        true
    }

    /// Drop every round awaiting confirmation from `peer`.
    pub(crate) fn remove(&mut self, peer: &KeyhivePeerId) {
        self.inner.remove(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(seed: u8) -> KeyhivePeerId {
        KeyhivePeerId::from_bytes([seed; 32])
    }

    #[test]
    fn each_recorded_round_ends_once() {
        let mut awaiting = AwaitingConfirmation::new();
        awaiting.record_round(peer(1));
        awaiting.record_round(peer(1));
        awaiting.record_round(peer(2));

        assert!(awaiting.end_round(&peer(1)));
        assert!(awaiting.end_round(&peer(1)));
        assert!(!awaiting.end_round(&peer(1)));
        assert!(awaiting.end_round(&peer(2)));
    }
}
