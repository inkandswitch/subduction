//! Per-peer tracking of sync rounds awaiting confirmation.
//!
//! A round is recorded when this side sends a peer events in it, as
//! responder in [`Message::SyncResponse`] or as requester in
//! [`Message::SyncOps`]. The peer ends such a round with a
//! [`Message::SyncConfirmation`] once it has ingested the events. A peer
//! also confirms rounds in which this side sent nothing, so a confirmation
//! only means the peer received this side's events if a round was awaiting
//! one.
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

    /// Whether `peer` has a round awaiting confirmation.
    pub(crate) fn is_awaiting(&self, peer: &KeyhivePeerId) -> bool {
        self.inner.contains_key(peer)
    }

    /// Mark one of `peer`'s rounds as confirmed. Does nothing if no round is
    /// awaiting confirmation.
    pub(crate) fn confirm_round(&mut self, peer: &KeyhivePeerId) {
        if let Some(count) = self.inner.get_mut(peer) {
            *count -= 1;
            if *count == 0 {
                self.inner.remove(peer);
            }
        }
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
    fn no_round_is_awaiting_by_default() {
        let awaiting = AwaitingConfirmation::new();
        assert!(!awaiting.is_awaiting(&peer(1)));
    }

    #[test]
    fn each_recorded_round_needs_its_own_confirmation() {
        let mut awaiting = AwaitingConfirmation::new();
        awaiting.record_round(peer(1));
        awaiting.record_round(peer(1));

        awaiting.confirm_round(&peer(1));
        assert!(awaiting.is_awaiting(&peer(1)));

        awaiting.confirm_round(&peer(1));
        assert!(!awaiting.is_awaiting(&peer(1)));
    }

    #[test]
    fn confirming_with_nothing_awaiting_does_nothing() {
        let mut awaiting = AwaitingConfirmation::new();
        awaiting.confirm_round(&peer(1));
        awaiting.record_round(peer(1));
        assert!(awaiting.is_awaiting(&peer(1)));
    }

    #[test]
    fn peers_are_tracked_separately() {
        let mut awaiting = AwaitingConfirmation::new();
        awaiting.record_round(peer(1));
        awaiting.record_round(peer(2));

        awaiting.confirm_round(&peer(1));
        assert!(!awaiting.is_awaiting(&peer(1)));
        assert!(awaiting.is_awaiting(&peer(2)));
    }

    #[test]
    fn remove_drops_every_round_for_a_peer() {
        let mut awaiting = AwaitingConfirmation::new();
        awaiting.record_round(peer(1));
        awaiting.record_round(peer(1));
        awaiting.remove(&peer(1));
        assert!(!awaiting.is_awaiting(&peer(1)));
    }
}
