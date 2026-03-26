//! Remote heads tracking and notification.
//!
//! [`RemoteHeads`] carries a peer's current heads for a sedimentree,
//! alongside a monotonic counter for ordering in the face of
//! out-of-order delivery on non-TCP transports.
//!
//! Two traits define the notification pipeline:
//!
//! - [`RemoteHeadsObserver`] — application-facing callback for heads updates
//! - [`RemoteHeadsNotifier`] — handler-level entry point with staleness filtering

use alloc::vec::Vec;

use sedimentree_core::{crypto::digest::Digest, id::SedimentreeId, loose_commit::LooseCommit};

use crate::peer::id::PeerId;

/// A remote peer's heads for a sedimentree, with a monotonic counter
/// for ordering in the face of out-of-order delivery.
///
/// The counter is scoped per-peer and incremented each time the sender
/// sends a message carrying heads. Receivers should only accept updates
/// where `counter` is strictly greater than the last seen value.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RemoteHeads {
    /// Monotonic per-peer counter — higher means newer.
    pub counter: u64,
    /// The heads (tip commits) of the sedimentree.
    pub heads: Vec<Digest<LooseCommit>>,
}

impl RemoteHeads {
    /// Returns `true` if there are no heads.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }
}

/// Observer for remote heads notifications.
///
/// Called with `(sedimentree_id, peer_id, heads)` whenever a remote peer
/// reports its heads — either via a `HeadsUpdate` message or via
/// `sender_heads` on subscription pushes.
pub trait RemoteHeadsObserver {
    /// Called when a remote peer reports its heads for a sedimentree.
    fn on_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads);
}

/// A no-op [`RemoteHeadsObserver`] that discards all notifications.
///
/// This is the default observer used when remote heads notifications
/// are not needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoRemoteHeadsObserver;

impl RemoteHeadsObserver for NoRemoteHeadsObserver {
    fn on_remote_heads(&self, _id: SedimentreeId, _peer: PeerId, _heads: RemoteHeads) {}
}

/// Trait for handlers that can notify the application of remote heads updates.
///
/// [`Subduction`] calls this when it receives `responder_heads` in a
/// [`BatchSyncResponse`] during sync. Handlers that also process
/// subscription pushes and `HeadsUpdate` messages (like [`SyncHandler`])
/// should call this from their own dispatch logic too.
///
/// Implementing this trait allows all remote-heads notifications to flow
/// through a single path regardless of which protocol step produced them.
///
/// [`Subduction`]: crate::subduction::Subduction
/// [`BatchSyncResponse`]: crate::connection::message::BatchSyncResponse
/// [`SyncHandler`]: crate::handler::sync::SyncHandler
pub trait RemoteHeadsNotifier {
    /// Notify the application of a remote peer's current heads for a sedimentree.
    fn notify_remote_heads(&self, id: SedimentreeId, peer: PeerId, heads: RemoteHeads);
}
