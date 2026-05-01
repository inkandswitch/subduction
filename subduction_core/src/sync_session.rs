//! A sketch for a sync session observation system that allows
//! observing the commits, peer and heads involved.
//!
//! Probably should be part of [`Handler`] or [`SyncStats`] instead.
//!
//! [`Handler`]: crate::handler::Handler
//! [`SyncStats`]: crate::connection::stats::SyncStats

use alloc::{sync::Arc, vec::Vec};

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

use crate::{peer::id::PeerId, remote_heads::RemoteHeads};

/// The different categories of sync sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncSessionKind {
    /// Sync session was initiated by the local node.
    OutboundBatch,
    /// Sync session was initiated by a remote node.
    InboundBatch,
    /// Sync sessions that happens as part of subscriptions.
    InboundPush,
}

/// A holstic description of a sync session
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncSession {
    /// The sedimentree involved in the session
    pub sedimentree_id: SedimentreeId,
    /// The peer involved in the session
    pub peer_id: PeerId,
    /// What it says on the tin
    pub kind: SyncSessionKind,
    /// The resulting remote heads of the session
    /// if it was observed
    pub remote_heads: Option<RemoteHeads>,
    /// The commits that were added to the local sedimentree
    pub received_commit_ids: Vec<CommitId>,
    /// The fragments that were added to the local sedimentree
    pub received_fragment_ids: Vec<CommitId>,
    /// The commits that were sent to the remote sedimentree
    pub sent_commit_ids: Vec<CommitId>,
    /// The fragments that were sent to the remote sedimentree
    pub sent_fragment_ids: Vec<CommitId>,
}

impl SyncSession {
    /// Constructor for use at start of any sync session with fields to be filled
    /// out later depending on sync happenings.
    #[must_use]
    pub const fn new(
        sedimentree_id: SedimentreeId,
        peer_id: PeerId,
        kind: SyncSessionKind,
    ) -> Self {
        Self {
            sedimentree_id,
            peer_id,
            kind,
            remote_heads: None,
            received_commit_ids: Vec::new(),
            received_fragment_ids: Vec::new(),
            sent_commit_ids: Vec::new(),
            sent_fragment_ids: Vec::new(),
        }
    }

    /// Indicates if anything really was exchanged in the session?
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.received_commit_ids.is_empty()
            || !self.received_fragment_ids.is_empty()
            || !self.sent_commit_ids.is_empty()
            || !self.sent_fragment_ids.is_empty()
    }
}

/// A trait to observe [`SyncSession`]s as they happen.
///
/// [`SyncSession`]: crate::sync_session::SyncSession
pub trait SyncSessionObserver {
    /// Observe.
    fn on_sync_session(&self, session: SyncSession);
}

/// A type erased trait object of [`SyncSessionObserver`].
///
/// [`SyncSessionObserver`]: crate::sync_session::SyncSessionObserver
pub type DynSyncSessionObserver = Arc<dyn SyncSessionObserver + Send + Sync>;
