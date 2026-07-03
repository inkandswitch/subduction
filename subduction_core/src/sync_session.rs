//! A sketch for a sync session observation system that allows
//! observing the commits, peer and heads involved.
//!
//! Probably should be part of [`Handler`] or [`SyncStats`] instead.
//!
//! [`Handler`]: crate::handler::Handler
//! [`SyncStats`]: crate::connection::stats::SyncStats

use alloc::{string::String, sync::Arc, vec::Vec};

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

use crate::{connection::message::RequestId, peer::id::PeerId, remote_heads::RemoteHeads};

/// Stable categories for policy rejections observed during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncPolicyRejectionKind {
    /// The policy has no local document/capability definition.
    DocumentNotFound,
    /// The policy knows the document but denies the requested operation.
    InsufficientAccess,
    /// The requested identifier was malformed.
    InvalidIdentifier,
    /// The policy rejected the operation for an implementation-specific reason.
    Other,
}

/// A structured policy rejection reported during sync.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncPolicyRejection {
    /// Stable policy-rejection category.
    pub kind: SyncPolicyRejectionKind,
    /// Human-readable rejection reason.
    pub reason: String,
}

impl SyncPolicyRejection {
    /// Create a structured policy rejection from a category and message.
    #[must_use]
    pub fn new(kind: SyncPolicyRejectionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
}

/// A rejection reported by the remote peer before any payload was exchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncRemoteRejection {
    /// The remote peer has no sedimentree for this identifier.
    NotFound,
    /// The remote peer refused the fetch without a finer policy category.
    Unauthorized,
    /// The remote peer's storage policy rejected the fetch.
    Policy(SyncPolicyRejectionKind),
}

/// The different categories of sync sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SyncSessionKind {
    /// Sync session was initiated by the local node.
    OutboundBatch { request_id: RequestId },
    /// Sync session was initiated by a remote node.
    InboundBatch { request_id: RequestId },
    /// Sync sessions that happen as part of subscriptions.
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
    /// The commits that were rejected by local policy.
    pub rejected_commit_ids: Vec<(CommitId, SyncPolicyRejection)>,
    /// The fragments that were rejected by local policy.
    pub rejected_fragment_ids: Vec<(CommitId, SyncPolicyRejection)>,
    /// Why the remote peer refused the requested sedimentree, if it did so.
    pub remote_rejection: Option<SyncRemoteRejection>,
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
            rejected_commit_ids: Vec::new(),
            rejected_fragment_ids: Vec::new(),
            remote_rejection: None,
        }
    }

    /// Indicates if anything really was exchanged in the session?
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.received_commit_ids.is_empty()
            && self.received_fragment_ids.is_empty()
            && self.sent_commit_ids.is_empty()
            && self.sent_fragment_ids.is_empty()
            && self.rejected_commit_ids.is_empty()
            && self.rejected_fragment_ids.is_empty()
    }

    /// Whether the session reported any policy rejections.
    #[must_use]
    pub const fn has_policy_rejections(&self) -> bool {
        !self.rejected_commit_ids.is_empty() || !self.rejected_fragment_ids.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_starts_empty() {
        use crate::connection::message::RequestId;
        let id = SedimentreeId::new([0; 32]);
        let peer = PeerId::new([0; 32]);
        let kind = SyncSessionKind::OutboundBatch {
            request_id: RequestId {
                requestor: peer,
                nonce: 1,
            },
        };
        let session = SyncSession::new(id, peer, kind);
        assert!(session.is_empty());
        assert!(!session.has_policy_rejections());
    }

    #[test]
    fn session_reports_policy_rejections() {
        use crate::connection::message::RequestId;
        let id = SedimentreeId::new([7; 32]);
        let peer = PeerId::new([8; 32]);
        let kind = SyncSessionKind::InboundBatch {
            request_id: RequestId {
                requestor: peer,
                nonce: 2,
            },
        };
        let mut session = SyncSession::new(id, peer, kind);

        let commit_rejection = SyncPolicyRejection::new(
            SyncPolicyRejectionKind::DocumentNotFound,
            "no local document for this sedimentree",
        );
        let fragment_rejection = SyncPolicyRejection::new(
            SyncPolicyRejectionKind::InsufficientAccess,
            "peer lacks read permission",
        );

        session
            .rejected_commit_ids
            .push((CommitId::new([1; 32]), commit_rejection));
        session
            .rejected_fragment_ids
            .push((CommitId::new([2; 32]), fragment_rejection));

        assert!(!session.is_empty());
        assert!(session.has_policy_rejections());
        assert_eq!(session.rejected_commit_ids.len(), 1);
        assert_eq!(session.rejected_fragment_ids.len(), 1);
        assert_eq!(
            session.rejected_commit_ids[0].1.kind,
            SyncPolicyRejectionKind::DocumentNotFound
        );
        assert_eq!(
            session.rejected_fragment_ids[0].1.kind,
            SyncPolicyRejectionKind::InsufficientAccess
        );

        // Remote rejection makes the session non-empty too.
        session.remote_rejection = Some(SyncRemoteRejection::Unauthorized);
        assert!(!session.is_empty());
    }

    #[test]
    fn rejection_kinds_are_distinct() {
        use SyncPolicyRejectionKind as K;
        assert_ne!(K::DocumentNotFound, K::InsufficientAccess);
        assert_ne!(K::DocumentNotFound, K::InvalidIdentifier);
        assert_ne!(K::DocumentNotFound, K::Other);
        assert_ne!(K::InsufficientAccess, K::InvalidIdentifier);
        assert_ne!(K::InsufficientAccess, K::Other);
        assert_ne!(K::InvalidIdentifier, K::Other);
    }
}
