//! Application-facing effect vocabulary shared by the machines.
//!
//! The composed driver-facing alphabet lives on
//! [`NodeEffect`](crate::node::NodeEffect); the per-machine alphabets
//! live with their machines. This module keeps the shared pieces:
//! [`AppEvent`] (surfaced to the application) and [`SyncStatus`].
//!
//! Timers are deliberately *not* effects: the machines keep their own
//! deadline maps and expose only the next deadline via `poll_timeout`
//! (quinn-proto style). The driver arms a single timer and sends a bare
//! wake on expiry — no timer ids, no cancellation races.

use alloc::vec::Vec;

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};

use crate::{id::ConnId, peer_id::PeerId, storage::StorageFailure};

/// How a batch sync request concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
pub enum SyncStatus {
    /// The response was processed (ingest durability is reported
    /// separately via `TreeUpdated`).
    Completed,

    /// The peer does not have the tree.
    NotFound,

    /// The peer says we may not read the tree.
    Unauthorized,

    /// No response arrived within the sync deadline.
    TimedOut,
}

/// An application-facing event surfaced by the machine.
///
/// Drivers translate these into callbacks, streams, or platform-native
/// notifications; they also feed tier-1/tier-3 telemetry.
// Not `Copy`: Phase 2 adds data-carrying variants (ingested commits, heads
// updates), and removing a `Copy` impl later is a breaking change.
#[allow(missing_copy_implementations)]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AppEvent {
    /// A connection completed the handshake and is now authenticated.
    PeerAuthenticated {
        /// The authenticated connection.
        conn: ConnId,
        /// The peer's verified identity.
        peer: PeerId,
    },

    /// A connection is gone (closed by us, by the peer, or by transport
    /// failure), after any authenticated state was torn down.
    ConnectionClosed {
        /// The closed connection.
        conn: ConnId,
        /// The peer identity, if the handshake had completed.
        peer: Option<PeerId>,
    },

    /// Locally-authored commits are sealed and durable
    /// ([`Command::AddCommits`](crate::command::Command::AddCommits)
    /// completed).
    CommitsStored {
        /// The tree appended to.
        tree: SedimentreeId,
        /// The stored commits' identities.
        heads: Vec<CommitId>,
    },

    /// Locally-authored fragments are sealed and durable
    /// ([`Command::AddFragments`](crate::command::Command::AddFragments)
    /// completed).
    FragmentsStored {
        /// The tree appended to.
        tree: SedimentreeId,
        /// The stored fragments' head identities.
        heads: Vec<CommitId>,
    },

    /// A tree was removed from storage
    /// ([`Command::RemoveTree`](crate::command::Command::RemoveTree)
    /// completed).
    TreeRemoved {
        /// The removed tree.
        tree: SedimentreeId,
    },

    /// A local storage operation failed; the application owns retry
    /// policy.
    StorageError {
        /// The tree the operation targeted.
        tree: SedimentreeId,
        /// Coarse failure class.
        failure: StorageFailure,
    },

    /// A batch sync request we initiated has concluded.
    SyncFinished {
        /// The connection it ran on.
        conn: ConnId,
        /// The tree that was synced.
        tree: SedimentreeId,
        /// How it went.
        status: SyncStatus,
    },

    /// Remote data was verified, persisted, and merged into the resident
    /// tree.
    TreeUpdated {
        /// The updated tree.
        tree: SedimentreeId,
        /// The peer the data came from.
        peer: PeerId,
    },

    /// A subscriber fell behind (too many unacked pushes) and its
    /// subscription was paused. It was nudged with a
    /// `HeadsUpdate`; if alive, it re-syncs and re-subscribes.
    SubscriberLagging {
        /// The lagging connection.
        conn: ConnId,
        /// The tree whose subscription was paused.
        tree: SedimentreeId,
    },

    /// A peer reported new heads for a tree (stale reports are filtered
    /// by the per-peer monotonic counter).
    RemoteHeadsUpdated {
        /// The tree the heads are for.
        tree: SedimentreeId,
        /// The reporting peer.
        peer: PeerId,
        /// The reported heads.
        heads: crate::remote_heads::RemoteHeads,
    },

    /// A message for an extension protocol (not Subduction's own) arrived
    /// on an authenticated connection.
    ///
    /// Extension protocols (ephemeral, keyhive, application-defined)
    /// multiplex over the same connection, distinguished by their 4-byte
    /// schema prefix. The machine only gates them on authentication and
    /// passes the bytes through untouched — routing beyond that is the
    /// application's job.
    ExtensionMessage {
        /// The receiving connection.
        conn: ConnId,
        /// The authenticated peer.
        peer: PeerId,
        /// The complete extension message, schema prefix included.
        bytes: Vec<u8>,
    },
}
