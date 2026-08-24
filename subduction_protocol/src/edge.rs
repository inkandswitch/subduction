//! The inter-machine edge: the sealed, sequenced channel between a
//! [`ConnMachine`] and the [`CoreMachine`].
//!
//! # Ocap discipline
//!
//! [`Sealed<M>`] can only be minted inside this crate (private field,
//! `pub(crate)` constructor). Platform drivers route sealed values
//! opaquely; they cannot construct one, so they cannot inject
//! "already-verified" data into the core — the forgery gate stays in
//! machine code on every platform. The router and both machine types are
//! shipped Rust; only _leaf_ effects (transport bytes, storage, signing,
//! clocks) cross to native driver code.
//!
//! # Edges and epochs
//!
//! An edge is identified by [`EdgeId`] = (connection, generation). A
//! supervisor restart of a connection's machine starts a _new
//! generation_: messages from the old incarnation fail the sequencer's
//! edge check and drop. Within an edge, messages carry a monotonic
//! [`Seq`]; the receiving side's [`sequencer::EdgeSequencer`] enforces _in-order,
//! exactly-once_ delivery — loss, duplication, and reordering between
//! machines are driver bugs made _detectable_ rather than trusted away.
//!
//! # The alphabet
//!
//! [`ConnToCore`] and [`CoreToConn`] are deliberately small and grow
//! additively. Bulk data never rides the
//! edge: verified items carry [`BlobRef`]s (see [`crate::blob_ref`]).
//!
//! [`ConnMachine`]: crate::conn_machine::ConnMachine
//! [`CoreMachine`]: crate::core_machine::CoreMachine

pub mod sequencer;

use alloc::vec::Vec;

use sedimentree_core::{fragment::Fragment, loose_commit::LooseCommit};
use subduction_crypto::{nonce::Nonce, signed::Signed};

use crate::{
    blob_ref::BlobRef,
    event::Direction,
    id::{ConnId, Generation, Seq},
    outcome::Fault,
    peer_id::PeerId,
    remote_heads::RemoteHeads,
    wire::{BatchSyncRequest, RequestId, RequestedData},
};

/// A sealed edge message: constructible only inside this crate.
///
/// The private field is the whole mechanism — drivers can hold, route,
/// and drop `Sealed` values but never create or alter one. Accessors are
/// crate-internal; the payload is invisible to driver code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed<M> {
    edge: EdgeId,
    seq: Seq,
    msg: M,
}

impl<M> Sealed<M> {
    /// Mint a sealed message. Crate-internal on purpose (ocap): only the
    /// machines and router can produce edge traffic.
    pub(crate) const fn mint(edge: EdgeId, seq: Seq, msg: M) -> Self {
        Self { edge, seq, msg }
    }

    /// The issuing edge.
    #[must_use]
    pub const fn edge(&self) -> EdgeId {
        self.edge
    }

    /// The per-edge sequence number.
    #[must_use]
    pub const fn seq(&self) -> Seq {
        self.seq
    }

    /// Open the envelope. Crate-internal: payloads are for machines.
    pub(crate) fn open(self) -> (EdgeId, Seq, M) {
        (self.edge, self.seq, self.msg)
    }
}

/// One connection-machine incarnation: the (connection, generation) pair
/// that scopes every edge message and every retained-frame epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[cfg_attr(feature = "bolero", derive(bolero::generator::TypeGenerator))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct EdgeId {
    /// The connection.
    pub conn: ConnId,

    /// The connection machine's incarnation.
    pub generation: Generation,
}

// ── the alphabet ────────────────────────────────────────────────────

/// A commit whose signature and blob digest the connection machine has
/// verified. The signed value is retained for fan-out re-encoding; the
/// blob rides the data plane by reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCommit {
    /// The verified signed commit.
    pub commit: Signed<LooseCommit>,

    /// The verified blob's bytes, by reference.
    pub blob: BlobRef,
}

/// A fragment counterpart of [`VerifiedCommit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedFragment {
    /// The verified signed fragment.
    pub fragment: Signed<Fragment>,

    /// The verified blob's bytes, by reference.
    pub blob: BlobRef,
}

/// The sync payloads a connection machine forwards after decoding and
/// verifying. Mirrors the wire vocabulary minus raw blob bytes; items
/// that failed verification are dropped at the connection machine and
/// only counted here (tier-3 telemetry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncForward {
    /// A batch sync request (nothing signed inside; forwarded decoded).
    Request(BatchSyncRequest),

    /// A batch sync response with its items verified.
    Response {
        /// The request this answers.
        req_id: RequestId,
        /// The tree synced.
        tree: sedimentree_core::id::SedimentreeId,
        /// Verified missing commits (`None` result variants collapse to
        /// empty vectors plus the wire status below).
        commits: Vec<VerifiedCommit>,
        /// Verified missing fragments.
        fragments: Vec<VerifiedFragment>,
        /// Fingerprints the responder asked back for.
        requesting: RequestedData,
        /// The responder's heads.
        responder_heads: RemoteHeads,
        /// Wire-level status (`Ok` / `NotFound` / `Unauthorized`), collapsed.
        status: ForwardStatus,
        /// Items dropped by verification at the connection machine.
        rejected: u32,
    },

    /// A single pushed commit, verified.
    Commit {
        /// The tree it belongs to.
        tree: sedimentree_core::id::SedimentreeId,
        /// The verified item.
        item: VerifiedCommit,
        /// The sender's heads rider.
        sender_heads: RemoteHeads,
    },

    /// A single pushed fragment, verified.
    Fragment {
        /// The tree it belongs to.
        tree: sedimentree_core::id::SedimentreeId,
        /// The verified item.
        item: VerifiedFragment,
        /// The sender's heads rider.
        sender_heads: RemoteHeads,
    },

    /// A heads notification.
    HeadsUpdate {
        /// The tree the heads are for.
        tree: sedimentree_core::id::SedimentreeId,
        /// The reported heads.
        heads: RemoteHeads,
    },

    /// The peer unsubscribed from trees.
    RemoveSubscriptions(Vec<sedimentree_core::id::SedimentreeId>),

    /// The peer rejected our data request (informational).
    DataRequestRejected(sedimentree_core::id::SedimentreeId),
}

/// Collapsed wire status of a forwarded batch response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardStatus {
    /// The responder answered with a diff.
    Ok,

    /// The responder does not have the tree.
    NotFound,

    /// The responder denied us.
    Unauthorized,
}

/// Connection-machine → core messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnToCore {
    /// A connection machine came up (pre-auth); the core opens a lease.
    Opened {
        /// Who initiated the underlying connection.
        direction: Direction,
    },

    /// The handshake completed; the edge is now attributable to a peer.
    Authenticated {
        /// The verified peer identity.
        peer: PeerId,
    },

    /// Handshake replay-protection claim (core is the nonce arbiter).
    /// Idempotent per (peer, nonce) within an edge generation, so a
    /// restarted connection machine may safely re-claim.
    ClaimNonce {
        /// The claiming (already signature-verified) initiator.
        peer: PeerId,
        /// The challenge nonce.
        nonce: Nonce,
        /// The challenge's signed wall-clock timestamp — the bucketing
        /// key (message time, not arrival time, so claims replay
        /// deterministically).
        timestamp: crate::wall_clock::TimestampSeconds,
    },

    /// A decoded, verified sync payload. Boxed: forwards dwarf the
    /// control variants, and edge messages move through queues.
    Inbound(alloc::boxed::Box<SyncForward>),

    /// The connection is gone; the core tears down sessions,
    /// subscriptions, and fan-out state for this edge. (The core's lease
    /// expiry covers the case where this message never arrives.)
    Closed {
        /// Why, for telemetry.
        fault: Option<Fault>,
    },
}

/// Core → connection-machine messages. Deliberately tiny: egress bytes
/// go straight from the core to the transport as external effects; only
/// control answers ride this direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreToConn {
    /// Answer to [`ConnToCore::ClaimNonce`].
    NonceVerdict {
        /// `true` = fresh, proceed; `false` = replayed, reject the
        /// handshake.
        granted: bool,
    },
}
