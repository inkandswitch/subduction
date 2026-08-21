//! The inter-machine edge: the sealed, sequenced channel between a
//! [`ConnMachine`] and the [`CoreMachine`] (ADR-015 conditions 1–2).
//!
//! # Ocap discipline
//!
//! [`Sealed<M>`] can only be minted inside this crate (private field,
//! `pub(crate)` constructor). Platform drivers route sealed values
//! opaquely; they cannot construct one, so they cannot inject
//! "already-verified" data into the core — the forgery gate stays in
//! machine code on every platform. The router and both machine types are
//! shipped Rust; only *leaf* effects (transport bytes, storage, signing,
//! clocks) cross to native driver code.
//!
//! # Edges and epochs
//!
//! An edge is identified by [`EdgeId`] = (connection, generation). A
//! supervisor restart of a connection's machine starts a **new
//! generation**: messages from the old incarnation fail the sequencer's
//! edge check and drop. Within an edge, messages carry a monotonic
//! [`Seq`]; the receiving side's [`EdgeSequencer`] enforces **in-order,
//! exactly-once** delivery — loss, duplication, and reordering between
//! machines are driver bugs made *detectable* rather than trusted away.
//!
//! # The alphabet
//!
//! [`ConnToCore`] and [`CoreToConn`] are deliberately small and grow
//! additively during the Phase 2.5 split. Bulk data never rides the
//! edge: verified items carry [`BlobRef`]s (see [`crate::blob_ref`]).
//!
//! [`ConnMachine`]: crate — lands with the Phase 2.5 split
//! [`CoreMachine`]: crate — lands with the Phase 2.5 split

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
    // TODO(phase-2.5): remove this allow once ConnMachine/CoreMachine land
    // and mint/open have real call sites — tracked in .ignore/TODO.md.
    #[allow(dead_code)]
    pub(crate) fn open(self) -> (EdgeId, Seq, M) {
        (self.edge, self.seq, self.msg)
    }
}

/// Receiving-side discipline for one edge: in-order, exactly-once.
///
/// The core keeps one sequencer per registered edge. Anything that is
/// not literally the next message on the current generation is rejected
/// with a reason — turning router/driver delivery bugs into loud,
/// observable events instead of silent state corruption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeSequencer {
    edge: EdgeId,
    next: Seq,
}

impl EdgeSequencer {
    /// Start accepting for `edge`, expecting the first sequence number.
    #[must_use]
    pub const fn new(edge: EdgeId) -> Self {
        Self {
            edge,
            next: Seq::FIRST,
        }
    }

    /// The edge this sequencer accepts.
    #[must_use]
    pub const fn edge(&self) -> EdgeId {
        self.edge
    }

    /// Validate and consume one envelope's addressing. On success the
    /// expected sequence advances.
    ///
    /// # Errors
    ///
    /// Rejects wrong-edge (stale generation or misrouted connection),
    /// replayed/duplicated, and gapped deliveries.
    pub fn accept(&mut self, edge: EdgeId, seq: Seq) -> Result<(), EdgeViolation> {
        if edge != self.edge {
            return Err(EdgeViolation::WrongEdge {
                expected: self.edge,
                got: edge,
            });
        }
        if seq < self.next {
            return Err(EdgeViolation::Replayed { seq });
        }
        if seq > self.next {
            return Err(EdgeViolation::Gap {
                expected: self.next,
                got: seq,
            });
        }
        self.next = self.next.next();
        Ok(())
    }
}

/// Why an edge message was rejected (driver/router bug classes, made
/// observable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EdgeViolation {
    /// Stale generation or misrouted connection.
    #[error("wrong edge: expected {expected:?}, got {got:?}")]
    WrongEdge {
        /// The edge this sequencer accepts.
        expected: EdgeId,
        /// The edge on the envelope.
        got: EdgeId,
    },

    /// Sequence number already consumed (duplicate delivery).
    #[error("replayed edge message: seq {seq:?}")]
    Replayed {
        /// The replayed sequence number.
        seq: Seq,
    },

    /// Sequence number skipped ahead (lost message in between).
    #[error("edge gap: expected {expected:?}, got {got:?}")]
    Gap {
        /// The next expected sequence number.
        expected: Seq,
        /// The sequence number that arrived.
        got: Seq,
    },
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
        /// The challenge's signed wall-clock timestamp (bucketing key —
        /// legacy parity: bucket by message time, not arrival time).
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

#[cfg(test)]
mod tests {
    use super::*;

    const fn edge(conn: u64, generation: Generation) -> EdgeId {
        EdgeId {
            conn: ConnId::new(conn),
            generation,
        }
    }

    #[test]
    fn sequencer_accepts_in_order_only() {
        let e = edge(1, Generation::FIRST);
        let mut sequencer = EdgeSequencer::new(e);

        assert_eq!(sequencer.accept(e, Seq::FIRST), Ok(()));
        let second = Seq::FIRST.next();
        assert_eq!(sequencer.accept(e, second), Ok(()));

        // Replay of the first message.
        assert_eq!(
            sequencer.accept(e, Seq::FIRST),
            Err(EdgeViolation::Replayed { seq: Seq::FIRST })
        );

        // Gap: skipping ahead.
        let far = second.next().next();
        assert_eq!(
            sequencer.accept(e, far),
            Err(EdgeViolation::Gap {
                expected: second.next(),
                got: far
            })
        );
    }

    #[test]
    fn sequencer_rejects_stale_generation_and_misrouting() {
        let current = edge(1, Generation::FIRST.next());
        let mut sequencer = EdgeSequencer::new(current);

        // Old incarnation of the same connection.
        let stale = edge(1, Generation::FIRST);
        assert!(matches!(
            sequencer.accept(stale, Seq::FIRST),
            Err(EdgeViolation::WrongEdge { .. })
        ));

        // A different connection entirely (router misdelivery).
        let other = edge(2, Generation::FIRST.next());
        assert!(matches!(
            sequencer.accept(other, Seq::FIRST),
            Err(EdgeViolation::WrongEdge { .. })
        ));

        // The real thing still works afterwards.
        assert_eq!(sequencer.accept(current, Seq::FIRST), Ok(()));
    }

    #[cfg(all(test, feature = "std", feature = "bolero"))]
    mod proptests {
        use super::*;

        /// Whatever (edge, seq) stream arrives, the sequencer accepts
        /// exactly the in-order prefix of the correct edge and nothing
        /// else — acceptance count equals the length of the correctly
        /// sequenced prefix delivered.
        #[test]
        fn prop_exactly_once_in_order() {
            bolero::check!()
                .with_type::<alloc::vec::Vec<(u8, u8)>>()
                .for_each(|stream| {
                    let e = edge(1, Generation::FIRST);
                    let mut sequencer = EdgeSequencer::new(e);
                    let mut accepted = 0u64;
                    for (conn, seq) in stream {
                        let candidate = edge(u64::from(*conn % 2), Generation::FIRST);
                        let mut s = Seq::FIRST;
                        for _ in 0..(*seq % 8) {
                            s = s.next();
                        }
                        if sequencer.accept(candidate, s).is_ok() {
                            accepted += 1;
                            assert_eq!(candidate, e, "only the right edge is accepted");
                        }
                    }
                    // The next expected seq equals the number accepted:
                    // no skips, no double-counts.
                    assert_eq!(sequencer.next.as_u64(), accepted);
                });
        }
    }
}
