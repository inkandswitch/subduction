//! Receiving-side edge discipline: in-order, exactly-once delivery.

use crate::id::Seq;

use super::EdgeId;

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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{ConnId, Generation};

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
