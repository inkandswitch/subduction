//! An in-memory network of nodes with point-to-point links.
//!
//! [`Net`] wires N [`TestDriver`]s together: `connect` runs a real
//! handshake by shuttling outboxes, `pump` delivers messages until the
//! network quiesces, and `drop_from`/`restore_from` simulate link
//! failures.

use std::collections::{BTreeMap, BTreeSet};

use subduction_protocol::{
    effect::AppEvent, event::Direction, handshake::audience::Audience, id::ConnId,
    node::NodeEvent,
};

use crate::{TestError, driver::TestDriver, ensure};

/// An in-memory network of nodes with point-to-point links.
pub struct Net {
    pub(crate) drivers: Vec<TestDriver>,
    /// (node index, local conn) → (peer index, peer's conn).
    pub(crate) links: BTreeMap<(usize, ConnId), (usize, ConnId)>,
    /// Endpoints whose outgoing messages are silently discarded.
    pub(crate) dropped: BTreeSet<(usize, ConnId)>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net")
            .field("drivers", &self.drivers)
            .field("links", &self.links)
            .field("dropped", &self.dropped)
            .finish()
    }
}

impl Net {
    /// One driver per seed (see [`TestDriver::new`]).
    #[must_use]
    pub fn new(seeds: &[u8]) -> Self {
        Self::from_drivers(seeds.iter().map(|s| TestDriver::new(*s)).collect())
    }

    /// A network over pre-built drivers (custom discovery, etc.).
    #[must_use]
    pub const fn from_drivers(drivers: Vec<TestDriver>) -> Self {
        Self {
            drivers,
            links: BTreeMap::new(),
            dropped: BTreeSet::new(),
        }
    }

    /// Shared access to node `i`'s driver.
    ///
    /// # Panics
    /// Panics when `i` is out of range — harness misuse, i.e. a test bug.
    #[must_use]
    pub fn driver(&self, i: usize) -> &TestDriver {
        #[allow(clippy::indexing_slicing)] // out-of-range = test bug
        &self.drivers[i]
    }

    /// Mutable access to node `i`'s driver.
    ///
    /// # Panics
    /// Panics when `i` is out of range — harness misuse, i.e. a test bug.
    #[must_use]
    pub fn driver_mut(&mut self, i: usize) -> &mut TestDriver {
        #[allow(clippy::indexing_slicing)] // out-of-range = test bug
        &mut self.drivers[i]
    }

    /// Connect `i` (initiator) to `j` (responder) and run the handshake
    /// to completion. Returns `(i's conn, j's conn)`.
    ///
    /// # Errors
    /// Fails when the handshake does not complete.
    pub fn connect(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        let peer_j = self.driver(j).peer_id();
        let (ci, cj) = self.connect_with_audience(i, j, Audience::known(peer_j))?;
        ensure(
            self.driver(i)
                .app
                .iter()
                .any(|e| matches!(e, AppEvent::PeerAuthenticated { conn, .. } if *conn == ci)),
            "handshake must complete",
        )?;
        Ok((ci, cj))
    }

    /// Like [`connect`](Self::connect) with an explicit dialed audience,
    /// and WITHOUT asserting the handshake succeeded — rejection paths
    /// use this.
    ///
    /// # Errors
    /// Propagates pump failures.
    pub fn connect_with_audience(
        &mut self,
        i: usize,
        j: usize,
        audience: Audience,
    ) -> Result<(ConnId, ConnId), TestError> {
        let (ci, cj) = self.wire(i, j)?;
        self.driver_mut(i).feed(NodeEvent::Connected {
            conn: ci,
            direction: Direction::Outbound,
            audience: Some(audience),
        })?;
        self.driver_mut(j).feed(NodeEvent::Connected {
            conn: cj,
            direction: Direction::Inbound,
            audience: None,
        })?;
        let _messages = self.pump()?;
        Ok((ci, cj))
    }

    /// Both ends dial simultaneously over one transport link (sim-open).
    ///
    /// # Errors
    /// Propagates pump failures.
    pub fn connect_simopen(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        let (ci, cj) = self.wire(i, j)?;
        let peer_i = self.driver(i).peer_id();
        let peer_j = self.driver(j).peer_id();
        self.driver_mut(i).feed(NodeEvent::Connected {
            conn: ci,
            direction: Direction::Outbound,
            audience: Some(Audience::known(peer_j)),
        })?;
        self.driver_mut(j).feed(NodeEvent::Connected {
            conn: cj,
            direction: Direction::Outbound,
            audience: Some(Audience::known(peer_i)),
        })?;
        let _messages = self.pump()?;
        Ok((ci, cj))
    }

    /// Register a bidirectional link between two already-allocated
    /// endpoints without feeding any events (simulator wiring).
    pub fn link(&mut self, i: usize, ci: ConnId, j: usize, cj: ConnId) {
        let _link = self.links.insert((i, ci), (j, cj));
        let _link = self.links.insert((j, cj), (i, ci));
    }

    fn wire(&mut self, i: usize, j: usize) -> Result<(ConnId, ConnId), TestError> {
        ensure(i != j, "cannot wire a node to itself")?;
        let ci = self.driver_mut(i).alloc_conn();
        let cj = self.driver_mut(j).alloc_conn();
        let _link = self.links.insert((i, ci), (j, cj));
        let _link = self.links.insert((j, cj), (i, ci));
        Ok((ci, cj))
    }

    /// Discard future messages sent from endpoint `(i, conn)`.
    pub fn drop_from(&mut self, i: usize, conn: ConnId) {
        let _new = self.dropped.insert((i, conn));
    }

    /// Stop discarding messages from endpoint `(i, conn)`.
    pub fn restore_from(&mut self, i: usize, conn: ConnId) {
        let _removed = self.dropped.remove(&(i, conn));
    }

    /// Shuttle messages until quiescence. Returns messages delivered —
    /// a bound on this is a damping assertion.
    ///
    /// # Errors
    /// Fails when the network does not quiesce (a thrash loop) or a
    /// data-plane invariant breaks during delivery.
    pub fn pump(&mut self) -> Result<usize, TestError> {
        let mut delivered = 0usize;
        for _round in 0..256 {
            let mut queue: Vec<(usize, ConnId, Vec<u8>)> = Vec::new();
            for (idx, driver) in self.drivers.iter_mut().enumerate() {
                for (conn, bytes) in driver.outbox.drain(..) {
                    if self.dropped.contains(&(idx, conn)) {
                        continue;
                    }
                    let Some((target, target_conn)) = self.links.get(&(idx, conn)) else {
                        return Err(format!("no link for node {idx} conn {conn:?}").into());
                    };
                    queue.push((*target, *target_conn, bytes));
                }
            }
            if queue.is_empty() {
                return Ok(delivered);
            }
            for (target, conn, bytes) in queue {
                self.driver_mut(target).deliver_on(conn, bytes)?;
                delivered += 1;
            }
        }
        Err("network did not quiesce (thrash loop?)".into())
    }

    /// Take a node's queued outbound messages without delivering them.
    pub fn take_outbox(&mut self, i: usize) -> Vec<(ConnId, Vec<u8>)> {
        self.driver_mut(i).outbox.drain(..).collect()
    }

    /// [`TestDriver::check_no_leaks`] across every node.
    ///
    /// # Errors
    /// Reports the first leaked frame.
    pub fn check_no_leaks(&self) -> Result<(), TestError> {
        for driver in &self.drivers {
            driver.check_no_leaks()?;
        }
        Ok(())
    }
}
