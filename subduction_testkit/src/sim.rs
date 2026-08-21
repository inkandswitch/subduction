//! Deterministic simulation testing (DST): a seeded scheduler over a
//! [`Net`] of nodes.
//!
//! Every source of nondeterminism — which message is delivered next,
//! when a storage completion lands, how much time passes — is drawn
//! from one PRNG seeded by a single `u64`. Same seed ⇒ bit-identical
//! run, recorded as a [`Choice`] journal. A failing seed is a perfect
//! reproduction; property tests sweep seeds.
//!
//! ```text
//!        seed ─▶ splitmix64 ─▶ pick one eligible work item per step
//!
//!   eligible work =  oldest undelivered message per (node, conn)
//!                    (transports are ordered streams — per-conn FIFO
//!                     is preserved; everything else interleaves)
//!                 ∪  every deferred storage completion, any order
//!                 ±  bounded random clock advances (wakes)
//! ```
//!
//! Signing stays inline: custody is connection-local and handshake
//! interleavings are already fuzzed by the adversarial property suite.

use subduction_protocol::{id::ConnId, ticket::StorageTicket};

use crate::{Net, TestError};

/// One scheduling decision, journaled for replay/debugging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    /// Deliver the oldest undelivered message on `(from, conn)`.
    Deliver {
        /// Sending node index.
        from: usize,
        /// Sending connection.
        conn: ConnId,
        /// Receiving node index.
        to: usize,
        /// Receiving connection.
        to_conn: ConnId,
        /// Message length (a cheap content fingerprint for journal diffs).
        len: usize,
    },

    /// Complete one deferred storage op.
    Storage {
        /// The node whose driver completes the op.
        node: usize,
        /// The op's ticket (identifies it in journal diffs).
        ticket: StorageTicket,
    },

    /// Advance one node's clock and deliver a wake.
    Advance {
        /// The node whose clock moves.
        node: usize,
        /// Milliseconds advanced.
        ms: u64,
    },
}

/// splitmix64: tiny, dependency-free, deterministic across platforms.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    const fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[allow(clippy::cast_possible_truncation)] // len < 2^32 in practice
    const fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Per-node cap on random clock advancement, so deadlines (30 s
/// handshake/sync windows) never fire spuriously mid-scenario.
const MAX_TOTAL_ADVANCE_MS: u64 = 10_000;

/// Max milliseconds per single advance.
const MAX_STEP_ADVANCE_MS: u64 = 50;

/// A seeded scheduler over a [`Net`]. Build the topology on
/// [`net`](Self::net) (drivers should set `defer_storage`), then
/// [`run`](Self::run) to quiescence between scenario phases.
#[derive(Debug)]
pub struct Sim {
    /// The simulated network (topology, drivers, invariant checks).
    pub net: Net,
    rng: SplitMix64,
    journal: Vec<Choice>,
    advanced_ms: Vec<u64>,
}

impl Sim {
    /// A simulator over `net`, all nondeterminism drawn from `seed`.
    /// Flips every driver into deferred-storage mode.
    #[must_use]
    pub fn new(seed: u64, mut net: Net) -> Self {
        for driver in &mut net.drivers {
            driver.defer_storage = true;
        }
        let advanced_ms = vec![0; net.drivers.len()];
        Self {
            net,
            rng: SplitMix64(seed),
            journal: Vec::new(),
            advanced_ms,
        }
    }

    /// The scheduling decisions made so far.
    #[must_use]
    pub fn journal(&self) -> &[Choice] {
        &self.journal
    }

    /// Run until no work remains. Returns the number of steps taken.
    ///
    /// # Errors
    /// Fails if the world does not quiesce within `max_steps` (a thrash
    /// loop) or any data-plane invariant breaks during execution.
    pub fn run(&mut self, max_steps: usize) -> Result<usize, TestError> {
        for step in 0..max_steps {
            // Occasionally let time pass at a random node (bounded).
            if self.rng.next().is_multiple_of(8) {
                let node = self.rng.below(self.net.drivers.len());
                let advanced = self.advanced_ms.get_mut(node).ok_or("advance index")?;
                let budget = MAX_TOTAL_ADVANCE_MS.saturating_sub(*advanced);
                let ms = (self.rng.next() % MAX_STEP_ADVANCE_MS).min(budget);
                if ms > 0 {
                    *advanced += ms;
                    self.journal.push(Choice::Advance { node, ms });
                    self.net.driver_mut(node).advance(ms)?;
                }
            }

            let work = self.eligible_work();
            if work.is_empty() {
                return Ok(step);
            }
            let choice = work
                .get(self.rng.below(work.len()))
                .copied()
                .ok_or("work index")?;
            self.execute(&choice)?;
            self.journal.push(choice);
        }
        Err("simulation did not quiesce (thrash loop?)".into())
    }

    /// Everything schedulable right now: the oldest undelivered message
    /// per `(node, conn)` endpoint, plus every deferred storage op.
    fn eligible_work(&self) -> Vec<Choice> {
        let mut work = Vec::new();
        for (idx, driver) in self.net.drivers.iter().enumerate() {
            let mut seen_conns = std::collections::BTreeSet::new();
            for (conn, bytes) in &driver.outbox {
                if !seen_conns.insert(*conn) {
                    continue; // per-conn FIFO: only the oldest is eligible
                }
                if self.net.dropped.contains(&(idx, *conn)) {
                    continue;
                }
                if let Some((to, to_conn)) = self.net.links.get(&(idx, *conn)) {
                    work.push(Choice::Deliver {
                        from: idx,
                        conn: *conn,
                        to: *to,
                        to_conn: *to_conn,
                        len: bytes.len(),
                    });
                }
            }
            for (ticket, _op) in &driver.pending_storage {
                work.push(Choice::Storage {
                    node: idx,
                    ticket: *ticket,
                });
            }
        }
        work
    }

    fn execute(&mut self, choice: &Choice) -> Result<(), TestError> {
        match choice {
            Choice::Deliver {
                from,
                conn,
                to,
                to_conn,
                ..
            } => {
                let driver = self.net.driver_mut(*from);
                let position = driver
                    .outbox
                    .iter()
                    .position(|(c, _)| c == conn)
                    .ok_or("scheduled message vanished")?;
                let (_conn, bytes) = driver.outbox.remove(position);
                self.net.driver_mut(*to).deliver_on(*to_conn, bytes)
            }
            Choice::Storage { node, ticket } => {
                let driver = self.net.driver_mut(*node);
                let position = driver
                    .pending_storage
                    .iter()
                    .position(|(t, _)| t == ticket)
                    .ok_or("scheduled storage op vanished")?;
                driver.complete_storage(position)
            }
            // Advances are executed inline in `run` (they are generated,
            // not picked from the work pool).
            Choice::Advance { .. } => Ok(()),
        }
    }
}
