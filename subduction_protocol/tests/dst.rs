//! Deterministic simulation tests: the full Design-D pipeline under
//! seeded random schedules. Delivery interleavings, deferred storage
//! completions, and clock advances are all drawn from a single seed —
//! a failing seed is a perfect reproduction.

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_protocol::{
    command::Command, effect::AppEvent, event::Direction, handshake::audience::Audience,
    node::NodeEvent,
};
use subduction_testkit::{Net, TestError, ensure, sim::Sim};
use testresult::TestResult;

/// Wire two nodes and run the handshake to completion UNDER the sim
/// schedule (sign completions inline, but message interleaving and
/// storage timing are seed-driven).
fn connect_sim(sim: &mut Sim, i: usize, j: usize) -> Result<(), TestError> {
    let peer_j = sim.net.driver(j).peer_id();
    let ci = sim.net.driver_mut(i).alloc_conn();
    let cj = sim.net.driver_mut(j).alloc_conn();
    sim.net.link(i, ci, j, cj);
    sim.net.driver_mut(i).feed(NodeEvent::Connected {
        conn: ci,
        direction: Direction::Outbound,
        audience: Some(Audience::known(peer_j)),
    })?;
    sim.net.driver_mut(j).feed(NodeEvent::Connected {
        conn: cj,
        direction: Direction::Inbound,
        audience: None,
    })?;
    let _steps = sim.run(10_000)?;
    ensure(
        sim.net
            .driver(i)
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { conn, .. } if *conn == ci)),
        "handshake must complete under the sim schedule",
    )
}

// Conn ids are allocated 1, 2, … per driver in wiring order.
const fn conn(n: u64) -> subduction_protocol::id::ConnId {
    subduction_protocol::id::ConnId::new(n)
}

/// The core DST scenario: divergent histories on two nodes converge
/// through one sync under an arbitrary schedule, leaking nothing.
fn two_node_convergence(seed: u64) -> Result<(), TestError> {
    let tree = SedimentreeId::new([21u8; 32]);
    let mut sim = Sim::new(seed, Net::new(&[1, 2]));
    connect_sim(&mut sim, 0, 1)?;

    for head in [0xA1, 0xA2] {
        sim.net.driver_mut(0).add_commit(tree, head)?;
    }
    sim.net.driver_mut(1).add_commit(tree, 0xB1)?;

    sim.net
        .driver_mut(1)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn: conn(1),
            tree,
            subscribe: true,
        }))?;
    let _steps = sim.run(10_000)?;

    let mut expected = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xB1; 32]),
    ];
    expected.sort();
    ensure(
        sim.net.driver(0).stored_commit_ids(tree) == expected,
        &format!("node 0 diverged under seed {seed}"),
    )?;
    ensure(
        sim.net.driver(1).stored_commit_ids(tree) == expected,
        &format!("node 1 diverged under seed {seed}"),
    )?;
    sim.net.check_no_leaks()?;
    Ok(())
}

/// Meshed triangle with concurrent writers everywhere, under an
/// arbitrary schedule: everyone converges to the union, the forward
/// storm damps (quiescence within the step budget), nothing leaks.
fn triangle_convergence(seed: u64) -> Result<(), TestError> {
    let tree = SedimentreeId::new([22u8; 32]);
    let mut sim = Sim::new(seed, Net::new(&[1, 2, 3]));
    connect_sim(&mut sim, 0, 1)?; // node0 conn1 ↔ node1 conn1
    connect_sim(&mut sim, 0, 2)?; // node0 conn2 ↔ node2 conn1
    connect_sim(&mut sim, 1, 2)?; // node1 conn2 ↔ node2 conn2

    // Node 0 seeds the tree; the others sync+subscribe along Ok
    // responses (mutual subscription needs an Ok — pinned semantics).
    sim.net.driver_mut(0).add_commit(tree, 0xA0)?;
    let _steps = sim.run(10_000)?;
    for (node, conn_id) in [(1, 1), (2, 1), (2, 2)] {
        sim.net
            .driver_mut(node)
            .feed(NodeEvent::Command(Command::SyncTree {
                conn: conn(conn_id),
                tree,
                subscribe: true,
            }))?;
        let _steps = sim.run(10_000)?;
    }

    // Concurrent writes at every node, all in flight at once.
    sim.net.driver_mut(0).add_commit(tree, 0xAA)?;
    sim.net.driver_mut(1).add_commit(tree, 0xBB)?;
    sim.net.driver_mut(2).add_commit(tree, 0xCC)?;
    let _steps = sim.run(20_000)?;

    let mut expected = vec![
        CommitId::new([0xA0; 32]),
        CommitId::new([0xAA; 32]),
        CommitId::new([0xBB; 32]),
        CommitId::new([0xCC; 32]),
    ];
    expected.sort();
    for node in 0..3 {
        ensure(
            sim.net.driver(node).stored_commit_ids(tree) == expected,
            &format!("node {node} diverged under seed {seed}"),
        )?;
    }
    sim.net.check_no_leaks()?;
    Ok(())
}

// Panicking is bolero's counterexample-reporting channel inside `for_each`
// closures — `?` cannot escape them; the seed is printed in the message.
#[allow(clippy::panic)]
#[test]
fn dst_two_nodes_converge_under_random_schedules() {
    bolero::check!()
        .with_iterations(64)
        .with_arbitrary::<u64>()
        .for_each(|seed| {
            if let Err(e) = two_node_convergence(*seed) {
                panic!("seed {seed}: {e}");
            }
        });
}

#[allow(clippy::panic)]
#[test]
fn dst_triangle_concurrent_writers_converge_and_damp() {
    bolero::check!()
        .with_iterations(24)
        .with_arbitrary::<u64>()
        .for_each(|seed| {
            if let Err(e) = triangle_convergence(*seed) {
                panic!("seed {seed}: {e}");
            }
        });
}

/// Determinism: the same seed produces the same journal and the same
/// final state — the property that makes any failing seed replayable.
#[test]
fn dst_same_seed_same_journal() -> TestResult {
    let tree = SedimentreeId::new([21u8; 32]);
    let run =
        |seed: u64| -> Result<(Vec<subduction_testkit::sim::Choice>, Vec<CommitId>), TestError> {
            let mut sim = Sim::new(seed, Net::new(&[1, 2]));
            connect_sim(&mut sim, 0, 1)?;
            sim.net.driver_mut(0).add_commit(tree, 0xA1)?;
            sim.net
                .driver_mut(1)
                .feed(NodeEvent::Command(Command::SyncTree {
                    conn: conn(1),
                    tree,
                    subscribe: true,
                }))?;
            let _steps = sim.run(10_000)?;
            Ok((
                sim.journal().to_vec(),
                sim.net.driver(1).stored_commit_ids(tree),
            ))
        };

    let (journal_a, state_a) = run(0x1CEB_00DA)?;
    let (journal_b, state_b) = run(0x1CEB_00DA)?;
    let (journal_c, _state_c) = run(0xDEAD_BEEF)?;

    assert_eq!(journal_a, journal_b, "same seed, same schedule");
    assert_eq!(state_a, state_b, "same seed, same world");
    assert_ne!(
        journal_a, journal_c,
        "different seeds explore different schedules"
    );
    Ok(())
}
