//! The full Design-D pipeline smoke tests: two Nodes (`ConnMachine`s +
//! Core + router) handshake, converge, and push through subscriptions.
//! The shared harness's frame/blob table enforces the data-plane
//! invariants (no use-after-free, no leak at quiescence) throughout.

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_protocol::{
    command::Command,
    effect::{AppEvent, SyncStatus},
    node::NodeEvent,
};
use subduction_testkit::Net;
use testresult::TestResult;

#[test]
fn full_pipeline_handshake_and_convergence() -> TestResult {
    let tree = SedimentreeId::new([7u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (alice, bob) = (0, 1);
    let (_ca, cb) = net.connect(alice, bob)?;

    // Divergent histories.
    net.driver_mut(alice).add_commit(tree, 0xA1)?;
    net.driver_mut(alice).add_commit(tree, 0xA2)?;
    net.driver_mut(bob).add_commit(tree, 0xB1)?;

    net.driver_mut(bob)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn: cb,
            tree,
            subscribe: false,
        }))?;
    let _messages = net.pump()?;

    assert!(net.driver(bob).app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished {
            status: SyncStatus::Completed,
            ..
        }
    )));

    let mut expected = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xB1; 32]),
    ];
    expected.sort();
    assert_eq!(
        net.driver(alice).stored_commit_ids(tree),
        expected,
        "alice converged"
    );
    assert_eq!(
        net.driver(bob).stored_commit_ids(tree),
        expected,
        "bob converged"
    );

    let mut ah = net
        .driver_mut(alice)
        .node
        .tree_heads(tree)
        .unwrap_or_default();
    let mut bh = net
        .driver_mut(bob)
        .node
        .tree_heads(tree)
        .unwrap_or_default();
    ah.sort();
    bh.sort();
    assert_eq!(ah, bh, "resident heads converged");

    // The data-plane invariant: nothing leaked anywhere.
    net.check_no_leaks()?;
    Ok(())
}

#[test]
fn full_pipeline_subscription_push() -> TestResult {
    let tree = SedimentreeId::new([8u8; 32]);
    let mut net = Net::new(&[3, 4]);
    let (alice, bob) = (0, 1);
    let (_ca, cb) = net.connect(alice, bob)?;

    net.driver_mut(alice).add_commit(tree, 0xA1)?;
    net.driver_mut(bob)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn: cb,
            tree,
            subscribe: true,
        }))?;
    let _messages = net.pump()?;
    assert_eq!(
        net.driver(bob).stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])]
    );

    // A new local commit at Alice reaches Bob via the REAL push path:
    // durability confirmed -> subscriber broadcast with inline blob
    // bytes -> Bob verifies, persists, and acks. No re-sync.
    net.driver_mut(alice).add_commit(tree, 0xA2)?;
    let _messages = net.pump()?;

    let mut expected = vec![CommitId::new([0xA1; 32]), CommitId::new([0xA2; 32])];
    expected.sort();
    assert_eq!(net.driver(bob).stored_commit_ids(tree), expected);

    net.check_no_leaks()?;
    Ok(())
}
