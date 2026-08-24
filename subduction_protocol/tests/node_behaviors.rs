//! Behavioral parity suite at node level: everything the old
//! single-machine tests proved, re-proven against the full Design-D
//! pipeline (`ConnMachine`s + `CoreMachine` + router) over an in-memory
//! network. Every test also holds the data-plane no-leak invariant.

use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use subduction_protocol::{
    command::Command,
    effect::{AppEvent, SyncStatus},
    node::NodeEvent,
};
use subduction_testkit::{TestError, ensure, net::Net};
use testresult::TestResult;

fn sync_tree(
    net: &mut Net,
    node: usize,
    conn: subduction_protocol::id::ConnId,
    tree: SedimentreeId,
    subscribe: bool,
) -> Result<(), TestError> {
    net.driver_mut(node)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn,
            tree,
            subscribe,
        }))?;
    let _messages = net.pump()?;
    Ok(())
}

fn tree_updated_count(net: &Net, node: usize, tree: SedimentreeId) -> usize {
    net.driver(node)
        .app
        .iter()
        .filter(|e| matches!(e, AppEvent::TreeUpdated { tree: t, .. } if *t == tree))
        .count()
}

// ── forwarding & damping ────────────────────────────────────────────

/// Legacy `remote_ingest_is_forwarded_to_other_subscribers`, now across
/// three real nodes: a push into the hub fans out to the other
/// subscriber but never echoes to its source.
#[test]
fn hub_forwards_remote_ingest_to_other_subscribers() -> TestResult {
    let tree = SedimentreeId::new([7u8; 32]);
    let mut net = Net::new(&[1, 2, 3]);
    let (hub, b, c) = (0, 1, 2);
    let (cb, _) = net.connect(b, hub)?;
    let (cc, _) = net.connect(c, hub)?;

    // The hub holds the tree (mutual subscription only forms on an Ok
    // response — pinned legacy semantics; NotFound does not subscribe).
    net.driver_mut(hub).add_commit(tree, 0xA0)?;

    // Mutual subscriptions via sync+subscribe.
    sync_tree(&mut net, b, cb, tree, true)?;
    sync_tree(&mut net, c, cc, tree, true)?;

    // B writes locally: push to hub (its subscriber), hub forwards to C.
    net.driver_mut(b).add_commit(tree, 0xB1)?;
    let _messages = net.pump()?;

    let mut expected = vec![CommitId::new([0xA0; 32]), CommitId::new([0xB1; 32])];
    expected.sort();
    assert_eq!(net.driver(hub).stored_commit_ids(tree), expected, "hub");
    assert_eq!(net.driver(c).stored_commit_ids(tree), expected, "c");
    // The hub persisted B's push exactly once (no echo back through B),
    // and C saw one persist from the initial sync plus one forward.
    assert_eq!(tree_updated_count(&net, hub, tree), 1);
    assert_eq!(tree_updated_count(&net, c, tree), 2);

    net.check_no_leaks()?;
    Ok(())
}

/// The #281 regression at full-pipeline scale: a fully-meshed triangle
/// with mutual subscriptions everywhere must converge and QUIESCE —
/// the freshness gate is the damping factor that prevents the forward
/// storm. Without it, `pump` would thrash to its round limit.
#[test]
fn meshed_triangle_converges_and_damps() -> TestResult {
    let tree = SedimentreeId::new([8u8; 32]);
    let mut net = Net::new(&[1, 2, 3]);
    let (_c01, c10) = net.connect(0, 1)?;
    let (_c02, c20) = net.connect(0, 2)?;
    let (_c12, c21) = net.connect(1, 2)?;

    // Node 0 holds the tree; everyone else subscribes along Ok
    // responses until the mesh is mutually subscribed everywhere.
    net.driver_mut(0).add_commit(tree, 0xA0)?;
    sync_tree(&mut net, 1, c10, tree, true)?;
    sync_tree(&mut net, 2, c20, tree, true)?;
    sync_tree(&mut net, 2, c21, tree, true)?;

    net.driver_mut(0).add_commit(tree, 0xA1)?;
    let delivered = net.pump()?;

    let mut expected = vec![CommitId::new([0xA0; 32]), CommitId::new([0xA1; 32])];
    expected.sort();
    for i in 0..3 {
        assert_eq!(net.driver(i).stored_commit_ids(tree), expected, "node {i}");
    }
    // Damping bound: pushes + acks + duplicate-suppressed forwards for
    // one commit across a triangle stay small. A thrash loop explodes
    // this number (or never quiesces at all).
    ensure(
        delivered <= 12,
        &format!("expected a damped forward storm, delivered {delivered} messages"),
    )?;

    // Nothing further moves.
    let residual = net.pump()?;
    assert_eq!(residual, 0);
    net.check_no_leaks()?;
    Ok(())
}

// ── subscriptions ───────────────────────────────────────────────────

#[test]
fn unsubscribe_stops_pushes() -> TestResult {
    let tree = SedimentreeId::new([9u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;

    sync_tree(&mut net, 1, cb, tree, true)?;
    net.driver_mut(0).add_commit(tree, 0xA1)?;
    let _messages = net.pump()?;
    assert_eq!(
        net.driver(1).stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])],
        "subscribed: push arrives"
    );

    net.driver_mut(1)
        .feed(NodeEvent::Command(Command::Unsubscribe {
            conn: cb,
            trees: vec![tree],
        }))?;
    let _messages = net.pump()?;

    net.driver_mut(0).add_commit(tree, 0xA2)?;
    let _messages = net.pump()?;
    assert_eq!(
        net.driver(1).stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])],
        "unsubscribed: no further pushes"
    );

    net.check_no_leaks()?;
    Ok(())
}

// ── fragments ───────────────────────────────────────────────────────

#[test]
fn fragments_sync_and_push() -> TestResult {
    let tree = SedimentreeId::new([10u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;

    // A has a commit and a fragment before B ever syncs.
    net.driver_mut(0).add_commit(tree, 0xA1)?;
    net.driver_mut(0).add_fragment(tree, 0xF1)?;
    sync_tree(&mut net, 1, cb, tree, true)?;

    assert_eq!(
        net.driver(1).stored_commit_ids(tree),
        vec![CommitId::new([0xA1; 32])]
    );
    assert_eq!(
        net.driver(1).stored_fragment_heads(tree),
        vec![CommitId::new([0xF1; 32])],
        "batch sync carries fragments"
    );

    // A new local fragment reaches the subscriber via the push path.
    net.driver_mut(0).add_fragment(tree, 0xF2)?;
    let _messages = net.pump()?;
    let mut expected = vec![CommitId::new([0xF1; 32]), CommitId::new([0xF2; 32])];
    expected.sort();
    assert_eq!(
        net.driver(1).stored_fragment_heads(tree),
        expected,
        "fragment push"
    );

    net.check_no_leaks()?;
    Ok(())
}

// ── sync edge cases ─────────────────────────────────────────────────

#[test]
fn sync_of_unknown_tree_reports_not_found() -> TestResult {
    let tree = SedimentreeId::new([11u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;

    sync_tree(&mut net, 1, cb, tree, false)?;
    assert!(net.driver(1).app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished {
            status: SyncStatus::NotFound,
            ..
        }
    )));
    net.check_no_leaks()?;
    Ok(())
}

#[test]
fn syncing_a_tree_we_lack_pulls_everything() -> TestResult {
    let tree = SedimentreeId::new([12u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;

    for head in [0xA1, 0xA2, 0xA3] {
        net.driver_mut(0).add_commit(tree, head)?;
    }
    net.driver_mut(0).add_fragment(tree, 0xF1)?;

    sync_tree(&mut net, 1, cb, tree, false)?;

    let mut expected = vec![
        CommitId::new([0xA1; 32]),
        CommitId::new([0xA2; 32]),
        CommitId::new([0xA3; 32]),
    ];
    expected.sort();
    assert_eq!(net.driver(1).stored_commit_ids(tree), expected);
    assert_eq!(
        net.driver(1).stored_fragment_heads(tree),
        vec![CommitId::new([0xF1; 32])]
    );
    net.check_no_leaks()?;
    Ok(())
}

#[test]
fn sync_request_times_out_without_response() -> TestResult {
    let tree = SedimentreeId::new([13u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;

    // The request vanishes on the wire.
    net.drop_from(1, cb);
    net.driver_mut(1)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn: cb,
            tree,
            subscribe: false,
        }))?;
    let _messages = net.pump()?;

    net.driver_mut(1).advance(31_000)?;
    assert!(net.driver(1).app.iter().any(|e| matches!(
        e,
        AppEvent::SyncFinished {
            status: SyncStatus::TimedOut,
            ..
        }
    )));
    net.check_no_leaks()?;
    Ok(())
}

// ── extension protocol────────────────────────────────────

#[test]
fn extension_messages_round_trip_post_handshake() -> TestResult {
    let mut net = Net::new(&[1, 2]);
    let (ca, _) = net.connect(0, 1)?;

    let payload = b"EXT1hello subduction".to_vec();
    net.driver_mut(0)
        .feed(NodeEvent::Command(Command::SendExtension {
            conn: ca,
            bytes: payload.clone(),
        }))?;
    let _messages = net.pump()?;

    let a_peer = net.driver(0).peer_id();
    assert!(
        net.driver(1).app.iter().any(|e| matches!(
            e,
            AppEvent::ExtensionMessage { peer, bytes, .. }
                if *peer == a_peer && *bytes == payload
        )),
        "extension message surfaces at the peer with sender identity"
    );
    net.check_no_leaks()?;
    Ok(())
}

#[test]
fn extension_send_is_gated_pre_handshake() -> TestResult {
    let mut net = Net::new(&[1, 2]);
    // Wire up transport but never run the handshake to completion:
    // node 0 dials, node 1 never even learns of the connection.
    let ca = net.driver_mut(0).alloc_conn();
    let peer_b = net.driver(1).peer_id();
    net.driver_mut(0).feed(NodeEvent::Connected {
        conn: ca,
        direction: subduction_protocol::event::Direction::Outbound,
        audience: Some(subduction_protocol::handshake::audience::Audience::known(
            peer_b,
        )),
    })?;

    net.driver_mut(0)
        .feed(NodeEvent::Command(Command::SendExtension {
            conn: ca,
            bytes: b"EXT1too early".to_vec(),
        }))?;

    // Only handshake traffic may be queued; no extension bytes leave.
    let queued = net.take_outbox(0);
    ensure(
        queued
            .iter()
            .all(|(_, bytes)| !bytes.windows(9).any(|w| w == b"too early")),
        "extension payload must not leave before authentication",
    )?;
    Ok(())
}

// ── simultaneous open ───────────────────────────────────────────────

#[test]
fn simultaneous_open_authenticates_both_sides() -> TestResult {
    let mut net = Net::new(&[1, 2]);
    let (ca, cb) = net.connect_simopen(0, 1)?;

    let a_peer = net.driver(0).peer_id();
    let b_peer = net.driver(1).peer_id();
    ensure(
        net.driver(0).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::PeerAuthenticated { conn, peer } if *conn == ca && *peer == b_peer
            )
        }),
        "initiator A authenticates B",
    )?;
    ensure(
        net.driver(1).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::PeerAuthenticated { conn, peer } if *conn == cb && *peer == a_peer
            )
        }),
        "initiator B authenticates A",
    )?;
    net.check_no_leaks()?;
    Ok(())
}

// ── adversarial delivery ────────────────────────────────────────────

/// Forgery gate: a push whose bytes were tampered anywhere must never
/// reach storage. This is machine code on every platform, so
/// it must hold at the node boundary with zero driver cooperation.
#[test]
fn tampered_push_never_persists() -> TestResult {
    let tree = SedimentreeId::new([14u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, chub) = net.connect(1, 0)?;
    net.driver_mut(0).add_commit(tree, 0xA0)?;
    sync_tree(&mut net, 1, cb, tree, true)?;

    // Node 1 mints a legitimate push destined for node 0…
    net.driver_mut(1).add_commit(tree, 0xB1)?;
    let frames = net.take_outbox(1);
    ensure(!frames.is_empty(), "expected a queued push")?;

    // …but every delivered copy is corrupted at a different offset.
    for (i, (_, bytes)) in frames.iter().enumerate() {
        for position in [bytes.len() - 1, bytes.len() / 2, 8] {
            let mut tampered = bytes.clone();
            let byte = tampered.get_mut(position).ok_or("position in bounds")?;
            *byte ^= 0x01;
            // Outcome may be ignore-or-disconnect; it must never persist.
            let _result = net.driver_mut(0).deliver_on(chub, tampered);
            let _ = i;
        }
    }
    let _messages = net.pump()?;

    ensure(
        net.driver(0).stored_commit_ids(tree) == vec![CommitId::new([0xA0; 32])],
        "tampered items must never reach storage",
    )?;
    assert_eq!(tree_updated_count(&net, 0, tree), 0);
    Ok(())
}

/// Replay of a legitimate push is idempotent: stored once, forwarded
/// once (freshness damping), and the network stays quiet.
#[test]
fn replayed_push_is_idempotent() -> TestResult {
    let tree = SedimentreeId::new([15u8; 32]);
    let mut net = Net::new(&[1, 2, 3]);
    let (hub, b, c) = (0, 1, 2);
    let (cb, chub_b) = net.connect(b, hub)?;
    let (cc, _) = net.connect(c, hub)?;
    net.driver_mut(hub).add_commit(tree, 0xA0)?;
    sync_tree(&mut net, b, cb, tree, true)?;
    sync_tree(&mut net, c, cc, tree, true)?;

    net.driver_mut(b).add_commit(tree, 0xB1)?;
    let frames = net.take_outbox(b);
    ensure(!frames.is_empty(), "expected a queued push")?;

    // Deliver every captured frame to the hub twice.
    for (_, bytes) in &frames {
        net.driver_mut(hub).deliver_on(chub_b, bytes.clone())?;
    }
    for (_, bytes) in &frames {
        net.driver_mut(hub).deliver_on(chub_b, bytes.clone())?;
    }
    let _messages = net.pump()?;

    let mut expected = vec![CommitId::new([0xA0; 32]), CommitId::new([0xB1; 32])];
    expected.sort();
    assert_eq!(
        net.driver(hub).stored_commit_ids(tree),
        expected,
        "stored once"
    );
    // C persisted twice total: the initial sync pull plus exactly ONE
    // forward — the replayed copy was damped by the freshness gate.
    assert_eq!(
        tree_updated_count(&net, c, tree),
        2,
        "forwarded to the other subscriber exactly once"
    );
    net.check_no_leaks()?;
    Ok(())
}

/// A malformed sync-schema frame is a protocol violation: the node must
/// ask the driver to close the connection.
#[test]
fn malformed_sync_frame_disconnects() -> TestResult {
    let mut net = Net::new(&[1, 2]);
    let (_, cb) = net.connect(0, 1)?;

    let mut garbage = b"SUM\0".to_vec();
    garbage.extend_from_slice(&[0xFF; 32]);
    net.driver_mut(1).deliver_on(cb, garbage)?;

    ensure(
        net.driver(1).disconnects.contains(&cb),
        "malformed sync frame must disconnect",
    )?;
    Ok(())
}

// ── handshake variants (ported from the old machine suite) ─────────

#[test]
fn discovery_audience_handshake_completes() -> TestResult {
    let discovery =
        subduction_protocol::handshake::audience::Audience::discover(b"sync.example.com");
    let mut net = Net::from_drivers(vec![
        subduction_testkit::driver::TestDriver::new(1),
        subduction_testkit::driver::TestDriver::with_discovery(2, Some(discovery)),
    ]);

    let (ca, _cb) = net.connect_with_audience(0, 1, discovery)?;
    let b_peer = net.driver(1).peer_id();
    ensure(
        net.driver(0).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::PeerAuthenticated { conn, peer } if *conn == ca && *peer == b_peer
            )
        }),
        "discovery dial must authenticate the responder",
    )?;
    net.check_no_leaks()?;
    Ok(())
}

#[test]
fn dialing_the_wrong_peer_never_authenticates() -> TestResult {
    let mut net = Net::new(&[1, 2]);
    // Node 0 dials node 1 but pins a DIFFERENT identity.
    let mallory = subduction_protocol::peer_id::PeerId::new([0xEE; 32]);
    let audience = subduction_protocol::handshake::audience::Audience::known(mallory);

    let (_ca, _cb) = net.connect_with_audience(0, 1, audience)?;
    ensure(
        !net.driver(0)
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { .. })),
        "pinned mismatch must never authenticate",
    )?;
    ensure(
        !net.driver(1)
            .app
            .iter()
            .any(|e| matches!(e, AppEvent::PeerAuthenticated { .. })),
        "responder must not authenticate a dialer who addressed someone else",
    )?;
    Ok(())
}

// ── lagging subscribers (pause + resync) ───────────────────

/// A subscriber whose acks stop coming gets paused after the credit
/// limit, with a `SubscriberLagging` event and a `HeadsUpdate` nudge —
/// and recovers fully via one re-sync once its link heals.
#[test]
fn lagging_subscriber_is_paused_then_recovers_by_resync() -> TestResult {
    let tree = SedimentreeId::new([16u8; 32]);
    let mut net = Net::from_drivers(vec![
        subduction_testkit::driver::TestDriver::custom(1, |c| c.max_outstanding_pushes = 3),
        subduction_testkit::driver::TestDriver::new(2),
    ]);
    let (publisher, subscriber) = (0, 1);
    let (cs, cp) = net.connect(subscriber, publisher)?;

    net.driver_mut(publisher).add_commit(tree, 0xA0)?;
    sync_tree(&mut net, subscriber, cs, tree, true)?;

    // The subscriber goes silent (acks vanish on the wire).
    net.drop_from(subscriber, cs);

    // Push past the credit limit.
    for head in [0xA1, 0xA2, 0xA3, 0xA4, 0xA5] {
        net.driver_mut(publisher).add_commit(tree, head)?;
        let _messages = net.pump()?;
    }

    ensure(
        net.driver(publisher).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::SubscriberLagging { conn, tree: t } if *conn == cp && *t == tree
            )
        }),
        "publisher must report the lagging subscriber",
    )?;
    // Pushes stopped: the subscriber is missing at least one commit.
    ensure(
        net.driver(subscriber).stored_commit_ids(tree).len() < 6,
        "paused subscriber must have missed pushes",
    )?;

    // The link heals; the subscriber re-syncs (as the nudge directs).
    net.restore_from(subscriber, cs);
    sync_tree(&mut net, subscriber, cs, tree, true)?;

    let mut expected: Vec<CommitId> = [0xA0u8, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5]
        .iter()
        .map(|b| CommitId::new([*b; 32]))
        .collect();
    expected.sort();
    assert_eq!(
        net.driver(subscriber).stored_commit_ids(tree),
        expected,
        "one re-sync fully recovers the paused subscriber"
    );

    // And the subscription is live again: a fresh push arrives.
    net.driver_mut(publisher).add_commit(tree, 0xA6)?;
    let _messages = net.pump()?;
    ensure(
        net.driver(subscriber)
            .stored_commit_ids(tree)
            .contains(&CommitId::new([0xA6; 32])),
        "re-subscription must be live after recovery",
    )?;

    net.check_no_leaks()?;
    Ok(())
}

// ── time discipline ─────────────────────────────────────────────────

/// A regressed driver clock (suspend/resume, broken monotonic source)
/// must never fire deadlines early: the node clamps to its high-water
/// mark, so a sync requested "in the past" still gets its full window.
#[test]
fn regressed_clock_never_fires_deadlines_early() -> TestResult {
    let tree = SedimentreeId::new([17u8; 32]);
    let mut net = Net::new(&[1, 2]);
    let (cb, _) = net.connect(1, 0)?;
    net.driver_mut(0).add_commit(tree, 0xA1)?;

    // Handshake happened at a large clock value…
    net.driver_mut(1).clock_ms = 1_000_000;
    net.driver_mut(1).feed(NodeEvent::Wake)?;

    // …then the driver's clock regresses to zero and a sync (whose
    // request never gets a response) is issued at the "old" time.
    net.drop_from(1, cb);
    net.driver_mut(1).clock_ms = 0;
    net.driver_mut(1)
        .feed(NodeEvent::Command(Command::SyncTree {
            conn: cb,
            tree,
            subscribe: false,
        }))?;

    // A wake "40s later" by the broken clock is still BEFORE the
    // clamped request deadline (1_000_000 + 30_000): no timeout.
    net.driver_mut(1).clock_ms = 40_000;
    net.driver_mut(1).feed(NodeEvent::Wake)?;
    ensure(
        !net.driver(1).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::SyncFinished {
                    status: SyncStatus::TimedOut,
                    ..
                }
            )
        }),
        "clamped clock must not fire the sync deadline early",
    )?;

    // Once real time passes the clamped deadline, the timeout fires.
    net.driver_mut(1).clock_ms = 1_031_000;
    net.driver_mut(1).feed(NodeEvent::Wake)?;
    ensure(
        net.driver(1).app.iter().any(|e| {
            matches!(
                e,
                AppEvent::SyncFinished {
                    status: SyncStatus::TimedOut,
                    ..
                }
            )
        }),
        "deadline fires at the clamped time",
    )?;
    Ok(())
}
