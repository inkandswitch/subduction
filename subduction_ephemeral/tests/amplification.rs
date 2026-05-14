//! Tests probing for ephemeral message amplification / re-echoing on relays.
//!
//! The user-reported hypothesis is that "we only check for the latest hop or
//! something", causing the same message to be processed and re-fanned-out
//! more than once. These tests build small topologies and count how many
//! times a given message is delivered/forwarded, then assert the count is
//! exactly what the protocol claims.

#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_used
)]

use std::{sync::Arc, time::Duration};

use async_lock::Mutex;
use future_form::Sendable;
use nonempty::NonEmpty;
use sedimentree_core::collections::Map;
use subduction_core::{
    authenticated::Authenticated, connection::test_utils::ChannelMockConnection, handler::Handler,
    peer::id::PeerId, timestamp::TimestampSeconds,
};
use subduction_crypto::{
    signed::Signed,
    signer::{Signer, memory::MemorySigner},
};
use subduction_ephemeral::{
    clock::fake::FakeClock,
    config::{EphemeralConfig, EphemeralEvent},
    handler::EphemeralHandler,
    message::{EphemeralMessage, EphemeralPayload},
    policy::OpenEphemeralPolicy,
    topic::Topic,
};
use testresult::TestResult;

type EphConn = ChannelMockConnection<EphemeralMessage>;
type EphAuth = Authenticated<EphConn, Sendable>;
type EphHandle =
    subduction_core::connection::test_utils::ChannelMockConnectionHandle<EphemeralMessage>;
type Connections = Arc<Mutex<Map<PeerId, NonEmpty<EphAuth>>>>;

const TEST_CLOCK_SECS: TimestampSeconds = TimestampSeconds::new(1_000);

const fn peer(n: u8) -> PeerId {
    PeerId::new([n; 32])
}

type OpenHandler = Arc<EphemeralHandler<Sendable, EphConn, OpenEphemeralPolicy, FakeClock>>;

fn make_open_handler(
    connections: Connections,
) -> (OpenHandler, async_channel::Receiver<EphemeralEvent>) {
    let (handler, rx) = EphemeralHandler::new(
        connections,
        OpenEphemeralPolicy,
        EphemeralConfig::default(),
        FakeClock::new(TEST_CLOCK_SECS),
    );
    (Arc::new(handler), rx)
}

async fn register_peer(connections: &Connections, peer_id: PeerId) -> (EphAuth, EphHandle) {
    let (conn, handle) = ChannelMockConnection::new_with_handle(peer_id);
    let auth = Authenticated::new_for_test(conn, peer_id);
    connections
        .lock()
        .await
        .insert(peer_id, NonEmpty::new(auth.clone()));
    (auth, handle)
}

#[allow(clippy::expect_used)]
fn rand_nonce() -> u64 {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).expect("getrandom failed");
    u64::from_le_bytes(buf)
}

async fn make_signed_ephemeral_for(
    signer: &MemorySigner,
    id: Topic,
    nonce: u64,
    payload: Vec<u8>,
) -> EphemeralMessage {
    let ep = EphemeralPayload {
        id,
        nonce,
        timestamp: TEST_CLOCK_SECS,
        payload,
    };
    let verified = Signed::seal::<Sendable, _>(signer, ep).await;
    EphemeralMessage::Ephemeral(Box::new(verified.into_signed()))
}

/// Drain all immediately-available messages from a connection handle.
///
/// `EphemeralHandler::handle` awaits each fan-out send sequentially over
/// the mock's unbounded channel, so by the time the caller reaches
/// `drain`, every message produced by the just-completed handler call is
/// already queued. `try_recv` is therefore non-blocking and not subject
/// to CI-scheduling jitter — replacing an earlier polling drain that
/// could intermittently stop early on slow runners.
fn drain(handle: &EphHandle) -> Vec<EphemeralMessage> {
    let mut out = Vec::new();
    while let Ok(msg) = handle.outbound_rx.try_recv() {
        out.push(msg);
    }
    out
}

/// Count the number of `Ephemeral` (non-Subscribe) messages in `msgs`.
fn count_ephemerals(msgs: &[EphemeralMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, EphemeralMessage::Ephemeral(_)))
        .count()
}

// ── H1: Self-bounce amplification ───────────────────────────────────────
//
// **Reghression**: `EphemeralHandler::publish()` does NOT add the
// published nonce to the nonce cache. If the same message is then
// delivered back to the publishing handler (e.g., a peer relayed it
// through a cycle), the handler re-processes it as if it were a new
// inbound message: delivers it to its own callback _and_ re-fans-out.
//
// This is a textbook "we only check for the latest hop" bug: the
// fan-out filter excludes the previous hop (the bouncing peer) and the
// originator (ourselves), but the bounce is still considered fresh
// because the publish path never seeded the dedup state.
//
// Expected (correct) behaviour:
// - Publish seeds the nonce cache.
// - A bounce-back is detected as a duplicate and dropped.
// - The publisher's own callback channel never fires for its own
//   message (the application already knows what it published).
// - No additional fan-out occurs.
//
// Split into two independent test cases (callback self-echo vs.
// re-fan-out) so both bug surfaces are visible even if one is fixed in
// isolation.

/// Shared setup for H1 callback / re-fanout tests.
///
/// Builds a server with two subscribed peers A and B, then has the
/// server publish a message and drains the legitimate fan-out copies
/// so the tests can observe behaviour from a clean baseline _after_ the
/// initial publish.
async fn setup_publishing_server() -> (
    OpenHandler,
    async_channel::Receiver<EphemeralEvent>,
    EphAuth,
    EphHandle,
    EphHandle,
    EphemeralMessage,
) {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());

    let topic = Topic::new([0x42; 32]);
    let server_signer = MemorySigner::generate();

    let (auth_a, handle_a) = register_peer(&connections, peer(10)).await;
    let (auth_b, handle_b) = register_peer(&connections, peer(11)).await;

    for auth in [&auth_a, &auth_b] {
        handler
            .handle(
                auth,
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(topic),
                },
            )
            .await
            .expect("subscribe");
    }

    // Server publishes its own message.
    let nonce = rand_nonce();
    let msg = make_signed_ephemeral_for(&server_signer, topic, nonce, vec![0xDE, 0xAD]).await;
    handler.publish(msg.clone()).await;

    // Drain the legitimate initial fan-out so tests can observe only
    // the bounce-back behaviour.
    drop(drain(&handle_a));
    drop(drain(&handle_b));

    (handler, event_rx, auth_a, handle_a, handle_b, msg)
}

/// H1a — `publish()`-then-bounce delivers the publisher's own message
/// back to its callback channel. The application has no way to
/// distinguish "I published this" from "someone sent me this", so it
/// will process its own ephemeral as if it had been received from a peer.
#[tokio::test]
async fn publish_bounce_back_triggers_self_callback() -> TestResult {
    let (handler, event_rx, auth_a, _handle_a, _handle_b, msg) = setup_publishing_server().await;

    // Sanity: nothing on the callback channel yet.
    let pre = tokio::time::timeout(Duration::from_millis(20), event_rx.recv()).await;
    assert!(pre.is_err(), "no events before bounce");

    // Peer A bounces the message back to us. relay=A, sender=server.
    handler.handle(&auth_a, msg).await?;

    let bounce_event = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(
        bounce_event.is_err(),
        "BUG: server delivered its own published message to its own callback channel \
         after a peer bounced it (publish() did not seed the nonce cache)"
    );

    Ok(())
}

/// H1b — `publish()`-then-bounce causes the server to re-fan-out the
/// message to its other subscribers. This is the network-visible
/// amplification: the more subscribers and the more cycles in the
/// topology, the more wasted bandwidth.
#[tokio::test]
async fn publish_bounce_back_triggers_re_fanout_to_other_subscribers() -> TestResult {
    let (handler, _event_rx, auth_a, handle_a, handle_b, msg) = setup_publishing_server().await;

    // Peer A bounces the message back.
    handler.handle(&auth_a, msg).await?;

    // Peer B (the other subscriber) must NOT receive a second copy.
    // If publish() seeded the cache, the nonce check fires and the
    // bounce is dropped before fan-out.
    let b_again = count_ephemerals(&drain(&handle_b));
    assert_eq!(
        b_again, 0,
        "BUG: server re-fanned out its own message to other subscribers when a peer \
         bounced it back ({b_again} extra copy/copies to B); this is the network \
         amplification surface of the missing publish-seed"
    );

    // A (the bouncer) must not get a copy back — relay filter excludes it.
    let a_again = count_ephemerals(&drain(&handle_a));
    assert_eq!(
        a_again, 0,
        "A (relay) should not get the message back via re-fan-out, but got {a_again}"
    );

    Ok(())
}

// ── H2: Cross-path concurrent arrival ───────────────────────────────────

/// A diamond-shaped delivery: the same `(originator, topic, nonce)` arrives
/// at server S from TWO different relays (R1, R2) in close succession.
/// The nonce cache must drop the second one and emit exactly ONE fan-out.
///
/// This is the "I see the same message from multiple paths" case. With
/// only `(sender, topic, nonce)` keyed dedup, the cache should catch this
/// regardless of which relay is the latest hop.
#[tokio::test]
async fn server_dedupes_same_message_arriving_via_two_relays() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());

    let topic = Topic::new([0x77; 32]);
    let originator_signer = MemorySigner::generate();

    // Two relays (the message is forwarded to us via either).
    let (auth_r1, _h_r1) = register_peer(&connections, peer(1)).await;
    let (auth_r2, _h_r2) = register_peer(&connections, peer(2)).await;

    // One downstream subscriber.
    let (auth_d, handle_d) = register_peer(&connections, peer(3)).await;
    handler
        .handle(
            &auth_d,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    let nonce = rand_nonce();
    let msg = make_signed_ephemeral_for(&originator_signer, topic, nonce, vec![1, 2, 3]).await;

    // First arrival, via R1.
    handler.handle(&auth_r1, msg.clone()).await?;
    // Second arrival of the same (sender, topic, nonce), via R2.
    handler.handle(&auth_r2, msg.clone()).await?;

    // Callback fires exactly once.
    let _first = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await??;
    let second = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(
        second.is_err(),
        "callback should fire exactly once for a single (sender,topic,nonce); got two"
    );

    // Downstream gets exactly one fan-out copy.
    assert_eq!(
        count_ephemerals(&drain(&handle_d)),
        1,
        "downstream subscriber should receive exactly one copy across both arrivals"
    );

    Ok(())
}

// ── H3: Multi-server gossip amplification ───────────────────────────────

/// Three servers in a fully-connected triangle, each subscribed to the
/// topic from the other two. An originator publishes via S1. Count the
/// number of fan-out network sends each server makes.
///
/// Expected (nonce cache must catch cross-deliveries):
///   - S1 receives from the originator, fans out to {S2, S3}: 2 sends.
///   - S2 receives from S1, fans out to {S3}: 1 send (subs[T]\{S1, A}).
///   - S3 receives from S1, fans out to {S2}: 1 send (subs[T]\{S1, A}).
///   - The S2↔S3 cross-deliveries are dropped by each receiver's nonce
///     cache, but they ARE sent over the wire (the sender can't know
///     the receiver has already seen this nonce).
///
/// We assert that the network is "1-amplified" — each downstream server
/// receives 1 useful copy (delivered to its callback) plus at most 1
/// wasted copy (the cross-edge from the sibling), and never more. If
/// the relay filter were buggy ("only check latest hop"), the cross
/// edge would re-fan out, doubling the work.
#[tokio::test]
async fn triangle_topology_does_not_exponentially_amplify() -> TestResult {
    // Each server has its own connections map + handler.
    let conns1: Connections = Arc::new(Mutex::new(Map::new()));
    let conns2: Connections = Arc::new(Mutex::new(Map::new()));
    let conns3: Connections = Arc::new(Mutex::new(Map::new()));

    let (h1, _rx1) = make_open_handler(conns1.clone());
    let (h2, rx2) = make_open_handler(conns2.clone());
    let (h3, rx3) = make_open_handler(conns3.clone());

    let topic = Topic::new([0x88; 32]);
    let orig_signer = MemorySigner::generate();
    let orig_peer = PeerId::from(Signer::<Sendable>::verifying_key(&orig_signer));

    let p1 = peer(1);
    let p2 = peer(2);
    let p3 = peer(3);

    // On each server, register the OTHER two servers as connected peers
    // and have them subscribe (so subs[T] = {other_two}).
    let (s1_view_of_2, h_s1_to_2) = register_peer(&conns1, p2).await;
    let (s1_view_of_3, h_s1_to_3) = register_peer(&conns1, p3).await;
    let (s2_view_of_1, h_s2_to_1) = register_peer(&conns2, p1).await;
    let (s2_view_of_3, h_s2_to_3) = register_peer(&conns2, p3).await;
    let (s3_view_of_1, h_s3_to_1) = register_peer(&conns3, p1).await;
    let (s3_view_of_2, h_s3_to_2) = register_peer(&conns3, p2).await;

    for (handler, auth) in [
        (&h1, &s1_view_of_2),
        (&h1, &s1_view_of_3),
        (&h2, &s2_view_of_1),
        (&h2, &s2_view_of_3),
        (&h3, &s3_view_of_1),
        (&h3, &s3_view_of_2),
    ] {
        handler
            .handle(
                auth,
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(topic),
                },
            )
            .await?;
    }

    // ── Originator publishes via S1. ────────────────────────────────────
    //
    // S1 already has its subs map set up; we model the originator as a
    // peer that S1 receives FROM (not as a publisher on the server-side
    // handler, since that's an unrelated path).
    let (s1_view_of_orig, _h_s1_to_orig) = register_peer(&conns1, orig_peer).await;
    let nonce = rand_nonce();
    let msg = make_signed_ephemeral_for(&orig_signer, topic, nonce, vec![0xCA, 0xFE]).await;

    h1.handle(&s1_view_of_orig, msg.clone()).await?;

    // S1 should fan out one copy to each of S2 and S3 — and no more.
    let s1_to_2 = drain(&h_s1_to_2);
    let s1_to_3 = drain(&h_s1_to_3);
    assert_eq!(
        count_ephemerals(&s1_to_2),
        1,
        "S1 fans out exactly once to S2"
    );
    assert_eq!(
        count_ephemerals(&s1_to_3),
        1,
        "S1 fans out exactly once to S3"
    );

    // Deliver S1's outbound to S2 and S3 as inbound on their handlers.
    let msg_at_s2 = first_ephemeral(s1_to_2).expect("S1 must have sent something to S2");
    let msg_at_s3 = first_ephemeral(s1_to_3).expect("S1 must have sent something to S3");

    h2.handle(&s2_view_of_1, msg_at_s2).await?;
    h3.handle(&s3_view_of_1, msg_at_s3).await?;

    // Both S2 and S3 deliver to their callback exactly once.
    let _e2 = tokio::time::timeout(Duration::from_millis(100), rx2.recv()).await??;
    let _e3 = tokio::time::timeout(Duration::from_millis(100), rx3.recv()).await??;

    // The cross-edges (S2→S3 and S3→S2) each happen exactly once.
    let s2_to_3 = drain(&h_s2_to_3);
    let s3_to_2 = drain(&h_s3_to_2);
    assert_eq!(
        count_ephemerals(&s2_to_3),
        1,
        "S2 fans out to S3 exactly once (the wasted cross-edge)"
    );
    assert_eq!(
        count_ephemerals(&s3_to_2),
        1,
        "S3 fans out to S2 exactly once (the wasted cross-edge)"
    );

    // CRUCIAL: when the cross-edge arrives at the sibling, the nonce
    // cache must drop it (no re-fan-out). If the handler "only checks
    // the latest hop", these cross-arrivals would each trigger another
    // fan-out, doubling the work each round and eventually exhausting
    // the channel buffers.
    let cross_to_s3 = first_ephemeral(s2_to_3).expect("S2 must have sent to S3");
    let cross_to_s2 = first_ephemeral(s3_to_2).expect("S3 must have sent to S2");

    h3.handle(&s3_view_of_2, cross_to_s3).await?;
    h2.handle(&s2_view_of_3, cross_to_s2).await?;

    // No second callback event on either side.
    let dup2 = tokio::time::timeout(Duration::from_millis(50), rx2.recv()).await;
    let dup3 = tokio::time::timeout(Duration::from_millis(50), rx3.recv()).await;
    assert!(
        dup2.is_err(),
        "S2 must not deliver a duplicate callback for the cross-edge"
    );
    assert!(
        dup3.is_err(),
        "S3 must not deliver a duplicate callback for the cross-edge"
    );

    // And no re-fan-out: S2 doesn't send anything more to S1 or S3,
    // S3 doesn't send anything more to S1 or S2.
    for (label, h) in [
        ("S2→S1", &h_s2_to_1),
        ("S2→S3", &h_s2_to_3),
        ("S3→S1", &h_s3_to_1),
        ("S3→S2", &h_s3_to_2),
    ] {
        assert_eq!(
            count_ephemerals(&drain(h)),
            0,
            "{label}: no further fan-out after cross-edge dedup"
        );
    }

    Ok(())
}

// ── H4: Inbound message round-trip via outgoing subscription ────────────

/// **Bug hypothesis**: When a server has `outgoing_subscriptions = {T}`
/// (it's a relay that actively wants T from upstream), and it receives
/// an inbound ephemeral for T, the fan-out filter only excludes the
/// immediate relay and the originator — but NOT the server's own
/// outgoing-relay peers, which the message arguably already covered.
///
/// More importantly: if the inbound message arrives via a peer that is
/// ALSO in the server's subscription map (a common case: bidirectional
/// subscriptions between two relays), the relay filter excludes that
/// peer. But what about a peer that arrived BEFORE — the chain of
/// relays through which this message previously passed? We have no
/// info about that. We must rely entirely on the nonce cache.
///
/// This test pins down: when a relay receives a message it has already
/// fanned out (the canonical "amplification" failure mode), the second
/// arrival from any peer must not re-fan-out.
#[tokio::test]
async fn relay_does_not_refanout_after_seeing_its_own_outbound_come_back() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());

    let topic = Topic::new([0x99; 32]);
    let originator_signer = MemorySigner::generate();

    // The originator and three downstream subscribers, plus one
    // "loop-back" peer that will replay the message.
    let (auth_origin, _h_origin) = register_peer(
        &connections,
        PeerId::from(Signer::<Sendable>::verifying_key(&originator_signer)),
    )
    .await;
    let (auth_sub1, handle_sub1) = register_peer(&connections, peer(20)).await;
    let (auth_sub2, handle_sub2) = register_peer(&connections, peer(21)).await;
    let (auth_sub3, handle_sub3) = register_peer(&connections, peer(22)).await;
    let (auth_loop, _h_loop) = register_peer(&connections, peer(99)).await;

    for auth in [&auth_sub1, &auth_sub2, &auth_sub3, &auth_loop] {
        handler
            .handle(
                auth,
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(topic),
                },
            )
            .await?;
    }

    let nonce = rand_nonce();
    let msg = make_signed_ephemeral_for(&originator_signer, topic, nonce, vec![7]).await;

    // First arrival: from the originator. Server fans out to subs[T]
    // minus the originator = {sub1, sub2, sub3, loop}.
    handler.handle(&auth_origin, msg.clone()).await?;
    assert_eq!(count_ephemerals(&drain(&handle_sub1)), 1);
    assert_eq!(count_ephemerals(&drain(&handle_sub2)), 1);
    assert_eq!(count_ephemerals(&drain(&handle_sub3)), 1);

    // Now the loop peer replays the exact same signed message back.
    handler.handle(&auth_loop, msg.clone()).await?;

    // NO subscriber should get a second copy — the nonce cache must
    // catch the replay regardless of which relay it came through.
    assert_eq!(
        count_ephemerals(&drain(&handle_sub1)),
        0,
        "sub1 must not get a second copy"
    );
    assert_eq!(
        count_ephemerals(&drain(&handle_sub2)),
        0,
        "sub2 must not get a second copy"
    );
    assert_eq!(
        count_ephemerals(&drain(&handle_sub3)),
        0,
        "sub3 must not get a second copy"
    );

    Ok(())
}

fn first_ephemeral(msgs: Vec<EphemeralMessage>) -> Option<EphemeralMessage> {
    msgs.into_iter()
        .find(|m| matches!(m, EphemeralMessage::Ephemeral(_)))
}

// ── Pre-verify fast-path: cross-edge duplicates don't trigger verify ─────
//
// The handler does a read-only `nonce_cache.contains()` probe _before_
// `Signed::try_verify`. This test pins down that ordering: after a valid
// message has been processed, a duplicate `(issuer, topic, nonce)` whose
// signature has been _deliberately corrupted_ must still be dropped
// silently — i.e., the cache short-circuit fires before the curve25519
// op would have failed and logged a verification error.
//
// Both the old (verify-first) and new (cache-first) orderings drop the
// corrupted duplicate; the assertion that proves the reorder happened is
// that no spurious side effect (callback re-fire, fan-out, error) is
// observed _and_ that the handle call returns Ok(()) just as fast as the
// hit path on a well-formed duplicate. We assert the no-side-effect part
// directly; the cost difference is documented in the step-order comment
// in `handler::recv_ephemeral` rather than asserted on the wall clock.

/// A duplicate arriving after a valid copy — even one whose signature
/// bytes have been flipped — is silently dropped by the cache fast path.
/// No callback re-fire, no fan-out, no panic on the bad signature.
#[tokio::test]
async fn cross_edge_duplicate_with_corrupted_signature_is_dropped_silently() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());

    let topic = Topic::new([0xAB; 32]);
    let signer = MemorySigner::generate();

    // A subscriber that would receive any successful fan-out.
    let (auth_sub, handle_sub) = register_peer(&connections, peer(50)).await;
    handler
        .handle(
            &auth_sub,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    // A relay peer that the duplicate will come through.
    let (auth_relay, _h_relay) = register_peer(&connections, peer(51)).await;

    // First arrival: a legitimate message. Cache gets seeded.
    let nonce = rand_nonce();
    let good = make_signed_ephemeral_for(&signer, topic, nonce, vec![0xCA, 0xFE]).await;
    handler.handle(&auth_relay, good.clone()).await?;

    // Drain the legitimate event + fan-out.
    let _first_event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await??;
    let first_fanout = drain(&handle_sub);
    assert_eq!(
        count_ephemerals(&first_fanout),
        1,
        "subscriber receives the original message exactly once"
    );

    // Build a duplicate with the same (issuer, topic, nonce) but a
    // mangled signature. With verify-first ordering, the handler would
    // log "signature verification failed". With the cache fast path,
    // the duplicate is dropped before we ever look at the signature.
    //
    // Flipping a byte inside the 64-byte Ed25519 signature region
    // leaves the wire structure intact — only the signature itself is
    // garbage — so `Signed::try_decode` must still succeed; if it ever
    // doesn't, the wire format has shifted and the assertion below
    // will catch it rather than silently skipping the test.
    let EphemeralMessage::Ephemeral(good_signed) = good else {
        panic!("expected Ephemeral");
    };
    let mut bytes = good_signed.into_bytes();
    let sig_start = bytes.len() - subduction_crypto::signed::SIGNATURE_SIZE;
    bytes[sig_start] ^= 0xFF;
    let tampered_signed = Signed::<EphemeralPayload>::try_decode(bytes)
        .expect("flipping a signature byte must leave the wire structure valid");
    let tampered = EphemeralMessage::Ephemeral(Box::new(tampered_signed));

    handler.handle(&auth_relay, tampered).await?;

    // No callback re-fire — the duplicate was dropped.
    let dup_event = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(
        dup_event.is_err(),
        "duplicate must not re-fire the callback channel"
    );

    // No re-fan-out — the subscriber sees no second copy.
    assert_eq!(
        count_ephemerals(&drain(&handle_sub)),
        0,
        "duplicate must not trigger fan-out (it was dropped by the cache)"
    );

    Ok(())
}
