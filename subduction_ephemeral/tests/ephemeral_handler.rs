//! Unit tests for [`EphemeralHandler`].
//!
//! Covers subscribe/unsubscribe, fan-out, policy enforcement,
//! payload size limits, disconnect cleanup, and the callback channel.

#![allow(clippy::expect_used, clippy::panic)]

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

// ── Helpers ─────────────────────────────────────────────────────────────

type EphConn = ChannelMockConnection<EphemeralMessage>;
type EphAuth = Authenticated<EphConn, Sendable>;
type EphHandle =
    subduction_core::connection::test_utils::ChannelMockConnectionHandle<EphemeralMessage>;
type Connections = Arc<Mutex<Map<PeerId, NonEmpty<EphAuth>>>>;

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
        FakeClock::new(TimestampSeconds::new(1_000)),
    );
    (Arc::new(handler), rx)
}

fn make_small_payload_handler(
    connections: Connections,
    max_payload_size: usize,
) -> (OpenHandler, async_channel::Receiver<EphemeralEvent>) {
    let config = EphemeralConfig {
        max_payload_size,
        ..EphemeralConfig::default()
    };
    let (handler, rx) = EphemeralHandler::new(
        connections,
        OpenEphemeralPolicy,
        config,
        FakeClock::new(TimestampSeconds::new(1_000)),
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

/// Timestamp used by the `FakeClock` in test handlers.
const TEST_CLOCK_SECS: TimestampSeconds = TimestampSeconds::new(1_000);

/// Create a signed ephemeral message using a deterministic signer.
///
/// Uses [`TEST_CLOCK_SECS`] as the timestamp so the handler's age check
/// passes (the `FakeClock` is initialized to the same value).
async fn make_signed_ephemeral(
    signer: &MemorySigner,
    id: Topic,
    payload: Vec<u8>,
) -> EphemeralMessage {
    let ep = EphemeralPayload {
        id,
        nonce: rand_nonce(),
        timestamp: TEST_CLOCK_SECS,
        payload,
    };
    let verified = Signed::seal::<Sendable, _>(signer, ep).await;
    EphemeralMessage::Ephemeral(Box::new(verified.into_signed()))
}

fn rand_nonce() -> u64 {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).expect("getrandom failed");
    u64::from_le_bytes(buf)
}

fn make_signer() -> MemorySigner {
    MemorySigner::generate()
}

// ── Subscribe / Unsubscribe ─────────────────────────────────────────────

#[tokio::test]
async fn subscribe_adds_peer() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth, handle) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(Topic::new([0xAA; 32])),
            },
        )
        .await?;

    // No SubscribeRejected should have been sent.
    assert!(
        handle.outbound_rx.try_recv().is_err(),
        "no rejection should be sent for open policy"
    );

    // Verify subscription by publishing from another peer.
    let (auth_b, _handle_b) = register_peer(&connections, peer(2)).await;
    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xAA; 32]), vec![42]).await;
    handler.handle(&auth_b, msg).await?;

    let forwarded =
        tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv()).await??;
    let EphemeralMessage::Ephemeral(ref fwd_signed) = forwarded else {
        panic!("expected Ephemeral, got {forwarded:?}");
    };
    let decoded = fwd_signed.try_decode_trusted_payload().expect("decode");
    assert_eq!(decoded.id, Topic::new([0xAA; 32]));
    assert_eq!(decoded.payload, vec![42]);

    Ok(())
}

#[tokio::test]
async fn unsubscribe_removes_peer() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;
    let (auth_b, _handle_b) = register_peer(&connections, peer(2)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(Topic::new([0xBB; 32])),
            },
        )
        .await?;
    handler
        .handle(
            &auth_a,
            EphemeralMessage::Unsubscribe {
                topics: NonEmpty::new(Topic::new([0xBB; 32])),
            },
        )
        .await?;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xBB; 32]), vec![99]).await;
    handler.handle(&auth_b, msg).await?;

    let result = tokio::time::timeout(Duration::from_millis(50), handle_a.outbound_rx.recv()).await;
    assert!(result.is_err(), "peer should not receive after unsubscribe");

    Ok(())
}

#[tokio::test]
async fn subscribe_multiple_topics() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;
    let (auth_b, _handle_b) = register_peer(&connections, peer(2)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::from_vec(vec![Topic::new([1; 32]), Topic::new([2; 32])])
                    .expect("non-empty"),
            },
        )
        .await?;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([2; 32]), vec![77]).await;
    handler.handle(&auth_b, msg).await?;

    let forwarded =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    let EphemeralMessage::Ephemeral(ref fwd_signed) = forwarded else {
        panic!("expected Ephemeral, got {forwarded:?}");
    };
    let decoded = fwd_signed.try_decode_trusted_payload().expect("decode");
    assert_eq!(decoded.id, Topic::new([2; 32]));
    assert_eq!(decoded.payload, vec![77]);

    Ok(())
}

// ── Fan-out ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn fan_out_excludes_sender() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(Topic::new([0xCC; 32])),
            },
        )
        .await?;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xCC; 32]), vec![1, 2, 3]).await;
    handler.handle(&auth_a, msg).await?;

    let result = tokio::time::timeout(Duration::from_millis(50), handle_a.outbound_rx.recv()).await;
    assert!(result.is_err(), "sender should not receive its own message");

    Ok(())
}

#[tokio::test]
async fn fan_out_to_multiple_subscribers() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());

    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;
    let (auth_b, handle_b) = register_peer(&connections, peer(2)).await;
    let (auth_c, _handle_c) = register_peer(&connections, peer(3)).await;

    for auth in [&auth_a, &auth_b] {
        handler
            .handle(
                auth,
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(Topic::new([0xDD; 32])),
                },
            )
            .await?;
    }

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xDD; 32]), vec![10, 20]).await;
    handler.handle(&auth_c, msg).await?;

    let msg_a =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    let msg_b =
        tokio::time::timeout(Duration::from_millis(100), handle_b.outbound_rx.recv()).await??;

    let EphemeralMessage::Ephemeral(ref signed_a) = msg_a else {
        panic!("expected Ephemeral, got {msg_a:?}");
    };
    let EphemeralMessage::Ephemeral(ref signed_b) = msg_b else {
        panic!("expected Ephemeral, got {msg_b:?}");
    };

    let decoded_a = signed_a.try_decode_trusted_payload().expect("decode a");
    let decoded_b = signed_b.try_decode_trusted_payload().expect("decode b");

    assert_eq!(decoded_a.id, Topic::new([0xDD; 32]));
    assert_eq!(decoded_a.payload, vec![10, 20]);
    assert_eq!(decoded_b.id, Topic::new([0xDD; 32]));
    assert_eq!(decoded_b.payload, vec![10, 20]);

    Ok(())
}

// ── Callback channel ────────────────────────────────────────────────────

#[tokio::test]
async fn callback_channel_receives_event() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xEE; 32]), vec![5, 6, 7]).await;
    handler.handle(&auth_a, msg).await?;

    let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await??;
    assert_eq!(event.id, Topic::new([0xEE; 32]));
    assert_eq!(
        event.sender,
        PeerId::from(Signer::<Sendable>::verifying_key(&signer))
    );
    assert_eq!(event.payload, vec![5, 6, 7]);

    Ok(())
}

// ── Payload size limits ─────────────────────────────────────────────────

#[tokio::test]
async fn inbound_payload_too_large_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_small_payload_handler(connections.clone(), 10);
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xFF; 32]), vec![0u8; 20]).await;
    handler.handle(&auth_a, msg).await?;

    let result = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(result.is_err(), "oversized payload should be dropped");

    Ok(())
}

#[tokio::test]
async fn publish_api_payload_too_large_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_small_payload_handler(connections.clone(), 10);
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;

    // Subscribe so peer would receive if the message went through.
    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(Topic::new([0x11; 32])),
            },
        )
        .await?;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0x11; 32]), vec![0u8; 20]).await;
    handler.publish(msg).await;

    let result = tokio::time::timeout(Duration::from_millis(50), handle_a.outbound_rx.recv()).await;
    assert!(result.is_err(), "publish() should drop oversized payloads");

    Ok(())
}

// ── Disconnect cleanup ──────────────────────────────────────────────────

#[tokio::test]
async fn disconnect_cleans_all_subscriptions() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;
    let (auth_b, _handle_b) = register_peer(&connections, peer(2)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::from_vec(vec![Topic::new([1; 32]), Topic::new([2; 32])])
                    .expect("non-empty"),
            },
        )
        .await?;

    Handler::<Sendable, EphConn>::on_peer_disconnect(&*handler, peer(1)).await;

    let signer = make_signer();
    for t in [Topic::new([1; 32]), Topic::new([2; 32])] {
        let msg = make_signed_ephemeral(&signer, t, vec![0]).await;
        handler.handle(&auth_b, msg).await?;
    }

    let result = tokio::time::timeout(Duration::from_millis(50), handle_a.outbound_rx.recv()).await;
    assert!(
        result.is_err(),
        "disconnected peer should not receive messages"
    );

    Ok(())
}

// ── Publish API ─────────────────────────────────────────────────────────

#[tokio::test]
async fn publish_api_sends_to_subscribers() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth_a, handle_a) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(Topic::new([0x22; 32])),
            },
        )
        .await?;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0x22; 32]), vec![88, 99]).await;
    handler.publish(msg).await;

    let received =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    let EphemeralMessage::Ephemeral(ref recv_signed) = received else {
        panic!("expected Ephemeral, got {received:?}");
    };
    let decoded = recv_signed.try_decode_trusted_payload().expect("decode");
    assert_eq!(decoded.id, Topic::new([0x22; 32]));
    assert_eq!(decoded.payload, vec![88, 99]);

    Ok(())
}

// ── Policy rejection ────────────────────────────────────────────────────

mod deny_policy {
    use core::fmt;

    use future_form::{FutureForm, Sendable};
    use subduction_core::peer::id::PeerId;
    use subduction_ephemeral::{policy::EphemeralPolicy, topic::Topic};

    #[derive(Debug, Clone, Copy)]
    pub(super) struct Denied;

    impl fmt::Display for Denied {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "denied")
        }
    }

    impl core::error::Error for Denied {}

    #[derive(Debug, Clone, Copy)]
    pub(super) struct DenyAll;

    impl EphemeralPolicy<Sendable> for DenyAll {
        type SubscribeDisallowed = Denied;
        type PublishDisallowed = Denied;

        fn authorize_subscribe(
            &self,
            _peer: PeerId,
            _id: Topic,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SubscribeDisallowed>> {
            Sendable::from_future(async { Err(Denied) })
        }

        fn authorize_publish(
            &self,
            _peer: PeerId,
            _id: Topic,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
            Sendable::from_future(async { Err(Denied) })
        }

        fn filter_authorized_subscribers(
            &self,
            _id: Topic,
            _peers: Vec<PeerId>,
        ) -> <Sendable as FutureForm>::Future<'_, Vec<PeerId>> {
            Sendable::from_future(async { vec![] })
        }
    }
}

#[tokio::test]
async fn subscribe_rejected_by_policy() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = EphemeralHandler::new(
        connections.clone(),
        deny_policy::DenyAll,
        EphemeralConfig::default(),
        FakeClock::new(TimestampSeconds::new(1_000)),
    );
    let handler = Arc::new(handler);
    let (auth, handle) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::from_vec(vec![Topic::new([0xAA; 32]), Topic::new([0xBB; 32])])
                    .expect("non-empty"),
            },
        )
        .await?;

    let rejected =
        tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv()).await??;
    match rejected {
        EphemeralMessage::SubscribeRejected { topics } => {
            assert_eq!(topics.len(), 2);
            assert!(topics.contains(&Topic::new([0xAA; 32])));
            assert!(topics.contains(&Topic::new([0xBB; 32])));
        }
        other @ (EphemeralMessage::Ephemeral(_)
        | EphemeralMessage::Subscribe { .. }
        | EphemeralMessage::Unsubscribe { .. }) => {
            panic!("expected SubscribeRejected, got {other:?}")
        }
    }

    Ok(())
}

#[tokio::test]
async fn publish_rejected_by_policy_no_callback() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = EphemeralHandler::new(
        connections.clone(),
        deny_policy::DenyAll,
        EphemeralConfig::default(),
        FakeClock::new(TimestampSeconds::new(1_000)),
    );
    let handler = Arc::new(handler);
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    let signer = make_signer();
    let msg = make_signed_ephemeral(&signer, Topic::new([0xCC; 32]), vec![1, 2, 3]).await;
    handler.handle(&auth_a, msg).await?;

    let result = tokio::time::timeout(Duration::from_millis(50), event_rx.recv()).await;
    assert!(
        result.is_err(),
        "unauthorized publish should not produce a callback event"
    );

    Ok(())
}

// ── SubscribeRejected is informational ──────────────────────────────────

#[tokio::test]
async fn subscribe_rejected_message_is_noop() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, _rx) = make_open_handler(connections.clone());
    let (auth, _handle) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth,
            EphemeralMessage::SubscribeRejected {
                topics: NonEmpty::new(Topic::new([0xFF; 32])),
            },
        )
        .await?;

    Ok(())
}

// ── Gossip Cycle Termination ────────────────────────────────────────────

/// Simulates a 3-node ring topology (A → B → C → A) and verifies that
/// gossip terminates: the message traverses the full ring but is never
/// relayed more than once per node.
///
/// ```text
///     ┌───────────────────────────┐
///     │                           │
///     ▼                           │
///   Node A ──relay──► Node B ──relay──► Node C
///   (origin)
/// ```
///
/// A publishes a message. Its handler fans out to B (A's subscriber).
/// B's handler receives it from A, inserts the nonce, and fans out to C
/// (excluding A as relay+originator). C's handler receives it from B,
/// inserts the nonce, and would fan out to A — but A is the originator,
/// so the fan-out filter excludes A. Gossip terminates.
///
/// Additionally, if C's message were to somehow reach A's handler (e.g.,
/// via a second path), A's nonce cache blocks re-processing.
#[tokio::test]
async fn gossip_terminates_in_ring_topology() -> TestResult {
    // Each node has its own connections map and handler.
    let conns_a: Connections = Arc::new(Mutex::new(Map::new()));
    let conns_b: Connections = Arc::new(Mutex::new(Map::new()));
    let conns_c: Connections = Arc::new(Mutex::new(Map::new()));

    let (handler_a, event_rx_a) = make_open_handler(conns_a.clone());
    let (handler_b, event_rx_b) = make_open_handler(conns_b.clone());
    let (handler_c, event_rx_c) = make_open_handler(conns_c.clone());

    let topic = Topic::new([0x42; 32]);
    let signer_a = make_signer();
    let peer_a = PeerId::from(Signer::<Sendable>::verifying_key(&signer_a));

    // Use stable peer IDs for B and C (distinct from A).
    let peer_b = peer(200);
    let peer_c = peer(201);

    // ── Wire ring: A sees B, B sees C, C sees A ────────────────────────
    //
    // For each node, register a ChannelMockConnection for its downstream
    // peer and subscribe that peer to the topic.
    let (_auth_a_b, handle_a_b) = register_peer(&conns_a, peer_b).await;
    let (_auth_b_c, handle_b_c) = register_peer(&conns_b, peer_c).await;
    let (_auth_c_a, handle_c_a) = register_peer(&conns_c, peer_a).await;

    // Subscribe downstream peers on each handler.
    // On Node A: peer B is subscribed.
    handler_a
        .handle(
            &Authenticated::new_for_test(
                ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_b).0,
                peer_b,
            ),
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    // On Node B: peer C is subscribed.
    handler_b
        .handle(
            &Authenticated::new_for_test(
                ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_c).0,
                peer_c,
            ),
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    // On Node C: peer A is subscribed.
    handler_c
        .handle(
            &Authenticated::new_for_test(
                ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_a).0,
                peer_a,
            ),
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    // ── Step 1: A publishes ─────────────────────────────────────────────
    let msg = make_signed_ephemeral(&signer_a, topic, vec![0xDE, 0xAD]).await;
    handler_a.publish(msg).await;

    // A's handler should have fanned out to B.
    let msg_at_b =
        tokio::time::timeout(Duration::from_millis(100), handle_a_b.outbound_rx.recv()).await??;

    // ── Step 2: B receives from A, fans out to C ────────────────────────
    // Simulate B's handler receiving the message relayed by A.
    let auth_from_a_on_b = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_a).0,
        peer_a,
    );
    handler_b.handle(&auth_from_a_on_b, msg_at_b).await?;

    // B's callback should have fired (B received the message).
    let b_event = tokio::time::timeout(Duration::from_millis(100), event_rx_b.recv()).await??;
    assert_eq!(b_event.payload, vec![0xDE, 0xAD]);
    assert_eq!(b_event.sender, peer_a);

    // B should have fanned out to C (B's subscriber, excluding A=relay+originator).
    let msg_at_c =
        tokio::time::timeout(Duration::from_millis(100), handle_b_c.outbound_rx.recv()).await??;

    // ── Step 3: C receives from B, tries to fan out to A ────────────────
    let auth_from_b_on_c = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_b).0,
        peer_b,
    );
    handler_c.handle(&auth_from_b_on_c, msg_at_c).await?;

    // C's callback should have fired.
    let c_event = tokio::time::timeout(Duration::from_millis(100), event_rx_c.recv()).await??;
    assert_eq!(c_event.payload, vec![0xDE, 0xAD]);
    assert_eq!(c_event.sender, peer_a);

    // C's fan-out should NOT reach A, because A is the originator
    // (filtered by `*p != sender` in the fan-out loop).
    let relay_back_to_a =
        tokio::time::timeout(Duration::from_millis(50), handle_c_a.outbound_rx.recv()).await;
    assert!(
        relay_back_to_a.is_err(),
        "originator A should be excluded from C's fan-out — gossip must terminate"
    );

    // A's callback should NOT have received the message from the gossip
    // ring (publish() doesn't deliver to the local callback; only
    // recv_ephemeral does, and A never received this message via recv).
    let a_bounceback = tokio::time::timeout(Duration::from_millis(50), event_rx_a.recv()).await;
    assert!(
        a_bounceback.is_err(),
        "originator A should not get a callback from its own published message"
    );

    Ok(())
}

/// A stronger variant: 4-node ring where the originator has _two_ paths
/// to receive the message back. Verifies that nonce dedup prevents
/// re-processing even when the originator filter alone is insufficient.
///
/// ```text
///   A ──► B ──► C ──► D ──► A
///   │                       ▲
///   └───────────────────────┘
///         (D also sees A)
/// ```
///
/// Additionally, we give D a _second_ subscriber E that connects back
/// to B, forming a longer cycle: A→B→C→D→E→B. B's nonce cache breaks
/// the cycle at E→B.
#[tokio::test]
async fn gossip_terminates_with_nonce_dedup_across_multiple_hops() -> TestResult {
    let conns_a: Connections = Arc::new(Mutex::new(Map::new()));
    let conns_b: Connections = Arc::new(Mutex::new(Map::new()));
    let conns_c: Connections = Arc::new(Mutex::new(Map::new()));
    let conns_d: Connections = Arc::new(Mutex::new(Map::new()));

    let (handler_a, _event_rx_a) = make_open_handler(conns_a.clone());
    let (handler_b, event_rx_b) = make_open_handler(conns_b.clone());
    let (handler_c, _event_rx_c) = make_open_handler(conns_c.clone());
    let (handler_d, event_rx_d) = make_open_handler(conns_d.clone());

    let topic = Topic::new([0x99; 32]);
    let signer_a = make_signer();
    let peer_a = PeerId::from(Signer::<Sendable>::verifying_key(&signer_a));
    let peer_b = peer(10);
    let peer_c = peer(11);
    let peer_d = peer(12);

    // ── Wire: A→B, B→C, C→D, D→B (back-edge creating cycle through B) ─
    let (_auth_a_b, handle_a_b) = register_peer(&conns_a, peer_b).await;
    let (_auth_b_c, handle_b_c) = register_peer(&conns_b, peer_c).await;
    let (_auth_c_d, handle_c_d) = register_peer(&conns_c, peer_d).await;
    // D fans out to B — this is the back-edge that creates the cycle.
    let (_auth_d_b, handle_d_b) = register_peer(&conns_d, peer_b).await;

    // Subscribe downstream peers on each handler.
    for (handler, subscriber) in [
        (&handler_a, peer_b),
        (&handler_b, peer_c),
        (&handler_c, peer_d),
        (&handler_d, peer_b),
    ] {
        handler
            .handle(
                &Authenticated::new_for_test(
                    ChannelMockConnection::<EphemeralMessage>::new_with_handle(subscriber).0,
                    subscriber,
                ),
                EphemeralMessage::Subscribe {
                    topics: NonEmpty::new(topic),
                },
            )
            .await?;
    }

    // ── Step 1: A publishes ─────────────────────────────────────────────
    let msg = make_signed_ephemeral(&signer_a, topic, vec![0xCA, 0xFE]).await;
    handler_a.publish(msg).await;

    let msg_at_b =
        tokio::time::timeout(Duration::from_millis(100), handle_a_b.outbound_rx.recv()).await??;

    // ── Step 2: B receives from A → fans out to C ───────────────────────
    let auth_a_on_b = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_a).0,
        peer_a,
    );
    handler_b.handle(&auth_a_on_b, msg_at_b).await?;

    let b_event = tokio::time::timeout(Duration::from_millis(100), event_rx_b.recv()).await??;
    assert_eq!(b_event.payload, vec![0xCA, 0xFE]);

    let msg_at_c =
        tokio::time::timeout(Duration::from_millis(100), handle_b_c.outbound_rx.recv()).await??;

    // ── Step 3: C receives from B → fans out to D ───────────────────────
    let auth_b_on_c = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_b).0,
        peer_b,
    );
    handler_c.handle(&auth_b_on_c, msg_at_c).await?;

    let msg_at_d =
        tokio::time::timeout(Duration::from_millis(100), handle_c_d.outbound_rx.recv()).await??;

    // ── Step 4: D receives from C → tries to fan out to B ───────────────
    let auth_c_on_d = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_c).0,
        peer_c,
    );
    handler_d.handle(&auth_c_on_d, msg_at_d.clone()).await?;

    let d_event = tokio::time::timeout(Duration::from_millis(100), event_rx_d.recv()).await??;
    assert_eq!(d_event.payload, vec![0xCA, 0xFE]);

    // D should have fanned out to B (its subscriber, excluding C=relay and A=originator).
    let msg_back_at_b =
        tokio::time::timeout(Duration::from_millis(100), handle_d_b.outbound_rx.recv()).await??;

    // ── Step 5: B receives the SAME message again from D ────────────────
    // B already processed this nonce in step 2. The nonce cache should
    // reject it, preventing further fan-out and breaking the cycle.
    let auth_d_on_b = Authenticated::new_for_test(
        ChannelMockConnection::<EphemeralMessage>::new_with_handle(peer_d).0,
        peer_d,
    );
    handler_b.handle(&auth_d_on_b, msg_back_at_b).await?;

    // B's callback should NOT fire again (duplicate nonce dropped).
    let b_second_event = tokio::time::timeout(Duration::from_millis(50), event_rx_b.recv()).await;
    assert!(
        b_second_event.is_err(),
        "B should drop the duplicate — nonce cache prevents re-processing"
    );

    // B should NOT have re-fanned out to C (no further relay).
    let c_second_msg =
        tokio::time::timeout(Duration::from_millis(50), handle_b_c.outbound_rx.recv()).await;
    assert!(
        c_second_msg.is_err(),
        "B should not re-relay — gossip cycle terminated by nonce dedup"
    );

    Ok(())
}

// ── Signature Verification ──────────────────────────────────────────────

/// A message with a tampered signature should be dropped by the handler.
/// The callback channel should receive nothing, and no fan-out should occur.
#[tokio::test]
async fn invalid_signature_is_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());

    // Peer A sends the message, Peer B is a subscriber.
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;
    let (auth_b, handle_b) = register_peer(&connections, peer(2)).await;

    // B subscribes to the topic.
    let topic = Topic::new([0xAA; 32]);
    handler
        .handle(
            &auth_b,
            EphemeralMessage::Subscribe {
                topics: NonEmpty::new(topic),
            },
        )
        .await?;

    // Build a valid signed message, then tamper with the signature bytes.
    let signer = make_signer();
    let valid_msg = make_signed_ephemeral(&signer, topic, vec![1, 2, 3]).await;
    let EphemeralMessage::Ephemeral(sealed) = valid_msg else {
        panic!("expected Ephemeral variant");
    };

    // Tamper: flip a bit in the signature (last 64 bytes of the wire bytes).
    let mut bytes = sealed.into_bytes();
    let sig_start = bytes.len() - 64;
    if let Some(byte) = bytes.get_mut(sig_start) {
        *byte ^= 0xFF;
    }

    // Reconstruct a Signed<EphemeralPayload> from the tampered bytes.
    // try_decode may succeed (the bytes are structurally valid, just the sig is wrong).
    let tampered = Signed::<EphemeralPayload>::try_decode(bytes);
    let tampered_msg = match tampered {
        Ok(s) => EphemeralMessage::Ephemeral(Box::new(s)),
        Err(_) => {
            // If decode fails, the message can't even reach the handler.
            // This is also a valid outcome — the tampered bytes are rejected.
            return Ok(());
        }
    };

    // Send the tampered message to the handler.
    handler.handle(&auth_a, tampered_msg).await?;

    // The callback channel should be empty — the invalid signature was dropped.
    let result = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
    assert!(
        result.is_err(),
        "callback channel should be empty (invalid signature dropped)"
    );

    // Peer B should not have received any forwarded message.
    let forwarded =
        tokio::time::timeout(Duration::from_millis(100), handle_b.outbound_rx.recv()).await;
    assert!(
        forwarded.is_err(),
        "peer B should not receive a forwarded message (invalid signature dropped)"
    );

    Ok(())
}

/// A message with a valid signature but a timestamp too far in the past
/// should be dropped by the handler.
#[tokio::test]
async fn stale_timestamp_is_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    let signer = make_signer();
    let topic = Topic::new([0xBB; 32]);

    // Create a message with a timestamp 60 seconds in the past.
    // The default max_message_age is 30 seconds, so this should be rejected.
    let stale_timestamp = TimestampSeconds::new(TEST_CLOCK_SECS.as_secs() - 60);
    let ep = EphemeralPayload {
        id: topic,
        nonce: rand_nonce(),
        timestamp: stale_timestamp,
        payload: vec![1, 2, 3],
    };
    let verified = Signed::seal::<Sendable, _>(&signer, ep).await;
    let msg = EphemeralMessage::Ephemeral(Box::new(verified.into_signed()));

    handler.handle(&auth_a, msg).await?;

    // Should be dropped — callback channel empty.
    let result = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
    assert!(
        result.is_err(),
        "stale message should be dropped (timestamp too old)"
    );

    Ok(())
}

/// A message with a duplicate nonce should be dropped on the second delivery.
#[tokio::test]
async fn duplicate_nonce_is_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    let signer = make_signer();
    let topic = Topic::new([0xCC; 32]);
    let nonce = 42_u64;

    // Create two messages with the same nonce.
    let ep1 = EphemeralPayload {
        id: topic,
        nonce,
        timestamp: TEST_CLOCK_SECS,
        payload: vec![1],
    };
    let ep2 = EphemeralPayload {
        id: topic,
        nonce,
        timestamp: TEST_CLOCK_SECS,
        payload: vec![2],
    };
    let msg1 = EphemeralMessage::Ephemeral(Box::new(
        Signed::seal::<Sendable, _>(&signer, ep1)
            .await
            .into_signed(),
    ));
    let msg2 = EphemeralMessage::Ephemeral(Box::new(
        Signed::seal::<Sendable, _>(&signer, ep2)
            .await
            .into_signed(),
    ));

    // First message should be accepted.
    handler.handle(&auth_a, msg1).await?;
    let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await??;
    assert_eq!(event.payload, vec![1]);

    // Second message (same nonce) should be dropped.
    handler.handle(&auth_a, msg2).await?;
    let result = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await;
    assert!(result.is_err(), "duplicate nonce should be dropped");

    Ok(())
}
