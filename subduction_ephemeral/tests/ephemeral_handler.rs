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
    signer::{memory::MemorySigner, Signer},
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
    EphemeralMessage::Ephemeral(verified.into_signed())
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
    let EphemeralMessage::Ephemeral(ref signed) = forwarded else {
        panic!("expected Ephemeral, got {forwarded:?}");
    };
    let decoded = signed.try_decode_trusted_payload().expect("decode");
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
    let EphemeralMessage::Ephemeral(ref signed) = forwarded else {
        panic!("expected Ephemeral, got {forwarded:?}");
    };
    let decoded = signed.try_decode_trusted_payload().expect("decode");
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
    let EphemeralMessage::Ephemeral(ref signed) = received else {
        panic!("expected Ephemeral, got {received:?}");
    };
    let decoded = signed.try_decode_trusted_payload().expect("decode");
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
