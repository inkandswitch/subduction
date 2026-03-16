//! Unit tests for [`EphemeralHandler`].
//!
//! Covers subscribe/unsubscribe, fan-out, policy enforcement,
//! payload size limits, disconnect cleanup, and the callback channel.

#![allow(clippy::expect_used, clippy::panic)]

use std::{sync::Arc, time::Duration};

use async_lock::Mutex;
use future_form::Sendable;
use nonempty::NonEmpty;
use sedimentree_core::{collections::Map, id::SedimentreeId};
use subduction_core::{
    authenticated::Authenticated, connection::test_utils::ChannelMockConnection, handler::Handler,
    peer::id::PeerId,
};
use subduction_ephemeral::{
    config::{EphemeralConfig, EphemeralEvent},
    handler::EphemeralHandler,
    message::EphemeralMessage,
    policy::OpenEphemeralPolicy,
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

const fn topic(n: u8) -> SedimentreeId {
    SedimentreeId::new([n; 32])
}

fn make_open_handler(
    connections: Connections,
) -> (
    Arc<EphemeralHandler<Sendable, EphConn, OpenEphemeralPolicy>>,
    async_channel::Receiver<EphemeralEvent>,
) {
    let (handler, rx) =
        EphemeralHandler::new(connections, OpenEphemeralPolicy, EphemeralConfig::default());
    (Arc::new(handler), rx)
}

fn make_small_payload_handler(
    connections: Connections,
    max_payload_size: usize,
) -> (
    Arc<EphemeralHandler<Sendable, EphConn, OpenEphemeralPolicy>>,
    async_channel::Receiver<EphemeralEvent>,
) {
    let config = EphemeralConfig {
        max_payload_size,
        ..EphemeralConfig::default()
    };
    let (handler, rx) = EphemeralHandler::new(connections, OpenEphemeralPolicy, config);
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
                ids: vec![topic(0xAA)],
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
    handler
        .handle(
            &auth_b,
            EphemeralMessage::Ephemeral {
                id: topic(0xAA),
                payload: vec![42],
            },
        )
        .await?;

    let forwarded =
        tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv()).await??;
    assert_eq!(
        forwarded,
        EphemeralMessage::Ephemeral {
            id: topic(0xAA),
            payload: vec![42],
        }
    );

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
                ids: vec![topic(0xBB)],
            },
        )
        .await?;
    handler
        .handle(
            &auth_a,
            EphemeralMessage::Unsubscribe {
                ids: vec![topic(0xBB)],
            },
        )
        .await?;

    handler
        .handle(
            &auth_b,
            EphemeralMessage::Ephemeral {
                id: topic(0xBB),
                payload: vec![99],
            },
        )
        .await?;

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
                ids: vec![topic(1), topic(2)],
            },
        )
        .await?;

    handler
        .handle(
            &auth_b,
            EphemeralMessage::Ephemeral {
                id: topic(2),
                payload: vec![77],
            },
        )
        .await?;

    let forwarded =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    assert_eq!(
        forwarded,
        EphemeralMessage::Ephemeral {
            id: topic(2),
            payload: vec![77],
        }
    );

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
                ids: vec![topic(0xCC)],
            },
        )
        .await?;
    handler
        .handle(
            &auth_a,
            EphemeralMessage::Ephemeral {
                id: topic(0xCC),
                payload: vec![1, 2, 3],
            },
        )
        .await?;

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
                    ids: vec![topic(0xDD)],
                },
            )
            .await?;
    }

    handler
        .handle(
            &auth_c,
            EphemeralMessage::Ephemeral {
                id: topic(0xDD),
                payload: vec![10, 20],
            },
        )
        .await?;

    let expected = EphemeralMessage::Ephemeral {
        id: topic(0xDD),
        payload: vec![10, 20],
    };

    let msg_a =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    let msg_b =
        tokio::time::timeout(Duration::from_millis(100), handle_b.outbound_rx.recv()).await??;

    assert_eq!(msg_a, expected);
    assert_eq!(msg_b, expected);

    Ok(())
}

// ── Callback channel ────────────────────────────────────────────────────

#[tokio::test]
async fn callback_channel_receives_event() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_open_handler(connections.clone());
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Ephemeral {
                id: topic(0xEE),
                payload: vec![5, 6, 7],
            },
        )
        .await?;

    let event = tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await??;
    assert_eq!(
        event,
        EphemeralEvent {
            id: topic(0xEE),
            sender: peer(1),
            payload: vec![5, 6, 7],
        }
    );

    Ok(())
}

// ── Payload size limits ─────────────────────────────────────────────────

#[tokio::test]
async fn inbound_payload_too_large_dropped() -> TestResult {
    let connections: Connections = Arc::new(Mutex::new(Map::new()));
    let (handler, event_rx) = make_small_payload_handler(connections.clone(), 10);
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Ephemeral {
                id: topic(0xFF),
                payload: vec![0u8; 20],
            },
        )
        .await?;

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
                ids: vec![topic(0x11)],
            },
        )
        .await?;

    handler.publish(topic(0x11), vec![0u8; 20]).await;

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
                ids: vec![topic(1), topic(2)],
            },
        )
        .await?;

    Handler::<Sendable, EphConn>::on_peer_disconnect(&*handler, peer(1)).await;

    for t in [topic(1), topic(2)] {
        handler
            .handle(
                &auth_b,
                EphemeralMessage::Ephemeral {
                    id: t,
                    payload: vec![0],
                },
            )
            .await?;
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
                ids: vec![topic(0x22)],
            },
        )
        .await?;

    handler.publish(topic(0x22), vec![88, 99]).await;

    let received =
        tokio::time::timeout(Duration::from_millis(100), handle_a.outbound_rx.recv()).await??;
    assert_eq!(
        received,
        EphemeralMessage::Ephemeral {
            id: topic(0x22),
            payload: vec![88, 99],
        }
    );

    Ok(())
}

// ── Policy rejection ────────────────────────────────────────────────────

mod deny_policy {
    use core::fmt;

    use future_form::{FutureForm, Sendable};
    use sedimentree_core::id::SedimentreeId;
    use subduction_core::peer::id::PeerId;
    use subduction_ephemeral::policy::EphemeralPolicy;

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
            _id: SedimentreeId,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::SubscribeDisallowed>> {
            Sendable::from_future(async { Err(Denied) })
        }

        fn authorize_publish(
            &self,
            _peer: PeerId,
            _id: SedimentreeId,
        ) -> <Sendable as FutureForm>::Future<'_, Result<(), Self::PublishDisallowed>> {
            Sendable::from_future(async { Err(Denied) })
        }

        fn filter_authorized_subscribers(
            &self,
            _id: SedimentreeId,
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
    );
    let handler = Arc::new(handler);
    let (auth, handle) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth,
            EphemeralMessage::Subscribe {
                ids: vec![topic(0xAA), topic(0xBB)],
            },
        )
        .await?;

    let rejected =
        tokio::time::timeout(Duration::from_millis(100), handle.outbound_rx.recv()).await??;
    match rejected {
        EphemeralMessage::SubscribeRejected { ids } => {
            assert_eq!(ids.len(), 2);
            assert!(ids.contains(&topic(0xAA)));
            assert!(ids.contains(&topic(0xBB)));
        }
        other => panic!("expected SubscribeRejected, got {other:?}"),
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
    );
    let handler = Arc::new(handler);
    let (auth_a, _handle_a) = register_peer(&connections, peer(1)).await;

    handler
        .handle(
            &auth_a,
            EphemeralMessage::Ephemeral {
                id: topic(0xCC),
                payload: vec![1, 2, 3],
            },
        )
        .await?;

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
                ids: vec![topic(0xFF)],
            },
        )
        .await?;

    Ok(())
}
