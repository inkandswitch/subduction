//! Tests for the transport layer: [`Multiplexer`], [`MuxTransport`], [`MessageTransport`].

use std::time::Duration;

use future_form::Sendable;
use std::collections::BTreeSet;

use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed, id::SedimentreeId, sedimentree::FingerprintSummary,
};
use subduction_core::{
    connection::{
        Connection, Roundtrip,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage, SyncResult},
        test_utils::{ChannelTransport, InstantTimeout},
    },
    multiplexer::Multiplexer,
    peer::id::PeerId,
    transport::{MessageTransport, MuxTransport},
};

fn test_peer_id() -> PeerId {
    PeerId::new([42u8; 32])
}

fn test_request_id(peer: PeerId, nonce: u64) -> RequestId {
    RequestId {
        requestor: peer,
        nonce,
    }
}

fn test_batch_sync_response(req_id: RequestId) -> BatchSyncResponse {
    BatchSyncResponse {
        req_id,
        id: SedimentreeId::new([1u8; 32]),
        result: SyncResult::NotFound,
    }
}

// ── Multiplexer ─────────────────────────────────────────────────────────

mod multiplexer {
    use super::*;

    #[test]
    fn next_request_id_uses_peer_id() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let id = mux.next_request_id();
        assert_eq!(id.requestor, test_peer_id());
    }

    #[test]
    fn next_request_id_increments() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let id1 = mux.next_request_id();
        let id2 = mux.next_request_id();
        let id3 = mux.next_request_id();
        assert_eq!(id2.nonce, id1.nonce + 1);
        assert_eq!(id3.nonce, id2.nonce + 1);
    }

    #[test]
    fn different_instances_have_different_starting_nonces() {
        let mux1 = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let mux2 = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        // Both are seeded from getrandom; collision is astronomically unlikely.
        assert_ne!(mux1.next_request_id().nonce, mux2.next_request_id().nonce);
    }

    #[tokio::test]
    async fn register_then_resolve_delivers_response() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let rx = mux.register_pending(req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(mux.resolve_pending(&resp).await);

        let received = rx.await.expect("should receive response");
        assert_eq!(received.req_id, req_id);
    }

    #[tokio::test]
    async fn resolve_unknown_request_returns_false() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let unknown_id = test_request_id(test_peer_id(), 9999);
        let resp = test_batch_sync_response(unknown_id);
        assert!(!mux.resolve_pending(&resp).await);
    }

    #[tokio::test]
    async fn cancel_prevents_resolution() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let mut rx = mux.register_pending(req_id).await;

        mux.cancel_pending(&req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(!mux.resolve_pending(&resp).await);
        assert!(rx.try_recv().is_err(), "receiver should be dropped");
    }

    #[tokio::test]
    async fn double_resolve_second_returns_false() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(mux.resolve_pending(&resp).await);
        assert!(!mux.resolve_pending(&resp).await);
    }

    #[test]
    fn encode_request_roundtrips_through_sync_message() {
        let req_id = test_request_id(test_peer_id(), 42);
        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([7u8; 32]),
            fingerprint_summary: FingerprintSummary::new(
                FingerprintSeed::random(),
                BTreeSet::new(),
                BTreeSet::new(),
            ),
            subscribe: true,
        };

        let bytes = Multiplexer::<InstantTimeout>::encode_request(&req);
        let decoded = SyncMessage::try_decode(&bytes).expect("should decode");

        match decoded {
            SyncMessage::BatchSyncRequest(r) => {
                assert_eq!(r.req_id, req_id);
                assert!(r.subscribe);
            }
            other => panic!("expected BatchSyncRequest, got {other:?}"),
        }
    }

    #[test]
    fn clone_preserves_counter_but_clears_pending() {
        let mux = Multiplexer::new(test_peer_id(), InstantTimeout, Duration::from_secs(30));
        let id1 = mux.next_request_id();
        let cloned = mux.clone();
        let id2 = cloned.next_request_id();
        // Counter is copied, so the cloned instance continues from the same value.
        // (The original advanced past id1, so the clone's first ID == original's next.)
        assert_eq!(id2.nonce, id1.nonce + 1);
    }
}

// ── MuxTransport ────────────────────────────────────────────────────────

mod mux_transport {
    use super::*;

    fn make_pair() -> (
        MuxTransport<ChannelTransport, InstantTimeout>,
        ChannelTransport,
    ) {
        let (client, server) = ChannelTransport::pair();
        let mux = MuxTransport::new(
            client,
            InstantTimeout,
            Duration::from_secs(30),
            test_peer_id(),
        );
        (mux, server)
    }

    #[tokio::test]
    async fn send_bytes_delegates_to_inner() {
        let (mux, server) = make_pair();
        use subduction_core::transport::Transport;

        Transport::<Sendable>::send_bytes(&mux, b"hello")
            .await
            .unwrap();
        let received = Transport::<Sendable>::recv_bytes(&server).await.unwrap();
        assert_eq!(received, b"hello");
    }

    #[tokio::test]
    async fn recv_bytes_passes_non_response_through() {
        let (mux, server) = make_pair();
        use subduction_core::transport::Transport;

        // Send a non-BatchSyncResponse message from the server side
        let msg = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0u8; 32]),
            digests: vec![],
        };
        Transport::<Sendable>::send_bytes(&server, &msg.encode())
            .await
            .unwrap();

        // MuxTransport should pass it through
        let bytes = Transport::<Sendable>::recv_bytes(&mux).await.unwrap();
        let decoded = SyncMessage::try_decode(&bytes).unwrap();
        assert!(matches!(decoded, SyncMessage::BlobsRequest { .. }));
    }

    #[tokio::test]
    async fn recv_bytes_intercepts_matching_response() {
        let (mux, server) = make_pair();
        use subduction_core::transport::Transport;

        // Register a pending call
        let req_id = mux.multiplexer().next_request_id();
        let rx = mux.multiplexer().register_pending(req_id).await;

        // Send a matching BatchSyncResponse from the server
        let resp = test_batch_sync_response(req_id);
        let resp_msg = SyncMessage::BatchSyncResponse(resp.clone());
        Transport::<Sendable>::send_bytes(&server, &resp_msg.encode())
            .await
            .unwrap();

        // Also send a non-response message after
        let ping = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0u8; 32]),
            digests: vec![],
        };
        Transport::<Sendable>::send_bytes(&server, &ping.encode())
            .await
            .unwrap();

        // recv_bytes should skip the response and return the BlobsRequest
        let bytes = Transport::<Sendable>::recv_bytes(&mux).await.unwrap();
        let decoded = SyncMessage::try_decode(&bytes).unwrap();
        assert!(matches!(decoded, SyncMessage::BlobsRequest { .. }));

        // The response should have been routed to the pending caller
        let received = rx.await.expect("should receive routed response");
        assert_eq!(received.req_id, req_id);
    }

    #[tokio::test]
    async fn call_sends_request_and_receives_response() {
        let (mux, server) = make_pair();
        use subduction_core::transport::Transport;

        let req_id =
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(&mux).await;

        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([5u8; 32]),
            fingerprint_summary: FingerprintSummary::new(
                FingerprintSeed::random(),
                BTreeSet::new(),
                BTreeSet::new(),
            ),
            subscribe: false,
        };

        // Spawn a task that reads the request from the server and responds
        let server_clone = server.clone();
        let response_task = tokio::spawn(async move {
            let bytes = Transport::<Sendable>::recv_bytes(&server_clone)
                .await
                .unwrap();
            let decoded = SyncMessage::try_decode(&bytes).unwrap();
            let SyncMessage::BatchSyncRequest(received_req) = decoded else {
                panic!("expected BatchSyncRequest");
            };

            let resp = test_batch_sync_response(received_req.req_id);
            let resp_bytes = SyncMessage::BatchSyncResponse(resp).encode();
            Transport::<Sendable>::send_bytes(&server_clone, &resp_bytes)
                .await
                .unwrap();
        });

        // We need something reading the mux's recv_bytes to intercept the response.
        // In practice, the connection_loop does this. Here we spawn a task for it.
        let mux_clone = mux.clone();
        let _recv_task = tokio::spawn(async move {
            // This will loop, intercepting the BatchSyncResponse
            drop(Transport::<Sendable>::recv_bytes(&mux_clone).await);
        });

        let resp = Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(
            &mux,
            req,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();

        assert_eq!(resp.req_id, req_id);
        response_task.await.unwrap();
    }
}

// ── MessageTransport ────────────────────────────────────────────────────

mod message_transport {
    use super::*;

    fn make_pair() -> (
        MessageTransport<ChannelTransport>,
        MessageTransport<ChannelTransport>,
    ) {
        let (a, b) = ChannelTransport::pair();
        (MessageTransport::new(a), MessageTransport::new(b))
    }

    #[tokio::test]
    async fn send_recv_roundtrip() {
        let (sender, receiver) = make_pair();

        let msg = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([3u8; 32]),
            digests: vec![],
        };

        Connection::<Sendable, SyncMessage>::send(&sender, &msg)
            .await
            .unwrap();
        let received = Connection::<Sendable, SyncMessage>::recv(&receiver)
            .await
            .unwrap();

        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn recv_returns_decode_error_on_garbage() {
        let (a, b) = ChannelTransport::pair();
        let sender = a;
        let receiver = MessageTransport::new(b);

        // Send raw garbage bytes
        use subduction_core::transport::Transport;
        Transport::<Sendable>::send_bytes(&sender, &[0xFF, 0xFF, 0xFF])
            .await
            .unwrap();

        let result = Connection::<Sendable, SyncMessage>::recv(&receiver).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn disconnect_delegates_to_inner() {
        let (a, _b) = make_pair();
        let result = Connection::<Sendable, SyncMessage>::disconnect(&a).await;
        assert!(result.is_ok());
    }
}
