//! Tests for the transport layer: [`Multiplexer`], [`MuxTransport`], [`MessageTransport`].

use std::{collections::BTreeSet, time::Duration};

use future_form::Sendable;
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
    transport::{Transport, message::MessageTransport, mux::MuxTransport},
};
use testresult::TestResult;

const fn test_peer_id() -> PeerId {
    PeerId::new([42u8; 32])
}

const fn test_request_id(peer: PeerId, nonce: u64) -> RequestId {
    RequestId {
        requestor: peer,
        nonce,
    }
}

const fn test_batch_sync_response(req_id: RequestId) -> BatchSyncResponse {
    BatchSyncResponse {
        req_id,
        id: SedimentreeId::new([1u8; 32]),
        result: SyncResult::NotFound,
    }
}

fn empty_fingerprint_summary() -> FingerprintSummary {
    FingerprintSummary::new(FingerprintSeed::random(), BTreeSet::new(), BTreeSet::new())
}

// ── Multiplexer ─────────────────────────────────────────────────────────

mod multiplexer {
    use super::*;

    #[test]
    fn next_request_id_uses_peer_id() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let id = mux.next_request_id();
        assert_eq!(id.requestor, test_peer_id());
    }

    #[test]
    fn next_request_id_increments() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let id1 = mux.next_request_id();
        let id2 = mux.next_request_id();
        let id3 = mux.next_request_id();
        assert_eq!(id2.nonce, id1.nonce + 1);
        assert_eq!(id3.nonce, id2.nonce + 1);
    }

    #[test]
    fn different_instances_have_different_starting_nonces() {
        let mux1 = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let mux2 = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        assert_ne!(mux1.next_request_id().nonce, mux2.next_request_id().nonce);
    }

    #[tokio::test]
    async fn register_then_resolve_delivers_response() -> TestResult {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let rx = mux.register_pending(req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(mux.resolve_pending(&resp).await);

        let received = rx.await?;
        assert_eq!(received.req_id, req_id);
        Ok(())
    }

    #[tokio::test]
    async fn resolve_unknown_request_returns_false() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let unknown_id = test_request_id(test_peer_id(), 9999);
        let resp = test_batch_sync_response(unknown_id);
        assert!(!mux.resolve_pending(&resp).await);
    }

    #[tokio::test]
    async fn cancel_prevents_resolution() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let mut rx = mux.register_pending(req_id).await;

        mux.cancel_pending(&req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(!mux.resolve_pending(&resp).await);
        assert!(rx.try_recv().is_err(), "receiver should be dropped");
    }

    #[tokio::test]
    async fn double_resolve_second_returns_false() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let req_id = mux.next_request_id();
        let _rx = mux.register_pending(req_id).await;

        let resp = test_batch_sync_response(req_id);
        assert!(mux.resolve_pending(&resp).await);
        assert!(!mux.resolve_pending(&resp).await);
    }

    #[test]
    fn encode_request_roundtrips_through_sync_message() -> TestResult {
        let req_id = test_request_id(test_peer_id(), 42);
        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([7u8; 32]),
            fingerprint_summary: empty_fingerprint_summary(),
            subscribe: true,
        };

        let bytes = SyncMessage::BatchSyncRequest(req).encode();
        let decoded = SyncMessage::try_decode(&bytes)?;

        let SyncMessage::BatchSyncRequest(r) = decoded else {
            return Err("expected BatchSyncRequest".into());
        };
        assert_eq!(r.req_id, req_id);
        assert!(r.subscribe);
        Ok(())
    }

    #[test]
    fn clone_preserves_counter_but_clears_pending() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let id1 = mux.next_request_id();
        let cloned = mux.clone();
        let id2 = cloned.next_request_id();
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
    async fn send_bytes_delegates_to_inner() -> TestResult {
        let (mux, server) = make_pair();

        Transport::<Sendable>::send_bytes(&mux, b"hello").await?;
        let received = Transport::<Sendable>::recv_bytes(&server).await?;
        assert_eq!(received, b"hello");
        Ok(())
    }

    #[tokio::test]
    async fn recv_bytes_passes_non_response_through() -> TestResult {
        let (mux, server) = make_pair();

        let msg = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0u8; 32]),
            digests: vec![],
        };
        Transport::<Sendable>::send_bytes(&server, &msg.encode()).await?;

        let bytes = Transport::<Sendable>::recv_bytes(&mux).await?;
        let decoded = SyncMessage::try_decode(&bytes)?;
        assert!(matches!(decoded, SyncMessage::BlobsRequest { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn recv_bytes_intercepts_matching_response() -> TestResult {
        let (mux, server) = make_pair();

        let req_id = mux.multiplexer().next_request_id();
        let rx = mux.multiplexer().register_pending(req_id).await;

        let resp = test_batch_sync_response(req_id);
        let resp_msg = SyncMessage::BatchSyncResponse(resp);
        Transport::<Sendable>::send_bytes(&server, &resp_msg.encode()).await?;

        let ping = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([0u8; 32]),
            digests: vec![],
        };
        Transport::<Sendable>::send_bytes(&server, &ping.encode()).await?;

        // recv_bytes should skip the response and return the BlobsRequest
        let bytes = Transport::<Sendable>::recv_bytes(&mux).await?;
        let decoded = SyncMessage::try_decode(&bytes)?;
        assert!(matches!(decoded, SyncMessage::BlobsRequest { .. }));

        let received = rx.await?;
        assert_eq!(received.req_id, req_id);
        Ok(())
    }

    #[tokio::test]
    async fn call_sends_request_and_receives_response() -> TestResult {
        let (mux, server) = make_pair();

        let req_id =
            Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::next_request_id(&mux).await;

        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([5u8; 32]),
            fingerprint_summary: empty_fingerprint_summary(),
            subscribe: false,
        };

        // Respond from the server side
        let server_task = {
            let server = server.clone();
            tokio::spawn(async move {
                let bytes = Transport::<Sendable>::recv_bytes(&server).await?;
                let SyncMessage::BatchSyncRequest(received_req) = SyncMessage::try_decode(&bytes)?
                else {
                    return Err("expected BatchSyncRequest".into());
                };

                let resp = test_batch_sync_response(received_req.req_id);
                let resp_bytes = SyncMessage::BatchSyncResponse(resp).encode();
                Transport::<Sendable>::send_bytes(&server, &resp_bytes).await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            })
        };

        // Drive the mux recv loop to intercept the response
        let mux_recv = mux.clone();
        let _recv_task = tokio::spawn(async move {
            drop(Transport::<Sendable>::recv_bytes(&mux_recv).await);
        });

        let resp = Roundtrip::<Sendable, BatchSyncRequest, BatchSyncResponse>::call(
            &mux,
            req,
            Some(Duration::from_secs(5)),
        )
        .await?;

        assert_eq!(resp.req_id, req_id);
        server_task.await??;
        Ok(())
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
    async fn send_recv_roundtrip() -> TestResult {
        let (sender, receiver) = make_pair();

        let msg = SyncMessage::BlobsRequest {
            id: SedimentreeId::new([3u8; 32]),
            digests: vec![],
        };

        Connection::<Sendable, SyncMessage>::send(&sender, &msg).await?;
        let got = Connection::<Sendable, SyncMessage>::recv(&receiver).await?;
        assert_eq!(got, msg);
        Ok(())
    }

    #[tokio::test]
    async fn recv_returns_decode_error_on_garbage() -> TestResult {
        let (raw_sender, b) = ChannelTransport::pair();
        let receiver = MessageTransport::new(b);

        Transport::<Sendable>::send_bytes(&raw_sender, &[0xFF, 0xFF, 0xFF]).await?;

        let result = Connection::<Sendable, SyncMessage>::recv(&receiver).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn disconnect_delegates_to_inner() -> TestResult {
        let (a, _b) = make_pair();
        Connection::<Sendable, SyncMessage>::disconnect(&a).await?;
        Ok(())
    }
}
