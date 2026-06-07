//! Tests for the transport layer: [`Multiplexer`] and [`MessageTransport`].
#![allow(clippy::expect_used)]

use std::{collections::BTreeSet, time::Duration};

use future_form::Sendable;
use sedimentree_core::{
    crypto::fingerprint::FingerprintSeed, id::SedimentreeId, sedimentree::FingerprintSummary,
};
use subduction_core::{
    connection::{
        Connection,
        message::{BatchSyncRequest, BatchSyncResponse, RequestId, SyncMessage, SyncResult},
        test_utils::ChannelTransport,
    },
    multiplexer::Multiplexer,
    peer::id::PeerId,
    remote_heads::RemoteHeads,
    transport::{Transport, message::MessageTransport},
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

fn test_batch_sync_response(req_id: RequestId) -> BatchSyncResponse {
    BatchSyncResponse {
        req_id,
        id: SedimentreeId::new([1u8; 32]),
        result: SyncResult::NotFound,
        responder_heads: RemoteHeads::default(),
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

        assert!(
            matches!(&decoded, SyncMessage::BatchSyncRequest(r) if r.req_id == req_id && r.subscribe),
            "expected matching BatchSyncRequest, got {decoded:?}"
        );
        Ok(())
    }

    // NOTE: `Multiplexer` is intentionally not `Clone` (it is always shared as
    // `Arc<Multiplexer>`); a value-clone would silently drop in-flight senders.
    // The former `clone_preserves_counter_but_clears_pending` test exercised
    // that removed footgun and no longer compiles, so it was deleted.
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

// ── ManagedConnection ───────────────────────────────────────────────────

mod managed_connection {
    use std::sync::Arc;

    use futures::future::BoxFuture;
    use subduction_core::{
        authenticated::Authenticated,
        connection::{
            managed::{CallError, ManagedCall, ManagedConnection},
            message::BatchSyncRequest,
            test_utils::ChannelMockConnection,
        },
        timeout::{TimedOut, Timeout},
    };

    use super::*;

    /// A [`Timeout`] that always immediately returns [`TimedOut`].
    #[derive(Debug, Clone, Copy)]
    struct AlwaysTimeout;

    impl Timeout<Sendable> for AlwaysTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            _fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, TimedOut>> {
            Box::pin(async { Err(TimedOut) })
        }
    }

    type MockHandle =
        subduction_core::connection::test_utils::ChannelMockConnectionHandle<SyncMessage>;

    fn make_managed() -> (
        ManagedConnection<ChannelMockConnection<SyncMessage>, Sendable, AlwaysTimeout>,
        Arc<subduction_core::multiplexer::Multiplexer>,
        MockHandle,
    ) {
        let peer_id = test_peer_id();
        let (conn, handle) = ChannelMockConnection::<SyncMessage>::new_with_handle(peer_id);
        let auth = Authenticated::new_for_test(conn, peer_id);
        let mux = Arc::new(Multiplexer::new(peer_id, Duration::from_secs(30)));
        let managed = ManagedConnection::new(auth, mux.clone(), AlwaysTimeout);
        (managed, mux, handle)
    }

    #[tokio::test]
    async fn call_returns_timeout_error() {
        let (managed, _mux, _handle) = make_managed();
        let req_id = managed.next_request_id();

        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([0xAA; 32]),
            fingerprint_summary: empty_fingerprint_summary(),
            subscribe: false,
        };

        let result =
            <ManagedConnection<ChannelMockConnection<SyncMessage>, Sendable, AlwaysTimeout> as ManagedCall<Sendable, SyncMessage>>::call(
                &managed,
                req,
                Some(Duration::from_millis(1)),
            )
            .await;

        assert!(
            matches!(result, Err(CallError::Timeout)),
            "expected Timeout, got {result:?}"
        );
    }

    #[tokio::test]
    async fn call_cancels_pending_on_timeout() {
        let (managed, mux, _handle) = make_managed();
        let req_id = managed.next_request_id();

        let req = BatchSyncRequest {
            req_id,
            id: SedimentreeId::new([0xBB; 32]),
            fingerprint_summary: empty_fingerprint_summary(),
            subscribe: false,
        };

        drop(
            <ManagedConnection<ChannelMockConnection<SyncMessage>, Sendable, AlwaysTimeout> as ManagedCall<Sendable, SyncMessage>>::call(
                &managed,
                req,
                Some(Duration::from_millis(1)),
            )
            .await,
        );

        // The pending entry should have been cleaned up by cancel_pending.
        let resp = test_batch_sync_response(req_id);
        assert!(
            !mux.resolve_pending(&resp).await,
            "pending entry should have been cancelled after timeout"
        );
    }
}

// ── Caller-owned deadline + cancel-safety ───────────────────────────────

mod call_deadline {
    use std::{sync::Arc, time::Duration as StdDuration};

    use futures::future::BoxFuture;
    use subduction_core::{
        authenticated::Authenticated,
        connection::{
            managed::{CallError, ManagedCall, ManagedConnection},
            message::BatchSyncRequest,
            test_utils::ChannelMockConnection,
        },
        multiplexer::Multiplexer,
        timeout::{TimedOut, Timeout},
    };

    use super::*;

    fn req(id: RequestId) -> BatchSyncRequest {
        BatchSyncRequest {
            req_id: id,
            id: SedimentreeId::new([0xCD; 32]),
            fingerprint_summary: empty_fingerprint_summary(),
            subscribe: false,
        }
    }

    /// A timeout that always fires immediately.
    #[derive(Clone, Copy)]
    struct AlwaysTimeout;

    impl Timeout<Sendable> for AlwaysTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            _fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, TimedOut>> {
            Box::pin(async { Err(TimedOut) })
        }
    }

    /// A timeout that never fires: it just awaits the inner future forever.
    /// Lets a `call` park indefinitely on its response receiver so a test
    /// can drop the call future mid-flight and assert cancel-safety.
    #[derive(Clone, Copy)]
    struct NeverTimeout;

    impl Timeout<Sendable> for NeverTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, TimedOut>> {
            Box::pin(async move { Ok(fut.await) })
        }
    }

    type MockHandle =
        subduction_core::connection::test_utils::ChannelMockConnectionHandle<SyncMessage>;

    /// Build a managed connection. **Keep the returned handle alive** for
    /// the duration of the call: dropping it closes the mock's receiver and
    /// makes `send` fail with `CallError::Send` before the wait is reached.
    fn managed_with<O: Timeout<Sendable>>(
        mux: Arc<Multiplexer>,
        timer: O,
    ) -> (
        ManagedConnection<ChannelMockConnection<SyncMessage>, Sendable, O>,
        MockHandle,
    ) {
        let peer_id = mux.peer_id();
        let (conn, handle) = ChannelMockConnection::<SyncMessage>::new_with_handle(peer_id);
        let auth = Authenticated::new_for_test(conn, peer_id);
        (ManagedConnection::new(auth, mux, timer), handle)
    }

    /// With a deadline and no response, the deadline elapsing yields `Timeout`
    /// promptly (no spinning).
    #[tokio::test]
    async fn times_out_when_no_response() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux, AlwaysTimeout);
        let id = managed.next_request_id();

        // `Some(_)` applies the deadline; `None` would be the uncapped path.
        let result = tokio::time::timeout(
            StdDuration::from_secs(2),
            <ManagedConnection<
                ChannelMockConnection<SyncMessage>,
                Sendable,
                AlwaysTimeout,
            > as ManagedCall<Sendable, SyncMessage>>::call(
                &managed,
                req(id),
                Some(Duration::from_millis(1)),
            ),
        )
        .await
        .expect("call must resolve promptly, not spin");

        assert!(
            matches!(result, Err(CallError::Timeout)),
            "expected Timeout with no response, got {result:?}"
        );
    }

    /// On deadline, the pending entry is cleaned up (the drop guard removes
    /// it), so a late response finds nothing to resolve.
    #[tokio::test]
    async fn timeout_leaves_no_pending_entry() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux.clone(), AlwaysTimeout);
        let id = managed.next_request_id();

        let result = <ManagedConnection<
            ChannelMockConnection<SyncMessage>,
            Sendable,
            AlwaysTimeout,
        > as ManagedCall<Sendable, SyncMessage>>::call(
            &managed,
            req(id),
            Some(Duration::from_millis(1)),
        )
        .await;
        assert!(matches!(result, Err(CallError::Timeout)));

        assert_eq!(
            mux.pending_len().await,
            0,
            "timed-out call must leave no pending entry"
        );
        assert!(
            !mux.resolve_pending(&test_batch_sync_response(id)).await,
            "a late response must find no pending caller"
        );
    }

    /// **Cancel-safety**: dropping the call future *before* it resolves
    /// (e.g. an outer `select!`/`timeout` lost, or shutdown) must remove the
    /// pending entry. Here `NeverTimeout` parks the call on its receiver; we
    /// drop the future and assert the pending map is empty.
    #[tokio::test]
    async fn dropping_call_future_midflight_leaves_no_pending_entry() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux.clone(), NeverTimeout);
        let id = managed.next_request_id();

        {
            let call = <ManagedConnection<
                ChannelMockConnection<SyncMessage>,
                Sendable,
                NeverTimeout,
            > as ManagedCall<Sendable, SyncMessage>>::call(
                &managed, req(id), None
            );
            futures::pin_mut!(call);

            // Poll once so the future registers the pending entry and parks
            // on the (never-resolving) receiver, then abandon it.
            let polled = futures::poll!(&mut call);
            assert!(
                polled.is_pending(),
                "call should be parked awaiting response"
            );
            assert_eq!(
                mux.pending_len().await,
                1,
                "call must have registered a pending entry"
            );
            // `call` dropped here at end of scope.
        }

        // The drop guard removed the entry synchronously (uncontended).
        assert_eq!(
            mux.pending_len().await,
            0,
            "dropping the call future mid-flight must leave no pending entry"
        );
    }

    /// An explicit per-call deadline is honored (overrides the default).
    #[tokio::test]
    async fn explicit_deadline_is_used() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux, AlwaysTimeout);
        let id = managed.next_request_id();

        let result = <ManagedConnection<
            ChannelMockConnection<SyncMessage>,
            Sendable,
            AlwaysTimeout,
        > as ManagedCall<Sendable, SyncMessage>>::call(
            &managed,
            req(id),
            Some(Duration::from_millis(1)),
        )
        .await;

        assert!(matches!(result, Err(CallError::Timeout)), "got {result:?}");
    }

    /// **Lower-level abortable API**: with `timeout: None` the call is uncapped
    /// (no internal deadline) — a *caller* owns the policy. Here the caller
    /// imposes its own bound via an outer `tokio::time::timeout`; when that
    /// fires it drops the (uncapped) call future, and the drop guard must leave
    /// no pending entry. This is the `CallTimeout::Uncapped` use case.
    #[tokio::test]
    async fn uncapped_call_is_bounded_by_outer_timeout_without_leaking() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        // `NeverTimeout` ensures the *inner* call never self-times-out; only the
        // outer `tokio::time::timeout` can end the wait.
        let (managed, _handle) = managed_with(mux.clone(), NeverTimeout);
        let id = managed.next_request_id();

        let outer = tokio::time::timeout(
            StdDuration::from_millis(50),
            <ManagedConnection<
                ChannelMockConnection<SyncMessage>,
                Sendable,
                NeverTimeout,
            > as ManagedCall<Sendable, SyncMessage>>::call(&managed, req(id), None),
        )
        .await;

        assert!(
            outer.is_err(),
            "outer deadline should elapse (uncapped inner call never resolves)"
        );

        // Dropping the uncapped future (because the outer timeout fired) must
        // have removed the pending entry.
        assert_eq!(
            mux.pending_len().await,
            0,
            "an uncapped call dropped by an outer deadline must leave no pending entry"
        );
        assert!(
            !mux.resolve_pending(&test_batch_sync_response(id)).await,
            "a late response must find no pending caller"
        );
    }
}
