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

    #[test]
    fn clone_preserves_counter_but_clears_pending() {
        let mux = Multiplexer::new(test_peer_id(), Duration::from_secs(30));
        let id1 = mux.next_request_id();
        let cloned = mux.clone();
        let id2 = cloned.next_request_id();
        assert_eq!(id2.nonce, id1.nonce + 1);
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
                None,
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
                None,
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

// ── Progress-aware ("idle") timeout behavior ────────────────────────────

mod idle_timeout {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration as StdDuration,
    };

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

    /// A timeout that fires immediately, but on its first `progress_beats`
    /// invocations it first records a completion for an unrelated request
    /// (advancing the connection's completion epoch) — simulating "another
    /// request completed during our idle window". Once the beats are
    /// exhausted, subsequent windows see no progress.
    #[derive(Clone)]
    struct ProgressingTimeout {
        mux: Arc<Multiplexer>,
        remaining: Arc<AtomicU32>,
    }

    impl Timeout<Sendable> for ProgressingTimeout {
        fn timeout<'a, T: 'a>(
            &'a self,
            _dur: Duration,
            _fut: BoxFuture<'a, T>,
        ) -> BoxFuture<'a, Result<T, TimedOut>> {
            Box::pin(async move {
                if self
                    .remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    let other = self.mux.next_request_id();
                    let rx = self.mux.register_pending(other).await;
                    self.mux
                        .resolve_pending(&test_batch_sync_response(other))
                        .await;
                    drop(rx);
                }
                Err(TimedOut)
            })
        }
    }

    /// A timeout that always fires immediately and never records progress.
    #[derive(Clone, Copy)]
    struct AlwaysIdleTimeout;

    impl Timeout<Sendable> for AlwaysIdleTimeout {
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

    /// Build a managed connection. **Keep the returned handle alive** for
    /// the duration of the call: dropping it closes the mock's receiver and
    /// makes `send` fail with `CallError::Send` before the timeout path is
    /// ever reached.
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

    /// While other requests keep completing on the same connection, a
    /// waiting call must NOT time out — it re-arms on each progress beat —
    /// and only fails once progress stops.
    #[tokio::test]
    async fn stays_patient_while_connection_makes_progress_then_times_out() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let remaining = Arc::new(AtomicU32::new(3));
        let timer = ProgressingTimeout {
            mux: mux.clone(),
            remaining: remaining.clone(),
        };
        let (managed, _handle) = managed_with(mux, timer);
        let id = managed.next_request_id();

        let result = <ManagedConnection<
            ChannelMockConnection<SyncMessage>,
            Sendable,
            ProgressingTimeout,
        > as ManagedCall<Sendable, SyncMessage>>::call(&managed, req(id), None)
        .await;

        assert!(
            matches!(result, Err(CallError::Timeout)),
            "expected Timeout once progress stopped, got {result:?}"
        );
        assert_eq!(
            remaining.load(Ordering::SeqCst),
            0,
            "the call must have re-armed through every progress beat"
        );
    }

    /// With no progress on the connection, the very first idle window
    /// elapsing yields `Timeout` (no spinning forever).
    #[tokio::test]
    async fn times_out_immediately_when_no_progress() {
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux, AlwaysIdleTimeout);
        let id = managed.next_request_id();

        let result = tokio::time::timeout(
            StdDuration::from_secs(2),
            <ManagedConnection<
                ChannelMockConnection<SyncMessage>,
                Sendable,
                AlwaysIdleTimeout,
            > as ManagedCall<Sendable, SyncMessage>>::call(&managed, req(id), None),
        )
        .await
        .expect("call must resolve promptly, not spin");

        assert!(
            matches!(result, Err(CallError::Timeout)),
            "expected Timeout with no progress, got {result:?}"
        );
    }

    /// End-to-end with a real timer: a short idle window, a slow "batch"
    /// that keeps completing *other* requests just under the window, and
    /// our own response arriving last — at a total wait that is several
    /// times the idle window. The call must succeed (never false-fire),
    /// which an absolute per-request deadline could not do.
    ///
    /// Uses small real durations (idle window 60ms; 5 beats at 40ms) so the
    /// whole test runs in ~250ms without paused-time support.
    #[tokio::test]
    async fn real_timer_survives_long_drain_then_succeeds() {
        // A tokio-backed idle timer.
        #[derive(Clone, Copy)]
        struct TokioIdle;
        impl Timeout<Sendable> for TokioIdle {
            fn timeout<'a, T: 'a>(
                &'a self,
                dur: Duration,
                fut: BoxFuture<'a, T>,
            ) -> BoxFuture<'a, Result<T, TimedOut>> {
                Box::pin(async move {
                    match tokio::time::timeout(dur, fut).await {
                        Ok(v) => Ok(v),
                        Err(_) => Err(TimedOut),
                    }
                })
            }
        }

        let idle = Duration::from_millis(60);
        let mux = Arc::new(Multiplexer::new(test_peer_id(), idle));
        let (managed, _handle) = managed_with(mux.clone(), TokioIdle);
        let my_id = managed.next_request_id();

        // Background: emit 5 unrelated completions, each 40ms apart (< 60ms
        // idle window), then deliver OUR response last at ~240ms — several
        // idle windows of total wait. An absolute deadline ≤ 240ms would
        // have killed us; progress keeps re-arming.
        let bg_mux = mux.clone();
        let drain = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(StdDuration::from_millis(40)).await;
                let other = bg_mux.next_request_id();
                let rx = bg_mux.register_pending(other).await;
                bg_mux
                    .resolve_pending(&test_batch_sync_response(other))
                    .await;
                drop(rx);
            }
            tokio::time::sleep(StdDuration::from_millis(20)).await;
            bg_mux
                .resolve_pending(&test_batch_sync_response(my_id))
                .await;
        });

        let result = <ManagedConnection<
            ChannelMockConnection<SyncMessage>,
            Sendable,
            TokioIdle,
        > as ManagedCall<Sendable, SyncMessage>>::call(&managed, req(my_id), None)
        .await;

        drain.await.expect("drain task");
        assert!(
            result.is_ok(),
            "call should have stayed patient through the drain and succeeded, got {result:?}"
        );
    }

    /// An explicit per-call idle timeout overrides the connection default.
    #[tokio::test]
    async fn explicit_idle_timeout_is_used() {
        // AlwaysIdleTimeout ignores the duration, but the override path
        // must still funnel through to a Timeout with no progress.
        let mux = Arc::new(Multiplexer::new(test_peer_id(), Duration::from_secs(30)));
        let (managed, _handle) = managed_with(mux, AlwaysIdleTimeout);
        let id = managed.next_request_id();

        let result = <ManagedConnection<
            ChannelMockConnection<SyncMessage>,
            Sendable,
            AlwaysIdleTimeout,
        > as ManagedCall<Sendable, SyncMessage>>::call(
            &managed,
            req(id),
            Some(Duration::from_millis(1)),
        )
        .await;

        assert!(matches!(result, Err(CallError::Timeout)), "got {result:?}");
    }
}
