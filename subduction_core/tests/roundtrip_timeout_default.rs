//! The per-call default deadline is **relocated** to the convenience layer:
//! `CallTimeout::Default` resolves against the value configured on the builder
//! via [`SubductionBuilder::roundtrip_timeout`], not a constant buried in the
//! low-level `call`. This test pins that end-to-end: a node built with a short
//! `roundtrip_timeout`, syncing with `CallTimeout::Default` against a wedged
//! peer, must time out at ~that configured value with `CallError::Timeout`.
//!
//! Distinct from `disconnect_aborts_in_flight_sync.rs` (which proves disconnect
//! cancels *before* the deadline): here nothing disconnects, so the *only* way
//! the call resolves is the configured default firing.

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::{sync::Arc, time::Duration};

use future_form::Sendable;
use sedimentree_core::{depth::CountLeadingZeroBytes, id::SedimentreeId};
use subduction_core::{
    authenticated::Authenticated,
    connection::{
        managed::CallError,
        test_utils::{PausableChannelTransport, TokioSpawn, TokioTimeout},
    },
    handler::sync::SyncHandler,
    peer::id::PeerId,
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::{Subduction, builder::SubductionBuilder},
    timeout::call::CallTimeout,
    transport::message::MessageTransport,
};
use subduction_crypto::signer::memory::MemorySigner;
use testresult::TestResult;

type Conn = MessageTransport<PausableChannelTransport>;

type TestSyncHandler =
    SyncHandler<Sendable, MemoryStorage, Conn, OpenPolicy, CountLeadingZeroBytes>;

type TestSubduction = Arc<
    Subduction<
        'static,
        Sendable,
        MemoryStorage,
        Conn,
        TestSyncHandler,
        OpenPolicy,
        MemorySigner,
        TokioTimeout,
        TokioSpawn,
    >,
>;

/// The configured default deadline under test. Short so the test is fast, but
/// far larger than the per-poll slack so the timing assertion is robust.
const CONFIGURED_DEFAULT: Duration = Duration::from_millis(400);

/// Upper bound: a `CallTimeout::Default` call must resolve within this. Well
/// above `CONFIGURED_DEFAULT` (scheduling slack) but far below the *built-in*
/// 30s `DEFAULT_ROUNDTRIP_TIMEOUT`, so if the builder value were ignored and
/// the global default used instead, this bound would blow and the test fails.
const BOUND: Duration = Duration::from_secs(3);

fn make_signer(seed: u8) -> MemorySigner {
    MemorySigner::from_bytes(&[seed; 32])
}

fn make_node_with_default(signer: MemorySigner, default: Duration) -> TestSubduction {
    let (sd, _h, listener, manager) = SubductionBuilder::new()
        .signer(signer)
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TokioSpawn)
        .timer(TokioTimeout)
        .roundtrip_timeout(default)
        .build::<Sendable, Conn>();
    tokio::spawn(listener);
    tokio::spawn(manager);
    sd
}

async fn connect_pair(
    a: &TestSubduction,
    a_signer: &MemorySigner,
    b: &TestSubduction,
    b_signer: &MemorySigner,
) -> TestResult<(PausableChannelTransport, PausableChannelTransport)> {
    let (t_a, t_b) = PausableChannelTransport::pair();

    let conn_a = MessageTransport::new(t_a.clone());
    let conn_b = MessageTransport::new(t_b.clone());

    let peer_a = PeerId::from(a_signer.verifying_key());
    let peer_b = PeerId::from(b_signer.verifying_key());

    let auth_a: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_a, peer_b);
    let auth_b: Authenticated<Conn, Sendable> = Authenticated::new_for_test(conn_b, peer_a);

    a.add_connection(auth_a).await?;
    b.add_connection(auth_b).await?;

    Ok((t_a, t_b))
}

#[tokio::test(flavor = "current_thread")]
async fn call_timeout_default_uses_builder_configured_deadline() -> TestResult {
    let a_signer = make_signer(50);
    let b_signer = make_signer(51);
    let a = make_node_with_default(a_signer.clone(), CONFIGURED_DEFAULT);
    let b = make_node_with_default(b_signer.clone(), CONFIGURED_DEFAULT);

    let (_t_a, t_b) = connect_pair(&a, &a_signer, &b, &b_signer).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Wedge B: it stays byte-connected but never answers, so nothing but the
    // configured default deadline can resolve A's call.
    t_b.pause();

    let sed_id = SedimentreeId::new([9u8; 32]);
    let started = std::time::Instant::now();

    // `CallTimeout::Default` must resolve against the builder's
    // `roundtrip_timeout(CONFIGURED_DEFAULT)`, not the 30s global default.
    let result = tokio::time::timeout(
        BOUND,
        a.sync_with_all_peers(sed_id, true, CallTimeout::Default),
    )
    .await;

    assert!(
        result.is_ok(),
        "CallTimeout::Default did not resolve within {BOUND:?}; the builder's \
         roundtrip_timeout({CONFIGURED_DEFAULT:?}) was likely ignored in favour \
         of the 30s global default"
    );
    let per_peer = result.expect("did not resolve in time")?;

    let elapsed = started.elapsed();
    assert!(
        elapsed >= CONFIGURED_DEFAULT,
        "call resolved in {elapsed:?}, before the configured \
         {CONFIGURED_DEFAULT:?} deadline could have elapsed"
    );

    let b_peer = PeerId::from(b_signer.verifying_key());
    let (success, _stats, conn_errs) = per_peer
        .get(&b_peer)
        .expect("wedged peer must be present in the result map");
    assert!(!success, "wedged peer should not have succeeded");
    assert!(
        matches!(conn_errs[0].1, CallError::Timeout),
        "a Default-timeout call against a wedged peer must resolve as Timeout; \
         got {:?}",
        conn_errs[0].1
    );

    Ok(())
}
