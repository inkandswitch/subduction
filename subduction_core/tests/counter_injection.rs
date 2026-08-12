//! The counter injected via `SubductionBuilder::send_counter` must be the
//! one that stamps messages, shared between the `SyncHandler` and
//! `Subduction` so both draw from one per-peer sequence.
//!
//! Guards the wiring, not the counter: a builder that silently dropped the
//! injected counter would disable seeding while every counter unit test
//! still passed.

#![allow(clippy::panic)]

use std::sync::Arc;

use future_form::Sendable;
use subduction_core::{
    connection::{
        message::SyncMessage,
        test_utils::{ChannelMockConnection, InstantTimeout, TestSpawn, test_signer},
    },
    peer::{counter::PeerCounter, id::PeerId},
    policy::open::OpenPolicy,
    storage::memory::MemoryStorage,
    subduction::builder::SubductionBuilder,
};
use testresult::TestResult;

const fn seed() -> u64 {
    5_000
}

/// `build()` threads the injected counter into both `Subduction` and the
/// `SyncHandler` it constructs — one shared sequence, starting from the
/// seed.
#[tokio::test]
async fn injected_send_counter_is_used_and_shared() -> TestResult {
    let (subduction, handler, _listener_fut, _actor_fut) =
        SubductionBuilder::<_, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .send_counter(PeerCounter::with_seed(seed))
            .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let peer = PeerId::new([1u8; 32]);

    // Subduction stamps from the injected seed…
    assert_eq!(
        subduction.stamp_send_counter(peer).await,
        5_001,
        "Subduction must stamp from the injected counter's seed"
    );

    // …and the handler continues the *same* sequence (shared instance,
    // not merely an identically-seeded copy).
    assert_eq!(
        handler.send_counter().next(peer).await,
        5_002,
        "the handler must share the injected counter with Subduction"
    );

    Ok(())
}

/// `build_composed()` — the path the CLI uses — has its own copy of the
/// counter wiring; guard it independently of `build()`.
#[tokio::test]
async fn injected_send_counter_flows_through_build_composed() -> TestResult {
    let (subduction, _listener_fut, _actor_fut, sync_handler) =
        SubductionBuilder::<_, _, _, _, _, 256>::new()
            .signer(test_signer())
            .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
            .spawner(TestSpawn)
            .timer(InstantTimeout)
            .send_counter(PeerCounter::with_seed(seed))
            .build_composed::<Sendable, ChannelMockConnection<SyncMessage>, _, _>(|sync| {
                (sync.clone(), sync)
            });

    let peer = PeerId::new([1u8; 32]);

    assert_eq!(
        subduction.stamp_send_counter(peer).await,
        5_001,
        "build_composed must hand Subduction the injected counter"
    );
    assert_eq!(
        sync_handler.send_counter().next(peer).await,
        5_002,
        "the composed SyncHandler must share the injected counter"
    );

    Ok(())
}

/// `build_with_handler()` uses the injected counter for `Subduction`
/// (sharing with a custom handler is the caller's documented job).
#[tokio::test]
async fn injected_send_counter_flows_through_build_with_handler() -> TestResult {
    // Any pre-built handler will do; borrow one from a throwaway build().
    let (_sd, handler, _l, _m) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TestSpawn)
        .timer(InstantTimeout)
        .build::<Sendable, ChannelMockConnection<SyncMessage>>();

    let (subduction, _listener_fut, _actor_fut) = SubductionBuilder::<_, _, _, _, _, 256>::new()
        .signer(test_signer())
        .storage(MemoryStorage::new(), Arc::new(OpenPolicy))
        .spawner(TestSpawn)
        .timer(InstantTimeout)
        .send_counter(PeerCounter::with_seed(seed))
        .build_with_handler::<Sendable, ChannelMockConnection<SyncMessage>, _>(handler);

    let peer = PeerId::new([1u8; 32]);
    assert_eq!(
        subduction.stamp_send_counter(peer).await,
        5_001,
        "build_with_handler must hand Subduction the injected counter, \
         not a fresh default"
    );

    Ok(())
}
