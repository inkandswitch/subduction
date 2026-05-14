//! Benchmarks for the inbound-ephemeral dedup fast path.
//!
//! Run with: `cargo bench -p subduction_ephemeral --bench recv_dedup`
//!
//! # What this measures
//!
//! The "Option A" change reorders [`recv_ephemeral`] so a read-only
//! [`EphemeralNonceCache::contains`] probe runs _before_
//! [`Signed::try_verify`]. The intent is to make cross-edge duplicates
//! in meshed topologies cost a hash-table lookup instead of an Ed25519
//! verify (~100µs).
//!
//! These benches expose the per-operation costs so the "before/after"
//! story is grounded in numbers. We measure the leaf operations
//! synchronously (no async runtime overhead), then compose them in two
//! "simulated recv path" benches to show the end-to-end difference on
//! the duplicate path.
//!
//! ## Groups
//!
//! | Group                | What it measures                                                |
//! |----------------------|-----------------------------------------------------------------|
//! | `nonce_cache`        | `contains` hit/miss vs. `check_and_insert` fresh/duplicate.     |
//! | `signed`             | Ed25519 verify vs. unverified field decode.                     |
//! | `recv_duplicate`     | Cost of dropping a duplicate, before-reorder vs. after-reorder. |
//!
//! ## What the numbers tell you
//!
//! `recv_duplicate::before_reorder` is the simulated _old_ ordering
//! (verify-then-nonce-check) on a duplicate. `recv_duplicate::after_reorder`
//! is the simulated _new_ ordering (decode-then-contains, no verify).
//! Their ratio is the speedup the reorder delivers on cross-edge
//! traffic.
//!
//! [`recv_ephemeral`]: subduction_ephemeral::handler::EphemeralHandler
//! [`EphemeralNonceCache::contains`]: subduction_ephemeral::nonce_cache::EphemeralNonceCache::contains
//! [`Signed::try_verify`]: subduction_crypto::signed::Signed::try_verify

#![allow(missing_docs, unreachable_pub, clippy::expect_used)]

use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use subduction_core::{peer::id::PeerId, timestamp::TimestampSeconds};
use subduction_crypto::{signed::Signed, signer::memory::MemorySigner};
use subduction_ephemeral::{
    message::{EphemeralMessage, EphemeralPayload},
    nonce_cache::EphemeralNonceCache,
    payload_header::EphemeralPayloadHeader,
    topic::Topic,
};

/// All benches share these constants so the numbers are comparable.
const WINDOW: Duration = Duration::from_secs(30);
const NOW: TimestampSeconds = TimestampSeconds::new(1_000_000);

/// Build a real signed ephemeral message. The bench reuses the same
/// message bytes across iterations so we measure the recv-side work,
/// not message construction or signing.
fn make_signed(payload_size: usize) -> Signed<EphemeralPayload> {
    let signer = MemorySigner::from_bytes(&[0xAB; 32]);
    let payload = EphemeralPayload {
        id: Topic::new([0x42; 32]),
        nonce: 0xDEAD_BEEF_CAFE_F00D,
        timestamp: NOW,
        payload: vec![0xCC; payload_size],
    };
    let verified = block_on(Signed::seal::<future_form::Sendable, _>(&signer, payload));
    verified.into_signed()
}

fn signer_peer_id() -> PeerId {
    let signer = MemorySigner::from_bytes(&[0xAB; 32]);
    PeerId::from(<MemorySigner as subduction_crypto::signer::Signer<
        future_form::Sendable,
    >>::verifying_key(&signer))
}

// ── Group: nonce_cache ──────────────────────────────────────────────────

fn bench_nonce_cache(c: &mut Criterion) {
    let mut g = c.benchmark_group("nonce_cache");

    let sender = PeerId::new([0x01; 32]);
    let topic = Topic::new([0x02; 32]);

    // contains() on a key the cache has never seen — single `HashMap::get`
    // returning `None`. No allocation, no mutation.
    g.bench_function("contains_miss_unknown_key", |b| {
        let cache = EphemeralNonceCache::new(WINDOW);
        b.iter(|| {
            let hit = cache.contains(sender, topic, 0xFFFF_FFFF);
            assert!(!hit);
        });
    });

    // contains() returns false on a populated entry — exercises the
    // dual-bucket lookup path against ~16 already-inserted nonces.
    g.bench_function("contains_miss_populated_entry", |b| {
        let mut cache = EphemeralNonceCache::new(WINDOW);
        for n in 0..16_u64 {
            assert!(cache.check_and_insert(sender, topic, n, NOW));
        }
        b.iter(|| {
            let hit = cache.contains(sender, topic, 0xFFFF_FFFF);
            assert!(!hit);
        });
    });

    // contains() finds the nonce — this is the duplicate-drop fast path
    // a cross-edge probe takes.
    g.bench_function("contains_hit_current_bucket", |b| {
        let mut cache = EphemeralNonceCache::new(WINDOW);
        assert!(cache.check_and_insert(sender, topic, 0xDEAD_BEEF, NOW));
        b.iter(|| {
            let hit = cache.contains(sender, topic, 0xDEAD_BEEF);
            assert!(hit);
        });
    });

    // check_and_insert() on a fresh nonce — the post-verify write path.
    g.bench_function("check_and_insert_fresh", |b| {
        let base = EphemeralNonceCache::new(WINDOW);
        b.iter_batched(
            || base.clone(),
            |mut cache| {
                let inserted = cache.check_and_insert(sender, topic, 0xCAFE, NOW);
                assert!(inserted);
            },
            BatchSize::SmallInput,
        );
    });

    // check_and_insert() on a duplicate — what the OLD code path called
    // post-verify-on-a-duplicate; compare with `contains_hit` above to
    // see the cost of `entry().or_insert_with(...)` vs `get_mut`.
    g.bench_function("check_and_insert_duplicate", |b| {
        let mut base = EphemeralNonceCache::new(WINDOW);
        assert!(base.check_and_insert(sender, topic, 0xBEEF, NOW));
        b.iter_batched(
            || base.clone(),
            |mut cache| {
                let inserted = cache.check_and_insert(sender, topic, 0xBEEF, NOW);
                assert!(!inserted);
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

// ── Group: signed ───────────────────────────────────────────────────────
//
// Pins down what the reorder skips on cross-edges: a successful
// Ed25519 verify. Also measures the cheap decode we now do up front, so
// the "after" math has a concrete subtraction.

fn bench_signed(c: &mut Criterion) {
    let mut g = c.benchmark_group("signed");

    for payload_size in [0_usize, 64, 1024] {
        let signed = make_signed(payload_size);

        g.bench_function(format!("try_verify_success/payload_{payload_size}B"), |b| {
            b.iter(|| {
                let result = signed.try_verify();
                assert!(result.is_ok());
            });
        });

        // What the post-Option-A `recv_ephemeral` actually does up
        // front: header-only decode (no payload `Vec` allocation, no
        // signature work).
        g.bench_function(format!("try_decode_header/payload_{payload_size}B"), |b| {
            let fields = signed.fields_bytes();
            b.iter(|| {
                let result = EphemeralPayloadHeader::try_decode(fields);
                assert!(result.is_ok());
            });
        });
    }

    g.finish();
}

// ── Group: recv_duplicate ───────────────────────────────────────────────
//
// Composes the leaf ops into the duplicate-handling segment of
// `recv_ephemeral`, then compares the OLD ordering against the NEW
// ordering on the exact same inputs.
//
// We deliberately model only the work done up to the point where the
// duplicate is dropped. Both orderings drop at the same logical step
// ("we already saw this nonce"), they just spend very different amounts
// of CPU getting there.

fn bench_recv_duplicate(c: &mut Criterion) {
    let mut g = c.benchmark_group("recv_duplicate");

    let issuer = signer_peer_id();
    let topic_id = Topic::new([0x42; 32]);
    let nonce: u64 = 0xDEAD_BEEF_CAFE_F00D;

    for payload_size in [0_usize, 64, 1024] {
        let signed = make_signed(payload_size);
        let msg = EphemeralMessage::Ephemeral(Box::new(signed.clone()));

        // Pre-seed a cache so the duplicate path actually finds a hit.
        let mut primed_cache = EphemeralNonceCache::new(WINDOW);
        assert!(primed_cache.check_and_insert(issuer, topic_id, nonce, NOW));

        // ── BEFORE: verify-then-check_and_insert (the old ordering) ─────
        //
        // We do the same work the old `recv_ephemeral` did on a
        // duplicate: full Ed25519 verify, then a nonce-cache write
        // that returns false. This is the cost a cross-edge used to
        // incur on every wasted hop.
        g.bench_function(format!("before_reorder/payload_{payload_size}B"), |b| {
            b.iter_batched(
                || (primed_cache.clone(), &msg),
                |(mut cache, msg)| {
                    let EphemeralMessage::Ephemeral(ref signed) = *msg else {
                        unreachable!();
                    };
                    // 1. Ed25519 verify (the expensive op).
                    let verified = signed.try_verify().expect("verify");
                    let ep = verified.payload();
                    // 2. (Old) size/age checks would happen here.
                    //    They're sub-µs; the verify dominates.
                    // 3. check_and_insert returns false for duplicate.
                    let inserted = cache.check_and_insert(issuer, ep.id, ep.nonce, NOW);
                    assert!(!inserted);
                },
                BatchSize::SmallInput,
            );
        });

        // ── AFTER: decode-then-contains (the new ordering) ──────────────
        //
        // Header-only field decode (no payload Vec allocation), then a
        // read-only cache probe. On a hit we return before paying for
        // verify. Mirrors the production `recv_ephemeral` fast path.
        g.bench_function(format!("after_reorder/payload_{payload_size}B"), |b| {
            b.iter_batched(
                || (primed_cache.clone(), &msg),
                |(cache, msg)| {
                    let EphemeralMessage::Ephemeral(ref signed) = *msg else {
                        unreachable!();
                    };
                    // 1. Header-only decode — no payload copy.
                    let header =
                        EphemeralPayloadHeader::try_decode(signed.fields_bytes()).expect("decode");
                    // 2. (New) size + age checks (sub-µs, skipped here
                    //    for clarity — they're identical to the old
                    //    path so they cancel out of the comparison).
                    // 3. Read-only cache probe: hit, short-circuit.
                    let hit = cache.contains(issuer, header.id, header.nonce);
                    assert!(hit);
                },
                BatchSize::SmallInput,
            );
        });
    }

    g.finish();
}

// ── Driver ──────────────────────────────────────────────────────────────

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50);
    targets =
        bench_nonce_cache,
        bench_signed,
        bench_recv_duplicate,
}

criterion_main!(benches);
