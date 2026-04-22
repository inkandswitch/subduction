//! Ephemeral messaging micro-benchmarks.
//!
//! Covers the three operations that dominate the ephemeral protocol's cost on any hot path:
//!
//! | Bench group                         | Measures                                            |
//! |-------------------------------------|-----------------------------------------------------|
//! | `signed_ephemeral_payload/seal`     | Ed25519 signing of `EphemeralPayload` by size       |
//! | `signed_ephemeral_payload/verify`   | Verify + decode on incoming ephemeral               |
//! | `signed_ephemeral_payload/encode`   | Wire-bytes production (sealed → `as_bytes`)         |
//! | `signed_ephemeral_payload/decode`   | Wire-bytes → `Signed<EphemeralPayload>`             |
//! | `nonce_cache/check_and_insert/*`    | Dedup cache at 1 k / 10 k / 100 k steady-state keys |
//! | `nonce_cache/rotation`              | Bucket-rotation cost at the expiry boundary         |
//!
//! ## Why these?
//!
//! - **Seal/verify/encode/decode** run on every published or received ephemeral message. At
//!   the scale of pub/sub broadcast these are multiplied by fan-out.
//! - **NonceCache** runs on every incoming ephemeral. Its behaviour at steady-state — and
//!   especially at the bucket-rotation boundary — governs per-message cost once a long-lived
//!   subscriber has accumulated cache state.
//!
//! ## What's **not** here
//!
//! - Full `EphemeralHandler::publish` fan-out across N subscribers — that needs a multi-peer
//!   harness and belongs in Phase 1.5 (many-peer scenarios) alongside the `Subduction` core
//!   broadcast benches. The single-message primitives below provide the lower bound.
//!
//! Run with:
//! ```sh
//! cargo bench -p subduction_ephemeral --bench ephemeral
//! ```

#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::{hint::black_box, time::Duration};

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use future_form::Sendable;
use futures::executor::block_on;
use subduction_bench_support::harness::criterion::default_criterion;
use subduction_core::{peer::id::PeerId, timestamp::TimestampSeconds};
use subduction_crypto::{signed::Signed, test_utils::signer_from_seed};
use subduction_ephemeral::{
    message::EphemeralPayload, nonce_cache::EphemeralNonceCache, topic::Topic,
};

// ============================================================================
// Fixture helpers
// ============================================================================

/// Build an `EphemeralPayload` of roughly `size` bytes in the `payload` field.
///
/// Other fields (topic, nonce, timestamp) are fixed and contribute ~50 bytes. We tune the
/// payload slice so sealing measures realistic sizes.
fn payload_of_size(size: usize) -> EphemeralPayload {
    EphemeralPayload {
        id: Topic::new([0x11; 32]),
        nonce: 0xdead_beef,
        timestamp: TimestampSeconds::new(1_700_000_000),
        payload: vec![0x5a; size],
    }
}

/// Seal an `EphemeralPayload` into a `Signed<EphemeralPayload>` via `block_on`.
fn seal(payload: EphemeralPayload) -> Signed<EphemeralPayload> {
    let signer = signer_from_seed(0);
    block_on(Signed::seal::<Sendable, _>(&signer, payload)).into_signed()
}

// ============================================================================
// Signed<EphemeralPayload> benches
// ============================================================================

fn bench_signed_ephemeral_seal(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_ephemeral_payload/seal");

    // 64 B: typical chat-message size
    // 1 KB: small doc state
    // 16 KB: a batch of updates
    for &size in &[64usize, 1024, 16 * 1024] {
        let payload = payload_of_size(size);
        let signer = signer_from_seed(0);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &payload, |b, payload| {
            b.iter_with_setup(
                || payload.clone(),
                |payload| {
                    let verified = block_on(Signed::seal::<Sendable, _>(&signer, payload));
                    black_box(verified)
                },
            );
        });
    }

    group.finish();
}

fn bench_signed_ephemeral_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_ephemeral_payload/verify");

    for &size in &[64usize, 1024, 16 * 1024] {
        let signed = seal(payload_of_size(size));

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &signed, |b, signed| {
            b.iter(|| {
                let ok = black_box(signed).try_verify().is_ok();
                black_box(ok)
            });
        });
    }

    group.finish();
}

fn bench_signed_ephemeral_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_ephemeral_payload/encode");

    for &size in &[64usize, 1024, 16 * 1024] {
        let signed = seal(payload_of_size(size));

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &signed, |b, signed| {
            b.iter(|| {
                let bytes = black_box(signed).as_bytes();
                black_box(bytes.len())
            });
        });
    }

    group.finish();
}

fn bench_signed_ephemeral_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_ephemeral_payload/decode");

    for &size in &[64usize, 1024, 16 * 1024] {
        let signed = seal(payload_of_size(size));
        let bytes = signed.as_bytes().to_vec();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &bytes, |b, bytes| {
            b.iter_with_setup(
                || bytes.clone(),
                |bytes| {
                    let decoded: Signed<EphemeralPayload> =
                        Signed::try_decode(bytes).expect("decode");
                    black_box(decoded)
                },
            );
        });
    }

    group.finish();
}

// ============================================================================
// NonceCache benches
// ============================================================================

/// Default 30 s retention window — long enough to mirror production but short enough that a
/// bench can exercise rotation deterministically.
const WINDOW: Duration = Duration::from_secs(30);

/// Populate the cache with `n` (peer, topic, nonce) entries drawn from `fan_out` unique
/// (peer, topic) pairs. Returns the prefilled cache + the list of peer/topic pairs for reuse.
fn prefill_nonce_cache(
    n: usize,
    fan_out: usize,
    now: TimestampSeconds,
) -> (EphemeralNonceCache, Vec<(PeerId, Topic)>) {
    let mut cache = EphemeralNonceCache::new(WINDOW);

    let keys: Vec<_> = (0..fan_out)
        .map(|i| {
            let peer = PeerId::new([(i & 0xff) as u8; 32]);
            let topic = Topic::new([((i >> 8) & 0xff) as u8; 32]);
            (peer, topic)
        })
        .collect();

    for i in 0..n {
        let (peer, topic) = keys[i % keys.len()];
        // Distinct nonce per insert — guaranteed unique across the range.
        let nonce = i as u64;
        let inserted = cache.check_and_insert(peer, topic, nonce, now);
        debug_assert!(inserted);
    }

    (cache, keys)
}

fn bench_nonce_cache_check_and_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("nonce_cache/check_and_insert");

    let now = TimestampSeconds::new(1_700_000_000);
    // Vary total pre-filled size. Fan-out (peers × topics) is fixed at 64 — this keeps the
    // per-key bucket size high enough that the HashSet path is exercised.
    for &prefill in &[1_000usize, 10_000, 100_000] {
        let (cache, keys) = prefill_nonce_cache(prefill, 64, now);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(prefill),
            &(cache, keys),
            |b, (template, keys)| {
                // Each iteration clones the prefilled cache so repeated inserts don't
                // compound. `Cell<u64>` holds a rolling counter so we touch different
                // peer/topic pairs across iterations (avoiding a degenerate hot-bucket).
                let counter = core::cell::Cell::new(0_u64);
                b.iter_batched(
                    || {
                        let base = counter.get();
                        counter.set(base.wrapping_add(1));
                        (template.clone(), base)
                    },
                    |(mut cache, nonce_base)| {
                        let (peer, topic) = keys[(nonce_base as usize) % keys.len()];
                        // Use a nonce that's provably fresh (outside the prefilled range).
                        let nonce = nonce_base.wrapping_add(10_000_000);
                        let fresh = cache.check_and_insert(peer, topic, nonce, now);
                        black_box(fresh);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_nonce_cache_rotation(c: &mut Criterion) {
    let mut group = c.benchmark_group("nonce_cache/rotation");

    let now = TimestampSeconds::new(1_700_000_000);
    // Prefill at `now`, then trigger rotation by inserting at `now + 2 * WINDOW`. This forces
    // the "current → previous, fresh current" code path on the insert.
    let after = TimestampSeconds::new(1_700_000_000 + 2 * WINDOW.as_secs() + 1);

    for &prefill in &[1_000usize, 10_000] {
        let (cache, keys) = prefill_nonce_cache(prefill, 64, now);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(prefill),
            &(cache, keys),
            |b, (template, keys)| {
                b.iter_batched(
                    || template.clone(),
                    |mut cache| {
                        // Force rotation on the first (peer, topic) entry.
                        let (peer, topic) = keys[0];
                        let fresh = cache.check_and_insert(peer, topic, 999_999, after);
                        black_box(fresh);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_signed_ephemeral_seal,
        bench_signed_ephemeral_verify,
        bench_signed_ephemeral_encode,
        bench_signed_ephemeral_decode,
        bench_nonce_cache_check_and_insert,
        bench_nonce_cache_rotation,
}
criterion_main!(benches);
