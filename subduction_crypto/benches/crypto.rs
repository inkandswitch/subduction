//! Cryptographic micro-benchmarks.
//!
//! Measures the primitives that run on every message: BLAKE3 content hashing, SipHash-2-4
//! fingerprinting, Ed25519 sign/verify (single and batch), and the full `Signed<T>` seal /
//! verify roundtrip for the protocol's two main signed payload types.
//!
//! ## Why these specifically?
//!
//! | Primitive             | Appears in…                                  |
//! |-----------------------|----------------------------------------------|
//! | BLAKE3                | `Digest::hash` (content-addressed storage)   |
//! | SipHash-2-4           | `Fingerprint` (sync-diff summaries)          |
//! | Ed25519 sign          | `Signer::sign`, handshakes, signed commits   |
//! | Ed25519 verify        | Every incoming `Signed<T>`                   |
//! | Ed25519 verify_batch  | Potential future optimisation (flagged in `DECISIONS.md`) |
//! | `Signed<T>::seal`     | Every outbound signed payload                |
//! | `VerifiedSignature::try_from_signed` | Every inbound signed payload  |
//!
//! Run with:
//! ```sh
//! cargo bench -p subduction_crypto --bench crypto
//! ```

#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs,
    unreachable_pub
)]

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use future_form::Sendable;
use futures::executor::block_on;
use rand::{rngs::StdRng, Rng, SeedableRng};
use sedimentree_core::{
    fragment::Fragment,
    loose_commit::LooseCommit,
    test_utils::{blob_from_seed, synthetic_commit, synthetic_fragment},
};
use siphasher::sip::SipHasher24;
use subduction_bench_support::harness::criterion::default_criterion;
use subduction_crypto::{
    signed::Signed, test_utils::signer_from_seed, verified_signature::VerifiedSignature,
};

// ============================================================================
// BLAKE3 — content hashing
// ============================================================================

fn bench_blake3(c: &mut Criterion) {
    let mut group = c.benchmark_group("blake3");

    for size in [32usize, 256, 4096, 65_536, 1_048_576] {
        let data = blob_from_seed(0, size);

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &data, |b, blob| {
            b.iter(|| {
                let hash = blake3::hash(black_box(blob.as_slice()));
                black_box(hash)
            });
        });
    }

    group.finish();
}

// ============================================================================
// SipHash-2-4 — fingerprint hashing
// ============================================================================

fn bench_siphash(c: &mut Criterion) {
    use std::hash::Hasher as _;

    let mut group = c.benchmark_group("siphash");

    // Typical fingerprint input is a 32-byte CommitId.
    for count in [1usize, 10, 100, 1_000, 10_000] {
        let ids: Vec<[u8; 32]> = (0..count)
            .map(|i| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let mut id = [0u8; 32];
                rng.fill(&mut id);
                id
            })
            .collect();

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &ids, |b, commit_ids| {
            b.iter(|| {
                let mut sum: u64 = 0;
                for id in commit_ids {
                    let mut hasher = SipHasher24::new_with_keys(0xdead_beef, 0xcafe_babe);
                    hasher.write(black_box(id));
                    sum ^= hasher.finish();
                }
                black_box(sum)
            });
        });
    }

    group.finish();
}

// ============================================================================
// Ed25519 — sign / verify / verify_batch
// ============================================================================

const SIGN_MESSAGE_SIZE: usize = 128;

fn sign_fixture(seed: u64) -> (SigningKey, VerifyingKey, [u8; SIGN_MESSAGE_SIZE]) {
    let bytes = [seed as u8; 32];
    let signing = SigningKey::from_bytes(&bytes);
    let verifying = signing.verifying_key();
    let mut msg = [0u8; SIGN_MESSAGE_SIZE];
    let mut rng = StdRng::seed_from_u64(seed);
    rng.fill(&mut msg);
    (signing, verifying, msg)
}

fn bench_ed25519_sign(c: &mut Criterion) {
    let mut group = c.benchmark_group("ed25519_sign");
    let (signing, _verifying, msg) = sign_fixture(0);

    group.bench_function("single", |b| {
        b.iter(|| {
            let sig: Signature = signing.sign(black_box(&msg));
            black_box(sig)
        });
    });

    group.finish();
}

fn bench_ed25519_verify(c: &mut Criterion) {
    let mut group = c.benchmark_group("ed25519_verify");
    let (signing, verifying, msg) = sign_fixture(0);
    let sig = signing.sign(&msg);

    group.bench_function("single", |b| {
        b.iter(|| {
            let ok = verifying.verify(black_box(&msg), black_box(&sig)).is_ok();
            black_box(ok)
        });
    });

    // Loop-based batch: the status quo in the codebase — verify each signature in a tight loop.
    // This is what `Signed<T>::try_verify` multiplied by message count looks like.
    for batch in [8usize, 64, 512] {
        let batch_data: Vec<_> = (0..batch)
            .map(|i| {
                let (s, v, m) = sign_fixture(i as u64 + 1);
                (v, m, s.sign(&m))
            })
            .collect();

        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::new("loop", batch), &batch_data, |b, data| {
            b.iter(|| {
                let mut ok = true;
                for (v, m, s) in data {
                    ok &= v.verify(black_box(m), black_box(s)).is_ok();
                }
                black_box(ok)
            });
        });

        // ed25519-dalek's batch-verify primitive. Potential future optimisation — if this
        // beats the loop by ≥2×, it's worth wiring into the handler hot path.
        let keys: Vec<_> = batch_data.iter().map(|(v, _, _)| *v).collect();
        let sigs: Vec<_> = batch_data.iter().map(|(_, _, s)| *s).collect();
        let msgs: Vec<&[u8]> = batch_data.iter().map(|(_, m, _)| m.as_slice()).collect();

        group.bench_with_input(
            BenchmarkId::new("verify_batch", batch),
            &(keys, sigs, msgs),
            |b, (keys, sigs, msgs)| {
                b.iter(|| {
                    let result = ed25519_dalek::verify_batch(
                        black_box(msgs.as_slice()),
                        black_box(sigs.as_slice()),
                        black_box(keys.as_slice()),
                    );
                    black_box(result.is_ok())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Signed<T> — full seal / verify roundtrips
// ============================================================================

fn bench_signed_loose_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_loose_commit");

    let signer = signer_from_seed(0);
    let commit: LooseCommit = synthetic_commit(1, Default::default());

    // Seal: the cost of producing a wire-ready `Signed<LooseCommit>`. This runs on every
    // outbound commit.
    group.bench_function("seal", |b| {
        b.iter_with_setup(
            || commit.clone(),
            |payload| {
                let verified: VerifiedSignature<LooseCommit> =
                    block_on(Signed::seal::<Sendable, _>(&signer, payload));
                black_box(verified)
            },
        );
    });

    // Pre-seal for the verify path — the verify side is what handlers pay on every incoming
    // message, so it matters more in aggregate.
    let signed = block_on(Signed::seal::<Sendable, _>(&signer, commit.clone())).into_signed();

    group.bench_function("try_verify", |b| {
        b.iter(|| {
            let verified = black_box(&signed).try_verify();
            black_box(verified.is_ok())
        });
    });

    // Full decode+verify: the realistic handler path — take wire bytes, decode to `Signed<T>`,
    // then verify. This is what the protocol actually pays per incoming commit.
    let bytes = signed.as_bytes().to_vec();

    group.bench_function("decode_and_verify", |b| {
        b.iter_with_setup(
            || bytes.clone(),
            |bytes| {
                let signed: Signed<LooseCommit> = Signed::try_decode(bytes).expect("decode");
                let verified = signed.try_verify().expect("verify");
                black_box(verified)
            },
        );
    });

    group.finish();
}

fn bench_signed_fragment(c: &mut Criterion) {
    let mut group = c.benchmark_group("signed_fragment");

    let signer = signer_from_seed(0);
    let fragment: Fragment = synthetic_fragment(
        /* head_seed */ 1, /* boundary */ 3, /* checkpoints */ 10,
        /* leading_zeros */ 1,
    );

    group.bench_function("seal", |b| {
        b.iter_with_setup(
            || fragment.clone(),
            |payload| {
                let verified: VerifiedSignature<Fragment> =
                    block_on(Signed::seal::<Sendable, _>(&signer, payload));
                black_box(verified)
            },
        );
    });

    let signed = block_on(Signed::seal::<Sendable, _>(&signer, fragment.clone())).into_signed();

    group.bench_function("try_verify", |b| {
        b.iter(|| {
            let verified = black_box(&signed).try_verify();
            black_box(verified.is_ok())
        });
    });

    let bytes = signed.as_bytes().to_vec();

    group.bench_function("decode_and_verify", |b| {
        b.iter_with_setup(
            || bytes.clone(),
            |bytes| {
                let signed: Signed<Fragment> = Signed::try_decode(bytes).expect("decode");
                let verified = signed.try_verify().expect("verify");
                black_box(verified)
            },
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = default_criterion();
    targets =
        bench_blake3,
        bench_siphash,
        bench_ed25519_sign,
        bench_ed25519_verify,
        bench_signed_loose_commit,
        bench_signed_fragment,
}
criterion_main!(benches);
