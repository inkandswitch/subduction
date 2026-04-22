//! Wasm-target cryptographic micro-benchmarks.
//!
//! Mirrors the coverage of `subduction_crypto/benches/crypto.rs` so the Wasm numbers can be
//! compared directly against the native numbers. This is the single most useful Wasm bench:
//! it tells us how much perf we lose by crossing the JIT boundary for the primitives that run
//! on every message.
//!
//! ## Coverage
//!
//! | Bench                     | Measures                                               |
//! |---------------------------|--------------------------------------------------------|
//! | `blake3/32b`, `.../1kb`, `.../64kb`, `.../1mb` | BLAKE3 hash throughput at various payload sizes |
//! | `ed25519/sign`            | Ed25519 single signing                                 |
//! | `ed25519/verify`          | Ed25519 single verification                            |
//! | `signed_loose_commit/seal` / `.../verify` | Full `Signed<T>` roundtrip                      |
//!
//! ## Running
//!
//! ```sh
//! # Node.js runtime (fastest; CI-canonical):
//! wasm-pack test --node subduction_wasm -- --test perf_crypto
//!
//! # Browsers (slower; for parity checks):
//! wasm-pack test --headless --chrome subduction_wasm -- --test perf_crypto
//! wasm-pack test --headless --firefox subduction_wasm -- --test perf_crypto
//! ```
//!
//! ## Output
//!
//! Each `#[wasm_bindgen_test]` emits JSON-formatted summary lines to the console:
//!
//! ```text
//! {"bench":"wasm/blake3/32b","unit":"ms","iters":50,"min":0.001,"median":0.002,...}
//! ```
//!
//! Downstream tooling (github-action-benchmark in Phase 4.1, a Playwright reporter for the
//! browser suite) parses these lines out of the test log.

#![cfg(target_family = "wasm")]
#![allow(
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs
)]

use std::hint::black_box;

use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _};
use future_form::Sendable;
use sedimentree_core::{
    loose_commit::LooseCommit,
    test_utils::{blob_from_seed, synthetic_commit},
};
use subduction_bench_support::harness::wasm::{Config, Summary, bench, bench_async};
use subduction_crypto::{
    signed::Signed, test_utils::signer_from_seed, verified_signature::VerifiedSignature,
};
use wasm_bindgen_test::wasm_bindgen_test;

// Default to Node for the primary CI target. Remove / duplicate with `_configure!` if a
// specific test needs a browser-only capability (e.g. WebCrypto, MessageChannel).
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_node_experimental);

// ============================================================================
// BLAKE3 — content hashing
// ============================================================================

#[wasm_bindgen_test]
fn blake3_32b() {
    let data = blob_from_seed(0, 32);
    let summary = bench("wasm/blake3/32b", Config::default(), || {
        let hash = blake3::hash(black_box(data.as_slice()));
        black_box(hash);
    });
    summary.report();
}

#[wasm_bindgen_test]
fn blake3_1kb() {
    let data = blob_from_seed(0, 1024);
    let summary = bench("wasm/blake3/1kb", Config::default(), || {
        let hash = blake3::hash(black_box(data.as_slice()));
        black_box(hash);
    });
    summary.report();
}

#[wasm_bindgen_test]
fn blake3_64kb() {
    let data = blob_from_seed(0, 64 * 1024);
    let summary = bench("wasm/blake3/64kb", Config::default(), || {
        let hash = blake3::hash(black_box(data.as_slice()));
        black_box(hash);
    });
    summary.report();
}

#[wasm_bindgen_test]
fn blake3_1mb() {
    let data = blob_from_seed(0, 1024 * 1024);
    let summary = bench(
        "wasm/blake3/1mb",
        // 1 MB hashes are long enough that we want fewer iters to keep total wall-clock sane.
        Config {
            warmup_iters: 2,
            measurement_iters: 20,
        },
        || {
            let hash = blake3::hash(black_box(data.as_slice()));
            black_box(hash);
        },
    );
    summary.report();
}

// ============================================================================
// Ed25519
// ============================================================================

const SIGN_MSG_SIZE: usize = 128;

#[wasm_bindgen_test]
fn ed25519_sign() {
    let key_bytes = [7u8; 32];
    let signing = SigningKey::from_bytes(&key_bytes);
    let mut msg = [0u8; SIGN_MSG_SIZE];
    for (i, b) in msg.iter_mut().enumerate() {
        // `i` is provably < 256 by construction.
        #[allow(clippy::cast_possible_truncation)]
        let byte = i as u8;
        *b = byte;
    }

    let summary = bench("wasm/ed25519/sign", Config::default(), || {
        let sig: Signature = signing.sign(black_box(&msg));
        black_box(sig);
    });
    summary.report();
}

#[wasm_bindgen_test]
fn ed25519_verify() {
    let key_bytes = [7u8; 32];
    let signing = SigningKey::from_bytes(&key_bytes);
    let verifying = signing.verifying_key();
    let mut msg = [0u8; SIGN_MSG_SIZE];
    for (i, b) in msg.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        let byte = i as u8;
        *b = byte;
    }
    let sig = signing.sign(&msg);

    let summary = bench("wasm/ed25519/verify", Config::default(), || {
        let ok = verifying.verify(black_box(&msg), black_box(&sig)).is_ok();
        black_box(ok);
    });
    summary.report();
}

// ============================================================================
// Signed<T> roundtrips
// ============================================================================

#[wasm_bindgen_test]
async fn signed_loose_commit_seal() {
    let signer = signer_from_seed(0);
    let commit: LooseCommit = synthetic_commit(1, Default::default());

    let summary = bench_async("wasm/signed_loose_commit/seal", Config::default(), || {
        let signer = &signer;
        let payload = commit.clone();
        async move {
            let verified: VerifiedSignature<LooseCommit> =
                Signed::seal::<Sendable, _>(signer, payload).await;
            black_box(verified);
        }
    })
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn signed_loose_commit_verify() {
    let signer = signer_from_seed(0);
    let commit: LooseCommit = synthetic_commit(1, Default::default());
    let sealed = Signed::seal::<Sendable, _>(&signer, commit.clone())
        .await
        .into_signed();

    // Sync verify is OK here (we already own the Signed<T>; verify is pure-Rust + no futures).
    let summary = bench("wasm/signed_loose_commit/verify", Config::default(), || {
        let verified = black_box(&sealed).try_verify();
        black_box(verified.is_ok());
    });
    summary.report();
}

// ============================================================================
// Harness sanity check
// ============================================================================

/// Verify the timing harness itself produces sensible numbers. Guards against regressions in
/// `performance.now()` discovery logic.
#[wasm_bindgen_test]
fn harness_sanity() {
    let summary = bench("wasm/harness_sanity", Config::quick(), || {
        // Do a tiny, non-optimisable op so the closure isn't inlined into nothing.
        let mut sum: u64 = 0;
        for i in 0..100u64 {
            sum = sum.wrapping_add(black_box(i));
        }
        black_box(sum);
    });

    assert_eq!(summary.iters, 10);
    // Timings should be non-negative and ordered: min ≤ median ≤ max.
    assert!(summary.min >= 0.0);
    assert!(summary.median >= summary.min);
    assert!(summary.max >= summary.median);
    // Sanity-print the summary anyway so CI has the data.
    summary.report();
    drop::<Summary>(summary);
}
