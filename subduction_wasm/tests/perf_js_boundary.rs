//! Empirically quantify the **wasm ↔ JS boundary cost** that every
//! `JsStorage::save_loose_commit` call pays in production.
//!
//! ## Why this bench exists
//!
//! `subduction_core/benches/stress.rs` showed that ~95 % of
//! `full_sync_with_peer`'s wall-time at 900 docs is per-commit data
//! transfer. The native bench uses `MemoryStorage` (in-process), so
//! storage I/O is essentially free.
//!
//! In the browser, every per-commit storage write goes:
//!
//! ```text
//! wasm core → wasm-bindgen shim → JS function call → Promise → JsFuture::await → wasm
//! ```
//!
//! Each crossing pays:
//! - argument lowering (Rust types → JS values via wasm-bindgen)
//! - JIT trampolining (`V8` / `SpiderMonkey` wasm function-call ABI)
//! - microtask queueing for Promise resolution
//! - argument lifting (return value JS → Rust)
//!
//! None of that is visible in native benches. Even if the JS side is a
//! no-op, the round-trip has measurable cost.
//!
//! This bench isolates the cost at four progressively-realistic layers:
//!
//! | Bench | Crossing | What it measures |
//! |---|---|---|
//! | `js_call_void` | sync wasm → JS → wasm | bare `Function.call0()` overhead |
//! | `js_promise_void` | wasm → JS → Promise → wasm await | + microtask queue |
//! | `js_save_commit_shape` | + 4 args matching `JsStorage::save_loose_commit` | + arg lowering |
//! | `js_save_commit_with_map_op` | + JS does `Map.set` for an in-mem store | + minimal JS work |
//!
//! The slope between adjacent rows is the cost of the boundary feature
//! that row adds.
//!
//! ## Findings (Node 22, release wasm; per-call cost amortized over batches)
//!
//! | Bench | x100 per-call | x2700 per-call |
//! |---|---|---|
//! | `js_call_void` (sync, no Promise) | ~33 ns | n/a |
//! | `js_promise_void` (Promise resolve) | ~1.07 µs | n/a |
//! | `js_save_commit_shape` (4 args + Promise) | ~1.00 µs | ~1.34 µs |
//! | `js_save_commit_with_map_op` (+ JS Map.set) | ~1.30 µs | ~1.91 µs |
//!
//! **Implication for an automerge-repo-style 900-doc × 3-commit startup
//! (2700 commits)**: the JS↔wasm boundary contributes only **~3.6–5.2 ms
//! of total cost** — including arg lowering and Promise resolution.
//!
//! That's ~100× less than the dominant per-commit cost (Ed25519 verify
//! at ~190 µs/commit ⇒ ~513 ms across 2700 commits). **The JS↔wasm
//! boundary is not a meaningful bottleneck for this workload.**
//!
//! Conclusion: batching commits across the boundary would shave ms, not
//! seconds. The startup-time levers are crypto and per-tree state, not
//! transport.
//!
//! ## Running
//!
//! ```sh
//! wasm-pack test --release --node subduction_wasm \
//!   --test perf_js_boundary -- --nocapture | grep '"bench"'
//! ```

#![cfg(target_family = "wasm")]
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::default_trait_access,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::similar_names,
    clippy::unwrap_used,
    missing_docs
)]

use js_sys::{Promise, Uint8Array};
use sedimentree_core::{id::SedimentreeId, loose_commit::id::CommitId};
use sedimentree_wasm::{commit_id::WasmCommitId, sedimentree_id::WasmSedimentreeId};
use subduction_bench_support::harness::wasm::{Config, bench, bench_async};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_node_experimental);

// ============================================================================
// Inline JS fixtures
// ============================================================================
//
// Defined here in the test file via `wasm_bindgen(inline_js = ...)` so the
// bench is self-contained — no JS module file to keep in sync.

#[wasm_bindgen(inline_js = r#"
    // Synchronous no-op.
    export function js_call_void() {
        return undefined;
    }

    // Async no-op: returns an already-resolved Promise.
    export function js_promise_void() {
        return Promise.resolve(undefined);
    }

    // Shape-matched to `JsStorage.saveLooseCommit(sedimentreeId, commitId,
    // signed, blob)` — accepts the same four args, ignores them, returns a
    // resolved Promise. This is the "if storage was free" lower bound for
    // the real call.
    export function js_save_commit_shape(_sed_id, _commit_id, _signed, _blob) {
        return Promise.resolve(undefined);
    }

    // Same shape but the JS side does a tiny synchronous op (Map.set on a
    // module-scoped Map). Approximates the "JS adapter handed off to a
    // synchronous in-memory store" case — no IndexedDB, no async I/O,
    // just the ABI plus minimal JS work.
    const _store = new Map();
    export function js_save_commit_with_map_op(_sed_id, commit_id_str, signed, blob) {
        // Stringify-keyed write so the JS side does *something* per call.
        _store.set(commit_id_str, { signed, blob });
        return Promise.resolve(undefined);
    }

    // Pre-allocate a key-string source so the bench loop doesn't have to
    // synthesise unique keys per iteration on the JS side.
    export function _make_unique_key(i) {
        return "k" + i;
    }
"#)]
extern "C" {
    fn js_call_void();
    fn js_promise_void() -> Promise;
    fn js_save_commit_shape(
        sedimentree_id: &JsValue,
        commit_id: &JsValue,
        signed: &JsValue,
        blob: &Uint8Array,
    ) -> Promise;
    fn js_save_commit_with_map_op(
        sedimentree_id: &JsValue,
        commit_id_str: &str,
        signed: &JsValue,
        blob: &Uint8Array,
    ) -> Promise;
}

// ============================================================================
// Helpers
// ============================================================================

/// Build a 4-byte-seeded `WasmSedimentreeId`.
fn wasm_sed_id(seed: u32) -> WasmSedimentreeId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seed.to_be_bytes());
    SedimentreeId::new(bytes).into()
}

/// Build a 4-byte-seeded `WasmCommitId`.
fn wasm_commit_id(seq: u32) -> WasmCommitId {
    let mut bytes = [0u8; 32];
    bytes[..4].copy_from_slice(&seq.to_be_bytes());
    CommitId::new(bytes).into()
}

// ============================================================================
// Layer 1: synchronous wasm → JS → wasm
// ============================================================================
//
// This is the bare minimum: no Promise, no args. Whatever number this
// produces is the absolute floor for any `JsStorage` op.

#[wasm_bindgen_test]
fn js_call_void_x1() {
    let summary = bench("wasm/boundary/js_call_void/x1", Config::default(), || {
        js_call_void();
    });
    summary.report();
}

#[wasm_bindgen_test]
fn js_call_void_x100() {
    let summary = bench(
        "wasm/boundary/js_call_void/x100",
        Config {
            warmup_iters: 5,
            measurement_iters: 50,
        },
        || {
            for _ in 0..100 {
                js_call_void();
            }
        },
    );
    summary.report();
}

// ============================================================================
// Layer 2: Promise resolution
// ============================================================================
//
// What a no-op async `JsStorage` op costs — bare ABI + microtask queue.

#[wasm_bindgen_test]
async fn js_promise_void_x1() {
    let summary = bench_async(
        "wasm/boundary/js_promise_void/x1",
        Config::default(),
        || async {
            let p = js_promise_void();
            JsFuture::from(p).await.expect("resolve");
        },
    )
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn js_promise_void_x100() {
    let summary = bench_async(
        "wasm/boundary/js_promise_void/x100",
        Config {
            warmup_iters: 5,
            measurement_iters: 50,
        },
        || async {
            for _ in 0..100 {
                let p = js_promise_void();
                JsFuture::from(p).await.expect("resolve");
            }
        },
    )
    .await;
    summary.report();
}

// ============================================================================
// Layer 3: arg-shape matched to JsStorage::save_loose_commit
// ============================================================================
//
// Same arg lowering cost as the real call: SedimentreeId → JsValue,
// CommitId → JsValue, a placeholder for SignedLooseCommit, and a
// Uint8Array for the blob.

#[wasm_bindgen_test]
async fn js_save_commit_shape_x1() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let commit_id: JsValue = wasm_commit_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    let summary = bench_async(
        "wasm/boundary/js_save_commit_shape/x1",
        Config::default(),
        || async {
            let p = js_save_commit_shape(&sed_id, &commit_id, &signed, &blob);
            JsFuture::from(p).await.expect("resolve");
        },
    )
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn js_save_commit_shape_x100() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let commit_id: JsValue = wasm_commit_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    let summary = bench_async(
        "wasm/boundary/js_save_commit_shape/x100",
        Config {
            warmup_iters: 5,
            measurement_iters: 50,
        },
        || async {
            for _ in 0..100 {
                let p = js_save_commit_shape(&sed_id, &commit_id, &signed, &blob);
                JsFuture::from(p).await.expect("resolve");
            }
        },
    )
    .await;
    summary.report();
}

// ============================================================================
// Layer 4: realistic shape with JS-side `Map.set` per call
// ============================================================================
//
// One step closer to a real adapter: JS does a synchronous `Map.set` per
// call. No IndexedDB, no async I/O — just the ABI cost plus the kind of
// trivial bookkeeping a JS adapter would do. The delta vs. layer 3 is
// the cost of "JS code actually doing something."

#[wasm_bindgen_test]
async fn js_save_commit_with_map_op_x1() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    // One iteration → one fixed key. We're not measuring write-amplification
    // collisions; we're measuring per-call cost.
    let key = "k0";

    let summary = bench_async(
        "wasm/boundary/js_save_commit_with_map_op/x1",
        Config::default(),
        || async {
            let p = js_save_commit_with_map_op(&sed_id, key, &signed, &blob);
            JsFuture::from(p).await.expect("resolve");
        },
    )
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn js_save_commit_with_map_op_x100() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    // Pre-build 100 distinct keys outside the timed region so each
    // iteration's `Map.set` writes a fresh entry.
    let keys: Vec<String> = (0..100u32).map(|i| format!("k{i}")).collect();

    let summary = bench_async(
        "wasm/boundary/js_save_commit_with_map_op/x100",
        Config {
            warmup_iters: 5,
            measurement_iters: 50,
        },
        || {
            let keys = &keys;
            let sed_id = &sed_id;
            let signed = &signed;
            let blob = &blob;
            async move {
                for k in keys {
                    let p = js_save_commit_with_map_op(sed_id, k, signed, blob);
                    JsFuture::from(p).await.expect("resolve");
                }
            }
        },
    )
    .await;
    summary.report();
}

// ============================================================================
// Aggregate: 2700 sequential round-trips (≈ 900 docs × 3 commits)
// ============================================================================
//
// Direct estimate of the JS-side storage I/O floor for an
// automerge-repo-style 900-doc startup. If this is small relative to the
// observed startup time, JS↔wasm boundary cost is *not* the bottleneck.
// If it dominates, batching commits across the boundary is the lever.

#[wasm_bindgen_test]
async fn js_save_commit_shape_x2700() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let commit_id: JsValue = wasm_commit_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    let summary = bench_async(
        "wasm/boundary/js_save_commit_shape/x2700",
        Config {
            warmup_iters: 1,
            measurement_iters: 5,
        },
        || async {
            for _ in 0..2700u32 {
                let p = js_save_commit_shape(&sed_id, &commit_id, &signed, &blob);
                JsFuture::from(p).await.expect("resolve");
            }
        },
    )
    .await;
    summary.report();
}

#[wasm_bindgen_test]
async fn js_save_commit_with_map_op_x2700() {
    let sed_id: JsValue = wasm_sed_id(0).into();
    let signed = JsValue::from_str("placeholder-signed");
    let blob_bytes = vec![0u8; 64];
    let blob = Uint8Array::from(blob_bytes.as_slice());

    let keys: Vec<String> = (0..2700u32).map(|i| format!("k{i}")).collect();

    let summary = bench_async(
        "wasm/boundary/js_save_commit_with_map_op/x2700",
        Config {
            warmup_iters: 1,
            measurement_iters: 5,
        },
        || {
            let keys = &keys;
            let sed_id = &sed_id;
            let signed = &signed;
            let blob = &blob;
            async move {
                for k in keys {
                    let p = js_save_commit_with_map_op(sed_id, k, signed, blob);
                    JsFuture::from(p).await.expect("resolve");
                }
            }
        },
    )
    .await;
    summary.report();
}
