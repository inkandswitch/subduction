//! Reproducer probes for the two claims in PR #174.
//!
//! 1. `select_next_some()` on an empty `FuturesUnordered` panics, even
//!    inside a `select_biased!` block.
//! 2. `Vec::with_capacity(arr.length() as usize)` from a JS-controlled
//!    length crashes Wasm via allocator failure.
//!
//! These are STANDALONE reproducers — they do NOT depend on any
//! Subduction machinery. The goal is to confirm or refute each claim
//! independently, before deciding what (if anything) to change in
//! production code.

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{
    FutureExt, StreamExt,
    future::{Either, ready, select},
    stream::FuturesUnordered,
};

// ────────────────────────────────────────────────────────────────────
// Claim 1: `select_next_some()` panics on empty FuturesUnordered.
// ────────────────────────────────────────────────────────────────────

/// Bare-bones: poll `select_next_some()` once on an empty
/// `FuturesUnordered`. Does it panic?
///
/// Expectation per Alex / PR #174: PANIC.
/// Hypothesis from reading futures-util 0.3.32 source: completes
/// returning `Pending`; assertion only fires if polled AGAIN after
/// `is_terminated` is set.
#[tokio::test]
async fn claim_1a_select_next_some_first_poll_on_empty() {
    let mut fu: FuturesUnordered<futures::future::Ready<()>> = FuturesUnordered::new();

    // We deliberately race it against a future that completes
    // immediately so we don't hang on the (expected) Pending result.
    let outcome = select(fu.select_next_some(), ready(())).await;
    match outcome {
        Either::Left(_) => panic!("select_next_some should not have completed"),
        Either::Right(((), _)) => {
            // ready(()) won. select_next_some returned Pending. No panic.
        }
    }
}

/// `select_biased!` over `select_next_some()` on an empty
/// `FuturesUnordered` + a channel. Does the loop panic on the first
/// iteration before any message arrives?
///
/// This mirrors the exact pattern in
/// `subduction_core/src/subduction.rs::listen`.
#[tokio::test]
async fn claim_1b_select_biased_with_empty_futures_unordered() {
    let mut fu: FuturesUnordered<futures::future::Ready<((), Result<(), ()>)>> =
        FuturesUnordered::new();
    let (tx, rx) = async_channel::unbounded::<()>();

    // Send a message after a short delay so the loop has time to do
    // its "no-work-yet" polling on the empty FuturesUnordered.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(()).await.unwrap();
    });

    let mut messages_received = 0;
    for _ in 0..3 {
        futures::select_biased! {
            result = fu.select_next_some() => {
                let _: ((), Result<(), ()>) = result;
                panic!("fu should be empty");
            }
            msg = rx.recv().fuse() => {
                if msg.is_ok() {
                    messages_received += 1;
                }
                break;
            }
        }
    }

    assert_eq!(messages_received, 1);
}

/// Push a future to `FuturesUnordered`, drain it, then re-enter the
/// `select_biased!` with the now-terminated stream. Does the second
/// iteration panic when `select_next_some` is created against the
/// terminated stream?
#[tokio::test]
async fn claim_1c_select_biased_after_drain() {
    let mut fu: FuturesUnordered<futures::future::Ready<i32>> = FuturesUnordered::new();
    let (tx, rx) = async_channel::unbounded::<i32>();

    fu.push(ready(1));

    let drained_count = AtomicUsize::new(0);
    let msg_count = AtomicUsize::new(0);

    // Send 2 messages with delay.
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        for n in 10..12 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            tx_clone.send(n).await.unwrap();
        }
    });

    // Three iterations:
    //   1. fu has the pushed `1`; drains it.
    //   2. fu now empty (is_terminated will be set on first poll);
    //      msg arrives.
    //   3. msg arrives.
    for _ in 0..3 {
        futures::select_biased! {
            n = fu.select_next_some() => {
                assert_eq!(n, 1);
                drained_count.fetch_add(1, Ordering::SeqCst);
            }
            msg = rx.recv().fuse() => {
                let n = msg.unwrap();
                assert!(n >= 10);
                msg_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    assert_eq!(drained_count.load(Ordering::SeqCst), 1);
    assert_eq!(msg_count.load(Ordering::SeqCst), 2);
}

/// Long-running version: 100 iterations of the pattern with random
/// pushes interleaved with messages. If the bug is real and depends
/// on timing, this is more likely to trigger it.
#[tokio::test]
async fn claim_1d_extended_select_biased_drain_cycle() {
    let mut fu: FuturesUnordered<futures::future::Ready<i32>> = FuturesUnordered::new();
    let (tx, rx) = async_channel::unbounded::<i32>();

    // Feed messages at a steady pace.
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        for n in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let _ = tx_clone.send(n).await;
        }
    });

    let mut drained = 0;
    let mut received = 0;
    while received < 50 {
        futures::select_biased! {
            n = fu.select_next_some() => {
                drained += 1;
                assert!(n >= 0);
            }
            msg = rx.recv().fuse() => {
                let n = msg.unwrap();
                received += 1;
                // Push another future on some iterations so fu
                // oscillates between empty and non-empty.
                if n % 3 == 0 {
                    fu.push(ready(n));
                }
            }
        }
    }

    assert_eq!(received, 50);
    // We pushed roughly 17 futures (every 3rd of 50); count is
    // approximate due to drain-vs-msg ordering.
    assert!(drained > 0, "should have drained at least one pushed future");
}

// ────────────────────────────────────────────────────────────────────
// Claim 2: `Vec::with_capacity(arr.length() as usize)` can fail
// catastrophically when the length is hostile/fabricated.
// ────────────────────────────────────────────────────────────────────

/// Probe the failure mode the bot claims to fix: hostile JS passes
/// `{length: u32::MAX}` and we naively reserve that many slots.
///
/// **The result depends entirely on the allocator**:
///
/// - **Linux x86_64** (this test): the kernel overcommits virtual
///   address space, so `try_reserve` of multi-GiB sizes succeeds
///   without ever touching physical RAM. The only failure mode is
///   `CapacityOverflow` when `n * size_of::<T>()` overflows `usize`.
///
/// - **wasm32-unknown-unknown** (NOT exercised by this test;
///   needs `wasm-bindgen-test`): linear memory is hard-capped (4 GiB
///   max in theory, often less in practice), and a failed
///   `__wbindgen_malloc` is unrecoverable. So this CAN panic in
///   the browser even when it doesn't here.
///
/// What this test actually pins down:
///   1. `try_reserve` on overcommit-friendly hosts is a bad proxy for
///      Wasm OOM behavior — don't trust passing native tests as
///      evidence that Wasm is safe.
///   2. The only RELIABLY-failing case on native is one that requires
///      `n * size_of::<T>` to overflow `usize`. With a u8 buffer that
///      needs more than `usize::MAX` items, which JS can't even
///      express in a single `Array.length`.
#[test]
fn claim_2a_vec_with_huge_capacity_native_is_a_bad_proxy_for_wasm() {
    // (a) On Linux x86_64, even `u32::MAX` u8 reservation overcommits:
    let mut v_bytes: Vec<u8> = Vec::new();
    let fabricated_len = u32::MAX as usize;
    let r1 = v_bytes.try_reserve_exact(fabricated_len);
    println!(
        "try_reserve_exact({fabricated_len}) of u8 on host: {}",
        if r1.is_ok() { "OK (overcommit)" } else { "Err" }
    );

    // (b) Even `u32::MAX` items of u64 (= 32 GiB virtual) overcommits:
    let mut v_u64: Vec<u64> = Vec::new();
    let r2 = v_u64.try_reserve_exact(fabricated_len);
    println!(
        "try_reserve_exact({fabricated_len}) of u64 on host: {}",
        if r2.is_ok() { "OK (overcommit)" } else { "Err" }
    );

    // (c) Reliable failure requires forcing `n * size_of::<T>` to
    // exceed `isize::MAX`. With size_of::<T>() = 1, n must exceed
    // `isize::MAX`. usize::MAX > isize::MAX always:
    let mut v_must_fail: Vec<u8> = Vec::new();
    let r3 = v_must_fail.try_reserve_exact(usize::MAX);
    assert!(
        r3.is_err(),
        "usize::MAX of u8 must fail via CapacityOverflow"
    );
}

/// Same probe but with a "merely large" length — what an honest user
/// with many items might pass: 1,000,000 commits. This is well within
/// limits; should succeed.
#[test]
fn claim_2b_vec_with_large_but_realistic_capacity() {
    let mut v: Vec<u64> = Vec::new();
    let realistic_len = 1_000_000;
    v.try_reserve_exact(realistic_len)
        .expect("1M u64s = 8 MiB; this should succeed on any host");
    assert!(v.capacity() >= realistic_len);
}

/// What the existing `cap_array_length` helper on the `wasm-hardening`
/// branch does. Re-implemented here as a sanity check — confirms that
/// the simple "cap at MAX, grow naturally if real data exceeds it"
/// pattern works without surprises.
#[test]
fn claim_2c_capped_with_capacity_is_safe() {
    const MAX_RESERVE: usize = 16 * 1024 * 1024; // 16M items
    let fabricated_len = u32::MAX as usize;
    let capped = fabricated_len.min(MAX_RESERVE);
    let v: Vec<u8> = Vec::with_capacity(capped);
    assert!(v.capacity() >= capped);
    // Honest caller with a small array gets full capacity:
    let small = 42usize;
    let v2: Vec<u8> = Vec::with_capacity(small.min(MAX_RESERVE));
    assert!(v2.capacity() >= small);
}
