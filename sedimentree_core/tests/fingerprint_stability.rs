//! **Cross-platform fingerprint stability tests.**
//!
//! The user's bug shows two peers (one Wasm browser, one native server)
//! producing "N missing, requesting N" diffs even when they should share
//! commits. One uncaught failure mode is: `Fingerprint::new(seed, value)`
//! producing different `u64`s on `wasm32-unknown-unknown` vs `x86_64-*`.
//!
//! This file hardcodes the expected `u64` for fixed inputs, asserted on
//! whatever target this test runs on. The companion test in
//! `sedimentree_wasm/tests/fingerprint_stability_wasm.rs` asserts the
//! same hardcoded outputs from the wasm32 target. If either side
//! disagrees with the hardcoded values, the platforms hash differently
//! — and the bug is in the std-library or hasher chain, not in the
//! Subduction protocol.
//!
//! To regenerate the hardcoded constants (e.g. after a `siphasher`
//! upgrade), uncomment the `regenerate_baselines` test and run with
//! `cargo test regenerate_baselines -- --nocapture`. Copy the printed
//! values back into the `const`s. Then verify the same printed values
//! match what `wasm-pack test --node sedimentree_wasm` prints.

#![allow(clippy::expect_used, clippy::unreadable_literal)]

use sedimentree_core::{
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::id::CommitId,
};

// ============================================================================
// Hardcoded expected u64 outputs for fixed inputs
// ============================================================================
//
// Generated on x86_64-unknown-linux-gnu with siphasher 1.0.x.
// If you change the inputs, regenerate via the test below.

const TEST_SEED_KEY0: u64 = 0x1234_5678_9ABC_DEF0;
const TEST_SEED_KEY1: u64 = 0xFEDC_BA98_7654_3210;

/// `Fingerprint::new(seed, CommitId([0u8; 32]))`
pub const EXPECTED_FP_ZEROES: u64 = 6_748_340_123_268_596_282;

/// `Fingerprint::new(seed, CommitId([0xFFu8; 32]))`
pub const EXPECTED_FP_ONES: u64 = 13_743_385_435_344_457_055;

/// `Fingerprint::new(seed, CommitId([1, 2, 3, ..., 32]))` (1..=32 as bytes)
pub const EXPECTED_FP_SEQUENTIAL: u64 = 10_722_668_375_651_339_477;

/// `Fingerprint::new(seed, CommitId([42; 32]))`
pub const EXPECTED_FP_REPEATING: u64 = 16_398_704_403_767_843_780;

fn test_seed() -> FingerprintSeed {
    FingerprintSeed::new(TEST_SEED_KEY0, TEST_SEED_KEY1)
}

fn id_zeroes() -> CommitId {
    CommitId::new([0u8; 32])
}

fn id_ones() -> CommitId {
    CommitId::new([0xFF; 32])
}

fn id_sequential() -> CommitId {
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = (i + 1) as u8;
    }
    CommitId::new(bytes)
}

fn id_repeating() -> CommitId {
    CommitId::new([42u8; 32])
}

// ============================================================================
// Tests
// ============================================================================

#[test]
fn fingerprint_of_all_zero_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_zeroes());
    assert_eq!(
        fp.as_u64(),
        EXPECTED_FP_ZEROES,
        "Fingerprint output for (seed={{0x{:016X}, 0x{:016X}}}, all-zero CommitId) \
         differs from the cross-platform baseline. This means SipHash or the \
         underlying Hash impl produces different bytes on this target than on \
         x86_64. The Subduction sync protocol cannot work across peers whose \
         hashes disagree.",
        TEST_SEED_KEY0,
        TEST_SEED_KEY1,
    );
}

#[test]
fn fingerprint_of_all_ones_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_ones());
    assert_eq!(fp.as_u64(), EXPECTED_FP_ONES);
}

#[test]
fn fingerprint_of_sequential_bytes_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_sequential());
    assert_eq!(fp.as_u64(), EXPECTED_FP_SEQUENTIAL);
}

#[test]
fn fingerprint_of_repeating_byte_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_repeating());
    assert_eq!(fp.as_u64(), EXPECTED_FP_REPEATING);
}

/// Helper test that prints the actual `u64`s for the four fixed inputs.
/// Used to (re)generate the `EXPECTED_FP_*` constants above.
///
/// **This test always passes** — its purpose is to print the values to
/// stdout so they can be copied into the constants. Run it on the host
/// AND on wasm32; if the printed values differ across platforms, the
/// bug is confirmed.
///
/// Run with `cargo test print_fingerprint_baselines -- --nocapture`.
#[test]
fn print_fingerprint_baselines() {
    let seed = test_seed();
    let fp_z: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_zeroes());
    let fp_o: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_ones());
    let fp_s: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_sequential());
    let fp_r: Fingerprint<CommitId> = Fingerprint::new(&seed, &id_repeating());

    eprintln!(
        "Fingerprint baselines (seed key0=0x{:016X}, key1=0x{:016X}):",
        TEST_SEED_KEY0, TEST_SEED_KEY1,
    );
    eprintln!("  zeroes     = {}", fp_z.as_u64());
    eprintln!("  ones       = {}", fp_o.as_u64());
    eprintln!("  sequential = {}", fp_s.as_u64());
    eprintln!("  repeating  = {}", fp_r.as_u64());
}
