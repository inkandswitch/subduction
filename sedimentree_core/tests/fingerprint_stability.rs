//! Asserts that `Fingerprint::new` produces fixed expected `u64`s for a
//! handful of inputs. The companion test
//! `sedimentree_wasm/tests/fingerprint_stability_wasm.rs` asserts the
//! same values from wasm32; if the two ever disagree, the protocol
//! cannot interop between targets.
//!
//! To regenerate after a `siphasher` upgrade, run the `#[ignore]`'d
//! helper:
//!
//! ```sh
//! cargo test --test fingerprint_stability print_fingerprint_baselines \
//!     -- --ignored --nocapture
//! ```
//!
//! Copy the printed values into both this file's `EXPECTED_FP_*` constants
//! and the matching constants in the wasm-side companion. Then run
//! `wasm-pack test --node sedimentree_wasm --test fingerprint_stability_wasm`
//! to confirm both targets agree.

#![allow(clippy::expect_used, clippy::unreadable_literal, missing_docs)]

use sedimentree_core::{
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::id::CommitId,
};

const TEST_SEED_KEY0: u64 = 0x1234_5678_9ABC_DEF0;
const TEST_SEED_KEY1: u64 = 0xFEDC_BA98_7654_3210;

pub const EXPECTED_FP_ZEROES: u64 = 6_748_340_123_268_596_282;
pub const EXPECTED_FP_ONES: u64 = 13_743_385_435_344_457_055;
pub const EXPECTED_FP_SEQUENTIAL: u64 = 10_722_668_375_651_339_477;
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

#[test]
fn fingerprint_of_all_zero_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_zeroes());
    assert_eq!(fp.as_u64(), EXPECTED_FP_ZEROES);
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

#[test]
#[ignore = "diagnostic helper; run with --ignored --nocapture to regenerate baselines"]
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
