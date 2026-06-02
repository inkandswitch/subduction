//! wasm32 companion to `sedimentree_core/tests/fingerprint_stability.rs`;
//! asserts the same baselines hold on wasm32. Constants must stay in sync
//! with the native side.
//!
//! Run with:
//! ```sh
//! wasm-pack test --node sedimentree_wasm --test fingerprint_stability_wasm
//! ```

#![cfg(target_arch = "wasm32")]
#![allow(missing_docs)]
// `test_seed` is a test helper; whether it can be `const` depends on the
// active feature set, so don't force it.
#![allow(clippy::missing_const_for_fn)]

use sedimentree_core::{
    crypto::fingerprint::{Fingerprint, FingerprintSeed},
    loose_commit::id::CommitId,
};
use wasm_bindgen_test::wasm_bindgen_test;

const TEST_SEED_KEY0: u64 = 0x1234_5678_9ABC_DEF0;
const TEST_SEED_KEY1: u64 = 0xFEDC_BA98_7654_3210;

const EXPECTED_FP_ZEROES: u64 = 6_748_340_123_268_596_282;
const EXPECTED_FP_ONES: u64 = 13_743_385_435_344_457_055;
const EXPECTED_FP_SEQUENTIAL: u64 = 10_722_668_375_651_339_477;
const EXPECTED_FP_REPEATING: u64 = 16_398_704_403_767_843_780;

fn test_seed() -> FingerprintSeed {
    FingerprintSeed::new(TEST_SEED_KEY0, TEST_SEED_KEY1)
}

fn id_sequential() -> CommitId {
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        #[allow(clippy::cast_possible_truncation)]
        {
            *b = (i + 1) as u8;
        }
    }
    CommitId::new(bytes)
}

#[wasm_bindgen_test]
fn wasm_fingerprint_of_all_zero_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &CommitId::new([0u8; 32]));
    assert_eq!(fp.as_u64(), EXPECTED_FP_ZEROES);
}

#[wasm_bindgen_test]
fn wasm_fingerprint_of_all_ones_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &CommitId::new([0xFFu8; 32]));
    assert_eq!(fp.as_u64(), EXPECTED_FP_ONES);
}

#[wasm_bindgen_test]
fn wasm_fingerprint_of_sequential_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &id_sequential());
    assert_eq!(fp.as_u64(), EXPECTED_FP_SEQUENTIAL);
}

#[wasm_bindgen_test]
fn wasm_fingerprint_of_repeating_commit_id_matches_baseline() {
    let fp: Fingerprint<CommitId> = Fingerprint::new(&test_seed(), &CommitId::new([42u8; 32]));
    assert_eq!(fp.as_u64(), EXPECTED_FP_REPEATING);
}
