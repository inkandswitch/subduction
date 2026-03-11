//! Deterministic instruction-count benchmarks using iai-callgrind.
//!
//! Measures CPU instructions, cache misses, and branch mispredictions via
//! Valgrind's Callgrind. Unlike wall-clock benchmarks, results are
//! _deterministic_ -- unaffected by system load, thermal throttling, or
//! scheduling noise. Ideal for CI regression detection.
//!
//! **Requires Linux with Valgrind installed.** Will not run on macOS or Windows.
//!
//! Run:
//!   cargo bench -p bijou64 --bench iai_shootout
//!
//! Install runner (one-time):
//!   cargo install iai-callgrind-runner

#![allow(
    missing_docs,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::indexing_slicing,
    clippy::unwrap_used
)]

use std::hint::black_box;

use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use rand::{rngs::SmallRng, Rng, SeedableRng};

// ---------------------------------------------------------------------------
// Value distributions (same as shootout.rs)
// ---------------------------------------------------------------------------

const BATCH: usize = 4096;
const SEED: u64 = 0xBEEF_CAFE_DEAD_F00D;

fn tiny_values() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..BATCH).map(|_| rng.gen_range(0..=247u64)).collect()
}

fn small_values() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..BATCH).map(|_| rng.gen_range(248..=65_535u64)).collect()
}

fn medium_values() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..BATCH)
        .map(|_| rng.gen_range(65_536..=u64::from(u32::MAX)))
        .collect()
}

fn large_values() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..BATCH)
        .map(|_| rng.gen_range(u64::from(u32::MAX) + 1..=u64::MAX))
        .collect()
}

fn uniform_values() -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(SEED);
    (0..BATCH).map(|_| rng.gen_range(0..=u64::MAX)).collect()
}

fn boundary_values() -> Vec<u64> {
    let boundaries: &[u64] = &[
        0,
        247,
        248,
        503,
        504,
        66_039,
        66_040,
        16_843_255,
        16_843_256,
        4_311_810_551,
        4_311_810_552,
        1_103_823_438_327,
        1_103_823_438_328,
        282_578_800_148_983,
        282_578_800_148_984,
        72_340_172_838_076_919,
        72_340_172_838_076_920,
        u64::MAX,
    ];
    boundaries.iter().copied().cycle().take(BATCH).collect()
}

// ---------------------------------------------------------------------------
// Pre-encode helpers for decode benchmarks
// ---------------------------------------------------------------------------

fn pre_encode_bijou64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 5);
    for &v in values {
        bijou64::encode(v, &mut buf);
    }
    buf
}

fn pre_encode_varu64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut tmp = [0u8; 9];
    for &v in values {
        let n = varu64::encode(v, &mut tmp);
        buf.extend_from_slice(&tmp[..n]);
    }
    buf
}

fn pre_encode_vu64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 5);
    for &v in values {
        let encoded = vu64::encode(v);
        buf.extend_from_slice(encoded.as_ref());
    }
    buf
}

fn pre_encode_leb128(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 5);
    for &v in values {
        leb128::write::unsigned(&mut buf, v).unwrap();
    }
    buf
}

// ---------------------------------------------------------------------------
// Encode benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encode_bijou64(values: Vec<u64>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 9);
    for &v in black_box(&values) {
        bijou64::encode(v, &mut buf);
    }
    buf
}

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encode_varu64(values: Vec<u64>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 9);
    let mut tmp = [0u8; 9];
    for &v in black_box(&values) {
        let n = varu64::encode(v, &mut tmp);
        buf.extend_from_slice(&tmp[..n]);
    }
    buf
}

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encode_vu64(values: Vec<u64>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 9);
    for &v in black_box(&values) {
        let encoded = vu64::encode(v);
        buf.extend_from_slice(encoded.as_ref());
    }
    buf
}

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encode_leb128(values: Vec<u64>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 10);
    for &v in black_box(&values) {
        leb128::write::unsigned(&mut buf, v).unwrap();
    }
    buf
}

// ---------------------------------------------------------------------------
// Decode benchmarks (stream decode from concatenated buffer)
// ---------------------------------------------------------------------------

#[library_benchmark]
#[benches::dist(
    pre_encode_bijou64(&tiny_values()),
    pre_encode_bijou64(&small_values()),
    pre_encode_bijou64(&medium_values()),
    pre_encode_bijou64(&large_values()),
    pre_encode_bijou64(&boundary_values()),
    pre_encode_bijou64(&uniform_values())
)]
fn decode_bijou64(buf: Vec<u8>) -> u64 {
    let buf = black_box(&buf);
    let mut pos = 0;
    let mut sum = 0u64;
    while pos < buf.len() {
        let (v, n) = bijou64::decode(&buf[pos..]).unwrap();
        sum = sum.wrapping_add(v);
        pos += n;
    }
    sum
}

#[library_benchmark]
#[benches::dist(
    pre_encode_varu64(&tiny_values()),
    pre_encode_varu64(&small_values()),
    pre_encode_varu64(&medium_values()),
    pre_encode_varu64(&large_values()),
    pre_encode_varu64(&boundary_values()),
    pre_encode_varu64(&uniform_values())
)]
fn decode_varu64(buf: Vec<u8>) -> u64 {
    let mut remaining = black_box(buf.as_slice());
    let mut sum = 0u64;
    while !remaining.is_empty() {
        let (v, rest) = varu64::decode(remaining).unwrap();
        sum = sum.wrapping_add(v);
        remaining = rest;
    }
    sum
}

#[library_benchmark]
#[benches::dist(
    pre_encode_vu64(&tiny_values()),
    pre_encode_vu64(&small_values()),
    pre_encode_vu64(&medium_values()),
    pre_encode_vu64(&large_values()),
    pre_encode_vu64(&boundary_values()),
    pre_encode_vu64(&uniform_values())
)]
fn decode_vu64(buf: Vec<u8>) -> u64 {
    let buf = black_box(&buf);
    let mut pos = 0;
    let mut sum = 0u64;
    while pos < buf.len() {
        let n = vu64::decoded_len(buf[pos]);
        let v = vu64::decode(&buf[pos..]).unwrap();
        sum = sum.wrapping_add(v);
        pos += n as usize;
    }
    sum
}

#[library_benchmark]
#[benches::dist(
    pre_encode_leb128(&tiny_values()),
    pre_encode_leb128(&small_values()),
    pre_encode_leb128(&medium_values()),
    pre_encode_leb128(&large_values()),
    pre_encode_leb128(&boundary_values()),
    pre_encode_leb128(&uniform_values())
)]
fn decode_leb128(buf: Vec<u8>) -> u64 {
    let mut cursor = black_box(buf.as_slice());
    let mut sum = 0u64;
    while !cursor.is_empty() {
        let v = leb128::read::unsigned(&mut cursor).unwrap();
        sum = sum.wrapping_add(v);
    }
    sum
}

// ---------------------------------------------------------------------------
// Encoded size benchmarks
// ---------------------------------------------------------------------------

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encoded_size_bijou64(values: Vec<u64>) -> usize {
    let mut total = 0usize;
    for &v in black_box(&values) {
        total += bijou64::encoded_len(v);
    }
    total
}

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encoded_size_varu64(values: Vec<u64>) -> usize {
    let mut total = 0usize;
    for &v in black_box(&values) {
        total += varu64::encoding_length(v);
    }
    total
}

#[library_benchmark]
#[benches::dist(
    tiny_values(),
    small_values(),
    medium_values(),
    large_values(),
    boundary_values(),
    uniform_values()
)]
fn encoded_size_vu64(values: Vec<u64>) -> usize {
    let mut total = 0usize;
    for &v in black_box(&values) {
        total += vu64::encoded_len(v) as usize;
    }
    total
}

// ---------------------------------------------------------------------------
// Groups + harness
// ---------------------------------------------------------------------------

library_benchmark_group!(
    name = encode_group;
    benchmarks = encode_bijou64, encode_varu64, encode_vu64, encode_leb128
);

library_benchmark_group!(
    name = decode_group;
    benchmarks = decode_bijou64, decode_varu64, decode_vu64, decode_leb128
);

library_benchmark_group!(
    name = encoded_size_group;
    benchmarks = encoded_size_bijou64, encoded_size_varu64, encoded_size_vu64
);

main!(
    library_benchmark_groups = encode_group,
    decode_group,
    encoded_size_group
);
