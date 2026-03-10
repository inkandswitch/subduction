//! Variable-length integer encoding shootout.
//!
//! Compares bijou64 against varu64, vu64, vu128, and leb128 across several
//! value distributions that reflect real-world usage in Subduction:
//!
//! - **Tiny** (0–247): blob counts, small lengths, enum tags
//! - **Small** (248–65 535): typical payload sizes
//! - **Medium** (65 536–4 294 967 295): large blob sizes, offsets
//! - **Large** (> 2³²): content hashes interpreted as integers, counters
//! - **Tier boundaries**: worst-case branch-predictor stress
//! - **Uniform random**: unbiased full-range comparison
//!
//! Run: `cargo bench -p bijou64 --bench shootout`

#![allow(missing_docs, unreachable_pub)]

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use criterion_pprof::criterion::{Output, PProfProfiler};
use rand::{rngs::SmallRng, Rng, SeedableRng};

// ---------------------------------------------------------------------------
// Value distributions
// ---------------------------------------------------------------------------

/// Fixed seed for reproducibility.
const SEED: u64 = 0xBEEF_CAFE_DEAD_F00D;

/// Number of values per batch (large enough to amortise loop overhead,
/// small enough to stay in L1 cache for the encoded buffers).
const BATCH: usize = 4096;

fn make_rng() -> SmallRng {
    SmallRng::seed_from_u64(SEED)
}

/// Returns a named set of value distributions.
fn distributions() -> Vec<(&'static str, Vec<u64>)> {
    let mut rng = make_rng();
    vec![
        (
            "tiny_0_247",
            (0..BATCH).map(|_| rng.gen_range(0..=247u64)).collect(),
        ),
        (
            "small_248_65535",
            (0..BATCH).map(|_| rng.gen_range(248..=65_535u64)).collect(),
        ),
        (
            "medium_64k_4G",
            (0..BATCH)
                .map(|_| rng.gen_range(65_536..=u64::from(u32::MAX)))
                .collect(),
        ),
        (
            "large_above_4G",
            (0..BATCH)
                .map(|_| rng.gen_range(u64::from(u32::MAX) + 1..=u64::MAX))
                .collect(),
        ),
        ("tier_boundaries", tier_boundary_values()),
        (
            "uniform_random",
            (0..BATCH).map(|_| rng.gen_range(0..=u64::MAX)).collect(),
        ),
    ]
}

/// Values at and around every bijou64 tier boundary — worst case for
/// branch predictors because the tag byte alternates between tiers.
fn tier_boundary_values() -> Vec<u64> {
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
// Encoding helpers (uniform interface for each library)
// ---------------------------------------------------------------------------

/// Pre-encode a batch of values for decode benchmarks.
/// Returns (encoded_bytes, offsets_into_bytes).
fn pre_encode_bijou64(values: &[u64]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut offsets = Vec::with_capacity(values.len());
    for &v in values {
        offsets.push(buf.len());
        bijou64::encode(v, &mut buf);
    }
    (buf, offsets)
}

fn pre_encode_varu64(values: &[u64]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut tmp = [0u8; 9];
    let mut offsets = Vec::with_capacity(values.len());
    for &v in values {
        offsets.push(buf.len());
        let n = varu64::encode(v, &mut tmp);
        buf.extend_from_slice(&tmp[..n]);
    }
    (buf, offsets)
}

fn pre_encode_vu64(values: &[u64]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut offsets = Vec::with_capacity(values.len());
    for &v in values {
        offsets.push(buf.len());
        let encoded = vu64::encode(v);
        buf.extend_from_slice(encoded.as_ref());
    }
    (buf, offsets)
}

fn pre_encode_vu128(values: &[u64]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut tmp = [0u8; 9];
    let mut offsets = Vec::with_capacity(values.len());
    for &v in values {
        offsets.push(buf.len());
        let n = vu128::encode_u64(&mut tmp, v);
        buf.extend_from_slice(&tmp[..n]);
    }
    (buf, offsets)
}

fn pre_encode_leb128(values: &[u64]) -> (Vec<u8>, Vec<usize>) {
    let mut buf = Vec::with_capacity(values.len() * 5);
    let mut offsets = Vec::with_capacity(values.len());
    for &v in values {
        offsets.push(buf.len());
        leb128::write::unsigned(&mut buf, v).unwrap();
    }
    (buf, offsets)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_encode(c: &mut Criterion) {
    for (dist_name, values) in &distributions() {
        let mut group = c.benchmark_group(format!("encode/{dist_name}"));
        group.throughput(Throughput::Elements(BATCH as u64));

        group.bench_function(BenchmarkId::new("bijou64", ""), |b| {
            b.iter_batched(
                || Vec::with_capacity(BATCH * 9),
                |mut buf| {
                    for &v in values {
                        bijou64::encode(v, &mut buf);
                    }
                    buf
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("varu64", ""), |b| {
            b.iter_batched(
                || (Vec::with_capacity(BATCH * 9), [0u8; 9]),
                |(mut buf, mut tmp)| {
                    for &v in values {
                        let n = varu64::encode(v, &mut tmp);
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    buf
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("vu64", ""), |b| {
            b.iter_batched(
                || Vec::with_capacity(BATCH * 9),
                |mut buf| {
                    for &v in values {
                        let encoded = vu64::encode(v);
                        buf.extend_from_slice(encoded.as_ref());
                    }
                    buf
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("vu128", ""), |b| {
            b.iter_batched(
                || (Vec::with_capacity(BATCH * 9), [0u8; 9]),
                |(mut buf, mut tmp)| {
                    for &v in values {
                        let n = vu128::encode_u64(&mut tmp, v);
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    buf
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function(BenchmarkId::new("leb128", ""), |b| {
            b.iter_batched(
                || Vec::with_capacity(BATCH * 10),
                |mut buf| {
                    for &v in values {
                        leb128::write::unsigned(&mut buf, v).unwrap();
                    }
                    buf
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

fn bench_decode(c: &mut Criterion) {
    for (dist_name, values) in &distributions() {
        let mut group = c.benchmark_group(format!("decode/{dist_name}"));
        group.throughput(Throughput::Elements(BATCH as u64));

        let (bijou_buf, bijou_off) = pre_encode_bijou64(values);
        let (varu_buf, varu_off) = pre_encode_varu64(values);
        let (vu64_buf, vu64_off) = pre_encode_vu64(values);
        let (vu_buf, vu_off) = pre_encode_vu128(values);
        let (leb_buf, leb_off) = pre_encode_leb128(values);

        group.bench_function(BenchmarkId::new("bijou64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &off in &bijou_off {
                    let (v, _) = bijou64::decode(&bijou_buf[off..]).unwrap();
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("varu64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &off in &varu_off {
                    let (v, _) = varu64::decode(&varu_buf[off..]).unwrap();
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &off in &vu64_off {
                    let v = vu64::decode(&vu64_buf[off..]).unwrap();
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu128", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &off in &vu_off {
                    // vu128 requires a &[u8; 9] — copy from the slice.
                    // In practice callers would have a buffer already;
                    // we include the copy to be fair.
                    let remaining = &vu_buf[off..];
                    let mut tmp = [0u8; 9];
                    let copy_len = remaining.len().min(9);
                    tmp[..copy_len].copy_from_slice(&remaining[..copy_len]);
                    let (v, _) = vu128::decode_u64(&tmp);
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("leb128", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &off in &leb_off {
                    let mut cursor = &leb_buf[off..];
                    let v = leb128::read::unsigned(&mut cursor).unwrap();
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        group.finish();
    }
}

fn bench_encode_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_array");

    for (dist_name, values) in &distributions() {
        group.throughput(Throughput::Elements(BATCH as u64));

        group.bench_function(BenchmarkId::new("bijou64", dist_name), |b| {
            b.iter(|| {
                let mut sum = 0usize;
                for &v in values {
                    let (_, len) = bijou64::encode_array(v);
                    sum += len;
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu64", dist_name), |b| {
            b.iter(|| {
                let mut sum = 0usize;
                for &v in values {
                    let encoded = vu64::encode(v);
                    sum += encoded.as_ref().len();
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu128", dist_name), |b| {
            b.iter(|| {
                let mut sum = 0usize;
                let mut buf = [0u8; 9];
                for &v in values {
                    sum += vu128::encode_u64(&mut buf, v);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("varu64", dist_name), |b| {
            b.iter(|| {
                let mut sum = 0usize;
                let mut buf = [0u8; 9];
                for &v in values {
                    sum += varu64::encode(v, &mut buf);
                }
                sum
            });
        });
    }

    group.finish();
}

fn bench_round_trip(c: &mut Criterion) {
    for (dist_name, values) in &distributions() {
        let mut group = c.benchmark_group(format!("round_trip/{dist_name}"));
        group.throughput(Throughput::Elements(BATCH as u64));

        group.bench_function(BenchmarkId::new("bijou64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &v in values {
                    let (arr, len) = bijou64::encode_array(v);
                    let (decoded, _) = bijou64::decode(&arr[..len]).unwrap();
                    sum = sum.wrapping_add(decoded);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("varu64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                let mut buf = [0u8; 9];
                for &v in values {
                    let n = varu64::encode(v, &mut buf);
                    let (decoded, _) = varu64::decode(&buf[..n]).unwrap();
                    sum = sum.wrapping_add(decoded);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu64", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                for &v in values {
                    let encoded = vu64::encode(v);
                    let decoded = vu64::decode(encoded.as_ref()).unwrap();
                    sum = sum.wrapping_add(decoded);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu128", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                let mut buf = [0u8; 9];
                for &v in values {
                    let _n = vu128::encode_u64(&mut buf, v);
                    let (decoded, _) = vu128::decode_u64(&buf);
                    sum = sum.wrapping_add(decoded);
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("leb128", ""), |b| {
            b.iter(|| {
                let mut sum = 0u64;
                let mut buf = Vec::with_capacity(10);
                for &v in values {
                    buf.clear();
                    leb128::write::unsigned(&mut buf, v).unwrap();
                    let mut cursor = buf.as_slice();
                    let decoded = leb128::read::unsigned(&mut cursor).unwrap();
                    sum = sum.wrapping_add(decoded);
                }
                sum
            });
        });

        group.finish();
    }
}

fn bench_encoded_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("encoded_size");

    for (dist_name, values) in &distributions() {
        group.throughput(Throughput::Elements(BATCH as u64));

        group.bench_function(BenchmarkId::new("bijou64", dist_name), |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &v in values {
                    total += bijou64::encoded_len(v);
                }
                total
            });
        });

        group.bench_function(BenchmarkId::new("varu64", dist_name), |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &v in values {
                    total += varu64::encoding_length(v);
                }
                total
            });
        });

        group.bench_function(BenchmarkId::new("vu64", dist_name), |b| {
            b.iter(|| {
                let mut total = 0usize;
                for &v in values {
                    total += vu64::encoded_len(v) as usize;
                }
                total
            });
        });

        // vu128 doesn't have a standalone encoded_len(u64) — only a
        // prefix-byte decoder. We skip it here since it would need
        // an encode first.
    }

    group.finish();
}

fn bench_stream_decode(c: &mut Criterion) {
    for (dist_name, values) in &distributions() {
        let mut group = c.benchmark_group(format!("stream_decode/{dist_name}"));
        group.throughput(Throughput::Elements(BATCH as u64));

        let (bijou_buf, _) = pre_encode_bijou64(values);
        let (varu_buf, _) = pre_encode_varu64(values);
        let (vu64_buf, _) = pre_encode_vu64(values);
        let (leb_buf, _) = pre_encode_leb128(values);

        group.bench_function(BenchmarkId::new("bijou64", ""), |b| {
            b.iter(|| {
                let mut pos = 0;
                let mut sum = 0u64;
                while pos < bijou_buf.len() {
                    let (v, n) = bijou64::decode(&bijou_buf[pos..]).unwrap();
                    sum = sum.wrapping_add(v);
                    pos += n;
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("varu64", ""), |b| {
            b.iter(|| {
                let mut remaining = varu_buf.as_slice();
                let mut sum = 0u64;
                while !remaining.is_empty() {
                    let (v, rest) = varu64::decode(remaining).unwrap();
                    sum = sum.wrapping_add(v);
                    remaining = rest;
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("vu64", ""), |b| {
            b.iter(|| {
                let mut pos = 0;
                let mut sum = 0u64;
                while pos < vu64_buf.len() {
                    let n = vu64::decoded_len(vu64_buf[pos]);
                    let v = vu64::decode(&vu64_buf[pos..]).unwrap();
                    sum = sum.wrapping_add(v);
                    pos += n as usize;
                }
                sum
            });
        });

        group.bench_function(BenchmarkId::new("leb128", ""), |b| {
            b.iter(|| {
                let mut cursor = leb_buf.as_slice();
                let mut sum = 0u64;
                while !cursor.is_empty() {
                    let v = leb128::read::unsigned(&mut cursor).unwrap();
                    sum = sum.wrapping_add(v);
                }
                sum
            });
        });

        // vu128 skipped for stream decode — its fixed &[u8; 9] API
        // doesn't naturally support streaming from a contiguous buffer
        // without knowing offsets ahead of time.

        group.finish();
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets =
        bench_encode,
        bench_decode,
        bench_encode_array,
        bench_round_trip,
        bench_encoded_size,
        bench_stream_decode,
}
criterion_main!(benches);
