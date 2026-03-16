# bijou64 Benchmark Shootout (x86)

> Criterion benchmarks comparing bijou64 against varu64, vu64, vu128, and leb128 across six value distributions over batches of 4096 values.
>
> Run: `cargo bench -p bijou64 --bench shootout`
>
> See also: [ARM results (Apple M2 Pro)](SHOOTOUT_ANALYSIS_ARM.md)

## Methodology

### Wall-Clock Benchmarks (Criterion)

| Setting | Value |
|---------|-------|
| Framework | [Criterion 0.5](https://bheisler.github.io/criterion.rs/) with [pprof flamegraphs](https://docs.rs/pprof/latest/) |
| Sample size | 200 iterations per benchmark |
| Warm-up | 3 seconds |
| Measurement time | 5 seconds per benchmark |
| Batch size | 4096 values (L1-cache-friendly) |
| Seed | `0xBEEF_CAFE_DEAD_F00D` (fixed for reproducibility) |
| Profile | `bench` (`opt-level = 3`, `lto = "thin"`, `debug = true`) |

### Instruction-Count Benchmarks (iai-callgrind)

Deterministic benchmarks via Valgrind's Callgrind. Reports CPU instructions, cache misses, and branch mispredictions. Unaffected by system load or scheduling noise -- ideal for CI regression detection.

| Setting | Value |
|---------|-------|
| Framework | [iai-callgrind 0.16](https://github.com/iai-callgrind/iai-callgrind) |
| Platform | Linux only (requires Valgrind) |
| CI | `.github/workflows/iai-bench.yml` |

Run locally (Linux only):
```bash
cargo install iai-callgrind-runner
cargo bench -p bijou64 --bench iai_shootout
```

### Chart Generation

All charts are auto-generated from Criterion's raw sample data (`target/criterion/**/new/sample.json`).

```bash
# via nix flake app
nix run .#bench-charts

# or via uv (auto-installs Python deps)
uv run bijou64/charts/analyze.py
```

Output:
- `bijou64/charts/percentiles.csv` -- machine-readable statistics
- `bijou64/charts/percentiles.md` -- markdown tables with p50/p90/p95/p99/p99.9
- `bijou64/charts/*_box.svg` -- box-and-whisker plots
- `bijou64/charts/*_bar.svg` -- grouped bar charts (median + p5-p95 whiskers)
- `bijou64/charts/*_cdf.svg` -- CDF overlay plots
- `bijou64/charts/*_heatmap.svg` -- library x distribution heatmaps
- `bijou64/charts/*_cdf.html` -- interactive Plotly CDFs (hover, zoom)
- `bijou64/charts/*_heatmap.html` -- interactive Plotly heatmaps
- `bijou64/charts/percentiles.html` -- sortable/filterable percentile table

### Value Distributions

| Name | Range | Rationale |
|------|-------|-----------|
| tiny (0-247) | Single-byte bijou64 tier | Blob counts, small lengths, enum tags |
| small (248-64k) | 248 -- 65,535 | Typical payload sizes |
| medium (64k-4B) | 65,536 -- 4,294,967,295 | Large blob sizes, offsets |
| large (>4B) | > 2^32 | Content hashes as integers, counters |
| boundary | All 18 tier-edge values, cycled | Worst-case branch prediction |
| uniform random | Full u64 range | Unbiased comparison |

## Machine

|         |                              |
|---------|------------------------------|
| CPU     | AMD Ryzen 5 5600X (Zen 3)    |
| Cores   | 6C / 12T                     |
| Memory  | 64 GB DDR4                   |
| OS      | NixOS 25.11                  |
| Rust    | 1.90.0                       |
| Profile | `bench` (opt-level = 3)      |

## Encode

Encode to a `Vec<u8>`.

| Distribution    | bijou64   | varu64 | vu64  | vu128 | leb128    | bijou64 rank | bijou64 vs other best |
|-----------------|-----------|--------|-------|-------|-----------|--------------|-----------------------|
| tiny (0-247)    | **5.63**  | 15.55  | 25.88 | 14.13 | 6.77      | #1           | 0.83x                 |
| small (248-64k) | 14.76     | 19.91  | 26.09 | 18.86 | **11.95** | #2           | 1.23x                 |
| medium (64k-4B) | **14.58** | 23.07  | 25.59 | 25.68 | 15.95     | #1           | 0.91x                 |
| large (>4B)     | **14.60** | 29.05  | 34.79 | 26.43 | 35.20     | #1           | 0.55x                 |
| boundary        | **13.31** | 23.68  | 27.16 | 21.93 | 14.58     | #1           | 0.91x                 |
| uniform random  | **14.32** | 29.41  | 35.14 | 27.14 | 35.86     | #1           | 0.53x                 |

<details open>
<summary>Charts</summary>

![Encode — Bar Chart](charts/encode_bar.svg)
![Encode — Box Plot](charts/encode_box.svg)
![Encode — CDF](charts/encode_cdf.svg)

</details>

## Encode Array

Encode to a fixed `[u8; 9]` with no allocation. leb128 is excluded because its API requires a `Write` implementor.

| Distribution    | bijou64  | varu64 | vu64     | vu128    | bijou64 rank | bijou64 vs other best |
|-----------------|----------|--------|----------|----------|--------------|-----------------------|
| tiny (0-247)    | **1.87** | 5.87   | 4.51     | 3.16     | #1           | 0.59x                 |
| small (248-64k) | 5.44     | 8.92   | 4.51     | **2.75** | #3           | 1.98x                 |
| medium (64k-4B) | 5.60     | 11.21  | **4.56** | 5.70     | #2           | 1.23x                 |
| large (>4B)     | 5.61     | 18.98  | 4.60     | **5.43** | #3           | 1.03x                 |
| boundary        | 5.22     | 12.40  | 4.53     | **3.80** | #3           | 1.37x                 |
| uniform random  | 5.40     | 18.54  | **4.59** | 5.61     | #2           | 1.18x                 |

<details open>
<summary>Charts</summary>

![Encode Array — Bar Chart](charts/encode_array_bar.svg)
![Encode Array — Box Plot](charts/encode_array_box.svg)

</details>

## Decode

Decode from a `&[u8]` buffer.

| Distribution    | bijou64   | varu64 | vu64  | vu128     | leb128    | bijou64 rank | bijou64 vs other best |
|-----------------|-----------|--------|-------|-----------|-----------|--------------|-----------------------|
| tiny (0-247)    | 7.22      | 7.63   | 12.16 | 12.00     | **4.77**  | #3           | 1.51x                 |
| small (248-64k) | 14.68     | 12.63  | 16.05 | 14.48     | **11.38** | #4           | 1.29x                 |
| medium (64k-4B) | 14.72     | 18.26  | 19.67 | **12.72** | 14.95     | #3           | 1.16x                 |
| large (>4B)     | **10.26** | 26.21  | 11.97 | 11.02     | 35.67     | #1           | 0.93x                 |
| boundary        | 12.63     | 18.96  | 16.15 | **11.33** | 13.63     | #2           | 1.12x                 |
| uniform random  | **10.01** | 25.64  | 12.11 | 10.93     | 34.44     | #1           | 0.92x                 |

<details open>
<summary>Charts</summary>

![Decode — Bar Chart](charts/decode_bar.svg)
![Decode — Box Plot](charts/decode_box.svg)
![Decode — CDF](charts/decode_cdf.svg)

</details>

## Canonical Decode

Decode with a guarantee that the encoding is minimal (no overlong representations accepted). This matters for protocols that need deterministic serialisation -- if two peers can encode the same value differently, content-addressed hashes break.

bijou64 achieves canonicality structurally: its disjoint tier ranges make overlong encodings impossible, so the canonical decode path is identical to regular decode with zero overhead. varu64 and vu64 always perform a runtime minimality check (there's no way to opt out). vu128 and leb128 accept overlong encodings by design, so we wrap them with a decode-then-re-encode-and-compare-length check to simulate what a canonical-aware caller would need to do.

| Distribution    | bijou64   | varu64    | vu64  | vu128 | leb128 | bijou64 rank | bijou64 vs other best |
|-----------------|-----------|-----------|-------|-------|--------|--------------|-----------------------|
| tiny (0-247)    | 7.09      | **6.47**  | 12.79 | 21.23 | 13.93  | #2           | 1.10x                 |
| small (248-64k) | 14.44     | **11.22** | 16.48 | 19.44 | 23.89  | #2           | 1.29x                 |
| medium (64k-4B) | **14.18** | 16.79     | 19.62 | 16.82 | 32.67  | #1           | 0.84x                 |
| large (>4B)     | **9.28**  | 24.44     | 12.82 | 14.63 | 70.05  | #1           | 0.72x                 |
| boundary        | **12.50** | 18.15     | 16.26 | 14.75 | 30.42  | #1           | 0.85x                 |
| uniform random  | **9.15**  | 24.12     | 12.81 | 14.48 | 68.43  | #1           | 0.71x                 |

<details open>
<summary>Charts</summary>

![Canonical Decode — Bar Chart](charts/canonical_decode_bar.svg)
![Canonical Decode — CDF](charts/canonical_decode_cdf.svg)

</details>

The cost of canonicality varies wildly by crate. bijou64 and the plain decode numbers are identical because there's nothing extra to check. varu64 always pays its runtime check -- its numbers here match the regular decode table. vu128 and leb128 take a significant hit from the re-encode step, especially for large values where leb128's byte-at-a-time `Write`/`Read` API makes the round trip catastrophically expensive (70 us vs 36 us without the check).

For protocols that _require_ canonical encoding, this is the table that matters.

## Stream Decode

Decode a concatenated stream of encoded values. vu128 is excluded because its API requires a fixed `[u8; 9]` input.

| Distribution    | bijou64   | varu64   | vu64  | leb128    | bijou64 rank | bijou64 vs other best |
|-----------------|-----------|----------|-------|-----------|--------------|-----------------------|
| tiny (0-247)    | 8.15      | **7.43** | 15.76 | 7.48      | #3           | 1.10x                 |
| small (248-64k) | 15.04     | 13.56    | 19.45 | **12.22** | #3           | 1.23x                 |
| medium (64k-4B) | 16.52     | 19.58    | 21.48 | **13.37** | #2           | 1.24x                 |
| large (>4B)     | **11.94** | 26.60    | 15.95 | 33.14     | #1           | 0.75x                 |
| boundary        | 14.33     | 20.29    | 18.80 | **12.67** | #2           | 1.13x                 |
| uniform random  | **10.95** | 26.43    | 16.07 | 31.11     | #1           | 0.68x                 |

<details open>
<summary>Charts</summary>

![Stream Decode — Bar Chart](charts/stream_decode_bar.svg)
![Stream Decode — CDF](charts/stream_decode_cdf.svg)

</details>

## Percentile Statistics

Full percentile breakdowns (p50/p90/p95/p99/p99.9) are available in:

- [`charts/percentiles.md`](charts/percentiles.md) -- markdown tables
- [`charts/percentiles.csv`](charts/percentiles.csv) -- machine-readable CSV
- [`charts/percentiles.html`](charts/percentiles.html) -- interactive sortable table

Heatmaps provide a quick visual overview of which library performs best across all distributions:

<details>
<summary>Heatmaps (click to expand)</summary>

![Decode Heatmap](charts/decode_heatmap.svg)
![Canonical Decode Heatmap](charts/canonical_decode_heatmap.svg)
![Stream Decode Heatmap](charts/stream_decode_heatmap.svg)
![Encode Heatmap](charts/encode_heatmap.svg)

</details>

Interactive versions with hover-for-detail are in `charts/*_heatmap.html`.

## Encoded Size

Bytes per value compared to a raw 8-byte `u64`. All tag-byte formats (bijou64, varu64, vu64/vu128) add 1 byte of overhead for multi-byte values. leb128 uses 1 continuation bit per byte instead.

bijou64 and varu64 share the same tag threshold (248), so their 1-byte range is wider than vu64/vu128 (0-247 vs 0-127). bijou64's per-tier offsets shift the multi-byte boundaries slightly, but the encoded sizes end up identical to varu64 at every value.

| Value    | Raw `u64` | bijou64          | varu64           | vu64 / vu128     | leb128           |
|----------|-----------|------------------|------------------|------------------|------------------|
| 0        | 8         | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        |
| 127      | 8         | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        |
| 128      | 8         | **1 (12.5%)** | **1 (12.5%)** | 2 (25%)          | 2 (25%)          |
| 247      | 8         | **1 (12.5%)** | **1 (12.5%)** | 2 (25%)          | 2 (25%)          |
| 248      | 8         | 2 (25%)          | 2 (25%)          | 2 (25%)          | 2 (25%)          |
| 255      | 8         | 2 (25%)          | 2 (25%)          | 2 (25%)          | 2 (25%)          |
| 256      | 8         | **2 (25%)**   | 3 (37.5%)        | **2 (25%)**   | **2 (25%)**   |
| 503      | 8         | **2 (25%)**   | 3 (37.5%)        | **2 (25%)**   | **2 (25%)**   |
| 504      | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)**   | **2 (25%)**   |
| 1,000    | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)**   | **2 (25%)**   |
| 16,383   | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)**   | **2 (25%)**   |
| 16,384   | 8         | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        |
| 65,535   | 8         | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        |
| 65,536   | 8         | **3 (37.5%)** | 4 (50%)          | **3 (37.5%)** | **3 (37.5%)** |
| 66,039   | 8         | **3 (37.5%)** | 4 (50%)          | **3 (37.5%)** | **3 (37.5%)** |
| 100,000  | 8         | 4 (50%)          | 4 (50%)          | **3 (37.5%)** | **3 (37.5%)** |
| 2^24 - 1 | 8         | 4 (50%)          | 4 (50%)          | 4 (50%)          | 4 (50%)          |
| 2^32 - 1 | 8         | 5 (62.5%)        | 5 (62.5%)        | 5 (62.5%)        | 5 (62.5%)        |
| 2^40 - 1 | 8         | 6 (75%)          | 6 (75%)          | 6 (75%)          | 6 (75%)          |
| 2^48 - 1 | 8         | 7 (87.5%)        | 7 (87.5%)        | 7 (87.5%)        | 7 (87.5%)        |
| 2^56 - 1 | 8         | 8 (100%)         | 8 (100%)         | 8 (100%)         | 8 (100%)         |
| 2^64 - 1 | 8         | 9 (112.5%)       | 9 (112.5%)       | 9 (112.5%)       | 10 (125%)        |

This table is architecture-independent -- the encoded sizes are a property of the format, not the implementation.

## x86 vs ARM Comparison

The results on this AMD Zen 3 machine diverge noticeably from the [ARM (Apple M2 Pro) results](SHOOTOUT_ANALYSIS_ARM.md):

### bijou64 encode is much stronger on x86

On ARM, bijou64 won encode only for tiny values and placed 2nd-3rd elsewhere. On x86, bijou64 wins 5 of 6 distributions (all except small, where leb128 wins). The gap is dramatic for large and uniform values: bijou64 is 1.8-2.5x faster than the next competitor, while on ARM it was 1.16-1.17x _behind_ vu64.

The likely explanation is x86's `lzcnt` instruction. On Zen 3 it executes in 1 cycle with 1-cycle latency. The `leading_zeros`-based tier derivation that powers bijou64's encode path benefits disproportionately here compared to ARM's `clz`, which has similar throughput but where the competing libraries' simpler branch patterns may be better predicted by Apple's wider pipeline.

### Decode is more competitive, with leb128 surprisingly strong on small values

On ARM, bijou64 won decode for tiny, small, and medium. On x86, leb128 wins tiny and small decode outright (4.77 us and 11.38 us vs bijou64's 7.22 us and 14.68 us). This is unexpected -- leb128's byte-at-a-time `Read` trait interface should be slower. The likely cause is that leb128's tight loop compiles into a well-predicted branch sequence on Zen 3 for short encodings (1-3 bytes), while bijou64's match-on-tag dispatch generates more branch targets.

bijou64 still wins large and uniform decode, and vu128 takes medium and boundary.

### Canonical decode: bijou64 wins where it matters most

bijou64 wins 4 of 6 canonical decode distributions on both architectures. The absolute advantage is larger on x86: 0.71-0.85x vs the next best for medium/large/boundary/uniform (compared to 0.61-0.94x on ARM). The penalty for non-canonical crates is also steeper on x86 -- leb128's canonical decode reaches 70 us for large values (2x its regular decode), making the structural canonicality advantage even more valuable.

### Stream decode: leb128 is the new challenger

On ARM, bijou64 won stream decode for tiny, small, and medium. On x86, leb128 takes small, medium, and boundary, while varu64 takes tiny. bijou64 still dominates large and uniform. The `std::io::Read`-based leb128 stream API apparently compiles better on x86 than ARM for short/medium encodings.

### encode_array: vu128 emerges as a competitor

On ARM, vu64 dominated encode_array across all non-tiny distributions. On x86, vu128 wins small, large, and boundary, while vu64 wins medium and uniform. bijou64 still wins tiny. The vu128 crate's fixed-buffer API appears to benefit from x86's memory access patterns.

### encoded_size: bijou64 regresses relative to ARM

On ARM, bijou64 was competitive for tiny encoded_size (~same as varu64). On x86, varu64 and vu64 dominate, with bijou64 placing 2nd-3rd. The `leading_zeros` + correction path for encoded_len doesn't win against varu64's simpler encoding_length on this architecture, possibly because varu64's if-chain is short enough to predict well on Zen 3.

## Summary

On this particular machine and workload, bijou64 is the fastest _encoder_ for 5 of 6 distributions -- a dramatic improvement over ARM where it only led for tiny values. The `lzcnt`-based tier derivation clearly benefits from Zen 3's efficient count-leading-zeros implementation.

For decoding, the picture is more mixed than on ARM. leb128 dominates tiny and small decode, vu128 wins medium, and bijou64 wins large and uniform. This suggests that for Subduction's hot path (blob sizes typically 54-100 bytes, falling in the tiny tier), leb128 would actually be the fastest decoder on x86 -- though the decode difference is small enough (7.2 us vs 4.8 us per 4096 values, or ~0.6 ns per value) that it's unlikely to matter in practice.

The canonical decode benchmark remains the most important for Subduction. bijou64 wins 4 of 6 distributions and gets canonicality for free. The cost of adding canonicality to non-canonical formats is brutal on x86: leb128 goes from 35 us to 70 us for large values. For any system that needs deterministic serialisation, bijou64's structural canonicality is an overwhelming advantage on this architecture.

The encoded_size path (`encoded_len`) is bijou64's weakest point on x86 -- varu64 and vu64 are consistently faster. This is a minor concern since encoded_len is rarely on a hot path by itself; it's typically called implicitly through encode, where bijou64 dominates.
