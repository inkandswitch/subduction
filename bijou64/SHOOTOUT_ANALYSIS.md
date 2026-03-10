# bijou64 Benchmark Shootout

> Criterion benchmarks comparing bijou64 against varu64, vu64, vu128, and leb128. All times are median µs over 4096 values. 🏆 marks the best in each row. The final two columns show bijou64's position: when leading, the margin over the runner-up; when trailing, the gap behind the leader.
>
> Run: `cargo bench -p bijou64 --bench shootout`

## Machine

|         |                         |
|---------|-------------------------|
| CPU     | Apple M2 Pro            |
| Memory  | 32 GB                   |
| OS      | macOS 26.3.1            |
| Rust    | 1.90.0                  |
| Profile | `bench` (opt-level = 3) |

## Encode

Encode to a `Vec<u8>`.

| Distribution    | bijou64      | varu64 | vu64         | vu128 | leb128       | bijou64 rank | bijou64 Δ (µs) | bijou64 vs other best |
|-----------------|--------------|--------|--------------|-------|--------------|--------------|----------------|-----------------------|
| tiny (0-247)    | **2.26** 🏆  | 10.96  | 20.69        | 16.23 | 4.21         | #1           | −1.95          | 0.54x                 |
| small (248-64k) | 11.41        | 20.00  | 22.96        | 20.17 | **7.16** 🏆  | #2           | +4.25          | 1.59x                 |
| medium (64k-4B) | 19.29        | 26.89  | 22.76        | 21.95 | **13.55** 🏆 | #2           | +5.74          | 1.42x                 |
| large (>4B)     | 13.11        | 28.19  | **11.30** 🏆 | 11.70 | 33.25        | #3           | +1.81          | 1.16x                 |
| tier boundaries | 16.26        | 29.47  | 18.52        | 19.15 | **12.03** 🏆 | #2           | +4.23          | 1.35x                 |
| uniform random  | 13.15        | 28.10  | **11.21** 🏆 | 11.84 | 34.17        | #3           | +1.94          | 1.17x                 |

```mermaid
xychart-beta
    title "Encode -- tiny (0-247)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 21
    bar [2.26, 10.96, 20.69, 16.23, 4.21]
```

```mermaid
xychart-beta
    title "Encode -- small (248-64k)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 23
    bar [11.41, 20.00, 22.96, 20.17, 7.16]
```

```mermaid
xychart-beta
    title "Encode -- medium (64k-4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 27
    bar [19.29, 26.89, 22.76, 21.95, 13.55]
```

```mermaid
xychart-beta
    title "Encode -- large (>4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 34
    bar [13.11, 28.19, 11.30, 11.70, 33.25]
```

```mermaid
xychart-beta
    title "Encode -- tier boundaries"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 30
    bar [16.26, 29.47, 18.52, 19.15, 12.03]
```

```mermaid
xychart-beta
    title "Encode -- uniform random"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 35
    bar [13.15, 28.10, 11.21, 11.84, 34.17]
```

## Encode Array

Encode to a fixed `[u8; 9]` with no allocation. leb128 is excluded because its API requires a `Write` implementor.

| Distribution    | bijou64     | varu64 | vu64        | vu128 | bijou64 rank | bijou64 Δ (µs) | bijou64 vs other best |
|-----------------|-------------|--------|-------------|-------|--------------|----------------|-----------------------|
| tiny (0-247)    | **1.27** 🏆 | 4.87   | 1.62        | 2.87  | #1           | −0.35          | 0.78x                 |
| small (248-64k) | 2.41        | 8.67   | **1.63** 🏆 | 3.51  | #2           | +0.78          | 1.48x                 |
| medium (64k-4B) | 2.59        | 12.38  | **1.65** 🏆 | 3.52  | #2           | +0.94          | 1.57x                 |
| large (>4B)     | 2.75        | 19.88  | **1.64** 🏆 | 3.51  | #2           | +1.11          | 1.68x                 |
| tier boundaries | 2.58        | 16.48  | **1.65** 🏆 | 3.41  | #2           | +0.93          | 1.56x                 |
| uniform random  | 2.54        | 19.92  | **1.65** 🏆 | 3.53  | #2           | +0.89          | 1.54x                 |

```mermaid
xychart-beta
    title "Encode Array -- tiny (0-247)"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 5
    bar [1.27, 4.87, 1.62, 2.87]
```

```mermaid
xychart-beta
    title "Encode Array -- small (248-64k)"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 9
    bar [2.41, 8.67, 1.63, 3.51]
```

```mermaid
xychart-beta
    title "Encode Array -- medium (64k-4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 13
    bar [2.59, 12.38, 1.65, 3.52]
```

```mermaid
xychart-beta
    title "Encode Array -- large (>4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 20
    bar [2.75, 19.88, 1.64, 3.51]
```

```mermaid
xychart-beta
    title "Encode Array -- tier boundaries"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 17
    bar [2.58, 16.48, 1.65, 3.41]
```

```mermaid
xychart-beta
    title "Encode Array -- uniform random"
    x-axis ["bijou64", "varu64", "vu64", "vu128"]
    y-axis "µs" 0 --> 20
    bar [2.54, 19.92, 1.65, 3.53]
```

## Decode

Decode from a `&[u8]` buffer.

| Distribution    | bijou64     | varu64 | vu64  | vu128       | leb128 | bijou64 rank | bijou64 Δ (µs) | bijou64 vs other best |
|-----------------|-------------|--------|-------|-------------|--------|--------------|----------------|-----------------------|
| tiny (0-247)    | **3.93** 🏆 | 6.62   | 14.40 | 22.76       | 12.57  | #1           | −2.69          | 0.59x                 |
| small (248-64k) | **9.36** 🏆 | 10.99  | 15.43 | 15.18       | 15.24  | #1           | −1.63          | 0.85x                 |
| medium (64k-4B) | **8.77** 🏆 | 16.24  | 15.37 | 10.70       | 16.09  | #1           | −1.93          | 0.82x                 |
| large (>4B)     | 10.05       | 22.27  | 8.80  | **8.67** 🏆 | 35.86  | #3           | +1.38          | 1.16x                 |
| tier boundaries | 11.59       | 19.21  | 12.27 | **10.78** 🏆| 15.39  | #2           | +0.81          | 1.07x                 |
| uniform random  | 10.34       | 23.86  | 9.30  | **9.22** 🏆 | 35.52  | #3           | +1.12          | 1.12x                 |

```mermaid
xychart-beta
    title "Decode -- tiny (0-247)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 23
    bar [3.93, 6.62, 14.40, 22.76, 12.57]
```

```mermaid
xychart-beta
    title "Decode -- small (248-64k)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 16
    bar [9.36, 10.99, 15.43, 15.18, 15.24]
```

```mermaid
xychart-beta
    title "Decode -- medium (64k-4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 17
    bar [8.77, 16.24, 15.37, 10.70, 16.09]
```

```mermaid
xychart-beta
    title "Decode -- large (>4B)"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 36
    bar [10.05, 22.27, 8.80, 8.67, 35.86]
```

```mermaid
xychart-beta
    title "Decode -- tier boundaries"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 20
    bar [11.59, 19.21, 12.27, 10.78, 15.39]
```

```mermaid
xychart-beta
    title "Decode -- uniform random"
    x-axis ["bijou64", "varu64", "vu64", "vu128", "leb128"]
    y-axis "µs" 0 --> 36
    bar [10.34, 23.86, 9.30, 9.22, 35.52]
```

## Stream Decode

Decode a concatenated stream of encoded values. vu128 is excluded because its API requires a fixed `[u8; 9]` input.

| Distribution    | bijou64     | varu64 | vu64         | leb128 | bijou64 rank | bijou64 Δ (µs) | bijou64 vs other best |
|-----------------|-------------|--------|--------------|--------|--------------|----------------|-----------------------|
| tiny (0-247)    | **3.98** 🏆 | 9.77   | 17.51        | 7.20   | #1           | −3.22          | 0.55x                 |
| small (248-64k) | **9.34** 🏆 | 20.82  | 18.84        | 13.02  | #1           | −3.68          | 0.72x                 |
| medium (64k-4B) | **9.34** 🏆 | 18.12  | 16.03        | 17.02  | #1           | −6.69          | 0.58x                 |
| large (>4B)     | 10.31       | 23.29  | **8.68** 🏆  | 35.76  | #2           | +1.63          | 1.19x                 |
| tier boundaries | 19.76       | 23.28  | **14.14** 🏆 | 14.15  | #3           | +5.62          | 1.40x                 |
| uniform random  | 9.81        | 22.48  | **8.57** 🏆  | 34.89  | #2           | +1.24          | 1.15x                 |

Bars left to right: bijou64, varu64, vu64, leb128.

```mermaid
xychart-beta
    title "Stream Decode -- median µs / 4096 values"
    x-axis ["tiny", "small", "medium", "large", "tier", "uniform"]
    y-axis "µs" 0 --> 36
    bar [3.98, 9.34, 9.34, 10.31, 19.76, 9.81]
    bar [9.77, 20.82, 18.12, 23.29, 23.28, 22.48]
    bar [17.51, 18.84, 16.03, 8.68, 14.14, 8.57]
    bar [7.20, 13.02, 17.02, 35.76, 14.15, 34.89]
```

## Round Trip

Encode then immediately decode each value.

| Distribution    | bijou64 | varu64 | vu64  | vu128       | leb128 | bijou64 rank | bijou64 Δ (µs) | bijou64 vs other best |
|-----------------|---------|--------|-------|-------------|--------|--------------|----------------|-----------------------|
| tiny (0-247)    | 4.96    | 10.49  | 22.94 | **4.73** 🏆 | 13.22  | #2           | +0.23          | 1.05x                 |
| small (248-64k) | 20.54   | 17.76  | 26.32 | **7.06** 🏆 | 22.90  | #3           | +13.48         | 2.91x                 |
| medium (64k-4B) | 27.87   | 28.07  | 22.52 | **2.91** 🏆 | 26.93  | #4           | +24.96         | 9.58x                 |
| large (>4B)     | 22.92   | 41.98  | 11.47 | **2.61** 🏆 | 57.63  | #3           | +20.31         | 8.78x                 |
| tier boundaries | 27.50   | 34.70  | 17.43 | **4.23** 🏆 | 25.89  | #4           | +23.27         | 6.51x                 |
| uniform random  | 23.57   | 43.46  | 12.07 | **2.77** 🏆 | 59.84  | #3           | +20.81         | 8.51x                 |

Bars left to right: bijou64, varu64, vu64, vu128, leb128.

```mermaid
xychart-beta
    title "Round Trip -- median µs / 4096 values"
    x-axis ["tiny", "small", "medium", "large", "tier", "uniform"]
    y-axis "µs" 0 --> 60
    bar [4.96, 20.54, 27.87, 22.92, 27.50, 23.57]
    bar [10.49, 17.76, 28.07, 41.98, 34.70, 43.46]
    bar [22.94, 26.32, 22.52, 11.47, 17.43, 12.07]
    bar [4.73, 7.06, 2.91, 2.61, 4.23, 2.77]
    bar [13.22, 22.90, 26.93, 57.63, 25.89, 59.84]
```

## Encoded Size

Bytes per value compared to a raw 8-byte `u64`. All tag-byte formats (bijou64, varu64, vu64/vu128) add 1 byte of overhead for multi-byte values. leb128 uses 1 continuation bit per byte instead.

bijou64 and varu64 share the same tag threshold (248), so their 1-byte range is wider than vu64/vu128 (0-247 vs 0-127). bijou64's per-tier offsets shift the multi-byte boundaries slightly, but the encoded sizes end up identical to varu64 at every value.

| Value    | Raw `u64` | bijou64          | varu64           | vu64 / vu128     | leb128           |
|----------|-----------|------------------|------------------|------------------|------------------|
| 0        | 8         | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        |
| 127      | 8         | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        | 1 (12.5%)        |
| 128      | 8         | **1 (12.5%)** 🏆 | **1 (12.5%)** 🏆 | 2 (25%)          | 2 (25%)          |
| 247      | 8         | **1 (12.5%)** 🏆 | **1 (12.5%)** 🏆 | 2 (25%)          | 2 (25%)          |
| 248      | 8         | 2 (25%)          | 2 (25%)          | 2 (25%)          | 2 (25%)          |
| 255      | 8         | 2 (25%)          | 2 (25%)          | 2 (25%)          | 2 (25%)          |
| 256      | 8         | **2 (25%)** 🏆   | 3 (37.5%)        | **2 (25%)** 🏆   | **2 (25%)** 🏆   |
| 503      | 8         | **2 (25%)** 🏆   | 3 (37.5%)        | **2 (25%)** 🏆   | **2 (25%)** 🏆   |
| 504      | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)** 🏆   | **2 (25%)** 🏆   |
| 1,000    | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)** 🏆   | **2 (25%)** 🏆   |
| 16,383   | 8         | 3 (37.5%)        | 3 (37.5%)        | **2 (25%)** 🏆   | **2 (25%)** 🏆   |
| 16,384   | 8         | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        |
| 65,535   | 8         | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        | 3 (37.5%)        |
| 65,536   | 8         | **3 (37.5%)** 🏆 | 4 (50%)          | **3 (37.5%)** 🏆 | **3 (37.5%)** 🏆 |
| 66,039   | 8         | **3 (37.5%)** 🏆 | 4 (50%)          | **3 (37.5%)** 🏆 | **3 (37.5%)** 🏆 |
| 100,000  | 8         | 4 (50%)          | 4 (50%)          | **3 (37.5%)** 🏆 | **3 (37.5%)** 🏆 |
| 2^24 - 1 | 8         | 4 (50%)          | 4 (50%)          | 4 (50%)          | 4 (50%)          |
| 2^32 - 1 | 8         | 5 (62.5%)        | 5 (62.5%)        | 5 (62.5%)        | 5 (62.5%)        |
| 2^40 - 1 | 8         | 6 (75%)          | 6 (75%)          | 6 (75%)          | 6 (75%)          |
| 2^48 - 1 | 8         | 7 (87.5%)        | 7 (87.5%)        | 7 (87.5%)        | 7 (87.5%)        |
| 2^56 - 1 | 8         | 8 (100%)         | 8 (100%)         | 8 (100%)         | 8 (100%)         |
| 2^64 - 1 | 8         | 9 (112.5%)       | 9 (112.5%)       | 9 (112.5%)       | 10 (125%)        |

🏆 marks the smallest encoding in each row. Three patterns emerge:

- **128-247**: bijou64 and varu64 win vs vu64/leb128. Their wider 1-byte tier (threshold 248 vs 128) keeps these values in a single byte.
- **256-503, 65536-66039, etc.**: bijou64 wins vs varu64. The per-tier offsets extend each tier's range slightly past the power-of-256 boundary where varu64 steps up. This pattern repeats at every tier boundary.
- **504-16383, 66040-2097151, etc.**: vu64 and leb128 win vs bijou64/varu64. vu64 packs 7 value bits into the first byte, giving it wider multi-byte tiers. This pattern also repeats at every tier.

For values 0-127, every format agrees: 1 byte, an 8x reduction over raw `u64`. The trade-offs only appear in the 128-16,383 range, and which format "wins" depends on which part of that range your workload hits. Above 16,384 the formats converge again and stay within 1 byte of each other all the way to `u64::MAX`.

## Summary

On this particular machine and workload, bijou64 is the fastest _decoder_ for tiny, small, and medium values -- the distributions we _believe_ dominate the protocol's hot path (blob sizes, counts, offsets under 64k). It also wins stream decode for those same ranges. On the encode side, bijou64 leads only for tiny values; leb128 is faster for small-and-above Vec encoding, and vu64 dominates encode-to-array across all non-tiny distributions. Different hardware, compiler versions, or real-world access patterns could easily shift the picture.

For large values and uniform random distributions, vu128 tends to win decode and vu64 wins stream decode. This is probably inherent to the format: their power-of-2 tier boundaries are cheaper to work with than bijou64's offset-adjusted boundaries.

vu128 dominates round-trip benchmarks across all distributions. I think this is because its fixed-size `[u8; 9]` API avoids allocation overhead entirely, but I haven't confirmed that with profiling.

The Vec-based encode path is bijou64's weakest point outside the tiny range. The `extend_from_slice` with a variable-length tail appears to inhibit vectorisation that competitors achieve with simpler layouts. The allocation-free `encode_array` path narrows the gap considerably (1.48-1.68x behind vu64, vs 1.59x behind leb128 for Vec encode), suggesting the overhead is partly in the Vec interaction rather than the tier calculation itself.
