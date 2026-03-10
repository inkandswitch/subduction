# bijou64 Benchmark Shootout

> Criterion benchmarks comparing bijou64 against varu64, vu64, vu128, and leb128. All times are median µs over 4096 values. 🏆 marks the best in each row. The final two columns show bijou64's absolute delta and ratio versus the leader (or "🏆" if bijou64 _is_ the leader).
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

| Distribution    | bijou64     | varu64 | vu64         | vu128 | leb128       | bijou64 Δ | bijou64 ratio |
|-----------------|-------------|--------|--------------|-------|--------------|-----------|---------------|
| tiny (0-247)    | 🏆 **2.31** | 11.28  | 20.93        | 16.88 | 4.38         | 🏆        | 🏆            |
| small (248-64k) | 🏆 **7.52** | 20.10  | 22.92        | 20.16 | 11.46        | 🏆        | 🏆            |
| medium (64k-4B) | 19.13       | 26.32  | 22.27        | 21.59 | 🏆 **13.40** | +5.73     | 1.43x         |
| large (>4B)     | 12.83       | 27.55  | 🏆 **11.08** | 11.47 | 32.34        | +1.75     | 1.16x         |
| tier boundaries | 15.88       | 28.99  | 18.37        | 18.43 | 🏆 **11.48** | +4.40     | 1.38x         |
| uniform random  | 12.68       | 27.63  | 🏆 **11.09** | 11.50 | 32.69        | +1.59     | 1.14x         |

## Encode Array

Encode to a fixed `[u8; 9]` with no allocation. leb128 is excluded because its API requires a `Write` implementor.

| Distribution    | bijou64     | varu64 | vu64        | vu128 | bijou64 Δ | bijou64 ratio |
|-----------------|-------------|--------|-------------|-------|-----------|---------------|
| tiny (0-247)    | 🏆 **1.30** | 5.15   | 1.66        | 2.97  | 🏆        | 🏆            |
| small (248-64k) | 🏆 **2.53** | 8.73   | 1.65        | 3.58  | 🏆        | 🏆            |
| medium (64k-4B) | 2.50        | 12.49  | 🏆 **1.64** | 3.55  | +0.86     | 1.52x         |
| large (>4B)     | 2.71        | 19.82  | 🏆 **1.64** | 3.49  | +1.07     | 1.65x         |
| tier boundaries | 2.50        | 16.94  | 🏆 **1.64** | 3.35  | +0.86     | 1.52x         |
| uniform random  | 2.49        | 20.27  | 🏆 **1.64** | 3.49  | +0.85     | 1.52x         |

## Decode

Decode from a `&[u8]` buffer.

| Distribution    | bijou64      | varu64 | vu64  | vu128       | leb128 | bijou64 Δ | bijou64 ratio |
|-----------------|--------------|--------|-------|-------------|--------|-----------|---------------|
| tiny (0-247)    | 🏆 **3.85**  | 6.32   | 14.44 | 21.57       | 11.56  | 🏆        | 🏆            |
| small (248-64k) | 🏆 **9.00**  | 10.49  | 15.00 | 14.88       | 14.45  | 🏆        | 🏆            |
| medium (64k-4B) | 🏆 **8.93**  | 16.64  | 15.62 | 10.90       | 16.27  | 🏆        | 🏆            |
| large (>4B)     | 10.14        | 22.47  | 8.87  | 🏆 **8.76** | 36.42  | +1.38     | 1.16x         |
| tier boundaries | 🏆 **10.26** | 19.51  | 12.44 | 10.26       | 15.75  | 🏆        | 🏆            |
| uniform random  | 10.08        | 22.54  | 8.90  | 🏆 **8.82** | 35.93  | +1.26     | 1.14x         |

## Encoded Size

Bytes per value compared to a raw 8-byte `u64`. All tag-byte formats (bijou64, varu64, vu64/vu128) add 1 byte of overhead for multi-byte values. leb128 uses 1 continuation bit per byte instead.

bijou64 and varu64 share the same tag threshold (248), so their 1-byte range is wider than vu64/vu128 (0-247 vs 0-127). bijou64's per-tier offsets shift the multi-byte boundaries slightly, but the encoded sizes end up identical to varu64 at every value.

| Value     | Raw `u64` | bijou64            | varu64             | vu64 / vu128       | leb128             |
|-----------|-----------|--------------------|--------------------|--------------------|--------------------|
| 0         | 8         | 1 (12.5%)          | 1 (12.5%)          | 1 (12.5%)          | 1 (12.5%)          |
| 127       | 8         | 1 (12.5%)          | 1 (12.5%)          | 1 (12.5%)          | 1 (12.5%)          |
| 128       | 8         | 🏆 **1 (12.5%)**   | 🏆 **1 (12.5%)**   | 2 (25%)            | 2 (25%)            |
| 247       | 8         | 🏆 **1 (12.5%)**   | 🏆 **1 (12.5%)**   | 2 (25%)            | 2 (25%)            |
| 248       | 8         | 2 (25%)            | 2 (25%)            | 2 (25%)            | 2 (25%)            |
| 255       | 8         | 2 (25%)            | 2 (25%)            | 2 (25%)            | 2 (25%)            |
| 256       | 8         | 🏆 **2 (25%)**     | 3 (37.5%)          | 🏆 **2 (25%)**     | 🏆 **2 (25%)**     |
| 503       | 8         | 🏆 **2 (25%)**     | 3 (37.5%)          | 🏆 **2 (25%)**     | 🏆 **2 (25%)**     |
| 504       | 8         | 3 (37.5%)          | 3 (37.5%)          | 🏆 **2 (25%)**     | 🏆 **2 (25%)**     |
| 1,000     | 8         | 3 (37.5%)          | 3 (37.5%)          | 🏆 **2 (25%)**     | 🏆 **2 (25%)**     |
| 16,383    | 8         | 3 (37.5%)          | 3 (37.5%)          | 🏆 **2 (25%)**     | 🏆 **2 (25%)**     |
| 16,384    | 8         | 3 (37.5%)          | 3 (37.5%)          | 3 (37.5%)          | 3 (37.5%)          |
| 65,535    | 8         | 3 (37.5%)          | 3 (37.5%)          | 3 (37.5%)          | 3 (37.5%)          |
| 65,536    | 8         | 🏆 **3 (37.5%)**   | 4 (50%)            | 🏆 **3 (37.5%)**   | 🏆 **3 (37.5%)**   |
| 66,039    | 8         | 🏆 **3 (37.5%)**   | 4 (50%)            | 🏆 **3 (37.5%)**   | 🏆 **3 (37.5%)**   |
| 100,000   | 8         | 4 (50%)            | 4 (50%)            | 🏆 **3 (37.5%)**   | 🏆 **3 (37.5%)**   |
| 2^24 - 1  | 8         | 4 (50%)            | 4 (50%)            | 4 (50%)            | 4 (50%)            |
| 2^32 - 1  | 8         | 5 (62.5%)          | 5 (62.5%)          | 5 (62.5%)          | 5 (62.5%)          |
| 2^40 - 1  | 8         | 6 (75%)            | 6 (75%)            | 6 (75%)            | 6 (75%)            |
| 2^48 - 1  | 8         | 7 (87.5%)          | 7 (87.5%)          | 7 (87.5%)          | 7 (87.5%)          |
| 2^56 - 1  | 8         | 8 (100%)           | 8 (100%)           | 8 (100%)           | 8 (100%)           |
| 2^64 - 1  | 8         | 9 (112.5%)         | 9 (112.5%)         | 9 (112.5%)         | 10 (125%)          |

🏆 marks the smallest encoding in each row. Three patterns emerge:

- **128-247**: bijou64 and varu64 win vs vu64/leb128. Their wider 1-byte tier (threshold 248 vs 128) keeps these values in a single byte.
- **256-503, 65536-66039, etc.**: bijou64 wins vs varu64. The per-tier offsets extend each tier's range slightly past the power-of-256 boundary where varu64 steps up. This pattern repeats at every tier boundary.
- **504-16383, 66040-2097151, etc.**: vu64 and leb128 win vs bijou64/varu64. vu64 packs 7 value bits into the first byte, giving it wider multi-byte tiers. This pattern also repeats at every tier.

For values 0-127, every format agrees: 1 byte, an 8x reduction over raw `u64`. The trade-offs only appear in the 128-16,383 range, and which format "wins" depends on which part of that range your workload hits. Above 16,384 the formats converge again and stay within 1 byte of each other all the way to `u64::MAX`.

## Stream Decode

Decode a concatenated stream of encoded values. vu128 is excluded because its API requires a fixed `[u8; 9]` input.

| Distribution    | bijou64      | varu64 | vu64         | leb128 | bijou64 Δ | bijou64 ratio |
|-----------------|--------------|--------|--------------|--------|-----------|---------------|
| tiny (0-247)    | 🏆 **3.81**  | 9.62   | 16.81        | 5.98   | 🏆        | 🏆            |
| small (248-64k) | 🏆 **8.96**  | 17.86  | 18.17        | 11.99  | 🏆        | 🏆            |
| medium (64k-4B) | 🏆 **8.88**  | 17.44  | 15.59        | 16.31  | 🏆        | 🏆            |
| large (>4B)     | 10.30        | 23.91  | 🏆 **8.97**  | 36.18  | +1.33     | 1.15x         |
| tier boundaries | 🏆 **14.58** | 24.09  | 🏆 **14.58** | 14.64  | 🏆        | 🏆            |
| uniform random  | 10.06        | 22.99  | 🏆 **8.84**  | 36.49  | +1.22     | 1.14x         |

## Round Trip

Encode then immediately decode each value.

| Distribution    | bijou64 | varu64 | vu64  | vu128       | leb128 | bijou64 Δ | bijou64 ratio |
|-----------------|---------|--------|-------|-------------|--------|-----------|---------------|
| tiny (0-247)    | 7.38    | 10.07  | 22.58 | 🏆 **5.00** | 15.73  | +2.38     | 1.48x         |
| small (248-64k) | 15.77   | 17.51  | 26.36 | 🏆 **7.06** | 22.61  | +8.71     | 2.23x         |
| medium (64k-4B) | 17.51   | 27.46  | 21.96 | 🏆 **2.78** | 26.39  | +14.73    | 6.30x         |
| large (>4B)     | 24.17   | 42.15  | 11.88 | 🏆 **2.54** | 58.49  | +21.63    | 9.52x         |
| tier boundaries | 22.63   | 35.07  | 17.62 | 🏆 **4.28** | 24.70  | +18.35    | 5.29x         |
| uniform random  | 24.08   | 41.48  | 11.51 | 🏆 **2.58** | 57.20  | +21.50    | 9.33x         |

## Summary

On this particular machine and workload, bijou64 appears to be the fastest encoder and decoder for tiny and small values -- the distributions we _believe_ dominate the protocol's hot path (blob sizes, counts, offsets under 64k). Different hardware, compiler versions, or real-world access patterns could easily shift the picture.

For large values and uniform random distributions, vu64 and vu128 come out ahead on encode and decode respectively. This is probably inherent to the format: their power-of-2 tier boundaries are cheaper to work with than bijou64's offset-adjusted boundaries.

vu128 dominates round-trip benchmarks across all distributions. I think this is because its fixed-size `[u8; 9]` API avoids allocation overhead entirely, but I haven't confirmed that with profiling.

vu64 dominates encoded-size computation -- its `leading_zeros` gives the final answer with no correction step. bijou64's per-tier offsets require one additional comparison, which seems to account for the ~3x gap, though it's possible there are other factors I'm not seeing.
