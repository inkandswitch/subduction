# bijou64 Benchmark Shootout

> Criterion benchmarks comparing bijou64 against varu64, vu64, vu128, and leb128 across six value distributions over batches of 4096 values.
>
> Run: `cargo bench -p bijou64 --bench shootout`

## Results by Architecture

| Architecture | CPU | Results |
|---|---|---|
| x86_64 | AMD Ryzen 5 5600X (Zen 3) | [SHOOTOUT_ANALYSIS_X86.md](SHOOTOUT_ANALYSIS_X86.md) |
| AArch64 | Apple M2 Pro | [SHOOTOUT_ANALYSIS_ARM.md](SHOOTOUT_ANALYSIS_ARM.md) |

## Quick Comparison

bijou64's relative strengths shift between architectures:

| Benchmark | x86 (Zen 3) | ARM (M2 Pro) |
|---|---|---|
| Encode (Vec) | Wins 5/6 distributions | Wins 1/6 (tiny only) |
| Encode Array | Wins 1/6 (tiny) | Wins 1/6 (tiny) |
| Decode | Wins 2/6 (large, uniform) | Wins 3/6 (tiny, small, medium) |
| Canonical Decode | Wins 5/6 | Wins 4/6 |
| Stream Decode | Wins 2/6 (large, uniform) | Wins 3/6 (tiny, small, medium) |
| Encoded Size | 2nd-3rd | 2nd-3rd |

The key takeaway: bijou64's canonical decode advantage holds across both architectures (and is even stronger on x86), and its encode path benefits substantially from x86's efficient `lzcnt`. The decode path varies -- ARM favours bijou64 for small values while x86 favours leb128 -- but the differences are small in absolute terms (~0.6 ns per value).

For Subduction's use case (canonical encoding required, hot path dominated by small blob sizes), bijou64 provides the best overall profile on both platforms.
