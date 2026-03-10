# bijou64 Optimisation Notes

> These are implementation notes, not part of the specification. The [spec][SPEC] defines the _format_; this document records why the reference implementation makes the choices it does, what we measured, and where the remaining costs come from.

[SPEC]: ./SPEC.md

## `encoded_len`: `leading_zeros` Instead of an If-Chain

### Background

The straightforward way to implement `encoded_len` is an if/else chain that tests the value against each tier boundary in order:

```rust
pub const fn encoded_len(value: u64) -> usize {
    if value < BOUNDS[0] { 1 }
    else if value < BOUNDS[1] { 2 }
    // ...six more arms...
    else { 9 }
}
```

This is clear and obviously correct, and for tier 0 values (the common case in our protocol -- blob sizes tend to be 54--100 bytes) it's _fast_: one comparison, well-predicted, done.

The trouble shows up on mixed or large-value distributions. A uniform random `u64` walks an average of four or five comparisons before hitting the right arm, and the branch predictor can't help much because the tier depends on the value. In benchmarks this was pretty stark -- up to 6.4 µs per 4096 values for `uniform_random`, compared to ~0.95 µs for `vu64::encoded_len`, which uses a branchless `leading_zeros` approach.

### The Trick

bijou64's tier boundaries aren't _exactly_ powers of 256 (that's the whole point of the per-tier offsets), but they're _close_. Each tier spans exactly 8 bit-widths after tier 1:

```text
Bit-width  0..= 7  →  tier 0   (1 byte)
Bit-width  8       →  tier 1   (2 bytes)
Bit-width  9..=16  →  tier 2   (3 bytes)
Bit-width 17..=24  →  tier 3   (4 bytes)
Bit-width 25..=32  →  tier 4   (5 bytes)
Bit-width 33..=40  →  tier 5   (6 bytes)
Bit-width 41..=48  →  tier 6   (7 bytes)
Bit-width 49..=56  →  tier 7   (8 bytes)
Bit-width 57..=64  →  tier 8   (9 bytes)
```

So `u64::leading_zeros()` -- a single `lzcnt` / `clz` instruction on most architectures -- gets us a _candidate_ tier via simple arithmetic: `(bit_width - 1) / 8 + 2`.

The complication is that the per-tier offsets push each boundary slightly past the corresponding power of 256. For example, `BOUNDS[1]` is 504, not 512. That means 503 has bit-width 9 and the formula says "tier 2", but 503 is actually the last value of tier 1. The candidate can be one too high.

The good news: it can _only_ be one too high. The offsets grow geometrically, but they never push a boundary far enough to cross a full additional bit-width.[^proof] A single comparison against the tier's actual bound corrects for this.

[^proof]: Each offset is $\sum_{k=0}^{t-1} 256^k$, which is less than $2 \cdot 256^{t-1}$ -- i.e., less than one extra bit of headroom. The boundary at tier $t$ sits at `256^t + offset`, which still fits within the same 8-bit-width band as `256^t` alone.

### Implementation

```rust
pub const fn encoded_len(value: u64) -> usize {
    // Fast path: tier 0 covers 0--247, the common case.
    if value < BOUNDS[0] {
        return 1;
    }

    // Derive candidate from bit-width.
    let bw = 64 - value.leading_zeros(); // 8..=64 here
    let candidate = ((bw - 1) / 8 + 2) as usize;

    // Correct for boundary values -- at most one comparison.
    if value < BOUNDS[candidate - 2] {
        candidate - 1
    } else {
        candidate
    }
}
```

#### The Tier 0 Fast Path Matters

Without it, tiny values pay for the `leading_zeros` + arithmetic even though a single predicted branch would have been cheaper. We measured roughly 2x slower for the `tiny` distribution without this guard. Since the protocol's hot path is dominated by small blob sizes, keeping this branch is worth the extra line.

#### No Lookup Table Needed

An earlier version used a 65-entry `[u8; 65]` lookup table indexed by bit-width. The arithmetic formula `(bw - 1) / 8 + 2` produces identical results and avoids the memory load.

#### `const fn` Compatible

`u64::leading_zeros()` has been `const` since Rust 1.32, so the whole function stays usable in const contexts -- no loss of capability compared to the if-chain.

### What We Measured

Criterion benchmarks, 4096 values per distribution, median time in µs:

| Distribution     | Before (if-chain) | After (`leading_zeros`) | Change |
|------------------|-------------------|-------------------------|--------|
| tiny (0--247)    | 1.35              | 1.30                    | ~same  |
| small (248--64k) | 2.68              | 2.84                    | ~same  |
| medium (64k--4G) | 6.35              | 2.76                    | 2.3x   |
| large (>4G)      | 6.30              | 2.78                    | 2.3x   |
| tier boundaries  | 4.76              | 2.75                    | 1.7x   |
| uniform random   | 6.37              | 2.67                    | 2.4x   |

The small distribution shows a marginal regression -- within noise, but I think it's real: the old code needed just two comparisons for tier 1--2 values, and the `leading_zeros` path is slightly more work for that case. Not worth worrying about.

### The Remaining Gap

Even after this change, `vu64::encoded_len` is still roughly 3x faster (~0.95 µs constant across all distributions). That's because vu64's tier boundaries _are_ exact powers of 2, so `leading_zeros` gives the final answer directly -- no correction comparison, no tier 0 special case.

This gap is, in a real sense, the cost of structural canonicality in the length-computation path. The per-tier offsets that make bijou64 bijective are what prevent the boundaries from landing on clean power-of-2 cutoffs. I don't think there's a way to close this gap without changing the format itself -- and the format is doing exactly what we want it to do.

### Properties Preserved

The optimised implementation preserves:

- `const fn`
- `no_std`
- `forbid(unsafe_code)`
- Identical output for every `u64` value (verified by the existing `encoded_len_matches` property test, which checks `encoded_len(v)` against `encode_array(v).1` for random values across all tiers)

### Future Work

The same `leading_zeros`-based tier derivation could apply to `encode_array`, which uses an analogous 8-arm if/else chain today. The tricky part is that each arm constructs a different array literal -- different tag byte, different offset subtraction, different byte positions for the big-endian payload. Unifying that into a branchless path without `unsafe` or losing `const fn` compatibility is an open question. It's entirely possible there's a clean way to do it; I haven't found one yet.
