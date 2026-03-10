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
| medium (64k--4B) | 6.35              | 2.76                    | 2.3x   |
| large (>4B)      | 6.30              | 2.78                    | 2.3x   |
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

## `encode_array` and `encode`: The Same Trick, Applied to Encoding

### Background

The original `encode_array` had the same 8-arm if/else structure as `encoded_len` -- each arm tested against a tier boundary and then constructed a literal `[u8; 9]` with the tag byte and big-endian payload hardcoded at the right positions. The `encode` function was a thin wrapper: call `encode_array`, then `extend_from_slice` the relevant prefix into the Vec.

This was _fine_ for tiny values (same first-comparison fast path), but medium-to-large values walked the full branch chain. The encode path was where bijou64 looked worst in the shootout -- consistently 4th or 5th across distributions.

### The Approach

The same `leading_zeros` trick from `encoded_len` applies here. Once you know the tier, the rest is mechanical:

- Tag byte: `247 + tier`
- Offset: `OFFSETS[tier]`
- Payload: `(value - offset).to_be_bytes()` -- always 8 bytes, last `tier` of which are relevant
- Output length: `tier + 1`

The challenge I'd flagged as an open question in the `encoded_len` section turned out to be solvable with a `while` loop copying byte-by-byte. It's not pretty, but it's `const fn` compatible and the compiler handles it well:

```rust
pub const fn encode_array(value: u64) -> ([u8; MAX_BYTES], usize) {
    if value < BOUNDS[0] {
        return ([(value & 0xFF) as u8, 0, 0, 0, 0, 0, 0, 0, 0], 1);
    }

    let bw = 64 - value.leading_zeros();
    let mut tier = ((bw - 1) / 8 + 1) as usize;
    if value < BOUNDS[tier - 1] {
        tier -= 1;
    }

    let tag = (247 + tier) as u8;
    let payload = (value - OFFSETS[tier]).to_be_bytes();

    let mut buf = [0u8; MAX_BYTES];
    buf[0] = tag;
    let start = 8 - tier;
    let mut i = 0;
    while i < tier {
        buf[1 + i] = payload[start + i];
        i += 1;
    }

    (buf, tier + 1)
}
```

### A Subtlety with `encode` (the Vec path)

The first version of this change applied the `leading_zeros` trick to `encode_array` and left `encode` as a wrapper that called it. This _regressed_ the Vec-pushing path by 8--29% on multi-byte distributions -- while `encode_array` itself got dramatically faster.

The reason: the old code returned array _literals_ with constant lengths. The compiler could see that `([0xF8, be[7], 0, 0, 0, 0, 0, 0, 0], 2)` had exactly 2 live bytes and emit a fixed-size copy. The new code builds the array with a `while` loop and returns a runtime-variable `tier + 1` as the length. `extend_from_slice(&arr[..len])` with a non-constant `len` generates worse code for the Vec copy.

The fix was to give `encode` its own implementation that writes directly to the Vec -- push the tag byte, then `extend_from_slice` the relevant tail of the `to_be_bytes()` array. This avoids the intermediate `[u8; 9]` entirely:

```rust
pub fn encode(value: u64, buf: &mut Vec<u8>) {
    if value < BOUNDS[0] {
        buf.push((value & 0xFF) as u8);
        return;
    }

    let bw = 64 - value.leading_zeros();
    let mut tier = ((bw - 1) / 8 + 1) as usize;
    if value < BOUNDS[tier - 1] {
        tier -= 1;
    }

    buf.push((247 + tier) as u8);
    let be = (value - OFFSETS[tier]).to_be_bytes();
    buf.extend_from_slice(&be[8 - tier..]);
}
```

### What We Measured

#### `encode_array` (no-alloc path)

| Distribution     | Before (if-chain) | After (`leading_zeros`) | Change |
|------------------|--------------------|-------------------------|--------|
| tiny (0--247)    | 3.79               | 1.30                    | 2.9x   |
| small (248--64k) | 7.68               | 2.53                    | 3.0x   |
| medium (64k--4B) | 10.12              | 2.50                    | 4.0x   |
| large (>4B)      | 15.14              | 2.71                    | 5.6x   |
| tier boundaries  | 12.08              | 2.50                    | 4.8x   |
| uniform random   | 15.12              | 2.49                    | 6.1x   |

bijou64 now _beats_ vu64 on `encode_array` for tiny values (1.30 vs 1.66 µs) and is within 1.5x for all other distributions. Previously it was 2--9x slower.

#### `encode` (Vec path)

| Distribution     | Before (if-chain) | After (direct push) | Change |
|------------------|--------------------|---------------------|--------|
| tiny (0--247)    | 10.27              | 2.31                | 4.4x   |
| small (248--64k) | 21.87              | 11.46               | 1.9x   |
| medium (64k--4B) | 26.66              | 19.13               | 1.4x   |
| large (>4B)      | 22.92              | 12.83               | 1.8x   |
| tier boundaries  | 25.65              | 15.88               | 1.6x   |
| uniform random   | 22.66              | 12.68               | 1.8x   |

The tiny encode improvement (4.4x) is particularly nice -- bijou64 is now the fastest encoder in the shootout for small values, ahead of leb128 (4.38 µs).

### Properties Preserved

Same as `encoded_len`: `const fn` (for `encode_array`), `no_std`, `forbid(unsafe_code)`, identical output for all `u64` values.
