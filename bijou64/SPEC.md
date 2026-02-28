# bijou64

> "Plurality must never be posited without necessity."
> — William of Ockham

## Authors

- [Brooklyn Zelenka]

## Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in [BCP 14] when, and only when, they appear in all capitals, as shown here.

## Name

bijou64 (**bij**ective **o**ffset **u64**) is pronounced /biːʒuː sɪksti fɔːr/ ("bee-zhoo-sixty-four"). The name encodes the format's three defining properties: bijectivity (canonical by construction), per-tier offset addition (the mechanism that achieves it), and the `u64` value type. That "bijou" is also French for "small jewel" is a happy coincidence for a compact encoding.

# Abstract

bijou64 is a [bijective][bijective numeration] variable-length encoding for unsigned 64-bit integers. It encodes values into 1–9 bytes using tag-byte framing inherited from [VARU64], modified with per-tier offsets so that canonicality is structural rather than checked at runtime.

# Introduction

Many binary protocols need a compact way to encode integers that are usually small but occasionally large. Variable-length integer encodings (varints) solve this, but most designs treat canonicality as an afterthought — something enforced by a runtime check in the decoder rather than by the structure of the encoding itself.

[VARU64] is a big-endian, tag-byte-framed varint. It admits a unique shortest encoding for every value, but the decoder must _actively reject_ overlong encodings. This rejection is a single `if` statement that, if omitted, does not break round-trip tests — only adversarial inputs expose the bug. In a canonical binary codec where encoders and decoders must agree on a single byte-level representation, a silently deletable canonicality check is a liability.

bijou64 eliminates this class of error by making the offset subtraction load-bearing. There is exactly one way to represent each number. Each tier subtracts a different cumulative offset from the value before encoding the payload. If you attempt to encode a value in the wrong tier, the offset arithmetic produces a _different value_, which fails any round-trip or hash comparison immediately. There is no overlong encoding to reject because the tier ranges are disjoint by construction.

## Design Goals

bijou64 was designed to satisfy the following properties:

| Property                  | Description                                                                                                                                                                                                                           |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Canonical by construction | Every value has exactly one encoding, enforced structurally by the format itself — not by a runtime check that can be omitted. This is the defining requirement; it motivates much of the design.                                     |
| Big-endian byte order     | Payload bytes are big-endian so that lexicographic byte comparison equals numeric comparison. This enables sorted storage and binary search over encoded values without decoding, and is easier to calculate by hand while debugging. |
| Length from first byte    | The total encoding length is determined by inspecting only the first byte. This enables $\mathcal{O}(1)$ skipping, streaming parsers, and other buffer management.                                                                    |
| Compact for small values  | Values that fit in one byte (0–247) encode as that single byte with no overhead. The common case in the target protocol is blob sizes of 54–100 bytes, which fall in this range.                                                      |
| Full `u64` range          | The encoding covers all values from 0 to $2^{64} − 1$, matching the protocol's `u64` size fields.                                                                                                                                     |
| Simple to implement       | The encoding and decoding algorithms are implementable in under 50 lines, in any language, with no dependencies or clever bit-shifting tricks. The format is easy to port (e.g. to TypeScript for a browser client).                  |
| Debuggable in a hexdump   | For single-byte values (the common case), the encoded byte is the value itself. For multibyte values, the payload is contiguous big-endian bytes readable with minimal mental arithmetic.                                             |

# Format

bijou64 encodes unsigned 64-bit integers into 1–9 bytes. The encoding is a bijection: every `u64` value maps to exactly one byte sequence, and every valid byte sequence maps to exactly one `u64` value.

## Tag Byte

The first byte of an encoding is the _tag byte_. Its value determines how many additional bytes follow:

| First byte      | Total length | Offset (decimal)       | Offset (hex)        |
|-----------------|--------------|------------------------|---------------------|
| `0x00` – `0xF7` | 1            | 0                      | `0x00`              |
| `0xF8`          | 2            | 248                    | `0xF8`              |
| `0xF9`          | 3            | 504                    | `0x1F8`             |
| `0xFA`          | 4            | 66,040                 | `0x101F8`           |
| `0xFB`          | 5            | 16,843,256             | `0x10101F8`         |
| `0xFC`          | 6            | 4,311,810,552          | `0x1010101F8`       |
| `0xFD`          | 7            | 1,103,823,438,328      | `0x101010101F8`     |
| `0xFE`          | 8            | 282,578,800,148,984    | `0x10101010101F8`   |
| `0xFF`          | 9            | 72,340,172,838,076,920 | `0x1010101010101F8` |

If the tag byte is below 248 (`0xF8`), the byte _is_ the encoded value and there are no additional bytes.

If the tag byte is 248 or above, let `tier = tag - 247` (giving tiers 1–8). The following `tier` bytes are the big-endian representation of a _payload_. The decoded value is:

```
value = OFFSET[tier] + payload
```

where the per-tier offsets are defined below.

## Offset Table

Each tier's offset is the first value not representable by any previous tier. The recurrence is:

```
OFFSET[0] = 0
OFFSET[1] = 248
OFFSET[n] = OFFSET[n-1] + 256^(n-1)    for n >= 2
```

Giving the concrete values:

| Tier | Tag    | Offset              | Start               | End (inclusive)      |
|------|--------|---------------------|---------------------|----------------------|
| 0    | —      | `0x00`              | `0x00`              | `0xF7`               |
| 1    | `0xF8` | `0xF8`              | `0xF8`              | `0x1F7`              |
| 2    | `0xF9` | `0x1F8`             | `0x1F8`             | `0x101F7`            |
| 3    | `0xFA` | `0x101F8`           | `0x101F8`           | `0x10101F7`          |
| 4    | `0xFB` | `0x10101F8`         | `0x10101F8`         | `0x1010101F7`        |
| 5    | `0xFC` | `0x1010101F8`       | `0x1010101F8`       | `0x101010101F7`      |
| 6    | `0xFD` | `0x101010101F8`     | `0x101010101F8`     | `0x10101010101F7`    |
| 7    | `0xFE` | `0x10101010101F8`   | `0x10101010101F8`   | `0x1010101010101F7`  |
| 8    | `0xFF` | `0x1010101010101F8` | `0x1010101010101F8` | `0xFFFFFFFFFFFFFFFF` |

## Encoding

To encode a value `v`:

1. If `v < 248`, emit a single byte with value `v`.
2. Otherwise, find the tier `t` (1–8) such that `OFFSET[t] <= v < OFFSET[t+1]` (with `OFFSET[9]` treated as $2^{64}$).
3. Emit tag byte `247 + t`.
4. Emit `v - OFFSET[t]` as a `t`-byte big-endian integer.

### Worked Example

There are two separate subtractions in the encoder, and it is important not to confuse them:

- **Tag byte**: always `247 + tier`. The constant 247 maps between the tier number (1–8) and the tag byte (`0xF8`–`0xFF`). This is the same for every multi-byte tier.

- **Payload**: always `value - OFFSET[tier]`. The offset is _different_ for each tier — it is the cumulative count of values representable by all previous tiers. This subtraction is what makes the encoding bijective.

Encoding the value **67,000**:

1. 67,000 ≥ 248, so it is not a single-byte value.
2. Find the tier: `OFFSET[3] = 66,040 ≤ 67,000 < 16,843,256 = OFFSET[4]`, so tier = 3.
3. Tag byte: `247 + 3 = 250` → emit `0xFA`.
4. Payload: `67,000 − 66,040 = 960` → emit as 3-byte big-endian `0x00 0x03 0xC0`.
5. Result: `FA 00 03 C0` (4 bytes).

Note that we subtracted **66,040** (the tier 3 offset), _not_ 247. If we had 300 instead, it would land in tier 1 and we would subtract **248** (the tier 1 offset). Each tier has its own offset:

| Tier | Offset (decimal)       | Offset (hex)         |
|------|------------------------|----------------------|
| 1    | 248                    | `0xF8`               |
| 2    | 504                    | `0x1F8`              |
| 3    | 66,040                 | `0x101F8`            |
| 4    | 16,843,256             | `0x10101F8`          |
| 5    | 4,311,810,552          | `0x1010101F8`        |
| 6    | 1,103,823,438,328      | `0x101010101F8`      |
| 7    | 282,578,800,148,984    | `0x10101010101F8`    |
| 8    | 72,340,172,838,076,920 | `0x01010101010101F8` |

The hex column shows a staircase pattern: each offset ends with `0xF8` (248, the tier 0 capacity) and prepends one `01` byte per tier. This is a consequence of the geometric recurrence — each $256^{n}$ term contributes a `0x01` in its corresponding byte position.

## Decoding

To decode from a byte buffer:

1. Read the tag byte. If the buffer is empty, the decoder MUST signal an error.
2. If `tag < 248`, the decoded value is `tag`. Consume 1 byte.
3. Otherwise, let `tier = tag - 247`. Read `tier` additional bytes. If fewer than `tier` bytes remain, the decoder MUST signal a buffer-too-short error.
4. Interpret the additional bytes as a big-endian unsigned integer (the _payload_).
5. Compute `value = OFFSET[tier] + payload`. If this addition overflows `u64` (possible only at tier 8), the decoder MUST signal an overflow error.
6. The decoded value is `value`. Consume `1 + tier` bytes total.

## Canonicality

> "The best error message is the one that never shows up."
> — Thomas Fuchs

bijou64 achieves canonicality _structurally_ (by construction) rather than by runtime rejection of overlong encodings.

### Disjoint Tier Ranges

Each tier's value range is disjoint by construction. The offset subtraction during encoding and offset addition during decoding guarantee that a value encoded at tier `t` cannot be decoded from a tier `t' ≠ t` encoding.

### No Overlong Encodings

There is no valid "overlong" encoding to reject. A byte sequence `[tag, payload...]` always decodes to `OFFSET[tier] + payload`, which always falls within the tier's range (or overflows on tier 8).

### Minimal Decoder Obligations

A conforming decoder MUST check for exactly two error conditions:

1. Buffer too short (not enough bytes for the tier).
2. Arithmetic overflow on tier 8 (`OFFSET[8] + payload > u64::MAX`).

No other validation is required. In particular, there is no "non-canonical encoding" error because non-canonical encodings are structurally impossible.

This is in contrast to [VARU64], where the decoder MUST explicitly reject non-minimal encodings — a check that, if omitted, silently produces incorrect but plausible values.

## Error Conditions

A conforming decoder MUST signal an error for:

1. **Buffer too short**: the input buffer contains fewer bytes than the tag byte requires.
2. **Overflow**: at tier 8, `OFFSET[8] + payload` exceeds $2^{64} - 1$.

No other error conditions exist. In particular, there is no "non-canonical encoding" error because non-canonical encodings are structurally impossible.

# Properties

- The encoding length is determined entirely by the first byte.
- Encodings sort in the same order as the values they represent (lexicographic byte order equals numeric order).
- Values 0–247 are encoded as a single byte equal to the value.
- Maximum encoding length is 9 bytes (for values near `u64::MAX`).
- The tag-byte framing is identical to [VARU64]; only the payload interpretation differs (offset addition vs. raw value).

# Test Vectors

Implementations SHOULD use these vectors to verify encoding compatibility.

| Value                      | Encoded bytes (hex)          |
|----------------------------|------------------------------|
| 0                          | `00`                         |
| 1                          | `01`                         |
| 42                         | `2A`                         |
| 247                        | `F7`                         |
| 248                        | `F8 00`                      |
| 300                        | `F8 34`                      |
| 503                        | `F8 FF`                      |
| 504                        | `F9 00 00`                   |
| 1,000                      | `F9 01 F0`                   |
| 65,535                     | `F9 FE 07`                   |
| 66,039                     | `F9 FF FF`                   |
| 66,040                     | `FA 00 00 00`                |
| 67,000                     | `FA 00 03 C0`                |
| 16,843,255                 | `FA FF FF FF`                |
| 16,843,256                 | `FB 00 00 00 00`             |
| 4,311,810,551              | `FB FF FF FF FF`             |
| 72,340,172,838,076,920     | `FF 00 00 00 00 00 00 00 00` |
| 18,446,744,073,709,551,615 | `FF FE FE FE FE FE FE FE 07` |

## Error Test Vectors

| Input bytes (hex)            | Expected error   | Rationale                                          |
|------------------------------|------------------|----------------------------------------------------|
| _(empty)_                    | Buffer too short | No tag byte present                                |
| `F9 00`                      | Buffer too short | Tag `F9` requires 2 payload bytes, only 1 provided |
| `FF FF FF FF FF FF FF FF FF` | Overflow         | `OFFSET[8]` + `0xFF..FF` exceeds `u64::MAX`        |

# Prior Art

bijou64 combines tag-byte framing from [VARU64] by [Aljoscha Meyer] with per-tier offsets — an instance of [bijective numeration], the same principle used by [Git's pack offset encoding]. [SQLite4's varint] uses a partial version of this offset idea (tiers 1–2 only).

bijou64 would not exist without these prior designs. Each of the formats below is well-engineered and well-suited to its original use case. The reasons bijou64 diverges from them are specific to the requirements of a content-addressed, canonical-by-construction protocol — not general criticisms of the formats themselves. LEB128 has been a reliable workhorse in DWARF, protobuf, and Wasm; VARU64 is an elegant design that bijou64 directly inherits most of its structure from.

## LEB128

[LEB128] is the most widely deployed varint (Wasm, protobuf, DWARF). It uses per-byte continuation bits and little-endian byte order. It is _not_ canonical: the same value can be encoded in multiple ways (e.g., `0x00` and `0x80 0x00` both decode to 0). Canonicality must be enforced by a runtime check at every decode site.

bijou64 was not built on LEB128 because:

- **Little-endian byte order.** The protocol using bijou64 is big-endian throughout. Mixing byte orders is an invitation to bugs.
- **No structural canonicality.** Overlong encodings are valid LEB128. In a content-addressed protocol, accepting a non-canonical encoding silently produces a different hash — a security issue.
- **Continuation-bit framing.** The encoding length cannot be determined from the first byte alone; the decoder must scan for the terminating byte. This also means a missing termination byte can cause a decoder to read past the end of the buffer or loop indefinitely.
- **Difficult to debug by hand.** Each byte interleaves one continuation bit with seven value bits. Reconstructing the original value from a hexdump requires masking and shifting every byte, then reassembling in little-endian order. Tag-byte framing (as in bijou64) keeps the payload bytes contiguous and big-endian, making hexdump inspection straightforward.

## vu128 / vu64

The [vu128] and [vu64] crates use UTF-8-style prefix bits in the first byte: leading `1` bits encode the length, remaining bits carry value data, and subsequent bytes are pure payload. Like LEB128, they are little-endian and not canonical by construction. `vu128` explicitly permits overlong encodings by design, stating that applications requiring canonicality should check it themselves.

bijou64 was not built on vu128/vu64 because:

- **Little-endian byte order**, same consideration as LEB128.
- **Explicitly non-canonical.** The library considers overlong encodings a feature, not a bug — a reasonable design choice, but the opposite of bijou64's canonical-by-construction goal.
- **Difficult to debug by hand.** The first byte mixes prefix bits with value bits, requiring masking to extract either. Combined with little-endian payload order, reconstructing a value from a hexdump is not straightforward.

## SQLite4 Varint

[SQLite4's varint] uses a tag-byte and big-endian payloads — the closest _structural_ analogue to bijou64. It applies offsets for the first two multi-byte tiers (`240 + 256*(A0-241) + A1` and `2288 + 256*A1 + A2`), but switches to raw big-endian payloads for 3+ byte tiers. This means it is _not_ canonical by construction for those tiers: `[250, 0x00, 0x00, 0x01]` (value 1 as 3-byte big-endian) and `[0x01]` (single byte) both decode to 1.

bijou64 was not built directly on SQLite4's varint because:

- **Partial offset coverage.** Offsets apply only to tiers 1–2. Tiers 3+ use raw big-endian payloads, so overlong encodings are possible and canonicality requires a runtime check — exactly the class of error bijou64 is designed to eliminate.
- **Different tag-byte threshold.** SQLite4 uses 241 as the tag threshold (vs. 248 in VARU64/bijou64). Adopting SQLite4's threshold would sacrifice compatibility with VARU64's framing for no structural benefit.

bijou64 extends SQLite4's partial offset approach through all tiers, making every tier canonical by construction.

## Git Pack Offset Encoding

[Git's pack offset encoding] uses continuation-bit (LEB128-style) framing with full bijective offsets across all tiers. It is canonical by construction — the same principle as bijou64 — but uses a different wire format: 7 value bits per byte with MSB continuation, big-endian byte significance.

bijou64 was not built on Git's pack offset encoding because:

- **No length from first byte.** It uses MSB continuation bits, so the decoder must scan forward byte-by-byte to find the end of the encoding. This prevents $\mathcal{O}(1)$ skipping and complicates streaming parsers and buffer pre-allocation.
- **No lexicographic sort order.** Continuation bits are interleaved with value bits across every byte, so lexicographic byte comparison does not equal numeric comparison. The protocol requires sorted storage and binary search over encoded values without decoding.
- **Difficult to debug in a hexdump.** Each byte mixes one control bit with seven value bits. Reconstructing the original value requires masking every byte and reassembling 7-bit chunks — the same issue as LEB128.

bijou64 applies Git's offset principle to VARU64's tag-byte framing instead, gaining length-from-first-byte and contiguous big-endian payloads.

## VARU64

[VARU64] is the closest relative of bijou64. It uses the same tag-byte framing (first byte determines length), big-endian payload bytes, and value range. bijou64 directly inherits its wire format structure.

The difference is in payload interpretation. In VARU64, the payload bytes are the raw big-endian value. This means multiple byte sequences can _represent_ the same number: `[0xF8, 0x00]` decodes to 0, and so does `[0x00]`. The VARU64 spec requires decoders to reject the longer form, but this rejection is a single `if` statement that, if omitted:

- Does not break round-trip tests (encode-decode-compare still passes for all values).
- Does not break any test that only uses honestly-encoded data.
- Only fails under adversarial input — which may not be tested.

bijou64 was not built directly on VARU64 because:

- **Canonicality is not structural.** The single runtime check that rejects overlong encodings is load-bearing for correctness but invisible to normal testing. Removing it does not break any test that only uses honestly-encoded data. In a content-addressed protocol, accepting a non-canonical encoding silently produces a different hash.
- **The check is silently deletable.** Because round-trip tests pass without it, the canonicality check can be accidentally removed (or never implemented in a new port) without any test failure. bijou64's offset addition is not deletable — removing it breaks _everything_.

In bijou64, the offset addition replaces this runtime check with a structural guarantee. Decoding `[0xF8, 0x00]` produces 248 (not 0), because the decoder adds `OFFSET[1] = 248` to the payload. The overlong encoding does not silently succeed — it produces a _different_ value entirely. There is no check to forget.

The trade-off is that bijou64 payloads are not the raw value, so hexdump inspection is less direct for values above 247. For values below 248 (the common case), the encoding is byte-identical to VARU64.

# Future Extensions

The bijou64 design generalizes naturally to other integer widths. The offset recurrence, tag-byte framing, and bijective property are not specific to 64-bit integers — they depend only on the tag threshold (248), the number of tiers, and the maximum payload width. A family of encodings could be defined:

| Variant  | Max value   | Max bytes | Tiers |
|----------|-------------|-----------|-------|
| bijou16  | `u16::MAX`  | 3         | 0–2   |
| bijou32  | `u32::MAX`  | 5         | 0–4   |
| bijou64  | `u64::MAX`  | 9         | 0–8   |
| bijou128 | `u128::MAX` | 17        | 0–16  |

Each variant would use the same tag-byte threshold (248), the same offset recurrence, and the same encoding/decoding algorithms — differing only in the number of tiers and the maximum payload width. bijou128 would require tag bytes beyond `0xFF`, which would need an extended framing scheme (e.g., `0xFF` followed by a secondary length byte).

This specification does not define these variants. They are noted here to show that the design is not ad hoc — it is a specific instance of a general construction that could be extended if the need arises.

# License

This specification is adapted from the [VARU64] specification by [Aljoscha Meyer], licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). The tag-byte framing and tier structure are inherited from VARU64; the per-tier offset addition (inspired by [Git's pack offset encoding] and [SQLite4's varint]) and surrounding specification text are new in bijou64.

This specification is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

<!-- Links -->

[Aljoscha Meyer]: https://aljoscha-meyer.de/
[BCP 14]: https://www.rfc-editor.org/info/bcp14
[Brooklyn Zelenka]: https://github.com/expede
[Git's pack offset encoding]: https://git-scm.com/docs/pack-format#_original_version_1_pack_idx_files_have_the_following_format
[LEB128]: https://en.wikipedia.org/wiki/LEB128
[SQLite4's varint]: https://www.sqlite.org/src4/doc/trunk/www/varint.wiki
[VARU64]: https://github.com/AljoschaMeyer/varu64-rs
[bijective numeration]: https://en.wikipedia.org/wiki/Bijective_numeration
[vu128]: https://crates.io/crates/vu128
[vu64]: https://crates.io/crates/vu64
