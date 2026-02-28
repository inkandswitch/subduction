# bijou64

Bijective variable-length encoding for unsigned 64-bit integers.

Pronounced "bee-zoo-sixty-four" — **bij**ective **o**ffset **u64**.

`bijou64` encodes `u64` values into 1–9 bytes using a tag-byte prefix
scheme derived from [VARU64], modified with per-tier offsets to achieve
_structural canonicality_ — each value has exactly one encoding, and
each encoding has exactly one value.

## Quick start

```rust
// Encode
let mut buf = Vec::new();
bijou64::encode(300, &mut buf);
assert_eq!(buf, [0xF8, 0x34]); // tag 0xF8, payload 300 - 248 = 52

// Decode
let (value, len) = bijou64::decode(&buf).unwrap();
assert_eq!(value, 300);
assert_eq!(len, 2);

// Stack-allocated encoding (no alloc needed)
let (bytes, len) = bijou64::encode_array(300);
assert_eq!(&bytes[..len], &[0xF8, 0x34]);

// Query encoded length without encoding
assert_eq!(bijou64::encoded_len(300), 2);
```

## Encoding

| First byte  | Total length | Offset     | Value range                |
|-------------|--------------|------------|----------------------------|
| 0x00 – 0xF7 | 1            | 0          | 0 – 247                    |
| 0xF8        | 2            | 248        | 248 – 503                  |
| 0xF9        | 3            | 504        | 504 – 66,039               |
| 0xFA        | 4            | 66,040     | 66,040 – 16,843,255        |
| 0xFB        | 5            | 16,843,256 | 16,843,256 – 4,311,810,551 |
| 0xFC – 0xFF | 6 – 9        | ...        | ... – u64::MAX             |

Values below 248 encode as a single byte equal to the value. Larger
values use a tag byte (`0xF8`–`0xFF`) followed by 1–8 big-endian
payload bytes encoding `value - OFFSET[tier]`.

See [SPEC.md](SPEC.md) for the full specification, offset table,
worked examples, and test vectors.

## Why not VARU64?

VARU64 uses the same tag-byte framing, but the payload is the raw
value. This means overlong encodings are _representable_ — the
decoder must reject them with a runtime check. If that check is
omitted, round-trip tests still pass; only adversarial inputs expose
the bug.

`bijou64` makes the offset subtraction load-bearing: decoding
`[0xF8, 0x00]` produces 248 (not 0), because the decoder adds
`OFFSET[1] = 248` to the payload. There is no overlong encoding to
reject, and no check to forget.

## Features

- `no_std` (requires `alloc` for `encode()`; `encode_array()` and
  `decode()` are allocation-free)
- `#![forbid(unsafe_code)]`
- Canonical by construction — no runtime canonicality checks
- Big-endian payloads — lexicographic byte order = numeric order
- Total encoding length determined from first byte alone
- Full `u64` range (0 to 2^64 − 1)

## Optional features

| Feature     | Description                                              |
|-------------|----------------------------------------------------------|
| `arbitrary` | `Arbitrary` impl for fuzz testing                        |
| `bolero`    | Property-based testing with bolero (implies `arbitrary`) |

## License

The code is licensed under MIT OR Apache-2.0 (workspace default).
The [specification](SPEC.md) is licensed under
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

[VARU64]: https://github.com/AljoschaMeyer/varu64-rs
