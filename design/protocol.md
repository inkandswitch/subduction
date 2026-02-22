# Protocol Design

This document describes the binary format and cryptographic choices for Subduction.

## Binary Format

All Subduction messages use a custom canonical binary codec:

| Category | Examples |
|----------|----------|
| **Signed payloads** | `LooseCommit`, `Fragment`, `Challenge`, `Response` |
| **Sync messages** | `BatchSyncRequest`, `BatchSyncResponse`, `BlobsRequest` |

The codec is designed for:
- **Determinism** — Required for signature verification
- **Compactness** — No field names, minimal overhead
- **Simplicity** — No CBOR/MessagePack edge cases or implementation variance
- **Predictability** — Fixed field order, big-endian integers

### Signed Payload Format

All signed payloads use a canonical binary format:

```
┌───────────────────────── Payload ─────────────────────────┬── Seal ──┐
╔════════╦══════════╦═══════════════════════════════════════╦══════════╗
║ Schema ║ IssuerVK ║         Type-Specific Fields          ║   Sig    ║
║   4B   ║   32B    ║             (variable)                ║   64B    ║
╚════════╩══════════╩═══════════════════════════════════════╩══════════╝
```

| Field       | Size     | Purpose                                      |
|-------------|----------|----------------------------------------------|
| **Schema**  | 4 bytes  | Type and version identifier (see below)      |
| **IssuerVK**| 32 bytes | Ed25519 verifying key of the signer          |
| **Fields**  | variable | Type-specific data (see [Serialization])     |
| **Signature**| 64 bytes| Ed25519 signature over bytes `[0..len-64]`   |

The signature covers _everything before it_ — schema, issuer, and all fields.

### Schema Header

The 4-byte schema header identifies the payload type and version:

```
╔═════════════════╦═══════════╦═════════╗
║     Prefix      ║   Type    ║ Version ║
║     2 bytes     ║   1 byte  ║ 1 byte  ║
╚═════════════════╩═══════════╩═════════╝
```

| Prefix | Namespace |
|--------|-----------|
| `ST` (0x53 0x54) | Sedimentree types (`LooseCommit`, `Fragment`) |
| `SU` (0x53 0x55) | Subduction types (`Challenge`, `Response`) |

The type byte identifies the specific type within the namespace.
The version byte enables forward-compatible evolution per type.

**Validation:** Decoders reject messages with unknown prefixes or unsupported versions.

[Serialization]: #serialization

## Cryptographic Choices

### Ed25519 Signatures

All signatures in Subduction use [Ed25519 (RFC 8032)](https://www.rfc-editor.org/rfc/rfc8032.html), a fixed choice rather than algorithm negotiation.

**Why Ed25519?**

| Property | Benefit |
|----------|---------|
| **Fast verification** | 71,000 signatures/sec on modern hardware |
| **Small signatures** | 64 bytes |
| **Small keys** | 32 bytes (public), 32 bytes (private) |
| **Deterministic** | Same message always produces same signature |
| **Secure** | No known practical attacks; conservative design |
| **Wide support** | Available in all major languages and HSMs |

**Why fixed algorithm (no negotiation)?**

- **Simplicity** — No downgrade attacks, no algorithm confusion
- **Security** — Eliminates entire class of negotiation vulnerabilities
- **Predictability** — All implementations behave identically
- **Future-proofing via versioning** — If Ed25519 is broken, bump protocol version

This follows the ["Cryptographic Doom Principle"](https://moxie.org/2011/12/13/the-cryptographic-doom-principle.html), which states that cryptographic data must be authenticated and verified before it is decrypted or otherwise processed—in short, verify _before_ processing. With a fixed algorithm, verification is unambiguous.

### Key Derivation

Peer identity (`PeerId`) is derived from the Ed25519 verifying key:

```
PeerId = verifying_key_bytes  // 32 bytes, no hashing
```

Direct use of the key bytes (rather than hashing) means:

- **Reversible** — Given a `PeerId`, you can verify signatures directly
- **Compact** — No additional storage for derived IDs
- **Simple** — No hash function dependencies for identity

### SipHash-2-4 Fingerprints

Batch sync uses [SipHash-2-4](https://www.aumasson.jp/siphash/siphash.pdf) for compact set reconciliation. Instead of exchanging full 32-byte BLAKE3 digests to determine what each peer has, peers exchange 8-byte keyed hashes (fingerprints) with a random per-request seed.

| Property | Value |
|----------|-------|
| Output size | 8 bytes (u64) |
| Key size | 16 bytes (two u64s) |
| Security | PRF (pseudorandom function) under 128-bit key |
| Speed | ~50-100ns per item on Wasm (no SIMD required) |

**Why SipHash over BLAKE3 for reconciliation?**

| | SipHash-2-4 | BLAKE3 keyed |
|--|-------------|-------------|
| **Purpose** | Keyed short hashing | Cryptographic MAC |
| **Output** | Native u64 | 32 bytes (must truncate) |
| **Speed (Wasm)** | Baseline | 5-10x slower |
| **Speed (native)** | Baseline | 3-5x slower |

At 1200 items per sync request, the server must hash every local item per incoming request. SipHash keeps this fast even on constrained runtimes.

**Why SipHash-2-4 over SipHash-1-3?**

SipHash-2-4 is the conservative original parameterization with proven PRF security. SipHash-1-3 has weaker security margins, designed primarily for hash table DoS resistance. Since the seed travels over the wire (attacker sees it), the stronger guarantees of 2-4 matter. The ~40% speed difference is negligible compared to the 3-10x win over BLAKE3.

See [batch sync](./sync/batch.md) for how fingerprints are used in the protocol.

### Content Addressing

All content is addressed by [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) hash:

| Property       | Value                        |
|----------------|------------------------------|
| Output size    | 32 bytes                     |
| Speed          | ~3 GB/s on modern CPUs       |
| Security       | 128-bit collision resistance |
| Parallelizable | Yes (tree structure)         |

BLAKE3 was chosen over SHA-256 for performance and over BLAKE2 for simplicity (single function, no variants).

## Message Types

### Handshake Messages

```rust
enum HandshakeMessage {
    SignedChallenge(Signed<Challenge>),
    SignedResponse(Signed<Response>),
    Rejection(Rejection),  // Unsigned
}
```

See [`handshake.md`](./handshake.md) for details.

### Sync Messages

```rust
enum Message {
    LooseCommit { id, commit, blob },
    Fragment { id, fragment, blob },
    BlobsRequest { id, digests },
    BlobsResponse { id, blobs },
    BatchSyncRequest(BatchSyncRequest),
    BatchSyncResponse(BatchSyncResponse),
    RemoveSubscriptions(RemoveSubscriptions),
}
```

See [`sync/`](./sync/) for details. Batch sync uses [fingerprint-based reconciliation](./sync/batch.md#fingerprint-based-reconciliation) for compact set diffing.

## Serialization

Signed payloads (`LooseCommit`, `Fragment`, `Challenge`, `Response`) use a custom canonical binary codec defined in `sedimentree_core::codec`. This section documents the encoding rules.

### Design Goals

| Goal | Rationale |
|------|-----------|
| **Deterministic** | Identical payloads produce identical bytes — required for signatures |
| **Compact** | No field names, no type tags for primitives |
| **Verifiable** | Schema header enables type checking before parsing |
| **Versionable** | Per-type version byte allows independent evolution |
| **`no_std`** | Compatible with `no_std` + `alloc` environments |

### Codec Traits

```rust
/// Type identity for schema validation.
trait Schema {
    /// 4-byte header: [prefix0, prefix1, type_byte, version].
    const SCHEMA: [u8; 4];
}

/// Encode to canonical bytes.
trait Encode: Schema {
    fn encode_fields(&self, buf: &mut Vec<u8>);
    fn fields_size(&self) -> usize;
}

/// Decode from canonical bytes.
trait Decode: Schema + Sized {
    const MIN_SIZE: usize;
    fn try_decode_fields(buf: &[u8]) -> Result<Self, DecodeError>;
}
```

### Replay Prevention

To prevent replay attacks, `LooseCommit` and `Fragment` embed their `SedimentreeId` directly as a field. This ID is included in the signed bytes, so a commit or fragment signed for one document cannot be replayed under a different document — the signature verification would fail.

| Type | Replay Prevention |
|------|-------------------|
| `LooseCommit` | Contains `sedimentree_id` field |
| `Fragment` | Contains `sedimentree_id` field |
| `Challenge` | Self-contained (includes audience and nonce) |
| `Response` | Self-contained (includes challenge digest) |

### Primitive Encoding

All multi-byte integers are **big-endian** (network byte order):

| Type | Encoding |
|------|----------|
| `u8` | 1 byte |
| `u16` | 2 bytes, big-endian |
| `u32` | 4 bytes, big-endian |
| `u64` | 8 bytes, big-endian |
| `[u8; N]` | N bytes, raw |
| `&[u8]` | Length-prefixed (u16 or u32) + raw bytes |

### Array Encoding

Variable-length arrays are encoded as:

```
╔═══════════╦═══════════╦═══════════╦═════╗
║  Count    ║  Item 0   ║  Item 1   ║ ... ║
║  2 bytes  ║  N bytes  ║  N bytes  ║     ║
╚═══════════╩═══════════╩═══════════╩═════╝
```

**Invariants:**
- Count is u16 big-endian (max 65,535 items)
- Items must be **sorted ascending** by their byte representation
- No duplicate items allowed
- Decoders reject unsorted or duplicate arrays

Sorted arrays enable:
- O(log n) lookups without additional indexing
- Deterministic encoding (no ordering ambiguity)
- Efficient set operations during sync

### Error Handling

Decoding returns structured errors with context:

| Error | Meaning |
|-------|---------|
| `BufferTooShort` | Not enough bytes to read a primitive |
| `InvalidSchema` | Schema header doesn't match expected type |
| `UnsortedArray` | Array elements not in ascending order |
| `InvalidEnumTag` | Unknown discriminant value |
| `SizeMismatch` | Declared size doesn't match actual data |
| `BlobTooLarge` | Blob exceeds maximum allowed size |
| `ArrayTooLarge` | Too many elements in array |
| `DuplicateElement` | Array contains duplicates |

All error types implement `#[from]` for ergonomic `?` propagation.

### Sync Message Envelope

Sync messages use a framing envelope:

```
╔════════╦══════════╦═════╦═════════════════════╗
║ Schema ║   Size   ║ Tag ║      Payload        ║
║   4B   ║    4B    ║ 1B  ║     (variable)      ║
╚════════╩══════════╩═════╩═════════════════════╝
```

| Field | Purpose |
|-------|---------|
| Schema | `SM` prefix + version (message envelope identity) |
| Size | Total message size in bytes (big-endian u32) |
| Tag | Message type discriminant |
| Payload | Type-specific fields using the same primitive encoding |

Unlike signed payloads, sync messages are not signed — they're authenticated by the connection layer (handshake establishes peer identity).

## Future Considerations

### Post-Quantum Migration

Ed25519 is not post-quantum secure. Migration options:

1. **Hybrid signatures** — Ed25519 + ML-DSA (future version)
2. **Protocol version bump** — Switch to pure post-quantum when practical

The version byte enables either approach without breaking existing deployments.

### Algorithm Agility

To keep the protocol simple and with a smaller security surface area, we avoid algorithmic agility.

## Implementation Notes

### Signer Abstraction

The `Signer` trait abstracts key management:

```rust
trait Signer<K: FutureKind> {
    fn sign(&self, message: &[u8]) -> K::Future<'_, Signature>;
    fn verifying_key(&self) -> VerifyingKey;
}
```

Implementations include:

- **MemorySigner** — In-memory keys for testing/development
- **WebCryptoSigner** — Browser WebCrypto API for Wasm
- **HSM signers** — Hardware security modules (future)

### Canonical Binary Codec

Signed payloads use a custom codec (not CBOR) to guarantee determinism:

- Fixed field order defined by `Encode` implementation
- Big-endian integers (no smallest-encoding ambiguity)
- Sorted arrays with no duplicates
- No optional fields or default values

See [Serialization](#serialization) for the full specification.
