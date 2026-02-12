# Protocol Design

This document describes the wire format and cryptographic choices for Subduction.

## Wire Format

All messages are encoded using [CBOR (RFC 8949)](https://www.rfc-editor.org/rfc/rfc8949.html), chosen for:

- **Compact binary encoding** — Smaller than JSON, no schema required
- **Self-describing** — Type information embedded in the encoding
- **Wide support** — Implementations in most languages
- **Deterministic encoding** — Required for signature verification

### Signed Payload Envelope

Signed payloads use a structured envelope:

```
┌─────────────────────────────────────────────────────────┐
│  Magic (3 bytes)  │  Version (1 byte)  │  Payload (N)   │
├───────────────────┼────────────────────┼────────────────┤
│       "SDN"       │        0x00        │  CBOR data     │
└─────────────────────────────────────────────────────────┘
```

| Field       | Purpose                                                        |
|-------------|----------------------------------------------------------------|
| **Magic**   | `SDN` (0x53 0x44 0x4E) — Identifies Subduction signed payloads |
| **Version** | Protocol version byte — Enables format evolution               |
| **Payload** | CBOR-encoded content being signed                              |

### Protocol Versioning

The protocol version is an enum allowing forward compatibility:

```rust
enum ProtocolVersion {
    V0_1 = 0,  // Current version
    // Future versions can be added here
}
```

**Upgrade path:**

1. New versions add new enum variants
2. Receivers check version before parsing payload
3. Unknown versions can be rejected gracefully
4. Version negotiation happens at connection time (future)

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

### Deterministic CBOR

For signature verification, CBOR encoding must be deterministic:

- Map keys sorted by encoded length, then lexicographically
- Integers use smallest encoding
- No indefinite-length arrays/maps

The `minicbor` crate enforces this by default.
