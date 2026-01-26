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

This follows the ["Cryptographic Doom Principle"](https://moxie.org/2011/12/13/the-cryptographic-doom-principle.html): verify _before_ processing. With a fixed algorithm, verification is unambiguous.

### Key Derivation

Peer identity (`PeerId`) is derived from the Ed25519 verifying key:

```
PeerId = verifying_key_bytes  // 32 bytes, no hashing
```

Direct use of the key bytes (rather than hashing) means:

- **Reversible** — Given a `PeerId`, you can verify signatures directly
- **Compact** — No additional storage for derived IDs
- **Simple** — No hash function dependencies for identity

### Content Addressing

All content is addressed by [BLAKE3](https://github.com/BLAKE3-team/BLAKE3) hash:

| Property | Value |
|----------|-------|
| Output size | 32 bytes |
| Speed | ~3 GB/s on modern CPUs |
| Security | 128-bit collision resistance |
| Parallelizable | Yes (tree structure) |

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
enum SyncMessage {
    BatchSyncRequest(BatchSyncRequest),
    BatchSyncResponse(BatchSyncResponse),
    LooseCommit(LooseCommit),
    RemoveSubscriptions(RemoveSubscriptions),
}
```

See [`sync/`](./sync/) for details.

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
