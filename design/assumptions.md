# Protocol Assumptions

This document lists the assumptions Subduction makes about its environment. Violating these assumptions may lead to incorrect behavior, data loss, or security vulnerabilities.

## Storage

### Idempotency

> **Assumption:** Storing the same data twice has the same effect as storing it once.

The sync protocol may deliver the same commit multiple times due to:

- Network retries
- Multiple paths in the peer graph
- Subscription overlap

Storage implementations must handle this gracefully:

```
store(commit_A) → Ok
store(commit_A) → Ok  // Same commit, no error
read(commit_A.id) → commit_A  // Same result
```

**Consequence of violation:** Duplicate entries, inconsistent state, storage errors.

### Durability

> **Assumption:** Once a write returns success, the data will survive restarts.

The protocol does not implement journaling or write-ahead logging. Durability guarantees come from the underlying storage:

| Storage Type | Durability |
|--------------|------------|
| Filesystem (fsync) | Survives process crash |
| IndexedDB | Browser-dependent |
| In-memory | Lost on restart |

**Consequence of violation:** Data loss, divergent state between peers.

### Atomicity (Per-Commit)

> **Assumption:** Individual commits are stored atomically.

A commit and its associated blob must be stored together or not at all. Partial writes should not occur:

```
store_commit(commit, blob)
  → Either both stored
  → Or neither stored (on failure)
```

Multi-commit atomicity is _not_ assumed — peers may see partial sync results.

**Consequence of violation:** Orphaned blobs, unresolvable commits, garbage accumulation.

## Network

### Reliable Delivery (Within Connection)

> **Assumption:** Messages sent on an established connection either arrive intact or the connection fails.

The protocol assumes the transport layer (WebSocket) handles:

- Ordering (messages arrive in send order)
- Integrity (no silent corruption)
- Delivery (messages don't disappear without error)

**Consequence of violation:** Protocol desync, corrupted state.

### No Assumption of Delivery Across Connections

The protocol does _not_ assume messages survive connection loss. Reconnection triggers fresh sync:

```
Connection 1: send(commit_A) → connection dies
Connection 2: reconnect → full sync → commit_A re-sent if needed
```

This simplifies recovery at the cost of redundant transfers.

## Time

### Bounded Clock Drift

> **Assumption:** Peer clocks are within ±10 minutes of each other.

The handshake protocol uses timestamps for freshness. `MAX_PLAUSIBLE_DRIFT` (10 minutes) bounds acceptable drift:

| Drift | Behavior |
|-------|----------|
| < 1 minute | Normal operation |
| 1-10 minutes | Accepted with warning; drift correction available |
| > 10 minutes | Connection rejected |

**Consequence of violation:** Unable to establish connections, potential replay vulnerabilities.

### Monotonic Time for Nonce Cache

> **Assumption:** System time does not jump backwards significantly.

The `NonceCache` uses time-based buckets for expiry. Large backward jumps could:

- Prematurely expire valid entries
- Allow replay of "expired" nonces

Small jumps (< bucket duration) are tolerated.

**Consequence of violation:** Replay attack window, spurious rejections.

## Cryptography

### Ed25519 Security

> **Assumption:** Ed25519 signatures cannot be forged without the private key.

The protocol provides no fallback if Ed25519 is broken. Migration would require:

1. Protocol version bump
2. Coordinated upgrade across all peers

### BLAKE3 Collision Resistance

> **Assumption:** Finding two inputs with the same BLAKE3 hash is computationally infeasible.

Content addressing relies on hash uniqueness. A collision would allow:

- Content substitution
- Integrity bypass

### Random Number Generation

> **Assumption:** Nonces are generated from a cryptographically secure source.

Nonces must be unpredictable to prevent:

- Replay attacks (predictable nonces)
- Correlation attacks (biased distribution)

The protocol uses `getrandom` on native platforms and `crypto.getRandomValues()` in browsers.

**Consequence of violation:** Replay attacks, authentication bypass.

## CBOR Encoding

### Deterministic Encoding

> **Assumption:** The same structured data produces the same bytes.

Signature verification requires byte-for-byte reproducibility:

```
encode(challenge) → bytes_1
sign(bytes_1) → signature

// On receiver:
encode(decoded_challenge) → bytes_2
verify(bytes_2, signature)  // Must match!
```

This requires:

- Sorted map keys (by encoded length, then lexicographically)
- Canonical integer encoding (smallest representation)
- No indefinite-length containers

**Consequence of violation:** Signature verification failures.

### No Duplicate Map Keys

> **Assumption:** CBOR maps do not contain duplicate keys.

Per RFC 8949, duplicate keys are invalid. The protocol currently does _not_ detect duplicates on decode (see TODOs).

**Consequence of violation:** Ambiguous parsing, potential security issues.

## Concurrency

### No Concurrent Writes to Same Document

> **Assumption:** Concurrent operations on different documents are independent.

The protocol does not provide transaction isolation across documents. Concurrent writes to the _same_ document are handled by the CRDT merge semantics, not by the sync protocol.

### Thread Safety (Native)

> **Assumption:** Storage and connection implementations are thread-safe when using `Sendable` futures.

The `FutureKind` abstraction requires:

- `Sendable`: Storage must be `Send + Sync`
- `Local`: No thread safety required

**Consequence of violation:** Data races, undefined behavior.

## Trust Model

### Peers Are Authenticated

> **Assumption:** The handshake establishes authentic peer identity.

After handshake, `PeerId` is trusted to represent the peer. This does _not_ imply:

- The peer is authorized (separate policy check)
- The peer is honest (Byzantine behavior possible)
- The peer's data is valid (content verification separate)

### Storage Is Trusted

> **Assumption:** The local storage backend does not lie about stored data.

The protocol trusts storage to:

- Return what was stored
- Not fabricate entries
- Accurately report what exists

A malicious storage layer could violate sync correctness.

## What We Don't Assume

For clarity, these are _not_ assumed:

| Non-Assumption | Protocol Handles |
|----------------|------------------|
| Peers are online | Eventual sync when reachable |
| Network is fast | Streaming, incremental sync |
| Peers agree on data | CRDT merge semantics |
| Clocks are synchronized | Drift tolerance + correction |
| Connections are stable | Reconnection + re-sync |
| Peers are honest | Authorization + content verification |
