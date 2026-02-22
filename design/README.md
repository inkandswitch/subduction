# Subduction Protocol Design

This directory contains protocol design documents for Subduction.

## Documents

| Document                          | Purpose                                      |
|-----------------------------------|----------------------------------------------|
| [`assumptions`](./assumptions.md) | Protocol assumptions and invariants          |
| [`handshake`](./handshake.md)     | Mutual authentication via Ed25519 signatures |
| [`protocol`](./protocol.md)       | Binary format, serialization, cryptographic choices |
| [`security/`](./security/)        | Threat model and security rationale          |
| [`sedimentree`](./sedimentree.md) | Depth-based data partitioning scheme         |
| [`sync/`](./sync/)                | Sync protocol overview and comparison        |

## Protocol Layers

```mermaid
block-beta
    columns 1
    Application
    Sync["Sync<br/>(Batch + Incremental)"]
    Connection["Connection<br/>(Handshake + Policy)"]
    Transport["Transport<br/>(WebSocket)"]
```

## Typical Flow

```mermaid
sequenceDiagram
    participant A as Peer A
    participant B as Peer B

    Note over A,B: 1. Connection Layer
    A->>B: Signed<Challenge>
    B->>A: Signed<Response>
    Note over A,B: Identities established

    Note over A,B: 2. Initial Sync + Subscribe (Batch)
    A->>B: BatchSyncRequest { fingerprint_summary, subscribe: true }
    B->>A: BatchSyncResponse { diff }
    Note over A,B: States reconciled via fingerprint diffing, A subscribed

    Note over A,B: 3. Ongoing Sync (Incremental)
    A->>B: LooseCommit { commit, blob }
    Note over B: Forward to subscribed peers
    Note over A,B: Real-time updates
```

## Design Principles

- **no_std compatible** — Core protocol logic works without std
- **Transport agnostic** — All messages use canonical binary format, transport is pluggable
- **Policy separation** — Authentication (handshake) is separate from authorization (policy)
- **Subscription-based** — Updates forwarded only to subscribed and authorized peers
- **Content addressed** — All data identified by BLAKE3 hash
- **Idempotent** — Receiving the same data twice is safe
