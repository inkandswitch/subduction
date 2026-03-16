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
    Connection["Connection + Roundtrip<br/>(Typed Messages + Request-Response)"]
    Transport["Transport<br/>(Byte-oriented: Handshake · Policy · Multiplexing)"]
    Backend["Backend<br/>(WebSocket · HTTP Long-Poll · Iroh/QUIC)"]
```

### Transport Composition

```
Backend (one impl per transport)
  └── Transport         send_bytes / recv_bytes / disconnect
       ├── MessageTransport<T>   Connection<K, M>  (typed encode/decode)
       └── MuxTransport<T, O>    Roundtrip<K, Req, Resp>  (request-response via Multiplexer)
```

## Typical Flow

```mermaid
sequenceDiagram
    participant A as Peer A
    participant B as Peer B

    Note over A,B: 1. Handshake (over Transport)
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
