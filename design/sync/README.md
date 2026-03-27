# Sync Protocols

Subduction uses two complementary sync protocols to keep sedimentrees consistent across peers, with an optional subscription mechanism for real-time updates.

## Overview

| Protocol | Direction | Scope | Use Case |
|----------|-----------|-------|----------|
| [Batch](./batch.md) | Pull (request/response) | Entire sedimentree | Initial sync, reconnection |
| [Incremental](./incremental.md) | Push (fire-and-forget) | Single commit/fragment | Real-time updates |
| [Subscriptions](./subscriptions.md) | Opt-in | Per sedimentree | Live update filtering |
| [Reconnection](./reconnection.md) | Automatic | Per connection | Connection recovery |

Batch sync uses [fingerprint-based reconciliation](./batch.md#fingerprint-based-reconciliation) — compact 8-byte SipHash keyed hashes instead of full 32-byte digests — to minimize request payload (~75% reduction).

## Typical Usage

```mermaid
sequenceDiagram
    participant A as Peer A
    participant B as Peer B

    Note over A,B: 1. Initial Sync + Subscribe (Batch)
    A->>B: BatchSyncRequest { fingerprint_summary, subscribe: true }
    B->>A: BatchSyncResponse { diff }
    Note over A,B: States reconciled, A subscribed

    Note over A,B: 2. Ongoing Sync (Incremental)
    A->>B: LooseCommit { commit, blob }
    Note over B: Forward to subscribed peers
    Note over A,B: Real-time updates

    Note over A,B: 3. Reconnection (Batch)
    Note over A: Connection lost...
    Note over A: Connection restored
    A->>B: BatchSyncRequest { fingerprint_summary, subscribe: true }
    B->>A: BatchSyncResponse { diff }
    Note over A,B: Caught up, re-subscribed
```

## Comparison

| Aspect | Batch Sync | Incremental Sync |
|--------|------------|------------------|
| **Direction** | Pull (request/response) | Push (fire-and-forget) |
| **Scope** | Entire sedimentree state | Single commit or fragment |
| **Latency** | One round-trip | Immediate |
| **Bandwidth** | Efficient for large diffs | Efficient for small changes |
| **Ordering** | Snapshot at request time | No guarantees |
| **Reliability** | Guaranteed delivery | Best-effort |

## When to Use Each

### Batch Sync

- First connection to a peer
- Reconnecting after disconnect
- Periodic consistency check
- Joining an existing document

### Incremental Sync

- Active editing sessions
- Real-time collaboration
- Low-latency updates
- Continuous synchronization

## Remote Heads Tracking

Both batch and incremental sync carry [`RemoteHeads`](./batch.md#remoteheads) — the sender's current tip commits for the sedimentree, alongside a per-peer monotonic counter. This enables:

- **Staleness detection**: receivers discard out-of-order updates on non-TCP transports
- **Application awareness**: the `RemoteHeadsObserver` callback notifies the application when a peer's heads change, enabling UI updates (e.g., showing which version a collaborator is viewing)
- **1.5 RTT acknowledgment**: after ingesting a commit/fragment, the receiver sends a `HeadsUpdate` back, so the sender knows its data was processed

The counter is shared across all handlers via [`PeerCounter`] — a single monotonic sequence per peer regardless of which handler (sync, ephemeral, keyhive) produced the message.

[`PeerCounter`]: ../../subduction_core/src/peer/counter.rs

## Consistency Model

Both protocols are **eventually consistent**:

1. Incremental sync provides low-latency propagation
2. Batch sync recovers from missed messages
3. Content-addressing ensures identical data has identical hashes
4. Idempotent storage means duplicates are harmless
5. Per-peer monotonic counters ensure heads are never reverted by out-of-order delivery

If incremental messages are lost (network issues, peer offline), batch sync on reconnection will catch up.
