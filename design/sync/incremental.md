# Incremental Sync Protocol

Incremental sync propagates individual changes to subscribed and authorized peers as they happen. It answers _"what just changed?"_ by pushing commits and fragments immediately.

## Overview

Incremental sync is a push-based protocol. When a peer adds a commit or fragment locally, it forwards the change to peers who have subscribed to that sedimentree and are authorized to receive it. There is no request/response — messages are fire-and-forget.

> [!NOTE]
> Incremental sync assumes peers are already roughly synchronized. Use batch sync first to establish a baseline (with `subscribe: true` to opt into updates), then incremental sync for ongoing updates. See [Subscriptions](./subscriptions.md) for details.

```mermaid
sequenceDiagram
    participant A as Sender
    participant S as Server
    participant B as Subscriber 1
    participant C as Subscriber 2

    Note over A: Local change occurs

    A->>S: LooseCommit { id, commit, blob }
    Note over S: Forward to subscribed + authorized peers
    S->>B: LooseCommit { id, commit, blob }
    S->>C: LooseCommit { id, commit, blob }

    Note over B: Store commit + blob
    Note over C: Store commit + blob
```

Changes propagate only to peers who have subscribed to that sedimentree.

## Message Types

### LooseCommit (Sender → Receivers)

```rust
Message::LooseCommit {
    id: SedimentreeId,             // Which sedimentree this commit belongs to
    commit: Signed<LooseCommit>,   // Signed commit metadata
    blob: Blob,                    // The commit's data
}
```

A loose commit is a change that hasn't yet been rolled into a fragment. The `Signed<LooseCommit>` envelope includes:
- Ed25519 signature from the author
- Author's verifying key (used for authorization)
- CBOR-encoded commit payload (content digest, parent references, blob metadata)

See [protocol.md](../protocol.md) for the `Signed<T>` envelope format.

### Fragment (Sender → Receivers)

```rust
Message::Fragment {
    id: SedimentreeId,          // Which sedimentree this fragment belongs to
    fragment: Signed<Fragment>, // Signed fragment metadata
    blob: Blob,                 // The fragment's data
}
```

A fragment is created when a commit's hash has enough leading zero bytes to trigger a checkpoint at that depth. Fragments consolidate multiple commits into a single structure.

## Propagation

When a change occurs locally:

1. Store the commit/fragment and blob locally
2. Forward to subscribed and authorized peers (not all connected peers)
3. Each peer stores and re-forwards to _their_ subscribers (gossip)

```mermaid
sequenceDiagram
    participant A as Origin
    participant S as Server
    participant B as Subscriber B
    participant C as Subscriber C
    participant D as Not Subscribed

    Note over A: Create commit

    A->>S: LooseCommit
    Note over S: Get subscribers for this sedimentree
    Note over S: Filter by authorization
    S->>B: LooseCommit
    S->>C: LooseCommit
    Note over S: D not subscribed, not forwarded

    Note over B: Store, then propagate to own subscribers
    Note over C: Store, then propagate to own subscribers
```

Peers deduplicate by content digest — receiving the same commit twice is idempotent.

> [!NOTE]
> The authorization check uses `filter_authorized_fetch` to batch-check which peers are allowed to receive the update. Peers whose access has been revoked simply stop receiving forwards — no explicit notification is sent.

## Wire Format

Messages are CBOR-encoded and wrapped in the `Message` enum:

```rust
enum Message {
    LooseCommit { id, commit, blob },
    Fragment { id, fragment, blob },
    // ... other variants ...
}
```

Sent as WebSocket binary frames. No request ID — these are one-way messages.

## Properties

| Property | Mechanism |
|----------|-----------|
| **Low latency** | Push immediately on change |
| **Consistency** | Content-addressed deduplication |
| **Idempotency** | Same commit can be received multiple times safely |
| **Ordering** | No guarantees; CRDT handles conflicts |

## Sequence Diagram (Commit)

```mermaid
sequenceDiagram
    participant A as Sender
    participant B as Receiver

    Note left of A: Create new commit locally
    Note left of A: Store commit + blob

    A->>B: LooseCommit { id, commit, blob }

    Note right of B: Verify authorization
    Note right of B: Store commit
    Note right of B: Store blob
    Note right of B: Update sedimentree

    Note over A,B: Commit Propagated
```

## Sequence Diagram (Fragment Boundary)

```mermaid
sequenceDiagram
    participant A as Sender
    participant B as Receiver

    Note left of A: Create commit with depth > 0
    Note left of A: Store commit + blob

    A->>B: LooseCommit { id, commit, blob }

    Note left of A: Create fragment at depth
    Note left of A: Store fragment + blob

    A->>B: Fragment { id, fragment, blob }

    Note right of B: Store commit
    Note right of B: Store fragment
    Note right of B: Prune commits rolled into fragment
```

## Implementation Notes

### Sending a Commit

```rust
// Store locally first
storage.save_loose_commit(id, commit.clone()).await?;
storage.save_blob(id, blob.clone()).await?;
sedimentree.add_commit(commit.clone());

// Forward to subscribed and authorized peers
let msg = Message::LooseCommit { id, commit, blob };
let subscriber_conns = get_authorized_subscriber_conns(id, &self.peer_id()).await;
for conn in subscriber_conns {
    if let Err(e) = conn.send(&msg).await {
        // Connection failed, unregister it
        unregister(&conn).await;
    }
}

// Check if we need to create a fragment
let depth = depth_metric.to_depth(commit.digest());
if depth > Depth(0) {
    // Request fragment creation from application layer
    return Ok(Some(FragmentRequested::new(commit.digest(), depth)));
}
```

### Receiving a Commit

```rust
let Message::LooseCommit { id, signed_commit, blob } = msg;

// Verify signature; author extracted from signature, not sender
let verified = signed_commit.verify()?;
let author = verified.author();

// Check authorization (author from signature, not sender)
let putter = policy.authorize_put(sender_peer_id, author, id).await?;

// CAS storage: keyed by digest
putter.save_loose_commit(verified).await?;
putter.save_blob(blob.clone()).await?;

// Forward to subscribed and authorized peers (excluding sender)
let subscriber_conns = get_authorized_subscriber_conns(id, &sender_peer_id).await;
for conn in subscriber_conns {
    conn.send(&msg).await?;
}
```

### Receiving a Fragment

```rust
let Message::Fragment { id, signed_fragment, blob } = msg;

// Verify signature; author extracted from signature
let verified = signed_fragment.verify()?;
let author = verified.author();

// Check authorization
let putter = policy.authorize_put(sender_peer_id, author, id).await?;

// CAS storage: keyed by digest
putter.save_fragment(verified).await?;
putter.save_blob(blob.clone()).await?;

// Prune loose commits that are now covered by this fragment
sedimentree.prune_commits_covered_by(&verified.payload());

// Forward to subscribed and authorized peers (excluding sender)
let subscriber_conns = get_authorized_subscriber_conns(id, &sender_peer_id).await;
for conn in subscriber_conns {
    conn.send(&msg).await?;
}
```

## Error Handling

Incremental sync is best-effort:

- **Send failures**: Unregister the failed connection, continue with others
- **Authorization failures**: Log and discard, don't propagate
- **Storage failures**: Log error, may retry on next sync
- **Duplicate data**: Silently ignore (idempotent storage)

Consistency is eventually achieved through batch sync if incremental messages are lost.
