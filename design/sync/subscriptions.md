# Subscriptions

Subscriptions enable peers to receive real-time updates for specific sedimentrees. Rather than broadcasting to all connected peers, updates are forwarded only to peers who have explicitly subscribed _and_ are authorized to receive them.

## Overview

Subscriptions are opt-in. A peer must send a `BatchSyncRequest` with `subscribe: true` to receive future updates for that sedimentree. The server tracks which peers are subscribed to which sedimentrees, and filters updates accordingly.

```mermaid
sequenceDiagram
    participant A as Peer A
    participant S as Server
    participant B as Peer B

    Note over A,S: A subscribes to doc-123
    A->>S: BatchSyncRequest { id: doc-123, subscribe: true }
    S->>A: BatchSyncResponse { diff }
    Note over S: A is now subscribed to doc-123

    Note over B,S: B makes a change
    B->>S: LooseCommit { id: doc-123, commit, blob }

    Note over S: Forward to subscribers
    S->>A: LooseCommit { id: doc-123, commit, blob }
```

## Subscription Model

### Subscribe via Batch Sync

Subscriptions are bundled with batch sync requests. This ensures the peer has current state before receiving incremental updates.

```rust
struct BatchSyncRequest {
    id: SedimentreeId,
    req_id: RequestId,
    fingerprint_summary: FingerprintSummary,
    subscribe: bool,  // Opt into live updates
}
```

When `subscribe: true`:
1. Server performs normal batch sync
2. Server adds peer to subscription set for that sedimentree
3. Peer receives future commits/fragments for that sedimentree

### Unsubscribe Explicitly

Peers unsubscribe by sending a `RemoveSubscriptions` message:

```rust
struct RemoveSubscriptions {
    ids: Vec<SedimentreeId>,  // Sedimentrees to unsubscribe from
}
```

### Automatic Cleanup

When a peer disconnects (all connections closed), the server automatically removes them from all subscription sets.

### Reconnection Restoration

Subduction tracks outgoing subscriptions per peer. After a successful reconnection, the client can automatically restore subscriptions by re-sending `BatchSyncRequest { subscribe: true }` for each tracked sedimentree. See [reconnection.md](./reconnection.md) for details.

## Subscription State

The server maintains a map of sedimentree IDs to subscribed peer IDs:

```rust
subscriptions: Map<SedimentreeId, Set<PeerId>>
```

### Multiple Connections

A peer may have multiple simultaneous connections (e.g., different browser tabs). Subscriptions are tracked per-peer, not per-connection:

- Subscribe on _any_ connection adds the peer to the subscription set
- Updates are sent to _all_ connections for that peer
- Cleanup only occurs when the _last_ connection for a peer closes

## Forward Path

When a commit or fragment arrives, the server forwards it to subscribed peers who are also authorized:

```mermaid
flowchart TD
    A[Receive commit for sedimentree X] --> B[Get subscribers for X]
    B --> C[Filter by authorization]
    C --> D[Get connections for authorized peers]
    D --> E[Forward to each connection]
```

### Authorization Check

Not all subscribers may be authorized to receive updates. The server uses `filter_authorized_fetch` to batch-check authorization:

```rust
trait StoragePolicy<K: FutureKind> {
    /// Filter sedimentree IDs to only those the peer is authorized to fetch.
    fn filter_authorized_fetch(
        &self,
        peer: PeerId,
        ids: Vec<SedimentreeId>,
    ) -> K::Future<'_, Vec<SedimentreeId>>;
}
```

This allows efficient authorization checking — for example, a Keyhive-based policy can look up the document's membership once and check if the peer has `Pull` access.

### Revocation

When a peer's access is revoked:
1. The peer learns about revocation via Keyhive (separate channel)
2. The server simply stops forwarding — the peer fails the `filter_authorized_fetch` check
3. No explicit "revocation notification" is needed from Subduction

## Upstream Propagation (Relay Topologies)

The forward path above handles the case where the publisher is directly
connected to the server. In relay topologies — where peer A is connected
to relay R, and R is connected to peer B who holds the data — A's
subscribe to R is not enough on its own. R has to also be subscribed to
B so that B's future commits reach R, and from there reach A via the
forward path.

To preserve that end-to-end reachability, every node that accepts an
inbound subscribing `BatchSyncRequest` also propagates the subscription
to every _other_ currently-connected peer. Forwarding _updates_
(`LooseCommit` / `Fragment`) and forwarding _subscription requests_
stay symmetric: both flow outward from every accepting node.

```mermaid
sequenceDiagram
    participant A as Peer A
    participant R as Relay R
    participant B as Peer B

    A->>R: BatchSyncRequest { id: doc-123, subscribe: true }
    R->>A: BatchSyncResponse { diff }
    Note over R: A is now subscribed to doc-123
    Note over R: propagate upstream
    R->>B: BatchSyncRequest { id: doc-123, subscribe: true }
    B->>R: BatchSyncResponse { diff }
    Note over R: R is now subscribed to doc-123 on B

    Note over B: B makes a change
    B->>R: LooseCommit { id: doc-123, commit, blob }
    Note over R: forward to subscribers
    R->>A: LooseCommit { id: doc-123, commit, blob }
```

### Authorization Gate

Propagation only runs when the originator is _authorized_ to fetch the
sedimentree. Handler success alone is too permissive: the handler returns
`Ok` even after sending a `SyncResult::Unauthorized` response. Without
this gate, an unauthorized peer could cause the relay to enroll in
upstream subscriptions whose traffic the egress filter would drop on the
return path — wasted bandwidth and dangling upstream state.

```rust
if handler_returned_ok
    && let Some(sed_id) = message.try_as_subscribe_request()
    && policy.authorize_fetch(originator, sed_id).await.is_ok()
{
    propagate_subscription(sed_id, originator).await;
}
```

### Idempotency

Each node tracks its _outgoing_ subscriptions per peer:

```rust
outgoing_subscriptions: Map<PeerId, Set<SedimentreeId>>
```

`sync_with_peer(.., subscribe = true, ..)` records `(peer, sedimentree)`
in this map on success. The propagation step skips any peer already
recorded for the requested sedimentree, so a second subscribe from A
for the same sedimentree does not cause R to re-issue its upstream
subscribe to B. Loops between mutually subscribed servers self-quench
after one round.

### Originator Exclusion

The propagation step iterates connected peers other than the originator.
A's subscribe does not cause R to send a `BatchSyncRequest` back to A.
This matters in topologies where A and B are mutual relays for each
other.

### Best Effort

Per-peer propagation failures are logged at `debug` and ignored. A
single peer being unreachable does not prevent the subscription from
reaching the remaining peers. The local handler-side `BatchSyncResponse`
to the originator has already been sent before propagation runs.

## Message Flow

### Subscribing

```mermaid
sequenceDiagram
    participant P as Peer
    participant S as Server

    P->>S: BatchSyncRequest { id, subscribe: true, summary }
    Note over S: Compute diff
    Note over S: Add peer to subscriptions[id]
    S->>P: BatchSyncResponse { diff }
    Note over P: Store diff, now subscribed
```

### Receiving Updates

```mermaid
sequenceDiagram
    participant A as Author
    participant S as Server
    participant B as Subscriber

    A->>S: LooseCommit { id, commit, blob }
    Note over S: Store commit
    Note over S: Get subscribers for id
    Note over S: Filter by authorization
    Note over S: B is subscribed and authorized
    S->>B: LooseCommit { id, commit, blob }
    Note over B: Store commit
```

### Unsubscribing

```mermaid
sequenceDiagram
    participant P as Peer
    participant S as Server

    P->>S: RemoveSubscriptions { ids: [id1, id2] }
    Note over S: Remove peer from subscriptions[id1]
    Note over S: Remove peer from subscriptions[id2]
    Note over S: No response needed
```

### Disconnect Cleanup

```mermaid
sequenceDiagram
    participant P as Peer
    participant S as Server

    Note over P,S: Connection closes
    Note over S: Was this peer's last connection?
    Note over S: Yes — remove from all subscription sets
```

## Design Rationale

### Why Bundle Subscribe with Batch Sync?

1. **Atomic operation** — peer gets current state and subscribes in one request
2. **No stale subscriptions** — subscription only created after successful sync
3. **Simpler protocol** — no separate "subscribe" message type

### Why Not Broadcast to All Peers?

1. **Bandwidth** — broadcasting to uninterested peers wastes bandwidth
2. **Privacy** — peers should only learn about documents they're authorized for
3. **Scale** — subscription sets are typically small relative to total connections

### Why Per-Peer Not Per-Connection?

1. **Consistency** — all tabs/windows see the same updates
2. **Simplicity** — client doesn't need to re-subscribe per connection
3. **Cleanup** — subscription persists across brief disconnects (within same session)

### Why No Pending/Activation Flow?

Earlier designs considered a "want list" where peers could express interest in documents they weren't yet authorized for, with activation notifications when access was granted. This was simplified:

1. **Keyhive handles auth notifications** — peers learn about access grants via Keyhive
2. **Peer can subscribe when ready** — once authorized, peer sends `BatchSyncRequest { subscribe: true }`
3. **Less state** — no "pending subscriptions" to track

## Implementation Notes

### Adding a Subscription

```rust
async fn add_subscription(&self, peer_id: PeerId, sedimentree_id: SedimentreeId) {
    let mut subscriptions = self.subscriptions.lock().await;
    subscriptions
        .entry(sedimentree_id)
        .or_default()
        .insert(peer_id);
}
```

### Removing Peer from All Subscriptions

```rust
async fn remove_peer_from_subscriptions(&self, peer_id: PeerId) {
    let mut subscriptions = self.subscriptions.lock().await;
    subscriptions.retain(|_id, peers| {
        peers.remove(&peer_id);
        !peers.is_empty()  // Remove entry if no subscribers left
    });
}
```

### Getting Authorized Subscriber Connections

```rust
async fn get_authorized_subscriber_conns(
    &self,
    sedimentree_id: SedimentreeId,
    exclude_peer: &PeerId,
) -> Vec<C> {
    // Get subscribed peers (excluding sender)
    let subscribers: Vec<PeerId> = {
        let subs = self.subscriptions.lock().await;
        subs.get(&sedimentree_id)
            .map(|peers| peers.iter()
                .filter(|p| *p != exclude_peer)
                .copied()
                .collect())
            .unwrap_or_default()
    };

    if subscribers.is_empty() {
        return Vec::new();
    }

    // Filter by authorization
    let authorized = self.policy
        .filter_authorized_fetch(/* for each subscriber */)
        .await;

    // Get connections for authorized peers
    self.get_connections_for_peers(&authorized).await
}
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Subscribe to unknown sedimentree | Subscription created (diff will be empty) |
| Unsubscribe from non-subscribed | Silently ignored |
| Forward fails | Connection unregistered, continues with others |
| Authorization revoked | Peer stops receiving (no explicit notification) |
