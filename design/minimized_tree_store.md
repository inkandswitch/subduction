# Minimized Tree Store

How a node keeps the minimized form of each sedimentree, and how to share that store safely between nodes.

## Why a store

Serving sync needs each tree's _minimized_ form. Building it takes a full storage read plus a minimization pass, so a node keeps built trees in a `MinimizedTreeStore` and serves hot reads from there.

The store holds derived data only. Storage stays the source of truth: any entry may be dropped at any time, and the next read rebuilds it.

## Who needs a shared store

A per-node store over storage that _other_ nodes write to never sees their writes. After node B writes, node A keeps serving the tree it last read. So nodes that share storage must also share a store. They must also share the `DepthMetric`, because the store is keyed by tree id alone.

Typical deployments:

| Deployment                                   | Store                                               |
|----------------------------------------------|-----------------------------------------------------|
| One node, its own storage                    | `MemoryMinimizedTreeStore` (the default)            |
| Several nodes in one process, shared storage | `SharedMemoryMinimizedTreeStore`                    |
| Many processes or isolates, shared storage   | A networked store, such as Durable Objects or Redis |

## The race a shared store must prevent

Between a reader's storage read and its `populate`, another node can write. Populating the store with the reader's copy would then hide that write from every node.

```mermaid
sequenceDiagram
    participant A as Node A (reader)
    participant S as Shared store
    participant St as Storage
    participant B as Node B (writer)

    A->>S: get(tree)
    S-->>A: Miss(g0)
    A->>St: read tree (old)
    B->>St: write commit c
    B->>S: merge(tree, c)
    Note over S: no tree resident, but g0 → g1
    A->>S: populate(old tree, seen g0)
    S-->>A: Stale (g1 ≠ g0)
    Note over A: serves its read, but the store keeps nothing
    A->>S: next read misses, rebuilds from storage (includes c)
```

A rejected populate is not retried. The rejected read is no staler than a hit could have been a moment earlier, and sync tolerates stale views, so the reader serves it and only declines to store it. Retrying would rescan storage without bound while other nodes keep writing.

Each tree has a _generation_ that changes whenever its stored state does. A miss returns the current generation, and `populate` succeeds only if the generation still matches.

## Rules for a shared store

1. **`populate` is a compare-and-set.** Checking `seen` against the current generation and writing the tree must be one atomic step.
2. **`merge` and `remove` change the generation even on a miss.** Without a resident tree, leave a _marker_: a generation with no tree. That's what the in-flight populate collides with.
3. **A generation never repeats for a tree.** See [Reclaiming markers](#reclaiming-markers).
4. **All four operations are serialized per tree.** A read-modify-write `merge` racing a `remove` can otherwise bring back a tree that storage no longer matches.
5. **Store calls time out.** The node holds a per-tree lock across store maintenance, so a hung call stalls that tree's writes. `ResilientMinimizedTreeStore` adds this to any store: it bounds each call with the node's `Timeout` effect and opens a circuit breaker after a run of consecutive failures, so an outage costs no remote calls and degrades to reading storage directly.
6. **Fail safe, for every node.** After a `merge` or `remove` returns an error, no node may be served the tree from before that call, and no populate from before it may succeed. See [Failed maintenance on a shared store](#failed-maintenance-on-a-shared-store).
7. **Never write back an entry.** A `Hit` carries no generation, so a write back of a node's (minimized) copy can overwrite another node's merge. Minimization stays local to the reader.
8. **A hit means the tree exists.** Keep empty trees exactly as populated, and never synthesize a hit.

`populate` may or may not change the generation. Both are safe.

## Failed maintenance on a shared store

When a `merge` or `remove` fails after a write (an error, a timeout, or `ResilientMinimizedTreeStore`'s open breaker), the call may not have changed the generation, but storage already holds the write. A tree from before the call is now stale, and serving it hides the write.

The node logs the error and keeps no record of it. Only the store knows whether the failed call reached it, and only the store is shared with the other nodes that might serve the stale tree. Rule 6 must hold for every node, not just the one that saw the error. Remembering the failure in the writing node's process is not enough on its own: the other nodes keep hitting the old tree.

The failed call is often the very thing that can't reach the store, so a networked store needs a way to converge without it:

| Approach                                   | How                                                                                                                                       | Staleness                          |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|
| Retry the invalidation until it lands      | Queue the failed call's `remove` on the writing node and retry it. Until it lands, that node misses for the tree and rejects old populates | Until the retry lands, for others  |
| Expire entries after a time-to-live        | Bound how long any entry lives, so a missed invalidation heals on its own                                                                 | Up to the TTL                      |
| Validate entries against storage           | Keep a per-tree version with storage, bumped by every write. Store it with each entry and compare it on a hit                             | None, at one small read per hit    |

`ResilientMinimizedTreeStore` implements the first approach for the process it runs in: it marks the tree, retries the `remove` on the next lookup, turns later merges into removes, and reports old populates as stale until a `remove` lands. On its own that leaves other nodes stale until the retry lands, and forever if the writing node dies first, so a networked store wrapped in it still needs one of the others. `SharedMemoryMinimizedTreeStore` cannot fail, so it needs none of these.

## Known gap: a write lost mid-maintenance

If the task running a write is dropped after storage accepts the write but before the store is updated, the store keeps the tree from before the write. The data is safe in storage, and the stale tree only ever lacks items, never adds them. Dropping an unsettled write also closes that node's storage coordination, so for a per-process store the stale tree goes away with the node.

A shared store outlives the node. Sync usually heals it: a peer that holds the missing commit sees it absent from the stale tree's diff, sends it, and the newness check merges it in. If the dropped write's node was the commit's only holder, nodes sharing the store hide that commit until the entry is evicted. A background healer that re-removes such trees would close this; it is not implemented yet.

## Reclaiming markers

Markers accumulate: one per tree ever written. Deleting one naively reintroduces the race through ABA. With the slot gone, the tree reads as never written again, and a reader holding the never-written generation from before the marker existed would match.

```mermaid
sequenceDiagram
    participant A as Reader
    participant S as Store

    A->>S: get → Miss(absent)
    Note over S: write: marker created at g1
    Note over S: marker deleted → reads as absent again
    A->>S: populate(seen absent)
    Note over S: ✗ matches, hides the write
```

Two safe schemes:

| Scheme                       | How                                                                                                                                                                                                                                                           | Suits                                                 |
|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------|
| Store-wide absent generation | Report one store-wide value for trees with no slot. Each sweep that deletes slots replaces it with a fresh value, so every pre-sweep reader goes `Stale`. One bump per sweep, not per slot.                                                                   | Stores with cheap global state: in-process, Redis     |
| Time-limited markers         | Issue a missing tree's generation as the time of the miss. `populate` rejects any generation older than the marker lifetime, so it can never match a marker that has since expired. Expire markers after that lifetime. Use the store's clock for both checks. | Per-tree stores without global state: Durable Objects |

`SharedMemoryMinimizedTreeStore` uses the first scheme. It bounds its slots with `with_capacity`, and sweeps least-recently-used slots down to a low-water mark. A sweep only deletes slots it can lock without waiting, and marks them retired, so an operation already queued on a deleted slot starts over instead of writing into it.

## Platforms

### Redis

Keep the in-process layout: one hash per tree holding the tree and its generation, plus one store-wide absent generation. Make each operation one Lua script, which makes the comparison and write atomic. An eviction sweep deletes a batch of keys and bumps the absent generation in the same script. `WATCH`/`MULTI` also works, at the cost of retries under contention.

### Cloudflare Durable Objects

One object per tree serializes every request to it, which gives rules 1 and 4 for free. Keep the tree and its generation in the object's storage, and implement each operation as one request.

There's no cheap store-wide absent generation across objects, so reclaim with time-limited markers. Delete expired markers from an alarm. Use the object's own clock, never the node's.

### Cloudflare KV

Unsafe as a writable shared store. It has no conditional writes, so rules 1 and 4 can't be met, and it is eventually consistent, so a node may not even read its own write.

## Layering

A fast per-node store in front of a shared one composes as a single store, using the shared store's generation. Its local copies go stale when other nodes write, so it must refresh them by one of:

| Strategy                                   | Hit cost             | Staleness      |
|--------------------------------------------|----------------------|----------------|
| Check the shared generation on each hit    | One small round trip | None           |
| Invalidate on a push from the shared store | Local memory         | The push delay |
| Expire entries after a time-to-live        | Local memory         | Up to the TTL  |

The first still saves a lot: it moves a few bytes rather than the whole tree.
