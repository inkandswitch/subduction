# Notes for the sedimentree fixups ADR

Discussion notes to support writing `sedimentree-fixups.typ`, rather than replacement ADR prose. The working decisions below reflect the discussion; unresolved mechanics are listed separately.

## Central architectural tradeoff

Replace permanent containment metadata (checkpoints) with bounded, reusable containment assertions supplied by peers that can inspect the commit graph.

Separate this architectural decision from the particular inactivity-based retention policy. The assertions provide the mechanism; the cache policy determines how effectively they propagate.

## Working decisions

### Convergence does not require minimality

Relays must be able to finish synchronizing a shared representation even when that representation contains redundancy. Minimization can wait for a cleartext peer.

For example:

1. Relay A holds fragment F and loose commit c, where c is contained in F.
2. A receives a fixup asserting `c ⊆ F` and removes c from its sync representation.
3. Eventually all copies of the fixup are evicted.
4. A long-offline relay B reconnects and advertises c.
5. Neither relay can establish containment, so A accepts c back into its tree.
6. Both relays can settle on `{F, c}` and finish syncing.
7. A cleartext peer can later rediscover the redundancy and issue a fresh fixup.

Forgetting fixups may cost redundant storage and transfer. It must not prevent sync completion. Minimality is best-effort and may depend on a cleartext peer becoming available again.

The sync protocol must therefore tolerate redundant entries and their apparent extra heads. Differences in fixup-cache contents or inactivity counters must not, by themselves, keep document synchronization running.

### Fixups assert containment

Model a fixup as “X is contained in F,” rather than “delete X.” This makes the replacement representation explicit.

Fixups are trusted assertions. Relays do not need cleartext access or containment proofs to accept them. Derived assertions inherit that trust. A false assertion could suppress data from synchronization, even though it does not itself delete stored payloads.

The precise authorization mechanism is not specified by these notes.

### Containment composes

Relays can derive containment transitively:

```text
c ⊆ F, F ⊆ G  ⇒  c ⊆ G
E ⊆ F, F ⊆ G  ⇒  E ⊆ G
```

Composition only requires the assertions; applying the result requires the replacement payload to be available.

A possible bounded implementation is to retarget cached assertions when their containing fragment is superseded, rather than retain every intermediate assertion or materialize an unbounded transitive closure. For example, replace `c ⊆ F` with `c ⊆ G` when F is replaced by G. This sacrifices usefulness to peers holding only F, which is compatible with best-effort minimization. The exact replacement rules remain to be specified.

Composition alone does not count as use for inactivity purposes.

### Applying a fixup changes the sync representation, not stored payloads

A relay may stop advertising a redundant item only once the containing fragment's payload is locally available. Receiving fragment metadata alone is insufficient: the payload might still be downloading or might fail to arrive.

For a composed assertion such as `c ⊆ G`, it is G's payload that must be available before removing c from the advertised tree.

Relays do not automatically delete stored payloads when applying fixups. Physical deletion is an operator decision and is outside the scope of this mechanism.

### The fixup list is a bounded propagation cache

Its purpose is to carry useful containment knowledge through relay networks, not to preserve all historical assertions or guarantee a globally minimal tree.

Inactivity is a heuristic measured in data-changing merges, not a reliable measure of elapsed time or recent use across the entire network.

The working retention rules are:

- Age entries on merges that introduce a fragment or loose commit not previously present at the receiving peer.
- Measure that introduction before applying fixups. A stale peer reintroducing c counts even if a fixup immediately removes c and the final tree is unchanged.
- Changes only to fixup metadata do not constitute a data-changing merge.
- For duplicate fixups, take the smaller inactivity value.
- Reset inactivity to zero when a fixup actually eliminates redundancy.
- Merely receiving or composing an assertion does not itself count as use. Taking the minimum for duplicate entries can nevertheless refresh an entry's score.
- Order by inactivity, then by a deterministic hash tie-breaker, and discard the tail to enforce the bound.

Idle documents do not need their fixups to expire. Aging matters when activity creates competition for the bounded cache.

### Dormant peers may refresh old assertions

An honest peer can use a fixup, retain inactivity zero, and then go offline. When it returns, taking the minimum may refresh another peer's much older score.

This is an accepted limitation of the heuristic, not necessarily Byzantine behavior. Giving a returning peer's assertions another opportunity to propagate may even be useful when that peer also brings back redundant data.

Do not claim that inactivity measures global recency or guarantees that unused assertions expire within a particular time.

## Mechanics still to specify

- **Identity and encoding:** What exactly identifies each side of a containment assertion? What is hashed for deduplication and tie-breaking?
- **Composition and retention:** How does a derived assertion inherit inactivity? Which input assertions remain, and when can they be replaced?
- **Processing order:** Define union, composition, aging, application, and eviction precisely. Consider applying useful assertions before eviction and ensure application order cannot suppress data without an available replacement.
- **Counting merges:** Clarify how the receiving-peer definition behaves during bidirectional exchanges, batched transfers, and retries.
- **No-op exchanges:** Specify how incoming cache entries are combined when no document data is introduced, without making cache equality a sync-completion requirement.
- **Bounds:** Is the limit on entries or bytes? An entry-count limit only bounds space if individual assertions have bounded size.
- **Wire and compatibility changes:** Removing checkpoints and adding fixups changes metadata and synchronization behavior; the ADR should identify the compatibility implications.

Deterministic sorting alone does not establish associativity or message-order independence for the cache merge. Decide which properties are actually needed rather than assuming the cache has the same convergence requirements as document data.

## Suggested ADR structure

1. **Problem and scope**
   - Checkpoint metadata grows with history.
   - Relays cannot always identify redundant loose commits or fragments.
   - Distinguish sync-metadata costs from payload/history storage costs.
2. **Decision**
   - Replace checkpoints with trusted containment assertions.
   - Permit composition and bounded propagation.
3. **Safety and convergence guarantees**
   - Available replacement payload before suppressing an item.
   - No automatic payload deletion.
   - Sync completion without minimality; best-effort cleanup.
4. **Propagation and retention**
   - Data-changing merges as the inactivity clock.
   - Reset on actual use, bounded eviction, and accepted heuristic limitations.
5. **Alternatives and consequences**
   - Retain checkpoints: keeps persistent containment metadata and its growth cost, without resolving every dangling-loose-commit case.
   - Use fixups without propagation: simplifies state, but relay-only exchanges cannot pass on discovered containment knowledge.
   - Retain every fixup: preserves knowledge but gives up bounded fixup storage and can impose substantial propagation costs.
6. **Unresolved details and validation**
   - Keep policy choices distinct from guarantees.

## Useful worked examples and tests

Before committing to exact merge wording, trace:

- Two relays converging on `{F, c}` after the relevant fixup has been forgotten.
- A cleartext peer later removing that redundancy via a fresh fixup.
- Composition from `c ⊆ F` and `F ⊆ G`, followed by an offline peer reintroducing c.
- A fixup arriving before its containing fragment's payload; the redundant entry must remain advertised until the payload is available.
- A data-changing merge whose final tree is unchanged because a fixup immediately removes the introduced redundancy.
- A dormant peer refreshing an old assertion's inactivity.
- A full cache receiving useful new fixups while stale low-inactivity entries are circulating.
- Reordered or repeated messages, including exchanges that differ only in fixup-cache contents.

These examples should establish the required safety and sync-completion behavior, while evaluating retention quality as a heuristic rather than a correctness guarantee.
