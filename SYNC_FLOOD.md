# Sync flood via subscription propagation

## Summary

A user connected to our public sync server reports receiving floods of
messages at apparently arbitrary times. The cause is the "upstream
propagation" behaviour of the subscription system: every node that accepts an
inbound subscribing `BatchSyncRequest` re-issues that subscribe to every other
peer it is currently connected to. This was designed to keep relay topologies
reachable, but on a hub topology — a public server with many unrelated
clients — "every other connected peer" is the entire user population. The
result is that each connected client receives a burst of `BatchSyncRequest`s
for documents it has never heard of whenever any *other* client connects,
reconnects, or opens documents. The timing looks arbitrary to the recipient
because it is driven entirely by other users' activity.

This is not a bug in the narrow sense: the code does exactly what
`design/sync/subscriptions.md` says it should. The problem is that the design
does not scale down from "mesh of mutually-relaying servers" to "one public
hub with N clients", and two secondary mechanisms (claim rollback on
`NotFound`, and claim invalidation on reconnect) defeat the idempotency that
was supposed to bound the traffic.

## The propagation mechanism

The chain from an inbound subscribe to the fan-out runs as follows.

When the listen loop dequeues an inbound message, it checks whether the
message is a subscribing batch sync request. `try_as_subscribe_request`
(`subduction_core/src/connection/message.rs:259`) matches exactly
`BatchSyncRequest { subscribe: true }` and yields the sedimentree ID. The
listen loop packages this up as a `propagate` hint alongside the dispatch
(`subduction_core/src/subduction.rs:3301`).

After the handler for the message completes successfully, the dispatch task
runs the propagation step (`subduction_core/src/subduction.rs:3957-3972`). The
only gate is an `authorize_fetch(originator, sed_id)` check against the
storage policy. If that passes, it calls `propagate_subscription`.

`propagate_subscription` (`subduction_core/src/subduction.rs:2063`) takes the
list of *all* currently connected peers except the originator and, for each
one not already claimed in the `outgoing_subscriptions` map, calls
`sync_with_peer(peer, id, subscribe = true, ..)`. That sends each of those
peers a fresh `BatchSyncRequest { subscribe: true }` for the document,
carrying our fingerprint summary for it. There is no filtering on whether the
target peer has the document, has ever expressed interest in it, or has any
relationship to it whatsoever.

On the public sync server the authorization gate is wide open: the CLI server
in open mode constructs its policy via `CliKeyhivePolicyHandle::open()` and
`CliHandlerOpenPolicy` (`subduction_cli/src/server.rs:369-377`), under which
`authorize_fetch` always succeeds. So on that deployment, every subscribe from
every client propagates to every other client.

## Why the idempotency claims don't bound the traffic

The design anticipates repeated subscribes and loops between mutually
connected servers, and defends against them with a claim map: before
propagating, each `(peer, sedimentree)` pair is inserted into
`outgoing_subscriptions`, and pairs already present are skipped. If this claim
were durable, each client would receive at most one propagated subscribe per
document per connection. It is not durable, for two reasons.

First, the claim is only kept when the propagated subscribe actually
*establishes* a subscription. A peer that does not have the document responds
`SyncResult::NotFound`, which comes back as "not established", and the claim
is rolled back so that "a later subscribe retries"
(`subduction_core/src/subduction.rs:2112-2122`, and the doc comment at
2047-2050). This inverts the intended behaviour for exactly the peers that
matter: a client that *has* the document ends up subscribed once and is never
asked again, while a client that has *no relationship to the document at all*
gets re-asked on every single subsequent subscribe for that document from
anyone, indefinitely.

Second, claims live only for the target peer's connection era. Whenever a peer
disconnects and reconnects, its claims are cleared
(`clear_stale_outgoing_claims`), so even established subscriptions are
re-propagated after any network blip.

## The amplification pattern

Clients bulk-subscribe. Per `design/sync/reconnection.md`, a client re-sends
`BatchSyncRequest { subscribe: true }` for every document it tracks each time
it reconnects. A client tracking D documents therefore causes the server to
emit D subscribe requests to each of the other N−1 connected peers: D×(N−1)
messages per reconnect of one client. With 50 connected clients and one
client tracking 1,000 documents, a single reconnect of that client produces
49,000 unsolicited `BatchSyncRequest`s fanned out to everyone else — and it
repeats on the next reconnect, because the `NotFound` responses rolled back
all the claims.

These are not necessarily small messages, either: each propagated request
carries the server's fingerprint summary for the document, which is
non-trivial for documents where the server holds real history.

The receiving side compounds the problem in two ways. The sync handler
registers the requestor as a subscriber *unconditionally*, before checking
whether the document exists or whether the requestor is authorized
(`add_subscription` at `subduction_core/src/handler/sync.rs:502`). So every
client accumulates subscription state naming the server as a subscriber for
every document anyone on the server touches — unbounded growth in the
`subscriptions` map on machines that never wanted those documents. And because
the handler returns `Ok` even when it responds `NotFound`
(`handler/sync.rs:501-508`), the recipient runs its *own* propagation step. A
client whose only connection is the server has nowhere to propagate to (the
originator is excluded), but any node connected to more than one peer relays
the subscribe onward. In a topology with several interconnected servers, one
subscribe cascades through the entire connected graph.

## Relation to the design documents

`design/sync/subscriptions.md`, section "Upstream Propagation (Relay
Topologies)", states the behaviour explicitly:

> To preserve that end-to-end reachability, every node that accepts an
> inbound subscribing `BatchSyncRequest` also propagates the subscription to
> every _other_ currently-connected peer.

The section's "Idempotency" subsection claims that "loops between mutually
subscribed servers self-quench after one round", but as described above this
only holds between nodes that actually hold the document; the `NotFound`
rollback re-arms the propagation everywhere else. The design was written with
a small mesh of relays in mind, where each node's peer set is a handful of
other relays. It was not evaluated against a hub with a large, mutually
unrelated client population, where the same rule turns every subscribe into a
broadcast.

## Possible directions for a fix

The report this document responds to only asked whether the flood is
plausible, so no fix is implemented here; these are the directions that seem
worth weighing.

The core issue is that "every connected peer" is the wrong propagation set for
a hub. The propagation exists so that a relay R, sitting between a subscriber
A and a data-holder B, subscribes to B on A's behalf. That purpose only
requires propagating towards peers that might plausibly *hold or later
receive* the document, whereas the current rule also propagates towards peers
that are merely other customers of the hub. Options, roughly in increasing
order of invasiveness:

1. **Scope propagation to designated upstream peers.** Give nodes a notion of
   which links are relay links (configuration, or a flag negotiated at
   handshake). A public server with no configured upstreams would propagate
   nothing; a mesh of servers would list each other. This preserves the relay
   topology story exactly, and is probably the smallest change that fixes the
   hub case outright.

2. **Gate propagation on prior interest.** Only propagate a subscribe for
   document X to peers that have previously subscribed to X, synced X, or
   announced holding X. This keeps zero-config relaying working in the cases
   where it can actually deliver data, at the cost of tracking per-peer
   interest and a bootstrapping question for genuinely new documents.

3. **Make `NotFound` a durable (negative) claim.** Even without changing the
   propagation set, keeping the claim after a `NotFound` — until that peer
   next announces the document or reconnects — would collapse the repeated
   re-asking of uninterested peers from once-per-subscribe to
   once-per-connection. This is a small change but only reduces the flood
   (to D×(N−1) once per connection era) rather than eliminating it.

Independently of the propagation set, the unconditional `add_subscription` on
the receiving side deserves a look: registering subscribers for documents a
node does not hold and may never hold is unbounded state growth driven
entirely by remote input.

## Reproducer

`subduction_core/tests/hub_subscribe_flood.rs` reproduces both halves of the
problem on the wire, using the same mock-connection harness as
`relay_topology_sync.rs`. A hub with an open policy has one subscribing
client and some bystander clients attached; the bystanders answer every
request `NotFound` and count the subscribing `BatchSyncRequest`s the hub
pushes at them. The tests assert the behaviour a client of a public hub
should be able to expect, so they fail today:

- `hub_does_not_fan_out_subscribes_to_unrelated_clients`: one client opening
  20 documents against a hub with 5 bystanders produces 100 unsolicited
  subscribe requests (20 per bystander; expected 0).
- `hub_does_not_reflood_bystander_on_every_resubscribe`: one client
  resubscribing to a single document 10 times produces 10 requests at the
  bystander (expected at most 1), and no claim survives toward it after each
  `NotFound`.

Run with `cargo test -p subduction_core --test hub_subscribe_flood`.
