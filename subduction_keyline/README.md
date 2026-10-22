# subduction_keyline

Authorization for Subduction, backed by [Keyline].

Keyline is Keyhive's convergent authority graph: a set of signed
delegations and revocations. Its answers depend only on which certificates
a replica holds, not on the order they arrived in.

This crate provides `KeylinePolicy`, which implements Subduction's
`ConnectionPolicy` and `StoragePolicy` over any `Keyline` store:

| Check   | Rule                                                      |
|---------|-----------------------------------------------------------|
| Connect | The peer ID is a valid identifier                         |
| Fetch   | The peer has at least `Relay` over the document           |
| Put     | The _requestor_ has at least `Relay` over the document    |

Put checks the connected peer, not the commit's author. Deciding whether
an author's write counts (for example after a revocation) is the
materializer's job. This keeps the transport convergent: whether a commit
is accepted never depends on how its author's standing changed over time.

The crate does not depend on `keyhive_core`. A relay can run the policy
with only a Keyline store.

> [!WARNING]
> Keyline is not yet published. This crate tracks the upstream
> `split/11-keyline-in-core` branch and is not published to crates.io.

[Keyline]: https://github.com/inkandswitch/keyhive/tree/split/11-keyline-in-core/keyline
