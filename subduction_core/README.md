# subduction_core

Core synchronization protocol for applications built on Sedimentree.

> [!WARNING]
> This is an early release preview with an unstable API. Do not use for production at this time.

## Overview

Subduction is a peer-to-peer sync protocol that enables efficient synchronization of encrypted, partitioned data. This crate provides the protocol implementation, connection management, and policy enforcement.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Subduction                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │   Handler    │  │  Connection  │  │  StoragePowerbox     │   │
│  │  (dispatch)  │  │   Manager    │  │  (Fetcher/Putter)    │   │
│  └──────────────┘  └──────────────┘  └──────────────────────┘   │
│           │                │                    │               │
│           └────────────────┴────────────────────┘               │
│                            │                                    │
│  ┌─────────────────────────┴─────────────────────────────────┐  │
│  │              ShardedMap<SedimentreeId, Sedimentree>       │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Storage access

Peer reads use an authorized `Fetcher`; core local reads use the same capability
minted internally for a specific tree. Hydration reads metadata only, while blob
access reads complete verified items. Global ID enumeration remains separate.

`LocalStorageAccess` no longer duplicates the four bulk tree-reading methods.
Trusted integrations that used those methods can call their `Storage` backend
through `LocalStorageAccess::storage()` instead. Remote reads must still go
through policy-authorized capabilities.

## Mutation ownership

A node installs its mutation runner in the powerbox before admitting work. All
put/delete capabilities share it, including capabilities minted before node
construction. The existing spawner owns persistence **and cache completion**;
cancelling a waiter does not abandon a later success or partial-failure repair.
Network propagation remains outside that unit. Shutdown rejects new mutations;
forced driver loss ends storage-observation validity, not foreign backend effects.

Standalone powerboxes execute inline, without cancellation ownership or a node
cache. Do not promote a powerbox while standalone writes are still running, and
use a fresh powerbox per node (cloned backend handles may still share persistence).
Custom handlers must use the node's shared powerbox or explicitly own their
independent writes; the node cannot observe writes through another backend handle.

`Storage` remains persistence-only, with no new supertrait or lifetime bounds.
Owned node tasks require a `'static` backend/error; Sendable tasks additionally
require a Send/Sync backend and Send errors. `StoragePowerbox` now carries the
future form as a third parameter (default: Sendable), inferred by the builder;
capability minting no longer takes a future-form turbofish. Put/delete methods
return `MutationError` to distinguish backend failures from interrupted drivers.
No protocol messages or sync success semantics change.

## `no_std` Support

This crate is `#![no_std]` compatible. Enable the `std` feature for standard library support.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
