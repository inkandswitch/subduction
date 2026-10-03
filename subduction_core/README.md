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

## `no_std` Support

This crate is `#![no_std]` compatible. Enable the `std` feature for standard library support.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
