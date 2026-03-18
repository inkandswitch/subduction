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

## `no_std` Support

This crate is `#![no_std]` compatible. Enable the `std` feature for standard library support.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
