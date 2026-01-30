# sedimentree_fs

Filesystem-based storage backend for Sedimentree.

## Overview

This crate provides `FsStorage`, a content-addressed filesystem storage implementation
that implements the `Storage` trait from `subduction_core`.

## Storage Layout

```text
root/
├── trees/
│   └── {sedimentree_id_hex}/
│       ├── commits/
│       │   └── {digest_hex}.cbor  ← Signed<LooseCommit>
│       └── fragments/
│           └── {digest_hex}.cbor  ← Signed<Fragment>
└── blobs/
    └── {digest_hex}               ← raw bytes
```

## Usage

```rust
use sedimentree_fs::FsStorage;
use std::path::PathBuf;

let storage = FsStorage::new(PathBuf::from("./data"))?;
```

## Features

- Content-addressed storage (files named by BLAKE3 digest)
- Atomic writes (write to `.tmp`, then rename)
- CBOR encoding for commits/fragments via minicbor
- Blobs stored as raw bytes
- In-memory cache of `SedimentreeId`s for fast lookup
- Implements both `Storage<Sendable>` and `Storage<Local>` traits

## License

MIT OR Apache-2.0
