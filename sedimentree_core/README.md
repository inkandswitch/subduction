# sedimentree_core

Core data partitioning scheme for efficient metadata-based synchronization.

> [!WARNING]
> This is an early release preview with an unstable API. Do not use for production at this time.

## Overview

Sedimentree organizes data into hierarchical layers (strata) based on the number of leading zero bytes in content hashes. This enables efficient diffing and synchronization of encrypted data without requiring decryption.

```
Depth 0: ████ ███ ████████ ███ ██   (strict filter → few commits per fragment)
         ↓ require more leading zero bytes
Depth 1: ████████ ████████ ██████   (looser filter → more commits per fragment)
         ↓ require more leading zero bytes
Depth 2: ████████████████████████   (loosest filter → most commits per fragment)
```

## Key Concepts

### Blobs and Digests

- **Blob** — Binary content (encrypted or plaintext), content-addressed by BLAKE3 hash
- **Digest\<T\>** — 32-byte BLAKE3 content hash, phantom-typed for safety

### Commits and Fragments

| Type          | Description                                                                        |
|---------------|------------------------------------------------------------------------------------|
| `LooseCommit` | Individual commit metadata with parent references (not yet rolled into a fragment) |
| `Fragment`    | A range of commits at a specific depth, bundled with checkpoint metadata           |
| `Sedimentree` | Complete metadata structure for a document                                         |

### Depth Metric

The `DepthMetric` trait determines how commits are partitioned into layers:

```rust
pub trait DepthMetric {
    fn depth(&self, digest: &Digest<LooseCommit>) -> Depth;
}
```

Default implementation `CountLeadingZeroBytes` uses the number of leading zero bytes in the hash, giving ~1/256 probability per depth level.

## `no_std` Support

This crate is `#![no_std]` compatible by default. Enable the `std` feature for standard library support.

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
