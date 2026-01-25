# Hacking on Subduction

This document explains the key engineering patterns and abstractions used in Subduction. It's intended for contributors and anyone trying to understand WTF is going on in the codebase.

**Start here**, then dive into `design/` for detailed protocol specs:
- `design/handshake.md` — Connection authentication protocol
- `design/sedimentree.md` — The core data structure
- `design/sync/` — Sync algorithm details
- `design/security/` — Threat model and mitigations

## Core Abstractions

### FutureKind: Portable Async Without Runtime Lock-in

The `FutureKind` trait (from `futures_kind`) is the foundation for making Subduction work across both native Rust (with Tokio) and WebAssembly (single-threaded, no Send/Sync).

```
                    ┌───────────────────────────┐
                    │        FutureKind         │
                    │  (trait from futures_kind)│
                    └────────────┬───────────────┘
                                 │
              ┌──────────────────┴────────────────┐
              ▼                                   ▼
     ┌────────────────┐                  ┌─────────────────┐
     │    Sendable    │                  │     Local       │
     │  (Send + Sync) │                  │  (single-thread)│
     │                │                  │                 │
     │  BoxFuture<T>  │                  │ LocalBoxFuture  │
     └────────────────┘                  └─────────────────┘
              │                                   │
              ▼                                   ▼
        Tokio runtime                      Wasm runtime
        Multi-threaded                     JS event loop
```

**Why?** JavaScript's single-threaded model means futures can't be `Send`. But we want the same core logic to work in both environments. `FutureKind` lets us write generic code that works with either.

**Usage pattern:**

```rust
// Trait definition - generic over FutureKind
pub trait Storage<K: FutureKind> {
    fn load(&self, id: Id) -> K::Future<'_, Result<Data, Error>>;
}

// Implementation for both kinds using the macro
#[futures_kind::kinds(Sendable, Local)]
impl<K: FutureKind> Storage<K> for MyStorage {
    fn load(&self, id: Id) -> K::Future<'_, Result<Data, Error>> {
        K::into_kind(async move {
            // async implementation
        })
    }
}
```

The `#[futures_kind::kinds(Sendable, Local)]` macro generates two impl blocks — one for each kind. `K::into_kind()` wraps the async block in the appropriate future type.

### Generic Parameters on Subduction

The main `Subduction` struct has many generic parameters:

```rust
pub struct Subduction<
    'a,
    F: FutureKind,           // Sendable or Local
    S: Storage<F>,           // Storage backend
    C: Connection<F>,        // Network connection type
    P: ConnectionPolicy<F> + StoragePolicy<F>,  // Access control
    M: DepthMetric,          // Hash → depth mapping
    const N: usize,          // ShardedMap shard count
>
```

This looks intimidating but serves a purpose: *compile-time configuration*. The entire sync stack is assembled at compile time with little dynamic dispatch for hot paths.

Note: `NonceCache` is a concrete field, not a generic parameter (YAGNI — only one implementation needed).

**Typical instantiations:**

| Context      | F          | S           | C                  | M                       |
|--------------|------------|-------------|--------------------| ------------------------|
| CLI server   | `Sendable` | `FsStorage` | `UnifiedWebSocket` | `CountLeadingZeroBytes` |
| Wasm browser | `Local`    | `JsStorage` | `JsConnection`     | `WasmHashMetric`        |

### Policy Traits: Capability-Based Access Control

Access control is split into two traits:

```rust
pub trait ConnectionPolicy<K: FutureKind> {
    type ConnectionDisallowed: Error;
    fn authorize_connect(&self, peer: PeerId) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>>;
}

pub trait StoragePolicy<K: FutureKind> {
    type FetchDisallowed: Error;
    type PutDisallowed: Error;

    fn generation(&self, id: SedimentreeId) -> K::Future<'_, u64>;
    fn authorize_fetch(&self, peer: PeerId, id: SedimentreeId) -> K::Future<'_, Result<(), Self::FetchDisallowed>>;
    fn authorize_put(&self, requestor: PeerId, author: PeerId, id: SedimentreeId) -> K::Future<'_, Result<(), Self::PutDisallowed>>;
}
```

**Why separate?** Connection-level auth (is this peer allowed to connect at all?) is different from document-level auth (can this peer read/write this specific document?). The `generation()` method supports capability revocation — if the generation changes, cached capabilities are invalidated.

**OpenPolicy** is the permissive default (allows everything). **KeyhivePolicy** integrates with the Keyhive access control system for real authorization.

### NonceCache: Replay Protection

The handshake protocol uses signed challenges with nonces. `NonceCache` prevents replay attacks:

```rust
pub struct NonceCache { /* ... */ }

impl NonceCache {
    pub async fn try_claim(&self, peer: PeerId, nonce: Nonce, timestamp: TimestampSeconds)
        -> Result<(), NonceReused>;
}
```

Uses time-based buckets for efficient expiry with lazy cleanup:

```
┌──────────┬──────────┬──────────┬──────────┐
│ Bucket 0 │ Bucket 1 │ Bucket 2 │ Bucket 3 │
│  0-3 min │  3-6 min │  6-9 min │ 9-12 min │
└──────────┴──────────┴──────────┴──────────┘
     ↑
  rotates as time advances
```

- 4 buckets × 3 min = 12 min window (covers 10 min `MAX_PLAUSIBLE_DRIFT`)
- Lazy GC: buckets cleared during `try_claim()` via `advance_horizon()`
- No background task needed
- Concrete type, not a trait (only one implementation needed)

### Spawn Trait: Task-per-Connection Parallelism

```rust
pub trait Spawn<K: FutureKind> {
    fn spawn<F>(&self, future: F) -> AbortHandle
    where
        F: Future<Output = ()> + 'static;
}
```

Each connection gets its own task, enabling:
- True parallelism for signature verification on multi-core
- Panic isolation (one bad connection doesn't crash the server)
- Clean abstraction over `tokio::spawn` vs `wasm_bindgen_futures::spawn_local`

## Sedimentree: The Data Structure

Sedimentree organizes CRDT data into depth-stratified layers based on content hash:

```
Depth 0: ████████████████████████  (all commits)
         ↓ filter by leading zero bytes
Depth 1: ████████                   (1+ leading zero bytes)
         ↓
Depth 2: ████                       (2+ leading zero bytes)
         ↓
Depth 3: ██                         (3+ leading zero bytes)
```

This enables efficient sync: compare summaries at higher depths first, then drill down only where differences exist. Like a B-tree for content-addressed data.

## Connection Lifecycle

```
Client                                          Server
  │                                               │
  │  1. TCP/WebSocket connect                     │
  │  ─────────────────────────────────────────►   │
  │                                               │
  │  2. Signed<Challenge>                         │
  │  ─────────────────────────────────────────►   │
  │     { audience, timestamp, nonce }            │
  │     Client identity from signature            │
  │                                               │
  │                      3. Signed<Response>      │
  │  ◄─────────────────────────────────────────   │
  │     { challenge_digest, server_timestamp }    │
  │     Server identity from signature            │
  │                                               │
  │  4. ConnectionPolicy::authorize_connect()     │
  │     checked on both sides                     │
  │                                               │
  ▼                                               ▼
Authenticated                              Authenticated
```

## Error Handling Pattern

Errors use associated types on traits, not concrete types:

```rust
pub trait Storage<K: FutureKind> {
    type Error: std::error::Error;
    // ...
}

pub trait Connection<K: FutureKind> {
    type SendError: std::error::Error;
    type RecvError: std::error::Error;
    type CallError: std::error::Error;
    // ...
}
```

This lets each implementation define its own error types while keeping the core generic. The `thiserror` crate is used for deriving `Error` implementations.

## Module Organization

```
subduction_core/
├── connection/
│   ├── handshake.rs      # Challenge/Response protocol
│   ├── manager.rs        # Spawn trait, connection lifecycle
│   ├── message.rs        # Wire protocol messages
│   └── nonce_cache.rs    # Replay protection
├── crypto/
│   ├── nonce.rs          # Cryptographic nonces
│   ├── signed.rs         # Signed<T> wrapper
│   └── signer.rs         # Signer trait
├── policy/
│   ├── capability.rs     # Fetcher/Putter fat capabilities
│   ├── connection.rs     # ConnectionPolicy trait
│   └── storage.rs        # StoragePolicy trait
├── subduction.rs         # Main sync logic
└── timestamp.rs          # TimestampSeconds newtype
```

The pattern is `foo.rs` + `foo/` for modules with submodules (Rust 2018+ style), not `foo/mod.rs`.

## Testing Strategy

| Level | Tool | Location |
|-------|------|----------|
| Unit tests | `#[test]` | Inline in modules |
| Property tests | `bolero` | Dev dependencies |
| Integration | Round-trip tests | `subduction_websocket/tests/` |
| E2E | Playwright | `subduction_wasm/e2e/` |

## Common Patterns

### Newtypes for Domain Concepts

```rust
pub struct PeerId([u8; 32]);
pub struct SedimentreeId([u8; 32]);
pub struct Nonce(u128);
pub struct TimestampSeconds(u64);
```

These prevent mixing up different 32-byte arrays or timestamps with other integers.

### Arc for Shared Ownership

`Subduction` stores `Arc<S>` for storage. This enables:
- Sharing across connection tasks
- Fat capabilities (`Fetcher`/`Putter`) that bundle storage access with authorization proof

`NonceCache` is also wrapped in `Arc` internally for sharing across handshakes.

### Compile-Time Validation

Prefer types that make invalid states unrepresentable:
- `Signed<T>` can only be created by signing
- `Verified<T>` can only be created by verification
- Capabilities encode what operations are permitted

## Getting Started

1. Read `subduction_core/src/subduction.rs` for the main sync logic
2. Check `subduction_websocket/src/tokio/server.rs` for a complete instantiation
3. Run tests: `cargo test --workspace`
4. Run the CLI: `cargo run -p subduction_cli -- server`

## Questions?

Check `.ignore/CONTEXT.md` for session-specific notes, or the design docs in `design/`.
