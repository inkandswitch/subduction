# Hacking on Subduction

This document explains the key engineering patterns and abstractions used in Subduction. It's intended for contributors and anyone trying to understand WTF is going on in the codebase.

**Start here**, then dive into `design/` for detailed protocol specs:
- `design/handshake.md` — Connection authentication protocol
- `design/sedimentree.md` — The core data structure
- `design/sync/` — Sync algorithm details
- `design/security/` — Threat model and mitigations

## Core Abstractions

### FutureForm: Portable Async Without Runtime Lock-in

The `FutureForm` trait (from `future_form`) is the foundation for making Subduction work across both native Rust (with Tokio) and WebAssembly (single-threaded, no Send/Sync).

```
                    ┌───────────────────────────┐
                    │        FutureForm         │
                    │  (trait from future_form) │
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

**Why?** JavaScript's single-threaded model means futures can't be `Send`. But we want the same core logic to work in both environments. `FutureForm` lets us write generic code that works with either.

**Usage pattern:**

```rust
// Trait definition - generic over FutureForm
pub trait Storage<K: FutureForm> {
    fn load(&self, id: Id) -> K::Future<'_, Result<Data, Error>>;
}

// Implementation for both forms using the macro
#[future_form::future_form(Sendable, Local)]
impl<K: FutureForm> Storage<K> for MyStorage {
    fn load(&self, id: Id) -> K::Future<'_, Result<Data, Error>> {
        K::from_future(async move {
            // async implementation
        })
    }
}
```

The `#[future_form::future_form(Sendable, Local)]` macro generates two impl blocks — one for each form. `K::from_future()` wraps the async block in the appropriate future type.

### Generic Parameters on Subduction

A node has six side effects: storage, policy, signing, spawning, timing, and keeping minimized trees. Giving each its own type parameter made every node and handler type long and let the two drift apart, so `Subduction` bundles them into one effects parameter `E`. The whole sync stack is still assembled at *compile time*, with little dynamic dispatch on hot paths.

The struct (bounds simplified; see `subduction_core/src/subduction.rs` for the real ones):

```rust
pub struct Subduction<
    'a,
    Async: FutureForm,                            // Sendable or Local
    E: Effects<Async>,                            // Storage, policy, signer, timer, spawner, tree store
    Conn: Connection<Async, Hdl::Message>,        // Network connection type
    Hdl: Handler<Async, Conn>,                    // Wire protocol handler
    Metric: DepthMetric = CountLeadingZeroBytes,  // Hash → depth mapping
>
```

`E` names each effect as an associated type (`E::Storage`, `E::Policy`, …) through one capability trait per effect, two for policy (`HasStorage`, `HasStoragePolicy`, `HasConnectionPolicy`, …), in `subduction_core::effects`. The stock implementation is the phantom `effects::config::Config`, with slots in order storage, policy, signer, timer, spawner, minimized tree store (the last defaults to the in-process `MemoryMinimizedTreeStore`); `Subduction::new` and the builder infer it from the components you pass in.

The minimized tree store (`subduction_core::minimized_tree_store`) keeps each tree's built, minimized form so hot reads skip a full storage scan. It is derived data: storage stays the source of truth, and a miss rebuilds from it. Implement `MinimizedTreeStore` to keep trees elsewhere, but read its module docs first: a store shared between nodes must do compare-and-set installs. Consumers bound `E` only by what they use: `SyncHandler` never sees a signer or timer.

Thread-safety is per form. `Effects<Async>` is form-agnostic; generic code that spawns node work bounds `E: SendableEffects` (every component `Send + Sync`, every error `Send`) or `E: LocalEffects` (`'static` only; on `Local`, everything but the spawner may be `!Send`).

**Typical instantiations:**

| Deployment   | Async      | E              | Conn               | Metric                  |
|--------------|------------|----------------|--------------------|-------------------------|
| CLI server   | `Sendable` | `CliEffects`   | `CliConn`          | `CountLeadingZeroBytes` |
| Wasm browser | `Local`    | `WasmEffects`  | `NodeConnection`   | `JsDepthMetric`         |

### Policy Traits: Capability-Based Access Control

Access control is split into two traits:

```rust
pub trait ConnectionPolicy<K: FutureForm> {
    type ConnectionDisallowed: Error;
    fn authorize_connect(&self, peer: PeerId) -> K::Future<'_, Result<(), Self::ConnectionDisallowed>>;
}

pub trait StoragePolicy<K: FutureForm> {
    type FetchDisallowed: Error;
    type PutDisallowed: Error;

    fn authorize_fetch(&self, peer: PeerId, id: SedimentreeId) -> K::Future<'_, Result<(), Self::FetchDisallowed>>;
    fn authorize_put(&self, requestor: PeerId, author: PeerId, id: SedimentreeId) -> K::Future<'_, Result<(), Self::PutDisallowed>>;
    fn filter_authorized_fetch(&self, peer: PeerId, ids: Vec<SedimentreeId>) -> K::Future<'_, Vec<SedimentreeId>>;
}
```

**Why separate?** Connection-level auth (is this peer allowed to connect at all?) is different from document-level auth (can this peer read/write this specific document?). The `filter_authorized_fetch` method enables efficient batch authorization for subscription-based forwarding.

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
pub trait Spawn<K: FutureForm> {
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
Depth 0: ████                       (0+ leading zero bytes → few per fragment)
         ↓ require more leading zero bytes
Depth 1: ████████                   (1+ leading zero bytes → more per fragment)
         ↓
Depth 2: ████████████████████████   (2+ leading zero bytes → most per fragment)
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
pub trait Storage<K: FutureForm> {
    type Error: std::error::Error;
    // ...
}

pub trait Connection<K: FutureForm> {
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
