# subduction_ephemeral

Ephemeral (non-persisted) messaging for Subduction.

> [!WARNING]
> This is an early release preview with an unstable API. Do not use for production at this time.

## Overview

This crate provides authenticated, fire-and-forget messaging scoped to `SedimentreeId` topics. Primary use cases include:

- Presence indicators
- Cursor positions
- Text selections
- Typing indicators
- Other transient application-level signals

Ephemeral messages are _never stored_ in Subduction; they are delivered to currently connected subscribers and then discarded.

## Architecture

```
┌────────────────────────────────────┐
│       subduction_ephemeral         │
│  EphemeralMessage, EphemeralHandler│
│  EphemeralPolicy, ComposedHandler  │
├────────────────────────────────────┤
│         subduction_core            │
│  Connection<K, M>, Handler<K, C>   │
│  SyncMessage, SyncHandler          │
└────────────────────────────────────┘
```

The core crate knows nothing about ephemeral behavior. This crate defines:

| Type               | Description                                                      |
|--------------------|------------------------------------------------------------------|
| `EphemeralMessage` | Wire message type with schema `SUE\x00`                          |
| `EphemeralPolicy`  | Authorization trait for subscribe/publish                        |
| `EphemeralHandler` | Handler implementing the ephemeral protocol                      |
| `ComposedHandler`  | Combines sync and ephemeral handlers for multiplexed connections |

Wire multiplexing (combining sync and ephemeral messages into a single envelope) is the application's responsibility. See `subduction_wasm` for an example `WireMessage` enum.

## Wire Protocol

Every ephemeral message begins with a 4-byte schema header (`SUE\x00`), followed by size and tag:

```
╔════════╦═══════════╦═════╦═════════════════════════════╗
║ Schema ║ TotalSize ║ Tag ║         Payload             ║
║   4B   ║    4B     ║ 1B  ║         variable            ║
╚════════╩═══════════╩═════╩═════════════════════════════╝
```

### Message Types

| Tag    | Message             | Description                            |
|--------|---------------------|----------------------------------------|
| `0x00` | `Ephemeral`         | Fire-and-forget payload for a topic    |
| `0x01` | `Subscribe`         | Subscribe to topics                    |
| `0x02` | `Unsubscribe`       | Unsubscribe from topics                |
| `0x03` | `SubscribeRejected` | Notification of rejected subscriptions |

## Usage

### Basic Setup

```rust
use subduction_ephemeral::{
    config::EphemeralConfig,
    handler::EphemeralHandler,
    policy::OpenEphemeralPolicy,
};

// Create the ephemeral handler (shares connections with Subduction)
let (ephemeral_handler, ephemeral_rx) = EphemeralHandler::new(
    connections.clone(),  // Arc<Mutex<Map<PeerId, NonEmpty<Authenticated<C, F>>>>>
    OpenEphemeralPolicy,
    EphemeralConfig::default(),
);
let ephemeral_handler = Arc::new(ephemeral_handler);

// Receive inbound ephemeral events
while let Ok(event) = ephemeral_rx.recv().await {
    println!(
        "Received ephemeral from {} on {}: {:?}",
        event.sender, event.id, event.payload
    );
}
```

### Publishing

```rust
// Publish to all subscribers of a topic
ephemeral_handler.publish(sedimentree_id, payload.to_vec()).await;
```

### Composed Handler (Sync + Ephemeral)

```rust
use subduction_ephemeral::composed::ComposedHandler;

// Combine sync and ephemeral handlers
let handler = Arc::new(ComposedHandler::new(sync_handler, ephemeral_handler));

// Use with Subduction::new() for multiplexed connections
let (subduction, listener, manager) = Subduction::new(
    handler,
    // ... other args
);
```

## Policy

The `EphemeralPolicy` trait controls authorization separately from storage:

```rust
pub trait EphemeralPolicy<K: FutureForm + ?Sized> {
    type SubscribeDisallowed: Error;
    type PublishDisallowed: Error;

    fn authorize_subscribe(&self, peer: PeerId, id: SedimentreeId)
        -> K::Future<'_, Result<(), Self::SubscribeDisallowed>>;

    fn authorize_publish(&self, peer: PeerId, id: SedimentreeId)
        -> K::Future<'_, Result<(), Self::PublishDisallowed>>;

    fn filter_authorized_subscribers(&self, id: SedimentreeId, peers: Vec<PeerId>)
        -> K::Future<'_, Vec<PeerId>>;
}
```

### Policy Hierarchy

| Policy | Question |
|--------|----------|
| `ConnectionPolicy` | Can this peer connect? |
| `StoragePolicy` | Can this peer read/write persistent data? |
| `EphemeralPolicy` | Can this peer subscribe/publish ephemeral messages? |

Each is independent. A peer may have storage access without ephemeral access, or vice versa.

## Configuration

```rust
pub struct EphemeralConfig {
    /// Maximum payload size in bytes (default: 64 KB)
    pub max_payload_size: usize,
    
    /// Capacity of the inbound event channel (default: 1024)
    pub channel_capacity: usize,
}
```

Messages exceeding `max_payload_size` are silently dropped.

## Features

- **`std`** — Enable standard library support (enabled by default for most consumers)

## `no_std` Support

This crate is `#![no_std]` compatible. Enable the `std` feature for standard library support.

## License

See the workspace [`LICENSE`](../LICENSE) file.
