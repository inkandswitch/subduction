# subduction_tokio

Owned Tokio clients with **erased transports**, public connectors, cancellation-safe attempt ownership, and opt-in reconnection. This is a new implementation of [TOKIO_UTILS.md](../TOKIO_UTILS.md), independent of PR #294.

The core remains runtime-independent. A single client can use different connector/transport implementations simultaneously. No transport enum or client transport type parameter is required.

## Quick start

Enable `websocket` for the bundled outbound connector (Rustls). The default feature set does not depend on WebSockets.

```rust,ignore
use subduction_tokio::{ExpectedPeer, ReconnectPolicy, TokioClient};
use subduction_tokio::websocket::WebSocketConnector;

let client = TokioClient::builder()
    .storage(storage)
    .policy(policy)
    .signer(signer)
    .start()?;
let node = client.handle();

let connection = node
    .connect(WebSocketConnector::new(url), ExpectedPeer::Known(server_id))
    .reconnect(ReconnectPolicy::exponential_backoff())
    .await?;

node.store_built_batch(tree_id, commits, fragments).await?;
let result = node
    .sync_with_peer(server_id, tree_id, true, subduction_tokio::CallTimeout::Default)
    .await?;

connection.close().await?;
let report = client.close().await;
// Inspect forced, unfinished_tasks, and failure; shutdown isn't always graceful.
```

See `examples/websocket.rs` for a complete runnable, ephemeral client. Identity and authorization are always explicit. The example's memory signer and open policy are test choices, not library defaults.

## Ownership

- `TokioClient` is the non-cloneable owner. Dropping it requests shutdown.
- `ClientHandle` is cloneable, admits tracked operations, and never exposes the raw core.
- `ConnectionHandle` observes/controls a stable logical connection across physical attempts. Dropping it does not stop the connection; `close()` does.
- Initial connection failure is returned without retry. Once initially connected, an enabled reconnect policy governs replacement attempts.
- Cancelling the initial connect future cancels that logical connection, including cleanup before authentication or registration finishes.
- Closing a logical connection, disconnecting its peer, or shutting down the client disables its retries before cancellation.
- Discovery pins the first authenticated identity on replacement. `.rediscover_on_reconnect()` explicitly permits a different discovered identity.

The public client/handle types are non-generic: storage and policy are checked by the typed builder, then kept behind a private core adapter. Backend errors retain their original `Error::source()` chains.

## Implementing a connector

Implement the object-safe `Connector` trait:

1. Use the supplied `AuthenticationContext` signer and audience with the shared Subduction handshake.
2. Guard resources as soon as they are acquired. Register async cleanup with `AttemptScope::on_cleanup` **before** an await can strand them.
3. Register driving tasks with `AttemptScope::spawn`; do not call detached `tokio::spawn`.
4. Return `attempt.established(authenticated_transport)`. This preserves the authentication witness, erases the transport, and registers transport disconnection.
5. Classify retryable failures through `classify_failure`. The runtime owns delays, limits, and replacement attempts; authentication/policy failures override backend retry recommendations.

Tasks submitted to `spawn` are **connection-driving tasks**: any completion ends the attempt. They are cancelled during cleanup. Use `on_cleanup` for async release work rather than relying on code after a task's final await. Tasks must not await their own attempt cleanup.

A scope is closed to further resource/task registration once cleanup begins. A rejected registration drops the supplied value/future without polling it, so retain a synchronous acquisition guard until registration succeeds. Externally owned endpoints or HTTP clients must not be shut down by a single connection's cleanup.

Each attempt gets independent physical identity, even if a backend considers two transport objects equal. Late old-reader notifications cannot match a replacement transport. The core's `on_unregistered` notification also informs the owner about decode/handler failures that do not originate as byte-I/O errors.

The first implementation exposes local mixed-batch storage, blob snapshots, tree enumeration, and per-peer sync (including its subscription flag). More convenience operations can be added without exposing unrestricted core access.

## Shutdown and failures

`shutdown()` requests teardown; `closed().await` observes it; `close().await` does both. The shutdown procedure continues if a waiter is cancelled. Repeated waits observe a cached `CloseReport`.

Default budgets:

| Operation | Budget |
| --- | --- |
| Dial + authenticate + register, per attempt | 10 seconds |
| Core request send + response wait | 10 seconds |
| Attempt cleanup | 2 seconds |
| Client graceful shutdown, shared across tasks | 5 seconds |
| Final join after forced task abort | 1 second |

Attempt cleanup failure stops retries and fails the runtime rather than silently accumulating unreleased resources. Shutdown reports forced cancellation and any unfinished tasks. Retained client handles do not keep the core/storage alive after successful teardown; unfinished non-cooperative tasks can still retain resources and are reported accordingly.

Connection status watches retain the latest state and terminal cause. The optional client event stream is bounded; consumers must handle lag. Event lag does not erase connection or client terminal status.

Deadlines require an executor that continues polling. Blocking code or a future that never yields cannot be forcibly terminated. Panic reporting applies when Rust unwinds; a build using `panic = "abort"` terminates the process instead (including this workspace's default release profile).

## Deliberately unchanged semantics

- Cancellation and timeout are **not rollback**. Storage may contain partial progress.
- A connected/reconnected transport is not necessarily caught up.
- This version **does not restore subscriptions or automatically initiate catch-up after reconnect**. Observe `Connected` and explicitly reissue sync/subscription operations as appropriate. It does not replay in-flight application writes.
- Sync reports preserve existing core semantics: local accepted-response processing, not remote acknowledgement. Received statistics count offered items, and the core may skip rejected items or log outbound failures separately.
- This crate does not provide inbound accept-loop management, a JavaScript runtime wrapper, or a new stored-item notification API.

## Validation

```sh
cargo test -p subduction_tokio
cargo test -p subduction_tokio --all-features
cargo clippy -p subduction_tokio --all-features --all-targets
```
