# subduction_tokio

A wrapper around `subduction_core` which uses `tokio` as a runtime and provides
convenient APIs for handling reconnection.

## Quick start

Enable `websocket` for the bundled outbound connector (Rustls). The default
feature set does not depend on WebSockets.

```rust,ignore
use subduction_tokio::{ExpectedPeer, ReconnectPolicy, Node};
use subduction_tokio::websocket::WebSocketConnector;

let node = Node::builder()
    .storage(storage)
    .policy(policy)
    .signer(signer)
    .start()?;
let handle = node.handle();

let connection = handle
    .connect(WebSocketConnector::new(url), ExpectedPeer::Known(server_id))
    .reconnect(ReconnectPolicy::exponential_backoff())
    .await?;

handle.store_built_batch(tree_id, commits, fragments).await?;
let result = handle
    .sync_with_peer(server_id, tree_id, true, subduction_tokio::CallTimeout::Default)
    .await?;

connection.close().await?;
let report = node.close().await;
// Inspect forced, unfinished_tasks, and failure; shutdown isn't always graceful.
```
