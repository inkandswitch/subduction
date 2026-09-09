# `subduction_tokio`

Tokio runtime integration for [Subduction](../README.md).

`subduction_core` has no runtime: it hands back futures and expects the caller
to drive them, and it is generic over a `Spawn` implementation. This crate
supplies the Tokio side of that contract:

| Item | Purpose |
|------|---------|
| `TokioSpawn` | Detached `tokio::spawn` spawner |
| `TrackedTokioSpawn` | Spawner that registers every task with a `TaskTracker` |
| `TimeoutTokio` | `Timeout` implementation over `tokio::time::timeout` |
| `TokioSubduction` | Owns a node: spawns and supervises its loops, scopes your background tasks to its lifetime, and tears everything down in the right order |

## Lifecycle

```text
TokioSubduction::start(|spawner, cancel| build(spawner, cancel))
    │
    ├─ spawner                    tasks the node waits for on stop (tracked)
    ├─ cancel                     child token: run handler loops under it so
    │                             stop can end them
    ├─ node.spawn(task)           tasks scoped to the node; tracked + cancelled
    │
    └─ node.stop().await          cancel tasks → Subduction::stop → tracker.wait
       drop(node)                 last Arc<Subduction> gone → storage released
```

`TokioSubduction::start_with(tracker, token, build)` folds the node into an
existing lifecycle and returns `(node, extra)`, where `extra` is whatever the
build closure hands back alongside the node (an ephemeral handler, a metrics
tally) — no need to smuggle values out through captured `Option`s.

Dropping a `TokioSubduction` without calling `stop` requests a stop and
cancels its tasks as a backstop, but only `stop().await` guarantees the
loops have exited before you continue — which matters if you intend to reopen
a file-locked storage backend at the same path.
