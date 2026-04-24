# subduction_keyhive_orchestrator

Orchestration layer (syncpoints, cache, rate limiting, storage policy) above the keyhive sync protocol.

## Overview

This crate wraps [`subduction_keyhive::KeyhiveProtocol`] with a passive
orchestrator that owns syncpoint tracking, a periodic event cache, and
storage-policy configuration. Drivers are responsible for all concurrency primitives.

Both `Local` (WASM/single-threaded) and `Sendable` (native server)
form variants are provided via `future_form::FutureForm`.

## License

See the workspace [`LICENSE`](../LICENSE) file.
