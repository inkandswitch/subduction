# Sans-IO Architecture

This document describes the sans-io rewrite of Subduction: why the core is
becoming a pure state machine, how the layers fit together, and the rules
each layer must follow. The previous implementation lives in `legacy/`
until the rewrite reaches parity.

## Why

The legacy core (`legacy/subduction_core`) is runtime-agnostic via
`future_form::FutureForm`, but it _drives_ IO itself: listener loops, a
connection-manager task, a request multiplexer, timeout tasks, and shared
state behind `async_lock` mutexes. Protocol decisions and async
orchestration are interleaved, which makes the protocol hard to test
exhaustively and hard to bind to platforms that want to own their event
loop (Swift concurrency, Kotlin coroutines, Python asyncio).

Sans-io inverts this: the core consumes _events_ and emits _effects_, and
never blocks, sleeps, spawns, locks, or tells the time. Drivers execute
effects and feed results back in.

## Layers

```
 L0  sedimentree_core + subduction_crypto     (pure; unchanged)
      │
 L1  subduction_protocol                      (sans-io, no_std + alloc)
      │   the state machine: every protocol decision, zero IO
      │
 L2  subduction_runtime                       (tagless-final driver)
      │   traits: Transport, Storage, Clock, Spawn, Rng, CryptoWorker
      │   one generic actor loop pumping the L1 machine
      │
 L3  platform crates
      · tokio: websocket, iroh, http_longpoll, redb, cli
      · wasm: browser/node bindings
      · later: uniffi (Swift/Kotlin), pyo3 (Python) binding L1 directly
```

## L1: The Machine

```rust
impl Machine {
    /// Feed one event; returns a structured outcome.
    fn handle(&mut self, now: Timestamp, event: Event) -> Outcome;

    /// Drain effects produced by handle() calls.
    fn poll_effect(&mut self) -> Option<Effect>;

    /// Next deadline the driver must wake us at.
    fn poll_timeout(&self) -> Option<Timestamp>;

    /// Snapshot of internal counters.
    fn stats(&self) -> Stats;
}
```

Inputs (`Event`): frames from the wire, connection up/down, timer expiry,
completions of driver-performed work (storage, crypto), and local commands
(add commit, sync with peer, subscribe, …).

Outputs (`Effect`): frames to send, disconnects, timers to set or cancel,
storage operations, crypto operations, and application events.

### Rules

1. _No IO, no clock, no locks, no allocation beyond protocol needs._
   `Timestamp` is an opaque monotonic value supplied by the driver with
   every `handle` call.
2. _FFI-stable boundary._ `Event`, `Effect`, and `Outcome` are plain
   enums/structs over bytes, ids, and tokens — no generics, no trait
   objects. The vocabulary freezes early and grows additively, because
   platform bindings (uniffi, pyo3) bind this surface directly.
3. _Expensive work is an effect._ Hashing, signing, verification, and
   large-payload codec runs are emitted as `Crypto` effects and executed
   by driver workers. The machine's turn stays cheap by construction.
4. _Wire compatibility is a hard constraint._ Message formats and the
   canonical codec (see `protocol.md`) are copied verbatim from legacy.
   Golden-bytes tests and a live legacy-interop test guard this.

### Interleaving safety without locks

Driver-performed work completes asynchronously, so completions can arrive
interleaved with anything. Instead of mutual exclusion:

- Completion tokens carry `(entity id, generation, sequence)`. Entities
  (connections, sync sessions) bump their generation on teardown; the
  machine drops completions with stale generations. A completion can never
  act on state it was not issued against.
- State that is mid-crypto or mid-storage is an explicit `Awaiting*`
  variant of the owning sub-machine. Events arriving in that state are
  queued or answered from the state — interleavings are handled by
  construction, not by exclusion.
- Tokens are phantom-typed where practical, so a completion for one
  connection cannot be applied to another at compile time.

Serial driver-side resources (e.g. a hardware-backed signer) are
serialized by a dedicated driver task, not by a shared lock.

## L2: The Driver

An actor funnel owns the machine — no locks in the driver either:

```text
 conn read tasks ─┐  events   ┌────────────────┐  effects
 timer wheel     ─┼─────────▶ │ driver task     │ ─────────▶ transports
 API handles     ─┘ (channel) │ (&mut Machine)  │            storage
                              └────────────────┘            crypto workers
                                      │ oneshot answers
                                      ▼
                                 API callers
```

Platform capabilities are traits over `FutureForm` (`Sendable` for tokio,
`Local` for single-threaded Wasm), so the loop is written once. The
bounded event channel is the single backpressure point, replacing the
legacy semaphores and spawn guards.

## Telemetry

Three tiers; the `metrics` facade never appears in L1, and legacy metric
names are preserved so existing dashboards keep working:

| Tier | What | Mechanism |
|------|------|-----------|
| 1 — Boundary | bytes/messages in/out, effect latencies, connection gauges | Derived in L2 by the driver executing effects |
| 2 — Internal counters | dedup hits, nonce evictions, sessions in flight | `Machine::stats()` pull snapshots (plain `u64`s) |
| 3 — Decision events | sync skipped, suppression fired, rejections | Structured `Outcome` return values, pattern-matched by the driver into `metrics`/`tracing` |

Tier-3 outcomes double as the primary test-assertion surface: property
tests drive the machine with event sequences and assert on outcomes.

## Testing Strategy

- _Pure protocol tests_ (L1): two machines wired back-to-back by shuttling
  `Effect::SendFrame` bytes — handshake completion, sync convergence from
  arbitrary divergent states, all without an async runtime.
- _Property/fuzz_ (bolero): arbitrary event sequences never panic; stale
  completions are always no-ops; handshakes survive adversarial
  interleavings.
- _Golden bytes_: new wire encodings byte-identical to legacy.
- _Interop_: a new node syncs with a `legacy/` node over an in-memory byte
  transport, in both directions.

## Known Risks

- _Machine serialization._ Legacy used sharded maps because parallel
  ingest was a measured bottleneck. The machine's hot path is decisions
  only — hashing, codec, and blob IO happen on driver workers — and the
  design keeps open the option of sharding by sedimentree if benchmarks
  demand it.
- _Pending-state complexity._ Storage and crypto as effects multiply
  `Awaiting*` states; contained by per-entity sub-machines with typestate
  and fuzzing over event orderings from day one.

## Status & Migration

Rewrite phases, the module-by-module migration map, and current progress
are tracked in the working plan (`.ignore/PLAN.md` in development clones).
Legacy crates remain building workspace members under `legacy/` until
Phase 4 reaches parity. No releases are cut from this tree during the
rewrite.
