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
   enums/structs over bytes, ids, and tickets — no generics, no trait
   objects. The vocabulary freezes early and grows additively, because
   platform bindings (uniffi, pyo3) bind this surface directly.
3. _Effects are for interactions with the world; pure computation runs
   inline._ Signing is the only crypto effect: signing keys deserve
   external custody (HSM, secure enclave, WebCrypto non-extractable
   keys, remote signers), which is genuinely asynchronous IO. Signature
   *verification* is pure computation — no key custody, no side effects
   — and runs inline in the machine (it only ever applies to handshake
   messages; ~100µs per connection setup). Hot-path bulk verification
   and blob-sized hashing are fused into storage/ingest effects: blob
   bytes never enter the machine, the driver verifies as part of
   persisting, and one request's items can fan across a worker pool.
4. _Wire compatibility is a hard constraint._ Message formats and the
   canonical codec (see `protocol.md`) are copied verbatim from legacy.
   Golden-bytes tests and a live legacy-interop test guard this.

### Interleaving safety without locks

Driver-performed work completes asynchronously, so completions can arrive
interleaved with anything. Instead of mutual exclusion:

- Completion tickets carry `(entity, generation, sequence)`. Entities
  (connections, sync sessions) bump their generation on teardown; the
  machine drops completions with stale generations. A completion can never
  act on state it was not issued against.
- State that is mid-crypto or mid-storage is an explicit `Awaiting*`
  variant of the owning sub-machine. Events arriving in that state are
  queued or answered from the state — interleavings are handled by
  construction, not by exclusion.
- Tickets are phantom-typed where practical, so a completion for one
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

### The frame table (blob data plane)

The driver retains each inbound wire frame in a buffer table keyed by
`FrameId` (monotonic, never reused — a stale ref reads as "gone", never
as wrong bytes). Machines reference blob regions inside retained frames
as `BlobRef { frame, offset, len }` and splice them into outbound
messages as scatter-gather `Part::Ref`s, so fan-out to N subscribers
costs N envelopes and zero blob copies.

Ownership rules (all machine-driven; the driver only obeys):

1. **Retain on delivery.** `MessageReceived { frame, bytes }` transfers
   the frame into the table.
2. **`ReleaseFrame(frame)`** — no refs escaped this frame; free it.
   Emitted by the connection machine when a message produced nothing
   that outlives the turn.
3. **`ReleaseBlob(ref)`** — one escaped ref is done (persisted or
   sent); decrement that frame's retention. Machines emit it strictly
   *after* the last effect that uses the ref, and drivers must execute
   effects in emission order — so a `Send` holding a ref always
   resolves before its release.
4. **Epoch bulk-free.** When a connection dies (`Disconnected`, or a
   machine-requested `Disconnect` completes), the driver frees *every*
   frame minted for that connection generation in one sweep — no
   per-frame releases arrive from a torn-down machine. Refs that
   escaped into still-pending storage ops resolve as "gone" and the op
   fails cleanly; monotonic ids make ABA impossible.

The testkit's frame table enforces all four rules as test invariants:
use-after-free, double-free, over-release, release-with-escaped-refs,
and leak-at-quiescence all fail tests loudly.

## Extension Protocols (Multiplexing)

Other protocols (ephemeral messages, keyhive, application-defined) share
authenticated connections with the sync protocol, distinguished by their
4-byte schema prefix — as in the legacy composed-envelope design, but as a
routing rule instead of a type parameter:

```text
MessageReceived { conn, bytes }
    ├─ pre-auth:  only SUH\0 (handshake) is legal; anything else faults
    └─ post-auth: match bytes[0..4]
         SUM\0     → sync sub-machine
         SUH\0     → fault (no re-handshake)
         otherwise → AppEvent::ExtensionMessage { conn, peer, bytes }
```

Unknown schema ≠ malformed: that is the extensibility guarantee. Extension
traffic is authentication-gated by construction because it routes through
the machine, and the byte-level surface means extension protocols can be
implemented natively on any platform — including as their own sans-io
machines fed by the same driver.

### Composition (the sub-protocol contract)

Extension protocols are themselves sans-io machines. The root machine does
not host them (that would reintroduce generics at the FFI boundary);
instead, L2 provides a combinator that wires them up once:

```text
          ┌─ Composite (L2) ─────────────────────────────────────┐
 events ─▶│ root Machine (handshake, sync, auth, routing)        │─▶ effects
          │   │ ExtensionMessage / PeerAuthenticated /           │
          │   │ ConnectionClosed            ▲ sends (auth-gated) │
          │   ▼                             │                    │
          │ extension machines, keyed by schema prefix           │
          └──────────────────────────────────────────────────────┘
```

An extension machine must accept this lifecycle vocabulary (whether via the
Rust `ProtocolMachine` trait in L2, or natively on platforms that bind L1
directly):

| Input                             | Meaning                                                         |
|-----------------------------------|-----------------------------------------------------------------|
| `PeerUp { conn, peer }`           | An authenticated connection is available                        |
| `PeerDown { conn, peer }`         | It is gone; drop all state for it                               |
| `MessageReceived { conn, bytes }` | A complete message bearing the extension's schema prefix        |
| `Wake`                            | Deadlines may be due (extension exposes its own `poll_timeout`) |

and may emit sends `(conn, bytes)` — delivered only on authenticated
connections — plus its own application events. Extensions never see
pre-handshake traffic and never manage connection lifecycle themselves.

## Telemetry

Three tiers; the `metrics` facade never appears in L1, and legacy metric
names are preserved so existing dashboards keep working:

| Tier                  | What                                                       | Mechanism                                                                                  |
|-----------------------|------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| 1 — Boundary          | bytes/messages in/out, effect latencies, connection gauges | Derived in L2 by the driver executing effects                                              |
| 2 — Internal counters | dedup hits, nonce evictions, sessions in flight            | `Machine::stats()` pull snapshots (plain `u64`s)                                           |
| 3 — Decision events   | sync skipped, suppression fired, rejections                | Structured `Outcome` return values, pattern-matched by the driver into `metrics`/`tracing` |

Tier-3 outcomes double as the primary test-assertion surface: property
tests drive the machine with event sequences and assert on outcomes.

### Legacy metric-name mapping

Every legacy `subduction_*` metric keeps its name at the driver; this
table records which tier now feeds it (audited against
`legacy/subduction_core/src/metrics/names.rs`):

| Legacy metric                                          | Tier | Fed by                                                                               |
|--------------------------------------------------------|------|--------------------------------------------------------------------------------------|
| `connections_active/total/closed`                      | 2    | `stats().connections_opened/closed` (+ driver gauge)                                 |
| `handshake_total{outcome}`                             | 2+3  | `stats().handshakes_completed/failed/timeouts`; outcome labels from `Fault` variants |
| `handshake_duration_seconds`                           | 1    | driver times `Connected` → `PeerAuthenticated`                                       |
| `network_frame_bytes`, `messages_total`                | 1    | driver counts `SendMessage`/`MessageReceived`                                        |
| `dispatch_*` (inflight, throttled, permit wait, dwell) | 1    | driver queue/executor properties — the machine has no queues                         |
| `batch_sync_requests/responses_total`                  | 2    | `stats().sync_requests_sent/received`, `sync_responses_received`                     |
| `sync_duration_seconds`                                | 1+3  | driver times `SyncTree` → `SyncFinished{status}`                                     |
| `sync_commits/fragments_received/sent_total`           | 1    | driver inspects `Ingest`/`SendMessage` effects                                       |
| `sync_call_failures_total`                             | 3    | `SyncFinished{status != Completed}`                                                  |
| `sync_verify_failures_total`                           | 1    | driver counts `Ingested.rejected` items it produced                                  |
| `top_requestor_*`, `requestor_window_*`                | 1    | driver-side tally (legacy `requestor_tally` moves to the driver)                     |
| `late_responses_total`                                 | 2    | `stats().stale_completions` + `Ignored(UnknownRequest)` outcomes                     |
| `keepalive_*`                                          | 1    | transport-level; never entered the machine                                           |
| `mux_pending/requests/cancelled`                       | 2    | `pending_sync_requests()`, `stats().sync_requests_sent`, timeout `SyncFinished`s     |
| `outbound_queue_*`                                     | 1    | driver send-queue properties                                                         |
| `subscribed_sedimentrees`                              | 2    | resident subscription map size (expose in `stats()` when needed)                     |
| `subscription_pushes_total`                            | 2    | `stats().subscription_pushes`                                                        |
| `storage_*` (latency, sizes)                           | 1    | driver times `Storage` effects                                                       |

## Testing Strategy

- _Pure protocol tests_ (L1): two machines wired back-to-back by shuttling
  `Effect::SendMessage` bytes — handshake completion, sync convergence from
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
